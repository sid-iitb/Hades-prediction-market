"""
V2 port of last_90s_limit_99: limit order in final window_seconds, time-weighted bid floor, stop-loss exit.
Stateless: evaluates entry/exit from WindowContext and my_orders only.
Records telemetry (skips and intent_fired) to v2_telemetry_last_90s in v2_state.db.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from bot.pipeline.context import WindowContext
from bot.pipeline.intents import ExitAction, OrderIntent, OrderRecord
from bot.pipeline.strategies.base import BaseV2Strategy

logger = logging.getLogger(__name__)

TELEMETRY_TABLE = "v2_telemetry_last_90s"


def _v2_db_path() -> Path:
    return Path(__file__).resolve().parents[2].parent / "data" / "v2_state.db"


def _ensure_telemetry_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TELEMETRY_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_id TEXT,
            asset TEXT,
            placed INTEGER,
            seconds_to_close REAL,
            bid INTEGER,
            distance REAL,
            reason TEXT,
            pre_data TEXT,
            timestamp REAL
        )
        """
    )
    conn.commit()


def _get_asset_config(value: Any, asset: str, default: Any) -> Any:
    """Resolve scalar or per-asset dict config."""
    if value is None:
        return default
    if isinstance(value, dict):
        a = (asset or "").strip().lower()
        return value.get(a, value.get(a.upper(), default))
    return value


def _min_spot_distance(ctx: WindowContext) -> Optional[float]:
    """
    Compute the minimum spot-to-strike distance using both oracles when available.
    Falls back to ctx.distance when only one source exists.
    """
    candidates = [d for d in (ctx.distance_kraken, ctx.distance_coinbase) if d is not None]
    if candidates:
        return min(candidates)
    return ctx.distance


class Last90sStrategy(BaseV2Strategy):
    """
    Last-90s limit-99 strategy: places a limit order in the final window_seconds when
    distance and bid floor pass; exits with stop_loss when loss exceeds threshold in danger zone.
    """

    def __init__(self, config: dict) -> None:
        super().__init__("last_90s_limit_99", config)
        path = _v2_db_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), check_same_thread=False)
        try:
            _ensure_telemetry_table(conn)
        finally:
            conn.close()

    def _record_telemetry(self, ctx: WindowContext, placed: int, reason: str) -> None:
        """
        Write one row to v2_telemetry_last_90s. All values from ctx only; no hardcoded 50.0, 45.0, or 97.
        pre_data = JSON of ctx.quote and ctx.spot_kraken.
        """
        pre_data = json.dumps({
            "quote": dict(ctx.quote),
            "spot_kraken": ctx.spot_kraken,
        })
        window_id = f"{ctx.interval}_{ctx.market_id}"
        asset_str = (ctx.asset or "").strip().lower()
        # Dynamic from ctx only; use -1.0 sentinel when missing (never 50.0, 45.0, or 97)
        seconds_to_close_raw = ctx.seconds_to_close
        seconds_to_close = float(seconds_to_close_raw) if seconds_to_close_raw is not None else -1.0
        # Bid from ctx.quote by configured side (yes|no|auto) for telemetry.
        interval_cfg = (ctx.config or {}).get(ctx.interval or "") or {}
        strat_cfg = (interval_cfg.get("strategies") or {}).get("last_90s_limit_99") or {}
        side_cfg = (str(strat_cfg.get("side", "no")).strip().lower() or "no")
        yes_bid_q = int(ctx.quote.get("yes_bid", 0) or 0)
        no_bid_q = int(ctx.quote.get("no_bid", 0) or 0)
        if side_cfg == "yes":
            side = "yes"
            bid = yes_bid_q
        elif side_cfg == "no":
            side = "no"
            bid = no_bid_q
        else:
            # side=auto: log whichever side currently has the higher bid.
            side = "yes" if yes_bid_q >= no_bid_q else "no"
            bid = yes_bid_q if side == "yes" else no_bid_q
        distance_raw = ctx.distance
        distance = float(distance_raw) if distance_raw is not None else -1.0
        path = _v2_db_path()
        try:
            conn = sqlite3.connect(str(path), check_same_thread=False)
            try:
                conn.execute(
                    f"""
                    INSERT INTO {TELEMETRY_TABLE}
                    (window_id, asset, placed, seconds_to_close, bid, distance, reason, pre_data, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        window_id,
                        asset_str,
                        placed,
                        seconds_to_close,
                        bid,
                        distance,
                        reason,
                        pre_data,
                        time.time(),
                    ),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as e:
            logger.debug("[last_90s] Telemetry write failed: %s", e)

    def _resolve_entry_side(self, cfg: Dict[str, Any], ctx: WindowContext) -> tuple[Optional[str], int]:
        """
        Resolve entry side and bid for this tick.
        - side=yes|no: fixed side; require that side's bid >= min_bid_cents.
        - side=auto: pick the side with the higher bid (yes if yes_bid >= no_bid, else no),
          then require that bid >= min_bid_cents.
        Returns (side or None, bid_cents_for_that_side).
        min_bid_cents: scalar or per-asset dict (e.g. { btc: 85, eth: 80, ... }).
        """
        side_cfg = (str(cfg.get("side", "no")).strip().lower() or "no")
        asset = (ctx.asset or "").strip().lower()
        min_bid_raw = _get_asset_config(cfg.get("min_bid_cents"), asset, 90)
        min_bid_cents = int(min_bid_raw) if min_bid_raw is not None else 90
        yes_bid = int(ctx.quote.get("yes_bid", 0) or 0)
        no_bid = int(ctx.quote.get("no_bid", 0) or 0)

        if side_cfg == "yes":
            side = "yes"
            bid = yes_bid
        elif side_cfg == "no":
            side = "no"
            bid = no_bid
        else:
            # side=auto: choose side with higher bid.
            side = "yes" if yes_bid >= no_bid else "no"
            bid = yes_bid if side == "yes" else no_bid

        # Log the choice so we can debug behavior end-to-end.
        logger.info(
            "[last_90s_v2_side] [%s] sec_to_close=%.0f side_cfg=%s yes_bid=%s no_bid=%s chosen=%s bid=%s min_bid_cents=%s",
            (ctx.asset or "").upper(),
            ctx.seconds_to_close or -1,
            side_cfg,
            yes_bid,
            no_bid,
            side,
            bid,
            min_bid_cents,
        )

        if bid < min_bid_cents:
            return (None, bid)
        return (side, bid)

    def evaluate_entry(
        self, ctx: WindowContext, my_orders: Optional[List[OrderRecord]] = None
    ) -> Optional[OrderIntent]:
        cfg = self._get_strategy_config(ctx)
        if not cfg or not cfg.get("enabled", False):
            return None

        asset = (ctx.asset or "").strip().lower()
        window_seconds = float(_get_asset_config(cfg.get("window_seconds"), asset, 90))
        if ctx.seconds_to_close is None or ctx.seconds_to_close > window_seconds:
            return None

        # From here on we are in the active hunting phase; record telemetry on any skip or intent.
        min_dist = _get_asset_config(cfg.get("min_distance_at_placement"), asset, 0.0)
        if min_dist is not None and ctx.distance is not None:
            try:
                min_d = float(min_dist)
                if min_d > 0 and ctx.distance < min_d:
                    self._record_telemetry(ctx, 0, "low_distance")
                    return None
            except (TypeError, ValueError):
                pass

        # Side (yes/no/auto) and min_bid gate: place only if chosen side's bid >= min_bid_cents.
        side, bid_cents = self._resolve_entry_side(cfg, ctx)
        if side is None:
            self._record_telemetry(ctx, 0, "low_bid")
            return None

        limit_price_cents = int(cfg.get("limit_price_cents", 99))
        max_cost_cents = int(cfg.get("max_cost_cents", 10000))
        # Per-asset order_count, capped only by what max_cost_cents allows (min 1).
        raw_order_count = _get_asset_config(cfg.get("order_count"), asset, None)
        try:
            desired_count = int(raw_order_count) if raw_order_count is not None else None
        except (TypeError, ValueError):
            desired_count = None
        # Max contracts allowed by per-window/asset cost cap.
        max_by_cost = max_cost_cents // limit_price_cents if limit_price_cents else 0
        if max_by_cost <= 0:
            # Safety: if config is nonsensical, do not place (would breach cost or 0-size).
            self._record_telemetry(ctx, 0, "max_cost_too_low")
            return None
        if desired_count is None:
            # No explicit order_count: use max allowed by cost (but at least 1).
            count = max(1, max_by_cost)
        else:
            # Use the configured order_count but respect the per-window/asset cost cap.
            count = max(1, min(desired_count, max_by_cost))

        self._record_telemetry(ctx, 1, "intent_fired")
        client_order_id = f"last90s:{uuid.uuid4().hex[:12]}"
        return OrderIntent(
            side=side,
            price_cents=limit_price_cents,
            count=count,
            order_type="limit",
            client_order_id=client_order_id,
        )

    def evaluate_exit(self, ctx: WindowContext, my_orders: List[OrderRecord]) -> List[ExitAction]:
        cfg = self._get_strategy_config(ctx)
        if not cfg:
            return []

        asset = (ctx.asset or "").strip().lower()
        stop_loss_pct = _get_asset_config(cfg.get("stop_loss_pct"), asset, 30)
        try:
            sl_pct = float(stop_loss_pct) / 100.0
        except (TypeError, ValueError):
            sl_pct = 0.30
        min_dist_placement = _get_asset_config(cfg.get("min_distance_at_placement"), asset, 0.0)
        try:
            min_d = float(min_dist_placement) if min_dist_placement is not None else 0.0
        except (TypeError, ValueError):
            min_d = 0.0
        factor = _get_asset_config(cfg.get("stop_loss_distance_factor"), asset, 0.8)
        try:
            factor_f = float(factor)
        except (TypeError, ValueError):
            factor_f = 0.8
        # Use MIN distance from both oracles for exit logic.
        distance_min = _min_spot_distance(ctx)
        danger_threshold = min_d * factor_f if min_d else None
        yes_bid = int(ctx.quote.get("yes_bid", 0) or 0)
        no_bid = int(ctx.quote.get("no_bid", 0) or 0)

        actions: List[ExitAction] = []
        for order in my_orders:
            side = (order.side or "no").strip().lower()
            if side not in ("yes", "no"):
                continue
            filled = order.status == "filled" or (order.filled_count or 0) > 0
            if not filled:
                continue
            entry_cents = order.limit_price_cents or 99
            if entry_cents <= 0:
                continue
            # Use bid for the side we hold: YES position -> yes_bid, NO position -> no_bid.
            current_bid = yes_bid if side == "yes" else no_bid
            loss_pct = (entry_cents - current_bid) / float(entry_cents) if current_bid is not None else 0.0

            # Panic stop-loss: if either spot feed has effectively reached the strike (distance very small),
            # close immediately regardless of the normal distance threshold.
            panic_eps_map = {"btc": 1.0, "eth": 0.1, "sol": 0.01, "xrp": 0.0005}
            panic_eps = panic_eps_map.get(asset, 0.0)
            if distance_min is not None and panic_eps > 0 and distance_min <= panic_eps and loss_pct > 0:
                actions.append(ExitAction(order_id=order.order_id, action="stop_loss"))
                logger.info(
                    "[last_90s] PANIC stop-loss: order_id=%s side=%s entry=%sc bid=%s loss_pct=%.2f "
                    "distance_min=%.4f panic_eps=%.4f",
                    order.order_id, side, entry_cents, current_bid, loss_pct, distance_min, panic_eps,
                )
                continue

            if loss_pct < sl_pct:
                continue
            if danger_threshold is not None and distance_min is not None:
                if distance_min > danger_threshold:
                    continue
            actions.append(ExitAction(order_id=order.order_id, action="stop_loss"))
            logger.info(
                "[last_90s] Stop-loss: order_id=%s side=%s entry=%sc bid=%s loss_pct=%.2f distance=%s threshold=%s",
                order.order_id, side, entry_cents, current_bid, loss_pct, distance_min, danger_threshold,
            )
        return actions
