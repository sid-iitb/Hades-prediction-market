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
from bot.pipeline.window_utils import logical_window_slot
from bot.pipeline.strategies.base import BaseV2Strategy

logger = logging.getLogger(__name__)

TELEMETRY_TABLE = "v2_telemetry_last_90s"


def _v2_db_path() -> Path:
    return Path(__file__).resolve().parents[2].parent / "data" / "v2_state.db"


# SQLite timeout (seconds) for cooldown read; avoid blocking when DB is locked during volatile events.
_SQLITE_COOLDOWN_READ_TIMEOUT = 5.0


def _get_last_stop_loss_timestamp(strategy_id: str) -> Optional[float]:
    """
    Read last_stop_loss_timestamp for strategy from v2_global_cooldown.
    Returns None if table/row missing or on error. Uses timeout and handles database locked.
    """
    if not (strategy_id or "").strip():
        return None
    path = _v2_db_path()
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(str(path), check_same_thread=False, timeout=_SQLITE_COOLDOWN_READ_TIMEOUT)
        try:
            row = conn.execute(
                "SELECT last_stop_loss_timestamp FROM v2_global_cooldown WHERE strategy_id = ?",
                (str(strategy_id).strip(),),
            ).fetchone()
            if row is not None:
                return float(row[0])
        finally:
            conn.close()
    except sqlite3.OperationalError as e:
        if "locked" in str(e).lower() or "busy" in str(e).lower():
            logger.debug("[last_90s] Cooldown read skipped (DB locked); treating as no cooldown. %s", e)
        else:
            logger.debug("[last_90s] global cooldown read failed strategy_id=%s: %s", strategy_id, e)
    except Exception as e:
        logger.debug("[last_90s] global cooldown read failed strategy_id=%s: %s", strategy_id, e)
    return None


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
    """Spot-to-strike distance from the single oracle (currently Coinbase)."""
    return ctx.distance


def _resolve_strategy_id(config: dict) -> str:
    """Use continuous_alpha_limit_99 if that block exists in any interval, else last_90s_limit_99."""
    for interval in ("fifteen_min", "hourly"):
        strategies = (config.get(interval) or {}).get("strategies") or {}
        if strategies.get("continuous_alpha_limit_99") is not None:
            return "continuous_alpha_limit_99"
    return "last_90s_limit_99"


class Last90sStrategy(BaseV2Strategy):
    """
    Limit-at-99 strategy: places a limit order when seconds_to_close <= window_seconds and
    distance/bid pass; exits with stop_loss using absolute distance buffer (entry_distance - buffer).
    Supports both config keys: continuous_alpha_limit_99 (300s horizon) and last_90s_limit_99.
    """

    def __init__(self, config: dict) -> None:
        strategy_id = _resolve_strategy_id(config)
        super().__init__(strategy_id, config)
        # Stateful persistence filter: asset -> first time stop-loss condition was detected (for wick/flash-crash protection).
        self.danger_timestamps: Dict[str, float] = {}
        path = _v2_db_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), check_same_thread=False)
        try:
            _ensure_telemetry_table(conn)
        finally:
            conn.close()

    def _get_strategy_config(self, ctx: WindowContext) -> dict:
        """Try continuous_alpha_limit_99 then last_90s_limit_99 so either YAML key works."""
        if not ctx.config or not ctx.interval:
            return {}
        interval_block = ctx.config.get(ctx.interval)
        if not isinstance(interval_block, dict):
            return {}
        strategies = interval_block.get("strategies")
        if not isinstance(strategies, dict):
            return {}
        out = strategies.get("continuous_alpha_limit_99") or strategies.get("last_90s_limit_99")
        return out if isinstance(out, dict) else {}

    def _record_telemetry(self, ctx: WindowContext, placed: int, reason: str) -> None:
        """
        Write one row to v2_telemetry_last_90s. All values from ctx only; no hardcoded 50.0, 45.0, or 97.
        pre_data = JSON of ctx.quote and ctx.spot (single oracle).
        """
        pre_data = json.dumps({
            "quote": dict(ctx.quote),
            "spot": ctx.spot,
        })
        # Use logical slot so window_id matches tick_log and is queryable by slot (e.g. 26MAR180100).
        slot = logical_window_slot(ctx.market_id or "")
        window_id = f"{ctx.interval}_{slot}"
        asset_str = (ctx.asset or "").strip().lower()
        # Dynamic from ctx only; use -1.0 sentinel when missing (never 50.0, 45.0, or 97)
        seconds_to_close_raw = ctx.seconds_to_close
        seconds_to_close = float(seconds_to_close_raw) if seconds_to_close_raw is not None else -1.0
        # Bid from ctx.quote by configured side (yes|no|auto) for telemetry.
        strat_cfg = self._get_strategy_config(ctx) or {}
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

        # Global macro cool-down: after any asset stop-loss/catastrophic override, refuse new entries for cooldown_seconds.
        cooldown_sec = int(cfg.get("global_cooldown_seconds", 0) or 0)
        if cooldown_sec > 0:
            last_ts = _get_last_stop_loss_timestamp(self.strategy_id)
            if last_ts is not None:
                elapsed = time.time() - last_ts
                if elapsed < cooldown_sec:
                    remaining = cooldown_sec - elapsed
                    logger.info(
                        "[last_90s] Entry skipped: %s is in global cooldown (%.0fs remaining); no new trades until cooldown expires.",
                        self.strategy_id, remaining,
                    )
                    return None

        asset = (ctx.asset or "").strip().lower()
        # Single bullet: at most one order per (window, asset). Do not average up.
        if my_orders and len(my_orders) > 0:
            return None

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
        # placement_bid_cents = bid at placement; used as entry cost for stop-loss (avoids phantom loss vs limit 99¢).
        # entry_distance = actual distance at placement; used for trailing stop (distance_decay) so decay is vs real entry, not config minimum.
        entry_dist = float(ctx.distance) if ctx.distance is not None else None
        return OrderIntent(
            side=side,
            price_cents=limit_price_cents,
            count=count,
            order_type="limit",
            client_order_id=client_order_id,
            placement_bid_cents=bid_cents,
            entry_distance=entry_dist,
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
        catastrophic_loss_pct = _get_asset_config(cfg.get("catastrophic_loss_pct"), asset, 25)
        try:
            catastrophic_pct = float(catastrophic_loss_pct) / 100.0
        except (TypeError, ValueError):
            catastrophic_pct = 0.25
        # Fallback reference distance from config (for orders placed before entry_distance was stored).
        min_dist_placement = _get_asset_config(cfg.get("min_distance_at_placement"), asset, 0.0)
        try:
            config_reference_distance = float(min_dist_placement) if min_dist_placement is not None else 0.0
        except (TypeError, ValueError):
            config_reference_distance = 0.0
        buffer_raw = _get_asset_config(cfg.get("stop_loss_absolute_buffer"), asset, None)
        try:
            buffer_f = float(buffer_raw) if buffer_raw is not None else None
        except (TypeError, ValueError):
            buffer_f = None
        distance_min = _min_spot_distance(ctx)
        yes_bid = int(ctx.quote.get("yes_bid", 0) or 0)
        no_bid = int(ctx.quote.get("no_bid", 0) or 0)
        persistence_sec = float(cfg.get("danger_persistence_seconds", 5.0) or 5.0)
        distance_decay_pct = float(cfg.get("distance_decay_pct", 0.75) or 0.75)
        sec_to_close = float(ctx.seconds_to_close) if ctx.seconds_to_close is not None else None
        current_time = time.time()

        actions: List[ExitAction] = []
        for order in my_orders:
            side = (order.side or "no").strip().lower()
            if side not in ("yes", "no"):
                continue
            filled = order.status == "filled" or (order.filled_count or 0) > 0
            if not filled:
                continue
            # Use placement bid as entry cost so loss_pct reflects true drawdown (fix phantom loss from using limit 99¢).
            entry_cents = (
                order.placement_bid_cents
                if getattr(order, "placement_bid_cents", None) is not None
                else (order.limit_price_cents or 99)
            )
            if entry_cents <= 0:
                continue
            current_bid = yes_bid if side == "yes" else no_bid
            loss_pct = (entry_cents - current_bid) / float(entry_cents) if current_bid is not None else 0.0

            # --- Per-order reference distance: actual at fill (trailing stop base) ---
            entry_dist = getattr(order, "entry_distance_at_fill", None)
            if entry_dist is None:
                # Fallback for legacy/older orders where entry_distance_at_fill was never stored.
                entry_dist = getattr(order, "entry_distance", None)
            try:
                reference_distance = float(entry_dist) if entry_dist is not None else config_reference_distance
            except (TypeError, ValueError):
                reference_distance = config_reference_distance
            if reference_distance is not None and reference_distance <= 0.0:
                reference_distance = config_reference_distance
            danger_threshold = None
            if buffer_f is not None and reference_distance is not None and reference_distance > 0.0:
                danger_threshold = reference_distance - buffer_f

            # --- 1. Hard circuit breaker (catastrophic): bypass all timers and window checks ---
            if loss_pct >= catastrophic_pct:
                self.danger_timestamps.pop(asset, None)
                actions.append(ExitAction(order_id=order.order_id, action="stop_loss", reason="sl_catastrophic"))
                logger.info(
                    "[last_90s] Stop-loss (CATASTROPHIC override): asset=%s order_id=%s loss_pct=%.2f",
                    asset, order.order_id, loss_pct,
                )
                continue

            # --- 2. Wick filter (normal SL + distance) + distance-decay trailing stop ---
            # Normal SL: loss_pct >= stop_loss_pct AND distance < danger_threshold (reference_distance - buffer).
            normal_sl_met = (
                loss_pct >= sl_pct
                and (danger_threshold is None or (distance_min is not None and distance_min < danger_threshold))
            )
            # Distance decay: current distance <= reference_distance * distance_decay_pct (dynamic trailing stop from actual entry).
            # IMPORTANT: require loss_pct >= stop_loss_pct so we don't stop out on tiny mark dips.
            is_distance_decayed = False
            if distance_min is not None and reference_distance is not None and reference_distance > 0.0:
                try:
                    dynamic_decay_threshold = abs(reference_distance) * distance_decay_pct
                    is_distance_decayed = distance_min <= dynamic_decay_threshold and loss_pct >= sl_pct
                except (TypeError, ValueError):
                    is_distance_decayed = False

            is_in_danger = normal_sl_met or is_distance_decayed

            # Rule A (Late-Window Panic): last 60 seconds should only consider stop_loss_pct
            # (ignore distance gate + distance-decay trailing-stop logic).
            if sec_to_close is not None and sec_to_close <= 60:
                if loss_pct >= sl_pct:
                    self.danger_timestamps.pop(asset, None)
                    actions.append(
                        ExitAction(order_id=order.order_id, action="stop_loss", reason="sl_pct_late_window")
                    )
                    logger.info(
                        "[last_90s] Stop-loss (late-window, loss_pct-only): order_id=%s asset=%s side=%s entry=%sc bid=%s loss_pct=%.2f sl_pct=%.2f",
                        order.order_id,
                        asset,
                        side,
                        entry_cents,
                        current_bid,
                        loss_pct,
                        sl_pct,
                    )
                # Regardless of whether SL fired, do not apply persistence logic in late-window mode.
                continue

            if is_in_danger:
                # Rule B (Persistence check): seconds_to_close > 60.
                if asset not in self.danger_timestamps:
                    self.danger_timestamps[asset] = current_time
                    if is_distance_decayed and not normal_sl_met:
                        reason = f'Distance Decay (curr_dist <= {distance_decay_pct:.2f} * init_dist)'
                    elif is_distance_decayed and normal_sl_met:
                        reason = f'Normal SL + Distance Decay (<= {distance_decay_pct:.2f} * init_dist)'
                    else:
                        reason = "Normal SL"
                    logger.debug(
                        "[last_90s] Danger zone entered: asset=%s reason=\"%s\" waiting %.1fs",
                        asset, reason, persistence_sec,
                    )
                    continue
                if (current_time - self.danger_timestamps[asset]) >= persistence_sec:
                    self.danger_timestamps.pop(asset, None)
                    reason_tag = "sl_decay_persistence" if is_distance_decayed else "sl_normal_persistence"
                    actions.append(ExitAction(order_id=order.order_id, action="stop_loss", reason=reason_tag))
                    logger.info(
                        "[last_90s] Stop-loss (persistence confirmed %.1fs): order_id=%s asset=%s side=%s entry=%sc bid=%s loss_pct=%.2f distance_min=%s danger_threshold=%s",
                        persistence_sec, order.order_id, asset, side, entry_cents, current_bid, loss_pct, distance_min, danger_threshold,
                    )
                    continue
                # Still in danger but < persistence_sec — wait longer (do not exit).
                continue

            # --- 3. Recovery: neither catastrophic nor normal SL met — clear danger state if present ---
            if asset in self.danger_timestamps:
                del self.danger_timestamps[asset]
                logger.debug("[last_90s] Danger cleared (recovery): asset=%s", asset)
            # No stop-loss for this order; continue with normal evaluation (e.g. take-profit handled elsewhere if added).

        # Cleanup: no filled orders for this asset (window closed or position finalized) — avoid leaking stale danger state.
        has_filled = any(
            (o.status == "filled" or (o.filled_count or 0) > 0)
            for o in my_orders
            if (o.side or "").strip().lower() in ("yes", "no")
        )
        if not has_filled:
            self.danger_timestamps.pop(asset, None)

        return actions
