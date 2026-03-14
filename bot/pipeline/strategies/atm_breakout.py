"""
V2 port of atm_breakout_strategy: directional breakout when market is coiled (parity) and momentum triggers.
Momentum is based on underlying spot price change (Smart Average of Kraken/Coinbase) over the 3s history window.
Exit: simple take_profit_cents / stop_loss_cents (no trailing stop).

Entry criteria (all must hold):
  1. Time: min_seconds_to_close <= seconds_to_close <= max_seconds_to_close (e.g. 120–840s).
  2. Coil: yes_ask and no_ask both in [min_price_parity, max_price_parity] (e.g. [45, 55]).
  3. Spread: abs(yes_bid - no_bid) <= max_price_parity.
  4. Spot: current_spot (Smart Avg of Kraken/Coinbase) available.
  5. Momentum: |current_spot - prev_spot| >= threshold over last N seconds; direction sets momentum (up/down).
  6. Position vs strike: trade only when position and momentum agree:
       - (spot > strike) AND (momentum is UP)   -> BUY YES
       - (spot < strike) AND (momentum is DOWN) -> BUY NO
     Conflict zone (spot above strike but momentum down, or spot below strike but momentum up) -> skip.
"""
from __future__ import annotations

import logging
import time
import uuid
import json
import sqlite3
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional

from bot.pipeline.context import WindowContext
from bot.pipeline.intents import ExitAction, OrderIntent, OrderRecord
from bot.pipeline.strategies.base import BaseV2Strategy

logger = logging.getLogger(__name__)

TELEMETRY_TABLE = "v2_telemetry_atm"


def _v2_db_path() -> Path:
    return Path(__file__).resolve().parents[2].parent / "data" / "v2_state.db"


def _ensure_telemetry_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TELEMETRY_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            window_id TEXT,
            asset TEXT,
            side TEXT,
            entry_price_cents INTEGER,
            strike REAL,
            spot_kraken REAL,
            spot_coinbase REAL,
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


def _window_id(ctx: WindowContext) -> str:
    return f"{ctx.interval}_{ctx.market_id}"


def _current_spot(ctx: WindowContext) -> float | None:
    """
    Single spot used for both momentum (up_move/down_move) and spot-vs-strike (YES/NO).
    Smart Average: (Kraken + Coinbase) / 2 when both present; else whichever is available.
    """
    k = ctx.spot_kraken
    c = ctx.spot_coinbase
    if k is not None and c is not None:
        return (k + c) / 2.0
    if k is not None:
        return float(k)
    if c is not None:
        return float(c)
    return None


class AtmBreakoutStrategy(BaseV2Strategy):
    """
    ATM Breakout: entry when market is in parity (coil) and spot momentum triggers;
    exit via take_profit_cents, stop_loss_cents, or market_sell at window end.
    """

    def __init__(self, config: dict) -> None:
        super().__init__("atm_breakout_strategy", config)
        # Per-window rolling history: window_id -> deque[(timestamp, spot_price)]
        # Deque avoids the previous bug where a single timestamp was overwritten every tick.
        self._history: Dict[str, Any] = {}

        # Ensure ATM telemetry table exists (same v2_state.db as last_90s).
        path = _v2_db_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), check_same_thread=False)
        try:
            _ensure_telemetry_table(conn)
        finally:
            conn.close()

    def _record_telemetry(
        self,
        ctx: WindowContext,
        side: str,
        entry_price_cents: int,
        reason: str,
        ref_spot: Optional[float] = None,
        momentum_delta: Optional[float] = None,
        seconds_to_close: Optional[float] = None,
        distance_buffer: Optional[float] = None,
        ref_spot_ts: Optional[float] = None,
        current_spot_ts: Optional[float] = None,
        momentum_window_seconds: Optional[float] = None,
    ) -> None:
        """
        Write one row to v2_telemetry_atm capturing strike, spot, side, entry price, and momentum context.
        pre_data = JSON of quote, raw oracle fields, momentum (ref_spot, momentum_delta), and seconds_to_close.
        """
        pre_data = json.dumps(
            {
                "quote": dict(ctx.quote),
                "spot_kraken": ctx.spot_kraken,
                "spot_coinbase": ctx.spot_coinbase,
                "distance_kraken": ctx.distance_kraken,
                "distance_coinbase": ctx.distance_coinbase,
                "ref_spot": ref_spot,
                "momentum_delta": momentum_delta,
                "seconds_to_close": seconds_to_close,
                "distance_buffer": distance_buffer,
                # New: timestamps and configured window so we can audit the lookback interval.
                "ref_spot_ts": ref_spot_ts,
                "current_spot_ts": current_spot_ts,
                "momentum_window_seconds": momentum_window_seconds,
            }
        )
        window_id = f"{ctx.interval}_{ctx.market_id}"
        asset_str = (ctx.asset or "").strip().lower()
        strike = float(ctx.strike) if ctx.strike is not None else -1.0
        spot_k = float(ctx.spot_kraken) if ctx.spot_kraken is not None else -1.0
        spot_c = float(ctx.spot_coinbase) if ctx.spot_coinbase is not None else -1.0
        distance_raw = ctx.distance
        distance = float(distance_raw) if distance_raw is not None else -1.0
        path = _v2_db_path()
        try:
            conn = sqlite3.connect(str(path), check_same_thread=False)
            try:
                conn.execute(
                    f"""
                    INSERT INTO {TELEMETRY_TABLE}
                    (window_id, asset, side, entry_price_cents, strike, spot_kraken, spot_coinbase, distance, reason, pre_data, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        window_id,
                        asset_str,
                        side,
                        int(entry_price_cents),
                        strike,
                        spot_k,
                        spot_c,
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
            logger.debug("[atm_breakout] Telemetry write failed: %s", e)

    def evaluate_entry(
        self, ctx: WindowContext, my_orders: Optional[List[OrderRecord]] = None
    ) -> OrderIntent | None:
        cfg = self._get_strategy_config(ctx)
        if not cfg or not cfg.get("enabled", False):
            return None

        # Strict position limit: one trade at a time. Do not place a new entry if we have
        # any orders (resting, filled, or partially filled) in this window.
        if my_orders and len(my_orders) > 0:
            return None

        secs = ctx.seconds_to_close
        if secs is None:
            return None
        min_sec = float(cfg.get("min_seconds_to_close", 120))
        max_sec = float(cfg.get("max_seconds_to_close", 840))
        # Hard time barrier: do not enter in final 2 minutes (secs < 120); leaves last_90s alone
        if secs < min_sec or secs > max_sec:
            return None

        asset = (ctx.asset or "").strip().lower()

        yes_bid = ctx.quote.get("yes_bid", 0) or 0
        no_bid = ctx.quote.get("no_bid", 0) or 0
        yes_ask = ctx.quote.get("yes_ask", 0) or 0
        no_ask = ctx.quote.get("no_ask", 0) or 0
        # Breakout confirmation pricing: willing to pay within [min_entry_price, max_entry_price]
        # for the side we are buying (ITM entry after spot crosses strike).
        min_entry_price = int(cfg.get("min_entry_price", 45))
        max_entry_price = int(cfg.get("max_entry_price", 70))
        # Spread protection: do not buy if the bid-ask spread on the chosen side is too wide.
        max_spread_cents = int(cfg.get("max_entry_spread_cents", 10))

        current_spot = _current_spot(ctx)
        if current_spot is None:
            return None
        strike = ctx.strike
        if strike is None:
            return None

        wid = _window_id(ctx)
        now = time.time()
        momentum_window_sec = float(cfg.get("momentum_window_seconds", 5))
        momentum_window_sec = max(1.0, min(60.0, momentum_window_sec))
        momentum_trigger = _get_asset_config(cfg.get("momentum_trigger_3s"), ctx.asset, 1.0)
        try:
            trigger_val = float(momentum_trigger)  # dollars, e.g. BTC 50.0
        except (TypeError, ValueError):
            trigger_val = 1.0

        # Rolling window: keep a deque of (timestamp, spot) and look back momentum_window_sec.
        dq = self._history.get(wid)
        if dq is None:
            dq = deque()
            self._history[wid] = dq
        dq.append((now, current_spot))

        # Drop entries that are far outside the window to bound memory.
        hard_cutoff = momentum_window_sec * 2.0
        while dq and now - dq[0][0] > hard_cutoff:
            dq.popleft()

        # Find the oldest price that is at least momentum_window_sec old.
        ref_spot = None
        ref_ts: Optional[float] = None
        for ts, spot_then in dq:
            if now - ts >= momentum_window_sec:
                ref_spot = spot_then
                ref_ts = ts
                break
        if ref_spot is None or ref_ts is None:
            # Not enough lookback yet; wait until we have a full window.
            return None

        up_move = current_spot - ref_spot
        down_move = ref_spot - current_spot
        momentum_up = up_move >= trigger_val and up_move >= down_move
        momentum_down = down_move >= trigger_val and down_move >= up_move
        if not momentum_up and not momentum_down:
            # Log near-misses for tuning momentum_trigger_3s (e.g. SOL/XRP fire very little).
            best_move = max(up_move, down_move)
            self._record_telemetry(
                ctx,
                side="yes" if up_move >= down_move else "no",
                entry_price_cents=0,
                reason="momentum_below_threshold",
                ref_spot=ref_spot,
                momentum_delta=best_move,
                seconds_to_close=float(secs) if secs is not None else None,
                distance_buffer=None,
                ref_spot_ts=ref_ts,
                current_spot_ts=now,
                momentum_window_seconds=momentum_window_sec,
            )
            return None

        # Distance buffer: after we have momentum context, skip if we're too close to strike.
        # We also log telemetry here so we can tune min_distance_at_placement from real momentum + distance examples.
        min_dist_cfg = _get_asset_config(cfg.get("min_distance_at_placement"), asset, 0.0)
        try:
            min_dist_required = float(min_dist_cfg) if min_dist_cfg is not None else 0.0
        except (TypeError, ValueError):
            min_dist_required = 0.0
        if min_dist_required > 0 and ctx.distance is not None and ctx.distance < min_dist_required:
            # Derive the "candidate" side from spot/strike + momentum, if any (purely for telemetry readability).
            if current_spot > strike and momentum_up:
                side_hint = "yes"
                momentum_delta = up_move
            elif current_spot < strike and momentum_down:
                side_hint = "no"
                momentum_delta = down_move
            else:
                side_hint = "skip"
                momentum_delta = up_move if momentum_up else down_move
            self._record_telemetry(
                ctx,
                side=side_hint,
                entry_price_cents=0,
                reason="low_distance_at_placement",
                ref_spot=ref_spot,
                momentum_delta=momentum_delta,
                seconds_to_close=float(secs) if secs is not None else None,
                distance_buffer=min_dist_required,
                ref_spot_ts=ref_ts,
                current_spot_ts=now,
                momentum_window_seconds=momentum_window_sec,
            )
            return None

        # Positional check vs strike (skip conflict zone). We only take:
        #  - YES when spot has broken above strike with upside momentum AND YES ask is within entry band.
        #  - NO  when spot has broken below strike with downside momentum AND NO ask is within entry band.
        # Spread protection is applied to the chosen side only.
        if current_spot > strike and momentum_up:
            ask_c = int(yes_ask)
            bid_c = int(yes_bid)
            if not (min_entry_price <= ask_c <= max_entry_price):
                # Momentum fired but YES ask not in [min_entry_price, max_entry_price].
                self._record_telemetry(
                    ctx,
                    side="yes",
                    entry_price_cents=0,
                    reason="entry_price_band",
                    ref_spot=ref_spot,
                    momentum_delta=up_move,
                    seconds_to_close=float(secs) if secs is not None else None,
                    distance_buffer=min_dist_required if min_dist_required > 0 else None,
                    ref_spot_ts=ref_ts,
                    current_spot_ts=now,
                    momentum_window_seconds=momentum_window_sec,
                )
                return None
            spread = ask_c - bid_c
            if spread > max_spread_cents:
                # Momentum fired but YES spread too wide.
                self._record_telemetry(
                    ctx,
                    side="yes",
                    entry_price_cents=0,
                    reason="entry_spread",
                    ref_spot=ref_spot,
                    momentum_delta=up_move,
                    seconds_to_close=float(secs) if secs is not None else None,
                    distance_buffer=min_dist_required if min_dist_required > 0 else None,
                    ref_spot_ts=ref_ts,
                    current_spot_ts=now,
                    momentum_window_seconds=momentum_window_sec,
                )
                return None
            side = "yes"
            price_cents = ask_c
        elif current_spot < strike and momentum_down:
            ask_c = int(no_ask)
            bid_c = int(no_bid)
            if not (min_entry_price <= ask_c <= max_entry_price):
                # Momentum fired but NO ask not in [min_entry_price, max_entry_price].
                self._record_telemetry(
                    ctx,
                    side="no",
                    entry_price_cents=0,
                    reason="entry_price_band",
                    ref_spot=ref_spot,
                    momentum_delta=down_move,
                    seconds_to_close=float(secs) if secs is not None else None,
                    distance_buffer=min_dist_required if min_dist_required > 0 else None,
                    ref_spot_ts=ref_ts,
                    current_spot_ts=now,
                    momentum_window_seconds=momentum_window_sec,
                )
                return None
            spread = ask_c - bid_c
            if spread > max_spread_cents:
                # Momentum fired but NO spread too wide.
                self._record_telemetry(
                    ctx,
                    side="no",
                    entry_price_cents=0,
                    reason="entry_spread",
                    ref_spot=ref_spot,
                    momentum_delta=down_move,
                    seconds_to_close=float(secs) if secs is not None else None,
                    distance_buffer=min_dist_required if min_dist_required > 0 else None,
                    ref_spot_ts=ref_ts,
                    current_spot_ts=now,
                    momentum_window_seconds=momentum_window_sec,
                )
                return None
            side = "no"
            price_cents = ask_c
        else:
            # Conflict zone: spot/strike disagrees with momentum direction (or exactly at strike)
            return None

        order_count = cfg.get("order_count")
        if order_count is not None:
            try:
                count = max(1, min(10, int(order_count)))
            except (TypeError, ValueError):
                count = max(1, int(cfg.get("contracts", 1)))
        else:
            count = max(1, int(cfg.get("contracts", 1)))

        # Record ATM telemetry at entry decision time so we can reconstruct strike/spot/side and momentum later.
        momentum_delta = up_move if side == "yes" else down_move
        self._record_telemetry(
            ctx,
            side=side,
            entry_price_cents=price_cents,
            reason="entry_intent",
            ref_spot=ref_spot,
            momentum_delta=momentum_delta,
            seconds_to_close=float(secs) if secs is not None else None,
            distance_buffer=min_dist_required if min_dist_required > 0 else None,
            ref_spot_ts=ref_ts,
            current_spot_ts=now,
            momentum_window_seconds=momentum_window_sec,
        )
        logger.info(
            "[atm_breakout_v2] [%s] ENTRY side=%s price=%sc count=%s sec_to_close=%.0f "
            "window=%.0fs trigger=%.4f ref_spot=%.4f current_spot=%.4f momentum_delta=%.4f",
            asset.upper(),
            side,
            price_cents,
            count,
            secs if secs is not None else -1,
            momentum_window_sec,
            trigger_val,
            ref_spot,
            current_spot,
            momentum_delta,
        )
        client_order_id = f"atm:{uuid.uuid4().hex[:12]}"
        return OrderIntent(
            side=side,
            price_cents=price_cents,
            count=count,
            order_type="limit",
            client_order_id=client_order_id,
        )

    def evaluate_exit(self, ctx: WindowContext, my_orders: List[OrderRecord]) -> List[ExitAction]:
        cfg = self._get_strategy_config(ctx)
        if not cfg:
            return []

        take_profit_cents = int(cfg.get("take_profit_cents", 75))
        stop_loss_cents = int(cfg.get("stop_loss_cents", 35))

        actions: List[ExitAction] = []
        for order in my_orders:
            if order.status != "filled":
                continue
            side = (order.side or "yes").lower()
            current_bid = (ctx.quote.get("yes_bid") or 0) if side == "yes" else (ctx.quote.get("no_bid") or 0)
            try:
                current_bid = int(current_bid)
            except (TypeError, ValueError):
                current_bid = 0

            # Evaluate every run: if either condition is met, emit one exit and executor will market sell.
            # No resting limit order; both TP and SL close at market (fill at best available price).
            if current_bid <= stop_loss_cents:
                actions.append(ExitAction(order_id=order.order_id, action="stop_loss"))
                logger.info(
                    "[atm_breakout] Stop-loss: order_id=%s bid=%s <= stop_loss_cents=%s → market sell",
                    order.order_id, current_bid, stop_loss_cents,
                )
                continue
            if current_bid >= take_profit_cents:
                actions.append(
                    ExitAction(order_id=order.order_id, action="take_profit", limit_price_cents=take_profit_cents),
                )
                logger.info(
                    "[atm_breakout] Take-profit: order_id=%s bid=%s >= take_profit_cents=%s → market sell",
                    order.order_id, current_bid, take_profit_cents,
                )
                continue
            # Else: no exit this tick; hold position until next run or window resolve.

        return actions
