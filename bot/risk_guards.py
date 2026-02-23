"""
Risk guardrails for the Kalshi trading bot.
In-memory runtime state. Prevents whipsaw, re-entry churn, ladder chasing.
"""
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# Type for signal-like objects (Signal or Signal15min)
SignalLike = Any


@dataclass
class TickerState:
    """Per-ticker runtime state."""
    last_yes_cents: Optional[int] = None
    last_no_cents: Optional[int] = None
    in_band_streak: int = 0
    recent_cross_flag: bool = False  # Price jumped from <from_below to >=to_at_least
    stopouts_count: int = 0
    disabled_until_expiry: bool = False
    parsed_strike: Optional[float] = None
    stopped_side: Optional[str] = None  # 15-min: side that was stopped out (yes/no)
    reversals_taken: int = 0  # 15-min: count of opposite-side entries after stop


@dataclass
class WindowState:
    """Per-window state (hourly: anchor one per side)."""
    window_id: str = ""
    anchor_yes_ticker: Optional[str] = None
    anchor_no_ticker: Optional[str] = None
    lock: bool = False
    total_evals: int = 0
    total_entries: int = 0
    total_exits: int = 0
    skips_by_reason: Dict[str, int] = field(default_factory=dict)
    exits_by_reason: Dict[str, int] = field(default_factory=dict)
    stopouts: int = 0
    reversals_taken_total: int = 0
    # Hourly roll-after-stop: allow 1 re-entry same direction after STOP_LOSS
    roll_available_side: Optional[str] = None  # "yes"|"no" when STOP_LOSS creates roll opportunity
    roll_stopped_ticker: Optional[str] = None  # ticker that stopped (for logging)
    roll_used_side: Optional[str] = None  # side that used its roll
    roll_exhausted_sides: set = field(default_factory=set)  # sides blocked after roll used+exited


def _get_ticker_state(
    store: Dict[str, Dict[str, TickerState]],
    window_id: str,
    ticker: str,
) -> TickerState:
    if window_id not in store:
        store[window_id] = {}
    if ticker not in store[window_id]:
        store[window_id][ticker] = TickerState()
    return store[window_id][ticker]


def _get_window_state(store: Dict[str, WindowState], window_id: str) -> WindowState:
    if window_id not in store:
        store[window_id] = WindowState(window_id=window_id)
    return store[window_id]


# Global in-memory stores (keyed by window_id)
_ticker_states: Dict[str, Dict[str, TickerState]] = {}
_window_states: Dict[str, WindowState] = {}


def reset_window_on_expiry(interval: str, window_id: str) -> None:
    """
    Call when a window expires. Cleans up ticker states for that window.
    Call emit_window_summary before this to log the summary.
    """
    _window_states.pop(window_id, None)
    _ticker_states.pop(window_id, None)


def emit_window_summary(
    logger,
    interval: str,
    window_id: str,
    asset: Optional[str] = None,
    build_summary_fn=None,
) -> None:
    """Log WINDOW_SUMMARY for a window that just expired. If build_summary_fn provided, use it to build canonical event."""
    ws = _window_states.get(window_id)
    if not ws:
        return
    entry = {
        "event": "WINDOW_SUMMARY",
        "interval": interval,
        "window_id": window_id,
        "asset": asset,
        "total_evals": ws.total_evals,
        "total_entries": ws.total_entries,
        "total_exits": ws.total_exits,
        "skips_by_reason": ws.skips_by_reason,
        "exits_by_reason": getattr(ws, "exits_by_reason", {}),
        "stopouts": ws.stopouts,
        "reversals_taken_total": getattr(ws, "reversals_taken_total", 0),
        "anchor_yes_ticker": ws.anchor_yes_ticker,
        "anchor_no_ticker": ws.anchor_no_ticker,
    }
    if build_summary_fn:
        entry = build_summary_fn(
            interval=interval,
            window_id=window_id,
            asset=asset,
            total_evals=ws.total_evals,
            total_entries=ws.total_entries,
            total_exits=ws.total_exits,
            skips_by_reason=ws.skips_by_reason,
            exits_by_reason=getattr(ws, "exits_by_reason", {}),
            stopouts_count_total=ws.stopouts,
            reversals_taken_total=getattr(ws, "reversals_taken_total", 0),
            anchor_yes_ticker=ws.anchor_yes_ticker,
            anchor_no_ticker=ws.anchor_no_ticker,
        )
    logger.info(json.dumps(entry, default=str))


def record_stopout(
    interval: str,
    window_id: str,
    ticker: str,
    max_stopouts: int,
    stopped_side: Optional[str] = None,
    reversal_after_stop_cfg: Optional[dict] = None,
) -> None:
    """
    Increment stopouts for ticker. Record stopped_side when reversal_after_stop enabled (15-min).
    For 15-min with reversal: do NOT disable; caller logs STOPLOSS_EXIT.
    For 15-min without reversal, or hourly: disable when >= max_stopouts.
    """
    ts = _get_ticker_state(_ticker_states, window_id, ticker)
    ts.stopouts_count += 1
    ws = _get_window_state(_window_states, window_id)
    ws.stopouts += 1

    if interval == "15min" and reversal_after_stop_cfg and reversal_after_stop_cfg.get("enabled", False):
        ts.stopped_side = (stopped_side or "").lower()
        if ts.stopped_side not in ("yes", "no"):
            ts.stopped_side = None
        # Do NOT set disabled_until_expiry - allow one reversal
    elif ts.stopouts_count >= max_stopouts:
        ts.disabled_until_expiry = True


def record_exit(interval: str, window_id: str, reason: Optional[str] = None) -> None:
    """Record that we exited a position (any exit type). reason = EXIT_STOPLOSS, EXIT_HARD_FLIP, EXIT_TAKE_PROFIT."""
    ws = _get_window_state(_window_states, window_id)
    ws.total_exits += 1
    if reason:
        ws.exits_by_reason[reason] = ws.exits_by_reason.get(reason, 0) + 1


def record_hourly_stoploss_for_roll(
    window_id: str,
    stopped_ticker: str,
    side: str,
    lock_on_stop_loss_only: bool = False,
) -> None:
    """
    Hourly only: when STOP_LOSS happens, prepare for one roll (same direction, different strike).
    Clears anchor for that side. If lock_on_stop_loss_only, sets lock=True (lock only on stop);
    otherwise unlocks to allow roll (legacy behavior).
    """
    ws = _get_window_state(_window_states, window_id)
    s = str(side).lower()
    if s not in ("yes", "no"):
        return
    ws.roll_available_side = s
    ws.roll_stopped_ticker = stopped_ticker
    ws.lock = lock_on_stop_loss_only  # Lock on stop; else unlock for roll
    if s == "yes":
        ws.anchor_yes_ticker = None
    else:
        ws.anchor_no_ticker = None


def is_hourly_roll_available(window_id: str, side: str, ticker: str) -> bool:
    """True if we have a roll opportunity for this side and ticker is different from stopped."""
    ws = _window_states.get(window_id)
    if not ws or ws.roll_available_side != str(side).lower():
        return False
    if ws.roll_stopped_ticker and ticker == ws.roll_stopped_ticker:
        return False  # Must be different strike
    return True


def consume_hourly_roll(window_id: str, side: str, rolled_ticker: str) -> None:
    """Mark roll as used after placing order on rolled_ticker."""
    ws = _window_states.get(window_id)
    if ws:
        ws.roll_used_side = str(side).lower()
        ws.roll_available_side = None
        ws.roll_stopped_ticker = None


def mark_hourly_roll_exhausted(window_id: str, side: str) -> None:
    """Call when rolled position exits - block further entries for that side."""
    ws = _window_states.get(window_id)
    if ws:
        ws.roll_exhausted_sides.add(str(side).lower())
        ws.roll_used_side = None


def get_roll_used_side(window_id: str) -> Optional[str]:
    """Return side that used its roll (if any), for exit handling."""
    ws = _window_states.get(window_id)
    return ws.roll_used_side if ws else None


def get_roll_stopped_ticker(window_id: str) -> Optional[str]:
    """Return ticker that stopped (for ROLL_AFTER_STOP logging)."""
    ws = _window_states.get(window_id)
    return ws.roll_stopped_ticker if ws else None


def record_entry(
    interval: str,
    window_id: str,
    ticker: str,
    side: str,
    lock_per_window: bool = False,
    is_reversal: bool = False,
    lock_on_stop_loss_only: bool = False,
) -> None:
    """Record that we entered a position. Updates anchor for hourly. For 15-min reversal, disables ticker.
    When lock_on_stop_loss_only is True, never set lock on entry (lock is set only when stop loss triggers).
    """
    ws = _get_window_state(_window_states, window_id)
    ws.total_entries += 1
    if interval == "hourly":
        if side == "yes":
            ws.anchor_yes_ticker = ticker
        else:
            ws.anchor_no_ticker = ticker
        if lock_per_window and not lock_on_stop_loss_only:
            ws.lock = True
    elif interval == "15min" and is_reversal:
        ts = _get_ticker_state(_ticker_states, window_id, ticker)
        ts.reversals_taken += 1
        ts.disabled_until_expiry = True
        ws.reversals_taken_total += 1


def _check_recent_cross(
    yes_cents: Optional[int],
    no_cents: Optional[int],
    cfg: dict,
    last_yes: Optional[int],
    last_no: Optional[int],
) -> bool:
    """True if price just crossed from below from_below_cents to at least to_at_least_cents."""
    if not cfg.get("enabled", False):
        return False
    from_below = cfg.get("from_below_cents", 90)
    to_at_least = cfg.get("to_at_least_cents", 94)
    # Check if previous poll had price < from_below and current has >= to_at_least
    price_cents = yes_cents if yes_cents is not None else no_cents
    last_cents = last_yes if last_yes is not None else last_no
    if price_cents is None or last_cents is None:
        return False
    return last_cents < from_below and price_cents >= to_at_least


def _guard_tune_knob(interval: str, is_hourly: bool, reason_code: str, asset: str) -> str:
    """Config knob to tune for this skip reason. Used for analysis/tuning."""
    prefix = "schedule.hourly_risk_guards" if is_hourly else "fifteen_min.risk_guards"
    knob_map = {
        "cutoff_seconds": f"{prefix}.no_new_entry_cutoff_seconds",
        "BLOCK_REVERSAL_CUTOFF": f"{prefix}.no_new_entry_cutoff_seconds",
        "persistence": f"{prefix}.persistence_polls",
        "distance_buffer": f"{prefix}.distance_buffer.assets.{asset.lower()}",
        "disabled_until_expiry": f"{prefix}.stop_once_disable",
        "anchor_mismatch": f"{prefix}.anchor_one_per_side",
        "roll_exhausted": f"{prefix}.stoploss_roll_reentry",
        "roll_same_ticker_blocked": f"{prefix}.stoploss_roll_reentry",
        "BLOCK_SAME_SIDE_AFTER_STOP": f"{prefix}.reversal_after_stop",
        "BLOCK_MAX_REVERSALS": f"{prefix}.reversal_after_stop",
        "BLOCK_REVERSAL_WEAK_SIGNAL": f"{prefix}.reversal_after_stop",
    }
    return knob_map.get(reason_code, f"{prefix}")


def _guard_checks_summary(
    reason_code: str,
    seconds_to_close: Optional[float],
    cutoff_secs: Optional[float],
    streak: Optional[int],
    required_polls: Optional[int],
    distance: Optional[float],
    min_dist: Optional[float],
) -> str:
    """Human-readable summary of which check failed and values. For tuning."""
    parts = []
    rc = (reason_code or "").lower()
    if rc in ("cutoff_seconds", "block_reversal_cutoff"):
        ttc = f"{seconds_to_close:.0f}s" if seconds_to_close is not None else "?"
        lim = f"{cutoff_secs}s" if cutoff_secs is not None else "?"
        parts.append(f"cutoff: FAIL (ttc={ttc} <= {lim})")
    elif rc == "persistence":
        s = streak if streak is not None else "?"
        r = required_polls if required_polls is not None else "?"
        parts.append(f"persistence: FAIL (streak {s} < {r})")
    elif rc == "distance_buffer":
        d = f"{distance:.1f}" if distance is not None else "?"
        m = f"{min_dist:.1f}" if min_dist is not None else "?"
        parts.append(f"distance_buffer: FAIL (dist={d} < min={m})")
    elif rc == "disabled_until_expiry":
        parts.append("disabled_until_expiry: FAIL (ticker disabled)")
    elif rc == "anchor_mismatch":
        parts.append("anchor_mismatch: FAIL (different strike)")
    else:
        parts.append(f"{reason_code}: FAIL")
    return "; ".join(parts)


def _min_distance_required(
    asset: str,
    spot: float,
    cfg: dict,
) -> Optional[float]:
    """Returns min USD distance from strike required, or None if disabled/unknown."""
    if not cfg.get("enabled", False):
        return None
    assets_cfg = cfg.get("assets", {}) or {}
    a = str(asset).lower()
    ac = assets_cfg.get(a) or assets_cfg.get("btc")
    if not ac:
        return None
    pct = ac.get("pct", 0.001)
    floor_usd = ac.get("floor_usd", 50)
    if spot is None or spot <= 0:
        return None
    return max(floor_usd, spot * pct)


def gate_allow_entry(
    interval: str,
    window_id: str,
    asset: str,
    ticker: str,
    side: str,
    yes_price: Optional[int],
    no_price: Optional[int],
    spot: Optional[float],
    strike: Optional[float],
    seconds_to_close: float,
    entry_band: str,
    guards_cfg: dict,
    is_hourly: bool,
    yes_price_all: Optional[int] = None,
    no_price_all: Optional[int] = None,
) -> Tuple[bool, str, dict]:
    """
    Gate check before order placement.
    Returns (allowed, reason_code, eval_payload).
    eval_payload contains fields for structured logging.
    """
    if not guards_cfg.get("enabled", False):
        return True, "ok", {}

    ts = _get_ticker_state(_ticker_states, window_id, ticker)
    ws = _get_window_state(_window_states, window_id)
    ws.total_evals += 1

    cutoff_secs = guards_cfg.get("no_new_entry_cutoff_seconds", 40)
    # Hourly only: block if side has exhausted its roll (second stop or rolled + exited)
    if is_hourly and str(side).lower() in (ws.roll_exhausted_sides or set()):
        eval_payload = {
            "event": "SKIP", "reason_code": "roll_exhausted",
            "interval": interval, "window_id": window_id, "asset": asset,
            "ticker": ticker, "side": side, "seconds_to_close": seconds_to_close,
            "cutoff_seconds": cutoff_secs,
            "guard_tune_knob": _guard_tune_knob(interval, is_hourly, "roll_exhausted", asset),
            "guard_checks_summary": "roll_exhausted: FAIL (side exhausted)",
        }
        return False, "roll_exhausted", eval_payload

    # Hourly only: in roll mode, block same ticker (roll must be different strike)
    if is_hourly and ws.roll_available_side == str(side).lower() and ticker == ws.roll_stopped_ticker:
        eval_payload = {
            "event": "SKIP", "reason_code": "roll_same_ticker_blocked",
            "interval": interval, "window_id": window_id, "asset": asset,
            "ticker": ticker, "side": side, "seconds_to_close": seconds_to_close,
            "cutoff_seconds": cutoff_secs,
            "guard_tune_knob": _guard_tune_knob(interval, is_hourly, "roll_same_ticker_blocked", asset),
            "guard_checks_summary": "roll_same_ticker_blocked: FAIL (must use different strike)",
        }
        return False, "roll_same_ticker_blocked", eval_payload

    def _parse_entry_band(band: str):
        # band is for persistence gating (in-band streak). Default matches original behavior.
        lo, hi = 94, 99
        try:
            s = str(band or "").strip()
            if "-" in s:
                a, b = s.split("-", 1)
                lo = int(a.strip())
                hi = int(b.strip())
        except Exception:
            lo, hi = 94, 99
        if lo > hi:
            lo, hi = hi, lo
        lo = max(0, min(lo, 100))
        hi = max(0, min(hi, 100))
        return lo, hi

    price_cents = yes_price if side == "yes" else no_price
    band_lo, band_hi = _parse_entry_band(entry_band)
    in_band = band_lo <= (price_cents or 0) <= band_hi

    # Base persistence and late override
    base_persistence = guards_cfg.get("persistence_polls", 2)
    late_override = (guards_cfg.get("late_persistence_override") or {})
    late_persistence_applied = False
    required_polls = base_persistence
    if late_override.get("enabled", False):
        sec_lte = late_override.get("seconds_to_close_lte")
        if sec_lte is not None and seconds_to_close is not None and seconds_to_close <= sec_lte:
            required_polls = late_override.get("persistence_polls", 1)
            late_persistence_applied = True
    persistence_required_effective = required_polls

    # Check recent cross BEFORE updating last prices
    recent_cfg = guards_cfg.get("recent_cross", {}) or {}
    ts.recent_cross_flag = _check_recent_cross(
        yes_price, no_price, recent_cfg,
        ts.last_yes_cents, ts.last_no_cents,
    )

    # Update last prices
    ts.last_yes_cents = yes_price
    ts.last_no_cents = no_price
    ts.parsed_strike = strike

    rev_cfg = guards_cfg.get("reversal_after_stop", {}) or {}
    reversal_candidate = False

    eval_payload = {
        "event": "EVAL",
        "interval": interval,
        "window_id": window_id,
        "asset": asset,
        "ticker": ticker,
        "side": side,
        "yes_price": yes_price_all if yes_price_all is not None else yes_price,
        "no_price": no_price_all if no_price_all is not None else no_price,
        "entry_band": entry_band,
        "seconds_to_close": seconds_to_close,
        "cutoff_seconds": cutoff_secs,
        "spot": spot,
        "strike": strike,
        "streak": ts.in_band_streak,
        "required_polls": required_polls,
        "persistence_required_base": base_persistence,
        "persistence_required_effective": persistence_required_effective,
        "late_persistence_applied": late_persistence_applied,
        "recent_cross_flag": ts.recent_cross_flag,
        "stopouts_count": ts.stopouts_count,
        "disabled_until_expiry": ts.disabled_until_expiry,
        "reversal_enabled": rev_cfg.get("enabled", False),
        "stopped_side": ts.stopped_side,
        "reversals_taken": ts.reversals_taken,
        "anchor_yes": ws.anchor_yes_ticker,
        "anchor_no": ws.anchor_no_ticker,
        "anchor_locked": ws.lock,
    }

    if not is_hourly and interval == "15min" and rev_cfg.get("enabled", False) and ts.stopped_side:
        opposite_side = "no" if ts.stopped_side == "yes" else "yes"
        max_rev = rev_cfg.get("max_reversals_per_ticker", 1)
        min_opp_price = rev_cfg.get("require_opposite_price_at_least_cents", 98)
        opp_price = no_price_all if opposite_side == "no" else yes_price_all
        if opp_price is None:
            opp_price = no_price if opposite_side == "no" else yes_price

        if side == ts.stopped_side:
            _inc_skip(ws, "block_same_side_after_stop")
            eval_payload["reason_code"] = "BLOCK_SAME_SIDE_AFTER_STOP"
            eval_payload["event"] = "SKIP"
            eval_payload["stopped_side"] = ts.stopped_side
            eval_payload["type"] = "risk_guard_reversal"
            eval_payload["guard_tune_knob"] = _guard_tune_knob(interval, is_hourly, "BLOCK_SAME_SIDE_AFTER_STOP", asset)
            eval_payload["guard_checks_summary"] = "BLOCK_SAME_SIDE_AFTER_STOP: FAIL (same side blocked)"
            return False, "BLOCK_SAME_SIDE_AFTER_STOP", eval_payload

        if side == opposite_side:
            if ts.reversals_taken >= max_rev:
                ts.disabled_until_expiry = True
                _inc_skip(ws, "block_max_reversals")
                eval_payload["reason_code"] = "BLOCK_MAX_REVERSALS"
                eval_payload["event"] = "SKIP"
                eval_payload["reversals_taken"] = ts.reversals_taken
                eval_payload["max_reversals"] = max_rev
                eval_payload["disabled_after_reversal_exhausted"] = True
                eval_payload["type"] = "risk_guard_reversal"
                eval_payload["guard_tune_knob"] = _guard_tune_knob(interval, is_hourly, "BLOCK_MAX_REVERSALS", asset)
                eval_payload["guard_checks_summary"] = f"BLOCK_MAX_REVERSALS: FAIL (reversals={ts.reversals_taken} >= {max_rev})"
                return False, "BLOCK_MAX_REVERSALS", eval_payload

            if (opp_price or 0) < min_opp_price:
                _inc_skip(ws, "block_reversal_weak_signal")
                eval_payload["reason_code"] = "BLOCK_REVERSAL_WEAK_SIGNAL"
                eval_payload["event"] = "SKIP"
                eval_payload["opposite_price"] = opp_price
                eval_payload["require_opposite_price_at_least_cents"] = min_opp_price
                eval_payload["type"] = "risk_guard_reversal"
                eval_payload["guard_tune_knob"] = _guard_tune_knob(interval, is_hourly, "BLOCK_REVERSAL_WEAK_SIGNAL", asset)
                eval_payload["guard_checks_summary"] = f"BLOCK_REVERSAL_WEAK_SIGNAL: FAIL (opp_price={opp_price} < {min_opp_price})"
                return False, "BLOCK_REVERSAL_WEAK_SIGNAL", eval_payload

            reversal_candidate = True

    # 1) Cutoff seconds - no new entries (reversal may bypass if allow_inside_cutoff)
    allow_inside_cutoff = rev_cfg.get("allow_inside_cutoff", True) if rev_cfg else True
    if cutoff_secs and seconds_to_close <= cutoff_secs:
        if not (reversal_candidate and allow_inside_cutoff):
            _inc_skip(ws, "cutoff_seconds")
            rc = "BLOCK_REVERSAL_CUTOFF" if (reversal_candidate and not allow_inside_cutoff) else "cutoff_seconds"
            if reversal_candidate and not allow_inside_cutoff:
                eval_payload["reason_code"] = "BLOCK_REVERSAL_CUTOFF"
                eval_payload["type"] = "risk_guard_reversal"
            else:
                eval_payload["reason_code"] = "cutoff_seconds"
            eval_payload["event"] = "SKIP"
            eval_payload["guard_tune_knob"] = _guard_tune_knob(interval, is_hourly, rc, asset)
            eval_payload["guard_checks_summary"] = _guard_checks_summary(
                rc, seconds_to_close, cutoff_secs, ts.in_band_streak, required_polls, None, None
            )
            # Add persistence/distance diagnostic for tuning (would be checked if cutoff passed)
            dist_cfg_pre = guards_cfg.get("distance_buffer", {}) or {}
            if strike is not None and strike > 0 and spot is not None and spot > 0:
                min_d = _min_distance_required(asset, spot, dist_cfg_pre)
                d = (spot - strike) if side == "yes" else (strike - spot)
                eval_payload["min_distance_required"] = min_d
                eval_payload["distance"] = d
            return False, eval_payload["reason_code"], eval_payload

    # 2) Disabled until expiry (stop once)
    stop_cfg = guards_cfg.get("stop_once_disable", {}) or {}
    if stop_cfg.get("enabled", False) and ts.disabled_until_expiry:
        _inc_skip(ws, "disabled_until_expiry")
        eval_payload["reason_code"] = "disabled_until_expiry"
        eval_payload["event"] = "SKIP"
        eval_payload["guard_tune_knob"] = _guard_tune_knob(interval, is_hourly, "disabled_until_expiry", asset)
        eval_payload["guard_checks_summary"] = _guard_checks_summary(
            "disabled_until_expiry", None, None, None, None, None, None
        )
        return False, "disabled_until_expiry", eval_payload

    # 3) Recent cross - extra persistence (applies on top of effective_persistence)
    if ts.recent_cross_flag and recent_cfg.get("enabled", False):
        extra = recent_cfg.get("extra_persistence_polls", 1)
        required_polls += extra
        persistence_required_effective = required_polls
        eval_payload["required_polls"] = required_polls
        eval_payload["persistence_required_effective"] = persistence_required_effective

    # Hourly stop-loss roll: allow immediate re-entry on next strike in the same direction.
    # Reduce persistence requirement so we can act on the current in-band observation.
    if is_hourly and ws.roll_available_side == str(side).lower():
        required_polls = 1
        eval_payload["required_polls"] = required_polls
        eval_payload["roll_mode"] = True

    # Final effective for logging (persistence_required_effective already set above; roll doesn't change it for logging)
    eval_payload["persistence_required_effective"] = persistence_required_effective

    # 4) Persistence
    if in_band:
        ts.in_band_streak += 1
    else:
        ts.in_band_streak = 0

    if ts.in_band_streak < required_polls:
        _inc_skip(ws, "persistence")
        eval_payload["reason_code"] = "persistence"
        eval_payload["event"] = "SKIP"
        eval_payload["min_distance_required"] = None
        eval_payload["guard_tune_knob"] = _guard_tune_knob(interval, is_hourly, "persistence", asset)
        eval_payload["guard_checks_summary"] = _guard_checks_summary(
            "persistence", seconds_to_close, cutoff_secs, ts.in_band_streak, required_polls, None, None
        )
        return False, "persistence", eval_payload

    # 5) Distance buffer (directional: YES requires (spot - strike) >= min_dist, NO requires (strike - spot) >= min_dist)
    dist_cfg = guards_cfg.get("distance_buffer", {}) or {}
    eval_payload["min_distance_required"] = None
    eval_payload["distance_buffer_directional"] = side
    strike_known = strike is not None and strike > 0
    eval_payload["distance_buffer_strike_known"] = strike_known
    spot_ok = spot is not None and spot > 0
    if not strike_known:
        eval_payload["distance_buffer_skipped_reason"] = "DISTANCE_BUFFER_SKIPPED_STRIKE_UNKNOWN"
    elif not spot_ok:
        eval_payload["distance_buffer_skipped_reason"] = "DISTANCE_BUFFER_SKIPPED_SPOT_UNKNOWN"
    else:
        min_dist = _min_distance_required(asset, spot, dist_cfg)
        eval_payload["min_distance_required"] = min_dist
        if min_dist is not None:
            if side == "yes":
                distance = spot - strike
            else:
                distance = strike - spot
            eval_payload["distance"] = distance
            if distance < min_dist:
                _inc_skip(ws, "distance_buffer")
                eval_payload["reason_code"] = "distance_buffer"
                eval_payload["event"] = "SKIP"
                eval_payload["guard_tune_knob"] = _guard_tune_knob(interval, is_hourly, "distance_buffer", asset)
                eval_payload["guard_checks_summary"] = _guard_checks_summary(
                    "distance_buffer", seconds_to_close, cutoff_secs,
                    ts.in_band_streak, required_polls, distance, min_dist
                )
                return False, "distance_buffer", eval_payload

    # 6) Anchor one per side (hourly only)
    if is_hourly:
        anchor_cfg = guards_cfg.get("anchor_one_per_side", {}) or {}
        if anchor_cfg.get("enabled", False) and anchor_cfg.get("lock_per_window", False):
            if ws.lock:
                # Already locked - must match anchor
                if side == "yes":
                    if ws.anchor_yes_ticker is not None and ws.anchor_yes_ticker != ticker:
                        _inc_skip(ws, "anchor_mismatch")
                        eval_payload["reason_code"] = "anchor_mismatch"
                        eval_payload["event"] = "SKIP"
                        eval_payload["guard_tune_knob"] = _guard_tune_knob(interval, is_hourly, "anchor_mismatch", asset)
                        eval_payload["guard_checks_summary"] = f"anchor_mismatch: FAIL (locked to {ws.anchor_yes_ticker})"
                        return False, "anchor_mismatch", eval_payload
                else:
                    if ws.anchor_no_ticker is not None and ws.anchor_no_ticker != ticker:
                        _inc_skip(ws, "anchor_mismatch")
                        eval_payload["reason_code"] = "anchor_mismatch"
                        eval_payload["event"] = "SKIP"
                        eval_payload["guard_tune_knob"] = _guard_tune_knob(interval, is_hourly, "anchor_mismatch", asset)
                        eval_payload["guard_checks_summary"] = f"anchor_mismatch: FAIL (locked to {ws.anchor_no_ticker})"
                        return False, "anchor_mismatch", eval_payload
            else:
                # First entry for this side - allow and set anchor
                pass

    eval_payload["reason_code"] = "REVERSAL_ENTRY_ALLOWED" if reversal_candidate else "ok"
    eval_payload["event"] = "ENTER_DECISION"
    if reversal_candidate:
        eval_payload["reversal_count"] = ts.reversals_taken
        eval_payload["stopped_side"] = ts.stopped_side
        eval_payload["type"] = "risk_guard_reversal"
    return True, eval_payload["reason_code"], eval_payload


def _inc_skip(ws: WindowState, reason: str) -> None:
    ws.skips_by_reason[reason] = ws.skips_by_reason.get(reason, 0) + 1


def set_window_lock(window_id: str, lock: bool) -> None:
    """Set lock flag for anchor_one_per_side (hourly)."""
    ws = _get_window_state(_window_states, window_id)
    ws.lock = lock


def get_window_summary(window_id: str) -> Optional[WindowState]:
    """Get window state for WINDOW_SUMMARY logging."""
    return _window_states.get(window_id)


def check_hard_flip_exit(
    side: str,
    spot: Optional[float],
    strike: Optional[float],
) -> bool:
    """
    True if we should exit immediately (hard flip).
    YES + spot < strike -> exit. NO + spot > strike -> exit.
    """
    if spot is None or strike is None or strike <= 0:
        return False
    s = str(side).lower()
    if s == "yes":
        return spot < strike
    if s == "no":
        return spot > strike
    return False


def apply_guards_filter(
    signals: List[SignalLike],
    interval: str,
    window_id: str,
    asset: str,
    spot: Optional[float],
    seconds_to_close: float,
    ticker_to_strike: Dict[str, float],
    guards_cfg: dict,
    is_hourly: bool,
    yes_price_all: Optional[int] = None,
    no_price_all: Optional[int] = None,
    entry_band: str = "94-99",
) -> Tuple[List[SignalLike], List[dict]]:
    """
    Filter signals through gate. Returns (allowed_signals, eval_logs).
    eval_logs contains one dict per signal (including skipped) for JSON logging.
    """
    if not guards_cfg.get("enabled", False):
        return signals, []

    allowed = []
    eval_logs = []
    for sig in signals:
        ticker = getattr(sig, "ticker", "")
        side = getattr(sig, "side", "")
        price = getattr(sig, "price", None)
        yes_price = price if side == "yes" else None
        no_price = price if side == "no" else None
        strike = ticker_to_strike.get(ticker) if ticker_to_strike else None

        allowed_flag, reason, payload = gate_allow_entry(
            interval=interval,
            window_id=window_id,
            asset=asset,
            ticker=ticker,
            side=side,
            yes_price=yes_price,
            no_price=no_price,
            spot=spot,
            strike=strike,
            seconds_to_close=seconds_to_close,
            entry_band=entry_band,
            guards_cfg=guards_cfg,
            is_hourly=is_hourly,
            yes_price_all=yes_price_all,
            no_price_all=no_price_all,
        )
        eval_logs.append(payload)
        if allowed_flag:
            allowed.append(sig)

    return allowed, eval_logs
