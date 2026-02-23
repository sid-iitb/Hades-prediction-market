"""
Analysis-grade structured logging schema for the bot.
Stable JSON event format for reconstructing runs from logs/bot.log.
"""
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Standard reason codes for analysis scripts
SKIP_NO_NEW_ENTRY_CUTOFF = "SKIP_NO_NEW_ENTRY_CUTOFF"
SKIP_DISABLED_UNTIL_EXPIRY = "SKIP_DISABLED_UNTIL_EXPIRY"
SKIP_PERSISTENCE = "SKIP_PERSISTENCE"
SKIP_DISTANCE_BUFFER = "SKIP_DISTANCE_BUFFER"
SKIP_RECENT_CROSS_EXTRA_CONFIRMATION = "SKIP_RECENT_CROSS_EXTRA_CONFIRMATION"
SKIP_ANCHOR_MISMATCH = "SKIP_ANCHOR_MISMATCH"
SKIP_CAP = "SKIP_CAP"
BLOCK_SAME_SIDE_AFTER_STOP = "BLOCK_SAME_SIDE_AFTER_STOP"
REVERSAL_ENTRY_ALLOWED = "REVERSAL_ENTRY_ALLOWED"
BLOCK_REVERSAL_WEAK_SIGNAL = "BLOCK_REVERSAL_WEAK_SIGNAL"
BLOCK_MAX_REVERSALS = "BLOCK_MAX_REVERSALS"
BLOCK_REVERSAL_CUTOFF = "BLOCK_REVERSAL_CUTOFF"
SKIP_ROLL_EXHAUSTED = "SKIP_ROLL_EXHAUSTED"
SKIP_ROLL_SAME_TICKER_BLOCKED = "SKIP_ROLL_SAME_TICKER_BLOCKED"
EXIT_STOPLOSS = "EXIT_STOPLOSS"
EXIT_HARD_FLIP = "EXIT_HARD_FLIP"
EXIT_TAKE_PROFIT = "EXIT_TAKE_PROFIT"
STOPLOSS_COUNTERFACTUAL = "STOPLOSS_COUNTERFACTUAL"

REASON_MAP = {
    "cutoff_seconds": SKIP_NO_NEW_ENTRY_CUTOFF,
    "BLOCK_REVERSAL_CUTOFF": BLOCK_REVERSAL_CUTOFF,
    "disabled_until_expiry": SKIP_DISABLED_UNTIL_EXPIRY,
    "persistence": SKIP_PERSISTENCE,
    "distance_buffer": SKIP_DISTANCE_BUFFER,
    "anchor_mismatch": SKIP_ANCHOR_MISMATCH,
    "BLOCK_SAME_SIDE_AFTER_STOP": BLOCK_SAME_SIDE_AFTER_STOP,
    "BLOCK_MAX_REVERSALS": BLOCK_MAX_REVERSALS,
    "BLOCK_REVERSAL_WEAK_SIGNAL": BLOCK_REVERSAL_WEAK_SIGNAL,
    "REVERSAL_ENTRY_ALLOWED": REVERSAL_ENTRY_ALLOWED,
    "roll_exhausted": SKIP_ROLL_EXHAUSTED,
    "roll_same_ticker_blocked": SKIP_ROLL_SAME_TICKER_BLOCKED,
}


def _norm_interval(interval: str) -> str:
    if interval == "15min":
        return "fifteen_min"
    if interval in ("daily", "weekly"):
        return interval
    return interval or "hourly"


def _parse_entry_band(s: Optional[str]) -> tuple:
    """Return (entry_band_min_cents, entry_band_max_cents)."""
    if not s or "-" not in str(s):
        return None, None
    try:
        parts = str(s).split("-")
        if len(parts) >= 2:
            return int(parts[0].strip()), int(parts[1].strip())
    except (ValueError, TypeError):
        pass
    return None, None


def build_analysis_event(
    payload: Dict[str, Any],
    cap_state: Optional[Dict[str, Any]] = None,
    exit_reason: Optional[str] = None,
    minutes_to_close: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Build a canonical analysis-grade JSON event from a guard eval payload or exit.
    Uses stable schema with null for unknown fields.
    """
    event = payload.get("event") or "EVAL"
    event_type = event if event in ("EVAL", "SKIP", "ENTER", "ENTER_DECISION", "EXIT", "WINDOW_SUMMARY",
                                    "ORDER_SUBMITTED", "ORDER_FAILED", "ORDER_SKIPPED") else "EVAL"
    interval = _norm_interval(payload.get("interval") or "")
    reason_code = payload.get("reason_code")
    if reason_code and reason_code in REASON_MAP:
        reason_code = REASON_MAP[reason_code]
    if exit_reason:
        reason_code = exit_reason

    band_lo, band_hi = _parse_entry_band(payload.get("entry_band"))

    out = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "interval": interval,
        "asset": payload.get("asset"),
        "window_id": payload.get("window_id"),
        "ticker": payload.get("ticker"),
        "side": payload.get("side"),
        "event_type": event_type,
        "seconds_to_close": payload.get("seconds_to_close"),
        "cutoff_seconds": payload.get("cutoff_seconds"),
        "minutes_to_close": minutes_to_close or (payload.get("seconds_to_close") / 60.0 if payload.get("seconds_to_close") is not None else None),
        "yes_price_cents": payload.get("yes_price"),
        "no_price_cents": payload.get("no_price"),
        "entry_band_min_cents": band_lo,
        "entry_band_max_cents": band_hi,
        "spot": payload.get("spot"),
        "strike": payload.get("strike"),
        "distance": payload.get("distance"),
        "min_distance_required": payload.get("min_distance_required"),
        "persistence_streak": payload.get("streak"),
        "persistence_required": payload.get("persistence_required_effective") or payload.get("required_polls"),
        "persistence_required_base": payload.get("persistence_required_base"),
        "persistence_required_effective": payload.get("persistence_required_effective"),
        "late_persistence_applied": payload.get("late_persistence_applied"),
        "recent_cross_flag": payload.get("recent_cross_flag"),
        "disabled_until_expiry": payload.get("disabled_until_expiry"),
        "stopouts_count": payload.get("stopouts_count"),
        "reversal_enabled": None,
        "stopped_side": payload.get("stopped_side"),
        "reversals_taken": payload.get("reversals_taken"),
        "anchor_yes_ticker": payload.get("anchor_yes"),
        "anchor_no_ticker": payload.get("anchor_no"),
        "anchor_locked": payload.get("anchor_locked"),
        "reason_code": reason_code,
        "cap_state": cap_state,
    }
    if payload.get("guard_tune_knob"):
        out["guard_tune_knob"] = payload["guard_tune_knob"]
    if payload.get("guard_checks_summary"):
        out["guard_checks_summary"] = payload["guard_checks_summary"]
    if payload.get("cooldown_skipped") is not None:
        out["cooldown_skipped"] = payload["cooldown_skipped"]
    for k in ("basket_entry_cost_cents", "yes_strike", "no_strike", "yes_entry_price", "no_entry_price",
              "yes_distance", "no_distance", "combined_spread_at_entry"):
        if payload.get(k) is not None:
            out[k] = payload[k]
    return out


def build_exit_analysis_event(
    interval: str,
    window_id: str,
    asset: str,
    action: str,
    ticker: Optional[str] = None,
    side: Optional[str] = None,
    pnl_pct: Optional[float] = None,
    seconds_to_close: Optional[float] = None,
    yes_price_cents: Optional[int] = None,
    no_price_cents: Optional[int] = None,
    spot: Optional[float] = None,
    strike: Optional[float] = None,
    distance: Optional[float] = None,
    min_distance_required: Optional[float] = None,
    entry_price_cents: Optional[int] = None,
    exit_price_cents: Optional[int] = None,
    dist_margin: Optional[float] = None,
    mfe_pct: Optional[float] = None,
    mae_pct: Optional[float] = None,
    mfe_ts: Optional[str] = None,
    mae_ts: Optional[str] = None,
    top_of_book_yes_bid: Optional[bool] = None,
    top_of_book_yes_ask: Optional[bool] = None,
    top_of_book_no_bid: Optional[bool] = None,
    top_of_book_no_ask: Optional[bool] = None,
    yes_bid_cents: Optional[int] = None,
    yes_ask_cents: Optional[int] = None,
    no_bid_cents: Optional[int] = None,
    no_ask_cents: Optional[int] = None,
    target_sl_price_cents: Optional[int] = None,
    tob_spread_cents: Optional[int] = None,
    guardrails_persistence_required: Optional[int] = None,
    guardrails_cutoff_seconds: Optional[float] = None,
    exit_eval: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build canonical EXIT event with stop-loss context when available."""
    if action == "STOP_LOSS":
        reason_code = EXIT_STOPLOSS
    elif action == "HARD_FLIP":
        reason_code = EXIT_HARD_FLIP
    elif action == "TAKE_PROFIT":
        reason_code = EXIT_TAKE_PROFIT
    else:
        reason_code = f"EXIT_{action}"
    out = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "interval": _norm_interval(interval),
        "asset": asset,
        "window_id": window_id,
        "ticker": ticker,
        "side": side,
        "event_type": "EXIT",
        "seconds_to_close": seconds_to_close,
        "minutes_to_close": seconds_to_close / 60.0 if seconds_to_close is not None else None,
        "yes_price_cents": yes_price_cents,
        "no_price_cents": no_price_cents,
        "entry_band_min_cents": None,
        "entry_band_max_cents": None,
        "spot": spot,
        "strike": strike,
        "distance": distance,
        "min_distance_required": min_distance_required,
        "persistence_streak": None,
        "persistence_required": None,
        "recent_cross_flag": None,
        "disabled_until_expiry": None,
        "stopouts_count": None,
        "reversal_enabled": None,
        "stopped_side": None,
        "reversals_taken": None,
        "anchor_yes_ticker": None,
        "anchor_no_ticker": None,
        "anchor_locked": None,
        "reason_code": reason_code,
        "cap_state": None,
        "pnl_pct": pnl_pct,
        "entry_price_cents": entry_price_cents,
    }
    if exit_price_cents is not None:
        out["exit_price_cents"] = exit_price_cents
    if dist_margin is not None:
        out["dist_margin"] = dist_margin
    if mfe_pct is not None:
        out["mfe_pct"] = mfe_pct
    if mae_pct is not None:
        out["mae_pct"] = mae_pct
    if mfe_ts is not None:
        out["mfe_ts"] = mfe_ts
    if mae_ts is not None:
        out["mae_ts"] = mae_ts
    if top_of_book_yes_bid is not None:
        out["top_of_book_yes_bid"] = top_of_book_yes_bid
    if top_of_book_yes_ask is not None:
        out["top_of_book_yes_ask"] = top_of_book_yes_ask
    if top_of_book_no_bid is not None:
        out["top_of_book_no_bid"] = top_of_book_no_bid
    if top_of_book_no_ask is not None:
        out["top_of_book_no_ask"] = top_of_book_no_ask
    if yes_bid_cents is not None:
        out["yes_bid_cents"] = yes_bid_cents
    if yes_ask_cents is not None:
        out["yes_ask_cents"] = yes_ask_cents
    if no_bid_cents is not None:
        out["no_bid_cents"] = no_bid_cents
    if no_ask_cents is not None:
        out["no_ask_cents"] = no_ask_cents
    if target_sl_price_cents is not None:
        out["target_sl_price_cents"] = target_sl_price_cents
    if tob_spread_cents is not None:
        out["tob_spread_cents"] = tob_spread_cents
    # Slippage: target_sl_price - actual_exit_price (positive = got less than target)
    if target_sl_price_cents is not None and exit_price_cents is not None:
        out["slippage_cents"] = target_sl_price_cents - exit_price_cents
    if guardrails_persistence_required is not None:
        out["guardrails_persistence_required"] = guardrails_persistence_required
    if guardrails_cutoff_seconds is not None:
        out["guardrails_cutoff_seconds"] = guardrails_cutoff_seconds
    if exit_eval:
        for k in ("mfe_pct", "mae_pct", "mfe_ts", "mae_ts", "top_of_book_yes_bid", "top_of_book_yes_ask",
                  "top_of_book_no_bid", "top_of_book_no_ask", "yes_bid_cents", "yes_ask_cents",
                  "no_bid_cents", "no_ask_cents", "target_sl_price_cents", "tob_spread_cents",
                  "basket_exit_value_cents", "net_basket_pnl_cents", "combined_spread_at_exit"):
            if k in exit_eval and out.get(k) is None:
                out[k] = exit_eval[k]
        if "slippage_cents" not in out and exit_eval.get("target_sl_price_cents") is not None and exit_price_cents is not None:
            out["slippage_cents"] = exit_eval["target_sl_price_cents"] - exit_price_cents
    return out


def build_stoploss_counterfactual_event(
    interval: str,
    window_id: str,
    asset: str,
    stopped_ticker: str,
    stopped_side: str,
    stopped_entry_price_cents: int,
    pnl_pct: float,
    candidates: list,
) -> Dict[str, Any]:
    """Build STOPLOSS_COUNTERFACTUAL event with candidate evaluations."""
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "interval": _norm_interval(interval),
        "asset": asset,
        "window_id": window_id,
        "event_type": "STOPLOSS_COUNTERFACTUAL",
        "reason_code": STOPLOSS_COUNTERFACTUAL,
        "stopped_ticker": stopped_ticker,
        "stopped_side": stopped_side,
        "stopped_entry_price_cents": stopped_entry_price_cents,
        "pnl_pct": pnl_pct,
        "candidates": candidates,
    }


def build_order_event(
    event_type: str,  # ORDER_SUBMITTED | ORDER_FAILED | ORDER_SKIPPED
    interval: str,
    window_id: str,
    asset: str,
    ticker: str,
    side: str,
    client_order_id: str,
    order_id: Optional[str] = None,
    price_cents: Optional[int] = None,
    contracts: Optional[int] = None,
    reason: Optional[str] = None,
    top_of_book_yes_bid: Optional[bool] = None,
    top_of_book_yes_ask: Optional[bool] = None,
    top_of_book_no_bid: Optional[bool] = None,
    top_of_book_no_ask: Optional[bool] = None,
    yes_bid_cents: Optional[int] = None,
    yes_ask_cents: Optional[int] = None,
    no_bid_cents: Optional[int] = None,
    no_ask_cents: Optional[int] = None,
    entry_band: Optional[str] = None,
    dist_margin: Optional[float] = None,
    persist_margin: Optional[float] = None,
    guard_eval: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build canonical ORDER_* event for reconciliation and execution quality diagnostics."""
    out = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "interval": _norm_interval(interval),
        "asset": asset,
        "window_id": window_id,
        "ticker": ticker,
        "side": side,
        "event_type": event_type,
        "client_order_id": client_order_id,
        "order_id": order_id,
        "price_cents": price_cents,
        "contracts": contracts,
        "reason_code": reason,
    }
    if event_type == "ORDER_FAILED":
        out["error_reason"] = reason
    elif event_type == "ORDER_SKIPPED":
        out["skipped_reason"] = reason
    if guard_eval:
        if entry_band is None:
            entry_band = guard_eval.get("entry_band")
        if dist_margin is None and guard_eval.get("distance") is not None and guard_eval.get("min_distance_required") is not None:
            dist_margin = float(guard_eval["distance"]) - float(guard_eval["min_distance_required"])
        if persist_margin is None and guard_eval.get("streak") is not None and guard_eval.get("required_polls") is not None:
            persist_margin = float(guard_eval["streak"]) - float(guard_eval["required_polls"])
        # Distance at this exact moment (per-fill, not retroactive) for FILLS array
        if guard_eval.get("distance") is not None:
            out["distance"] = guard_eval["distance"]
    if top_of_book_yes_bid is not None:
        out["top_of_book_yes_bid"] = top_of_book_yes_bid
    if top_of_book_yes_ask is not None:
        out["top_of_book_yes_ask"] = top_of_book_yes_ask
    if top_of_book_no_bid is not None:
        out["top_of_book_no_bid"] = top_of_book_no_bid
    if top_of_book_no_ask is not None:
        out["top_of_book_no_ask"] = top_of_book_no_ask
    if yes_bid_cents is not None:
        out["tob_yes_bid_cents"] = yes_bid_cents
    if yes_ask_cents is not None:
        out["tob_yes_ask_cents"] = yes_ask_cents
    if no_bid_cents is not None:
        out["tob_no_bid_cents"] = no_bid_cents
    if no_ask_cents is not None:
        out["tob_no_ask_cents"] = no_ask_cents
    if entry_band is not None:
        out["entry_band"] = entry_band
    if dist_margin is not None:
        out["dist_margin"] = dist_margin
    if persist_margin is not None:
        out["persist_margin"] = persist_margin
    return out


def build_window_summary_event(
    interval: str,
    window_id: str,
    asset: Optional[str],
    total_evals: int,
    total_entries: int,
    total_exits: int,
    skips_by_reason: Dict[str, int],
    exits_by_reason: Dict[str, int],
    stopouts_count_total: int,
    reversals_taken_total: Optional[int],
    anchor_yes_ticker: Optional[str],
    anchor_no_ticker: Optional[str],
    realized_pnl: Optional[float] = None,
) -> Dict[str, Any]:
    """Build canonical WINDOW_SUMMARY event."""
    total_skips = sum(skips_by_reason.values()) if skips_by_reason else 0
    norm = _norm_interval(interval)
    anchors_used = (anchor_yes_ticker is not None or anchor_no_ticker is not None) if norm == "hourly" else None
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "interval": _norm_interval(interval),
        "asset": asset,
        "window_id": window_id,
        "ticker": None,
        "side": None,
        "event_type": "WINDOW_SUMMARY",
        "seconds_to_close": None,
        "minutes_to_close": None,
        "yes_price_cents": None,
        "no_price_cents": None,
        "entry_band_min_cents": None,
        "entry_band_max_cents": None,
        "spot": None,
        "strike": None,
        "distance": None,
        "min_distance_required": None,
        "persistence_streak": None,
        "persistence_required": None,
        "recent_cross_flag": None,
        "disabled_until_expiry": None,
        "stopouts_count": None,
        "reversal_enabled": None,
        "stopped_side": None,
        "reversals_taken": None,
        "anchor_yes_ticker": anchor_yes_ticker,
        "anchor_no_ticker": anchor_no_ticker,
        "anchor_locked": None,
        "reason_code": None,
        "cap_state": None,
        "total_evals": total_evals,
        "total_enters": total_entries,
        "total_exits": total_exits,
        "total_skips": total_skips,
        "skips_by_reason": skips_by_reason,
        "exits_by_reason": exits_by_reason,
        "stopouts_count_total": stopouts_count_total,
        "reversals_taken_total": reversals_taken_total,
        "anchors_used": anchors_used,
        "realized_pnl": realized_pnl,
    }
