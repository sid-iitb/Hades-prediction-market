"""
Strategy Tuning Ledger: per-trade-attempt rows and per-interval+asset summary for threshold tuning.
Uses ENTER_DECISION as the definition of a trade attempt; reconciles with ORDER_* and EXIT events.
Output: reports/hourly/<YYYYMMDD>/<HH>/strategy_ledger_<window>.tsv, strategy_summary_<window>.tsv
"""
from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

LA_TZ = pytz.timezone("America/Los_Angeles")

ERR_TOP_OF_BOOK_MISSING = "ERR_TOP_OF_BOOK_MISSING"
ERR_EXIT_NO_LIQUIDITY = "ERR_EXIT_NO_LIQUIDITY"
EXIT_STOPLOSS = "EXIT_STOPLOSS"
EXIT_TAKE_PROFIT = "EXIT_TAKE_PROFIT"
EXIT_HARD_FLIP = "EXIT_HARD_FLIP"
HELD_TO_EXPIRY = "HELD_TO_EXPIRY"
CAP_REASON = "SKIPPED_CAP_REACHED"

# Concise columns for email / tuning (--format concise). Decision-useful minimal set.
CONCISE_LEDGER_COLS = [
    "ts_first_seen",
    "interval",
    "asset",
    "window_id",
    "ticker",
    "side",
    "exec_status",
    "exec_reason",
    "entry_price_cents",
    "entry_cost_cents",
    "contracts",
    "exit_price_cents",
    "pnl_pct",
    "seconds_to_close_at_entry",
    "cutoff_seconds",
    "strike",
    "spot_at_entry",
    "distance",
    "min_distance_required",
    "dist_margin",
    "persistence_streak",
    "persistence_required",
    "persist_margin",
    "top_of_book_ok",
    "tob_missing_side",
    "mfe_pct",
    "mae_pct",
    "exit_reason",
    "seconds_held",
    "entry_fill_count",
    "tob_spread_at_entry",
    "tob_spread_at_exit",
    "mfe_cents",
    "mae_cents",
    "target_sl_price",
    "actual_exit_price",
    "slippage_cents",
    "guard_tune_knob",
    "guard_checks_summary",
    "resolved_as",
    "basket_entry_cost_cents",
    "yes_strike",
    "no_strike",
    "yes_entry_price",
    "no_entry_price",
    "yes_distance",
    "no_distance",
    "combined_spread_at_entry",
    "basket_exit_value_cents",
    "net_basket_pnl_cents",
    "combined_spread_at_exit",
    "cooldown_skipped",
]

STRATEGY_LEDGER_COLS = [
    "ts_first_seen",
    "ts_last_decision",
    "count_decisions",
    "interval",
    "asset",
    "window_id",
    "ticker",
    "side",
    "entry_price_cents",
    "yes_price_cents",
    "no_price_cents",
    "seconds_to_close",
    "cutoff_seconds",
    "cutoff_status",
    "persistence_streak",
    "persistence_required",
    "persist_margin",
    "persist_status",
    "strike",
    "spot",
    "distance",
    "min_distance_required",
    "dist_margin",
    "dist_status",
    "entry_reason_compact",
    "exec_status",
    "exec_reason",
    "exit_reason",
    "pnl_pct",
    "stoploss_context",
    "seconds_held",
    "entry_fill_count",
    "tob_spread_at_entry",
    "tob_spread_at_exit",
    "mfe_cents",
    "mae_cents",
    "target_sl_price",
    "actual_exit_price",
    "slippage_cents",
    "guard_tune_knob",
    "guard_checks_summary",
    "resolved_as",
    "basket_entry_cost_cents",
    "yes_strike",
    "no_strike",
    "yes_entry_price",
    "no_entry_price",
    "combined_spread_at_entry",
    "basket_exit_value_cents",
    "net_basket_pnl_cents",
    "combined_spread_at_exit",
    "cooldown_skipped",
]

STRATEGY_SUMMARY_COLS = [
    "hour_start_ts",
    "hour_end_ts",
    "interval",
    "asset",
    "eval_count",
    "trade_attempts",
    "submitted_count",
    "failed_count",
    "skipped_count",
    "exited_count",
    "stoploss_count",
    "top_3_skip_reasons",
    "top_1_error",
    "dist_fail_rate",
    "persist_fail_rate",
    "cutoff_block_rate",
    "cap_block_rate",
    "avg_dist_margin",
    "p10_dist_margin",
    "p50_dist_margin",
    "p90_dist_margin",
    "avg_entry_price_cents",
]


def _parse_ts(ts: str) -> float:
    if not ts:
        return 0.0
    try:
        s = str(ts)[:26].replace("Z", "+00:00")
        if "T" in s and "+" not in s and "-" not in s[10:]:
            s = s + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def _hour_window_pt(report_hour_la: datetime) -> Tuple[float, float]:
    """Return (start_utc_ts, end_utc_ts) for [report_hour-1, report_hour) in LA."""
    start_la = report_hour_la - timedelta(hours=1)
    end_la = report_hour_la
    start_la = LA_TZ.normalize(start_la) if start_la.tzinfo else LA_TZ.localize(start_la)
    end_la = LA_TZ.normalize(end_la) if end_la.tzinfo else LA_TZ.localize(end_la)
    return start_la.timestamp(), end_la.timestamp()


def _filter_events_in_window(events: List[Dict], start_utc: float, end_utc: float) -> List[Dict]:
    return [e for e in events if start_utc <= _parse_ts(e.get("ts") or "") < end_utc]


def _trade_lifecycles_enter_decision_only(events: List[Dict]) -> Dict[Tuple, List[Dict]]:
    """Group by (interval, asset, window_id, ticker, side). Include:
    - ENTER_DECISION: trade attempts that passed guards (submitted/failed/skipped/exited)
    - SKIP: guard-skipped signals (blocked by persistence, distance, cutoff, etc.) for tuning analysis.
    """
    from tools.analyze_bot_log import _build_trade_lifecycles

    lifecycles = _build_trade_lifecycles(events)
    out = {}
    for key, evs in lifecycles.items():
        has_enter = any(e.get("event_type") == "ENTER_DECISION" for e in evs)
        has_skip = any(e.get("event_type") == "SKIP" for e in evs)
        if has_enter or has_skip:
            out[key] = evs
    return out


def _entry_reason_compact(ev: dict, interval: str) -> str:
    """Short token string: IN_BAND+PERSIST_[PASS/FAIL]+DIST_[PASS/FAIL/NA]+CUTOFF_[PASS/BLOCK]+CAP_[OK/BLOCK]+ANCHOR_[OK/BLOCK]."""
    tokens = []
    lo = ev.get("entry_band_min_cents")
    hi = ev.get("entry_band_max_cents")
    yes_p = ev.get("yes_price_cents")
    no_p = ev.get("no_price_cents")
    side = str(ev.get("side") or "").lower()
    side_price = yes_p if side == "yes" else no_p
    tokens.append("IN_BAND" if (lo is not None and hi is not None and side_price is not None and lo <= side_price <= hi) else "BAND_NA")

    streak = ev.get("persistence_streak") or ev.get("streak")
    required = ev.get("persistence_required_effective") or ev.get("persistence_required") or ev.get("required_polls")
    if streak is not None and required is not None:
        tokens.append("PERSIST_PASS" if streak >= required else "PERSIST_FAIL")
    else:
        tokens.append("PERSIST_NA")

    dist = ev.get("distance")
    min_dist = ev.get("min_distance_required")
    if dist is not None and min_dist is not None:
        tokens.append("DIST_PASS" if dist >= min_dist else "DIST_FAIL")
    else:
        tokens.append("DIST_NA")

    sec = ev.get("seconds_to_close")
    cutoff_secs = ev.get("cutoff_seconds") or (40 if interval == "fifteen_min" else 90)
    rc = ev.get("reason_code") or ""
    if rc in ("SKIP_NO_NEW_ENTRY_CUTOFF", "cutoff_seconds") or (sec is not None and sec <= cutoff_secs and "REVERSAL" not in rc):
        tokens.append("CUTOFF_BLOCK")
    else:
        tokens.append("CUTOFF_PASS")

    cap = ev.get("cap_state") or {}
    ct = cap.get("current_total_orders")
    mt = cap.get("max_total_orders_per_window")
    if ct is not None and mt is not None:
        tokens.append("CAP_OK" if ct < mt else "CAP_BLOCK")
    else:
        tokens.append("CAP_NA")

    if interval == "hourly":
        anchor_ok = not ev.get("anchor_locked")
        tokens.append("ANCHOR_OK" if anchor_ok else "ANCHOR_BLOCK")

    return "+".join(tokens)


def _stoploss_context(ev: dict) -> str:
    """If EXIT_STOPLOSS: 'pnl=-XX% ttc=Y' plus distance/strike/spot if known."""
    parts = []
    pnl = ev.get("pnl_pct")
    if pnl is not None:
        parts.append("pnl=%.2f%%" % (pnl * 100))
    sec = ev.get("seconds_to_close")
    if sec is not None:
        parts.append("ttc=%.1f" % (sec / 60.0))
    dist = ev.get("distance")
    if dist is not None:
        parts.append("dist=%s" % dist)
    strike = ev.get("strike")
    if strike is not None:
        parts.append("strike=%s" % strike)
    spot = ev.get("spot")
    if spot is not None:
        parts.append("spot=%s" % spot)
    return " ".join(parts) if parts else ""


def _normalize_fail_reason(ev: dict, side: str) -> str:
    msg = (ev.get("error_reason") or ev.get("reason") or ev.get("message") or str(ev.get("error", ""))).lower()
    if "top-of-book" in msg or "top of book" in msg or "orderbook" in msg or "no top" in msg:
        return f"{ERR_TOP_OF_BOOK_MISSING}({side.upper()})"
    return ev.get("reason_code") or ev.get("error_reason") or ev.get("reason") or "ERR_UNKNOWN"


def _tob_flags_from_events(events: List[Dict], side: str) -> Tuple[Optional[bool], str]:
    """Derive top_of_book_ok and tob_missing_side from ORDER_* events."""
    yes_ok = False
    no_ok = False
    for ev in events:
        if ev.get("event_type") in ("ORDER_FAILED", "ORDER_SKIPPED", "ORDER_SUBMITTED"):
            yes_ok = yes_ok or ev.get("top_of_book_yes_bid") or ev.get("top_of_book_yes_ask")
            no_ok = no_ok or ev.get("top_of_book_no_bid") or ev.get("top_of_book_no_ask")
    if yes_ok and no_ok:
        return True, ""
    if not yes_ok and not no_ok:
        return False, "BOTH"
    if not yes_ok:
        return False, "YES"
    return False, "NO"


def _lifecycle_to_concise_ledger_row(key: Tuple, events: List[Dict]) -> Dict[str, Any]:
    """Convert lifecycle to concise trade row for tuning."""
    row = _lifecycle_to_strategy_ledger_row(key, events)
    exec_status = row.get("exec_status", "SKIPPED")
    if exec_status == "OPEN":
        exec_status = "SUBMITTED"
    entry_price = row.get("entry_price_cents")
    contracts = 1
    for ev in events:
        if ev.get("event_type") == "ORDER_SUBMITTED":
            contracts = ev.get("contracts") or 1
            break
    entry_cost = (float(entry_price) * int(contracts)) if entry_price is not None else None
    exit_price = None
    mfe_pct = None
    mae_pct = None
    for ev in events:
        if ev.get("event_type") == "EXIT":
            exit_price = ev.get("exit_price_cents") or ev.get("yes_price_cents") or ev.get("no_price_cents")
            mfe_pct = ev.get("mfe_pct")
            mae_pct = ev.get("mae_pct")
            break
    top_of_book_ok, tob_missing_side = _tob_flags_from_events(events, str(key[-1]))
    return {
        "ts_first_seen": row.get("ts_first_seen", ""),
        "interval": row.get("interval", ""),
        "asset": row.get("asset", ""),
        "window_id": row.get("window_id", ""),
        "ticker": row.get("ticker", ""),
        "side": row.get("side", ""),
        "exec_status": exec_status,
        "exec_reason": row.get("exec_reason", ""),
        "entry_price_cents": entry_price,
        "entry_cost_cents": entry_cost,
        "contracts": contracts,
        "exit_price_cents": exit_price,
        "pnl_pct": row.get("pnl_pct"),
        "seconds_to_close_at_entry": row.get("seconds_to_close"),
        "cutoff_seconds": row.get("cutoff_seconds"),
        "strike": row.get("strike"),
        "spot_at_entry": row.get("spot"),
        "distance": row.get("distance"),
        "min_distance_required": row.get("min_distance_required"),
        "dist_margin": row.get("dist_margin"),
        "persistence_streak": row.get("persistence_streak"),
        "persistence_required": row.get("persistence_required"),
        "persist_margin": row.get("persist_margin"),
        "top_of_book_ok": top_of_book_ok,
        "tob_missing_side": tob_missing_side or "",
        "mfe_pct": mfe_pct,
        "mae_pct": mae_pct,
        "exit_reason": row.get("exit_reason") or "",
        "seconds_held": row.get("seconds_held"),
        "entry_fill_count": row.get("entry_fill_count"),
        "tob_spread_at_entry": row.get("tob_spread_at_entry"),
        "tob_spread_at_exit": row.get("tob_spread_at_exit"),
        "mfe_cents": row.get("mfe_cents"),
        "mae_cents": row.get("mae_cents"),
        "target_sl_price": row.get("target_sl_price"),
        "actual_exit_price": exit_price or row.get("actual_exit_price"),
        "slippage_cents": row.get("slippage_cents"),
        "guard_tune_knob": row.get("guard_tune_knob", ""),
        "guard_checks_summary": row.get("guard_checks_summary", ""),
        "resolved_as": row.get("resolved_as", ""),
        "basket_entry_cost_cents": row.get("basket_entry_cost_cents", ""),
        "yes_strike": row.get("yes_strike", ""),
        "no_strike": row.get("no_strike", ""),
        "yes_entry_price": row.get("yes_entry_price", ""),
        "no_entry_price": row.get("no_entry_price", ""),
        "yes_distance": row.get("yes_distance", ""),
        "no_distance": row.get("no_distance", ""),
        "combined_spread_at_entry": row.get("combined_spread_at_entry", ""),
        "basket_exit_value_cents": row.get("basket_exit_value_cents", ""),
        "net_basket_pnl_cents": row.get("net_basket_pnl_cents", ""),
        "combined_spread_at_exit": row.get("combined_spread_at_exit", ""),
        "cooldown_skipped": row.get("cooldown_skipped", ""),
    }


def _lifecycle_to_strategy_ledger_row(key: Tuple, events: List[Dict]) -> Dict[str, Any]:
    interval, asset, window_id, ticker, side = key
    side_upper = str(side).upper()

    enter_evs = [e for e in events if e.get("event_type") == "ENTER_DECISION"]
    if not enter_evs:
        enter_evs = [e for e in events if e.get("event_type") in ("ENTER", "SKIP")]
    entry_ev = enter_evs[-1] if enter_evs else None

    ts_first = ""
    ts_last_decision = ""
    count_decisions = len([e for e in events if e.get("event_type") == "ENTER_DECISION"])
    for ev in events:
        t = (ev.get("ts") or "")[:26]
        if not ts_first:
            ts_first = t
        if ev.get("event_type") == "ENTER_DECISION":
            ts_last_decision = t

    if not ts_last_decision and entry_ev:
        ts_last_decision = (entry_ev.get("ts") or "")[:26]

    # Execution status
    has_exit = any(e.get("event_type") == "EXIT" for e in events)
    has_submit = any(e.get("event_type") == "ORDER_SUBMITTED" and (e.get("order_id") or e.get("client_order_id")) for e in events)
    has_fail = any(e.get("event_type") == "ORDER_FAILED" for e in events)
    has_skip = any(e.get("event_type") == "ORDER_SKIPPED" for e in events)

    exec_status = "SKIPPED"
    exec_reason = "SKIPPED_GUARD"
    exit_reason = ""
    if has_exit:
        exec_status = "EXITED"
        for e in events:
            if e.get("event_type") == "EXIT":
                exec_reason = e.get("reason_code") or "EXIT"
                rc = (e.get("reason_code") or "").upper()
                if "STOPLOSS" in rc or rc == EXIT_STOPLOSS:
                    exit_reason = EXIT_STOPLOSS
                elif "TAKE_PROFIT" in rc or rc == EXIT_TAKE_PROFIT:
                    exit_reason = EXIT_TAKE_PROFIT
                elif "HARD_FLIP" in rc or rc == EXIT_HARD_FLIP:
                    exit_reason = EXIT_HARD_FLIP
                else:
                    exit_reason = exec_reason
                break
    elif has_submit:
        exec_status = "OPEN"
        exec_reason = ""
        exit_reason = HELD_TO_EXPIRY  # Position still open
    elif has_fail:
        exec_status = "FAILED"
        for e in events:
            if e.get("event_type") == "ORDER_FAILED":
                exec_reason = _normalize_fail_reason(e, side)
                break
    elif has_skip:
        exec_status = "SKIPPED"
        for e in events:
            if e.get("event_type") == "ORDER_SKIPPED":
                exec_reason = e.get("reason_code") or e.get("skipped_reason") or CAP_REASON
                break
    else:
        # Guard-skipped: use reason from SKIP event (persistence, distance_buffer, cutoff_seconds, etc.)
        if entry_ev and entry_ev.get("event_type") == "SKIP":
            exec_reason = entry_ev.get("reason_code") or "SKIPPED_GUARD"
        else:
            exec_reason = "SKIPPED_UNKNOWN"

    entry_price_cents = None
    yes_price_cents = None
    no_price_cents = None
    seconds_to_close = None
    cutoff_seconds = None
    cutoff_status = "PASS"
    persistence_streak = None
    persistence_required = None
    persist_margin = ""
    persist_status = "NA"
    strike = ""
    spot = ""
    distance = None
    min_distance_required = None
    dist_margin = ""
    dist_status = "NA"
    entry_reason_compact = ""
    pnl_pct = None
    stoploss_context = ""

    if entry_ev:
        entry_price_cents = entry_ev.get("yes_price_cents") if side == "yes" else entry_ev.get("no_price_cents")
        if entry_price_cents is None:
            entry_price_cents = entry_ev.get("price_cents")
        yes_price_cents = entry_ev.get("yes_price_cents")
        no_price_cents = entry_ev.get("no_price_cents")
        for e in events:
            if e.get("event_type") == "ORDER_SUBMITTED":
                entry_price_cents = entry_price_cents or e.get("price_cents") or (e.get("yes_price_cents") if side == "yes" else e.get("no_price_cents"))
                break
        seconds_to_close = entry_ev.get("seconds_to_close")
        cutoff_seconds = entry_ev.get("cutoff_seconds") or (40 if interval == "fifteen_min" else 90)
        rc = entry_ev.get("reason_code") or ""
        if rc in ("SKIP_NO_NEW_ENTRY_CUTOFF", "cutoff_seconds") or (seconds_to_close is not None and cutoff_seconds is not None and seconds_to_close <= cutoff_seconds):
            cutoff_status = "BLOCK"
        persistence_streak = entry_ev.get("persistence_streak") or entry_ev.get("streak")
        persistence_required = entry_ev.get("persistence_required_effective") or entry_ev.get("persistence_required") or entry_ev.get("required_polls")
        if persistence_streak is not None and persistence_required is not None:
            persist_margin = persistence_streak - persistence_required
            persist_status = "PASS" if persistence_streak >= persistence_required else "FAIL"
        strike = entry_ev.get("strike") if entry_ev.get("strike") is not None else ""
        spot = entry_ev.get("spot") if entry_ev.get("spot") is not None else ""
        distance = entry_ev.get("distance")
        min_distance_required = entry_ev.get("min_distance_required")
        if distance is not None and min_distance_required is not None:
            dist_margin = distance - min_distance_required
            dist_status = "PASS" if distance >= min_distance_required else "FAIL"
        entry_reason_compact = _entry_reason_compact(entry_ev, interval)
        if entry_ev.get("basket_entry_cost_cents") is not None:
            basket_entry_cost_cents = entry_ev.get("basket_entry_cost_cents")
        if entry_ev.get("yes_strike") is not None:
            yes_strike = entry_ev.get("yes_strike")
        if entry_ev.get("no_strike") is not None:
            no_strike = entry_ev.get("no_strike")
        if entry_ev.get("yes_entry_price") is not None:
            yes_entry_price = entry_ev.get("yes_entry_price")
        if entry_ev.get("no_entry_price") is not None:
            no_entry_price = entry_ev.get("no_entry_price")
        if entry_ev.get("yes_distance") is not None:
            yes_distance = entry_ev.get("yes_distance")
        if entry_ev.get("no_distance") is not None:
            no_distance = entry_ev.get("no_distance")
        if entry_ev.get("combined_spread_at_entry") is not None:
            combined_spread_at_entry = entry_ev.get("combined_spread_at_entry")

    guard_tune_knob = None
    guard_checks_summary = None
    cooldown_skipped = None
    for ev in events:
        if ev.get("event_type") == "SKIP":
            if ev.get("guard_tune_knob") or ev.get("guard_checks_summary"):
                guard_tune_knob = guard_tune_knob or ev.get("guard_tune_knob")
                guard_checks_summary = guard_checks_summary or ev.get("guard_checks_summary")
                if guard_tune_knob and guard_checks_summary:
                    break
            if ev.get("cooldown_skipped") is not None:
                cooldown_skipped = 1 if ev.get("cooldown_skipped") else 0

    mfe_pct = None
    mae_pct = None
    mfe_cents = None
    mae_cents = None
    target_sl_price = None
    actual_exit_price = None
    slippage_cents = None
    tob_spread_at_entry = None
    tob_spread_at_exit = None
    basket_entry_cost_cents = None
    yes_strike = None
    no_strike = None
    yes_entry_price = None
    no_entry_price = None
    yes_distance = None
    no_distance = None
    combined_spread_at_entry = None
    basket_exit_value_cents = None
    net_basket_pnl_cents = None
    combined_spread_at_exit = None
    seconds_held = None
    entry_fill_count = 0
    ts_first_entry = ""
    ts_exit_ts = ""
    for ev in events:
        if ev.get("event_type") == "ORDER_SUBMITTED":
            if not ts_first_entry:
                ts_first_entry = (ev.get("ts") or "")[:26]
            entry_fill_count += 1
    for e in events:
        if e.get("event_type") == "EXIT":
            pnl_pct = e.get("pnl_pct")
            mfe_pct = e.get("mfe_pct")
            mae_pct = e.get("mae_pct")
            target_sl_price = e.get("target_sl_price_cents")
            actual_exit_price = e.get("exit_price_cents")
            slippage_cents = e.get("slippage_cents")
            tob_spread_at_exit = e.get("tob_spread_cents")
            if e.get("basket_exit_value_cents") is not None:
                basket_exit_value_cents = e.get("basket_exit_value_cents")
            if e.get("net_basket_pnl_cents") is not None:
                net_basket_pnl_cents = e.get("net_basket_pnl_cents")
            if e.get("combined_spread_at_exit") is not None:
                combined_spread_at_exit = e.get("combined_spread_at_exit")
            ts_exit_ts = (e.get("ts") or "")[:26]
            ep = e.get("entry_price_cents") or entry_price_cents
            if ep is not None and mfe_pct is not None:
                mfe_cents = round(float(ep) * float(mfe_pct))
            if ep is not None and mae_pct is not None:
                mae_cents = round(float(ep) * float(mae_pct))
            if ts_first_entry and ts_exit_ts:
                t1 = _parse_ts(ts_first_entry)
                t2 = _parse_ts(ts_exit_ts)
                if t1 > 0 and t2 > 0:
                    seconds_held = int(round(t2 - t1))
            if (e.get("reason_code") or "").startswith("EXIT_STOPLOSS") or "STOPLOSS" in (e.get("reason_code") or ""):
                stoploss_context = _stoploss_context(e)
            break

    def _fmt(v: Any) -> Any:
        if v is None or v == "":
            return ""
        if isinstance(v, float) and v != v:  # NaN
            return ""
        return v

    return {
        "ts_first_seen": _fmt(ts_first),
        "ts_last_decision": _fmt(ts_last_decision),
        "count_decisions": count_decisions,
        "interval": interval,
        "asset": asset,
        "window_id": window_id or "",
        "ticker": ticker or "",
        "side": side_upper,
        "entry_price_cents": _fmt(entry_price_cents),
        "yes_price_cents": _fmt(yes_price_cents),
        "no_price_cents": _fmt(no_price_cents),
        "seconds_to_close": _fmt(seconds_to_close),
        "cutoff_seconds": _fmt(cutoff_seconds),
        "cutoff_status": cutoff_status,
        "persistence_streak": _fmt(persistence_streak),
        "persistence_required": _fmt(persistence_required),
        "persist_margin": _fmt(persist_margin) if persist_margin != "" else "",
        "persist_status": persist_status,
        "strike": _fmt(strike),
        "spot": _fmt(spot),
        "distance": _fmt(distance),
        "min_distance_required": _fmt(min_distance_required),
        "dist_margin": _fmt(dist_margin) if dist_margin != "" else "",
        "dist_status": dist_status,
        "entry_reason_compact": entry_reason_compact,
        "exec_status": exec_status,
        "exec_reason": exec_reason,
        "exit_reason": exit_reason,
        "pnl_pct": _fmt(pnl_pct),
        "stoploss_context": stoploss_context,
        "mfe_pct": _fmt(mfe_pct),
        "mae_pct": _fmt(mae_pct),
        "seconds_held": _fmt(seconds_held),
        "entry_fill_count": _fmt(entry_fill_count) if entry_fill_count else "",
        "tob_spread_at_entry": _fmt(tob_spread_at_entry),
        "tob_spread_at_exit": _fmt(tob_spread_at_exit),
        "mfe_cents": _fmt(mfe_cents),
        "mae_cents": _fmt(mae_cents),
        "target_sl_price": _fmt(target_sl_price),
        "actual_exit_price": _fmt(actual_exit_price),
        "slippage_cents": _fmt(slippage_cents),
        "guard_tune_knob": _fmt(guard_tune_knob) if guard_tune_knob else "",
        "guard_checks_summary": _fmt(guard_checks_summary) if guard_checks_summary else "",
        "resolved_as": "",
        "basket_entry_cost_cents": _fmt(basket_entry_cost_cents),
        "yes_strike": _fmt(yes_strike),
        "no_strike": _fmt(no_strike),
        "yes_entry_price": _fmt(yes_entry_price),
        "no_entry_price": _fmt(no_entry_price),
        "yes_distance": _fmt(yes_distance),
        "no_distance": _fmt(no_distance),
        "combined_spread_at_entry": _fmt(combined_spread_at_entry),
        "basket_exit_value_cents": _fmt(basket_exit_value_cents),
        "net_basket_pnl_cents": _fmt(net_basket_pnl_cents),
        "combined_spread_at_exit": _fmt(combined_spread_at_exit),
        "cooldown_skipped": _fmt(cooldown_skipped),
    }


def _quantile(lst: list, q: float) -> Optional[float]:
    if not lst:
        return None
    s = sorted(lst)
    i = max(0, min(len(s) - 1, int(len(s) * q)))
    return round(s[i], 2)


def build_tuning_summary(
    ledger_rows: List[Dict],
    config: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Build tuning summary for strategy adjustment: failure breakdown, near-miss stats,
    stoploss diagnostics, what-if tables.
    """
    config = config or {}
    dist_cfg = config.get("schedule", {}).get("hourly_risk_guards", {}).get("distance_buffer", {}) or {}
    assets_cfg = dist_cfg.get("assets", {}) or {}
    thresholds = config.get("thresholds", {}) or {}
    yes_max = thresholds.get("yes_max") or 99
    no_max = thresholds.get("no_max") or 99

    eval_count = len(ledger_rows)
    submitted = sum(1 for r in ledger_rows if r.get("exec_status") in ("SUBMITTED", "OPEN", "EXITED"))
    failed = sum(1 for r in ledger_rows if r.get("exec_status") == "FAILED")
    skipped = sum(1 for r in ledger_rows if r.get("exec_status") == "SKIPPED")
    exited = sum(1 for r in ledger_rows if r.get("exec_status") == "EXITED")
    stoploss = sum(1 for r in ledger_rows if r.get("exec_status") == "EXITED" and "STOPLOSS" in (r.get("exec_reason") or ""))

    error_reasons = defaultdict(int)
    skip_reasons = defaultdict(int)
    guard_tune_knob_by_reason: Dict[str, str] = {}
    dist_margins_fail = []
    persist_margins_fail = []
    sec_to_close_fail = []
    entry_prices_submitted = []
    entry_prices_stoploss = []
    stoploss_rows = []

    for r in ledger_rows:
        if r.get("exec_status") == "FAILED":
            err = r.get("exec_reason") or "ERR_UNKNOWN"
            error_reasons[err] += 1
        if r.get("exec_status") == "FAILED" or r.get("dist_status") == "FAIL":
            dm = r.get("dist_margin")
            if dm is not None and dm != "":
                try:
                    dist_margins_fail.append(float(dm))
                except (TypeError, ValueError):
                    pass
        if r.get("exec_status") == "FAILED" or r.get("persist_status") == "FAIL":
            pm = r.get("persist_margin")
            if pm is not None and pm != "":
                try:
                    persist_margins_fail.append(float(pm))
                except (TypeError, ValueError):
                    pass
        if r.get("exec_status") == "FAILED":
            sec = r.get("seconds_to_close_at_entry") or r.get("seconds_to_close")
            if sec is not None and sec != "":
                try:
                    sec_to_close_fail.append(float(sec))
                except (TypeError, ValueError):
                    pass
        if r.get("exec_status") == "SKIPPED":
            skip = r.get("exec_reason") or "SKIP"
            skip_reasons[skip] += 1
            knob = r.get("guard_tune_knob")
            if knob and str(knob).strip() and skip not in guard_tune_knob_by_reason:
                guard_tune_knob_by_reason[skip] = str(knob).strip()
        if r.get("exec_status") in ("SUBMITTED", "OPEN", "EXITED") and submitted:
            ep = r.get("entry_price_cents")
            if ep is not None and ep != "":
                try:
                    entry_prices_submitted.append(float(ep))
                except (TypeError, ValueError):
                    pass
        if r.get("exec_status") == "EXITED" and "STOPLOSS" in (r.get("exec_reason") or ""):
            ep = r.get("entry_price_cents")
            if ep is not None and ep != "":
                try:
                    entry_prices_stoploss.append(float(ep))
                except (TypeError, ValueError):
                    pass
            stoploss_rows.append({
                "entry_price_cents": r.get("entry_price_cents"),
                "dist_margin": r.get("dist_margin"),
                "seconds_to_close_at_entry": r.get("seconds_to_close_at_entry") or r.get("seconds_to_close"),
                "mae_pct": r.get("mae_pct"),
                "mfe_pct": r.get("mfe_pct"),
                "ticker": r.get("ticker"),
                "side": r.get("side"),
            })

    def _dist_stats(lst):
        if not lst:
            return {}
        return {
            "min": round(min(lst), 2),
            "p10": _quantile(lst, 0.10),
            "median": _quantile(lst, 0.50),
            "p90": _quantile(lst, 0.90),
        }

    cutoff_block = sum(1 for r in ledger_rows if r.get("cutoff_status") == "BLOCK")
    entry_bucket_submitted = defaultdict(int)
    for ep in entry_prices_submitted:
        if ep >= 98:
            entry_bucket_submitted["98-99"] += 1
        elif ep >= 96:
            entry_bucket_submitted["96-97"] += 1
        elif ep >= 94:
            entry_bucket_submitted["94-95"] += 1
        else:
            entry_bucket_submitted["<94"] += 1
    entry_bucket_stoploss = defaultdict(int)
    for ep in entry_prices_stoploss:
        if ep >= 98:
            entry_bucket_stoploss["98-99"] += 1
        elif ep >= 96:
            entry_bucket_stoploss["96-97"] += 1
        elif ep >= 94:
            entry_bucket_stoploss["94-95"] += 1
        else:
            entry_bucket_stoploss["<94"] += 1

    # What-if: floor_usd +10/+25
    whatif_floor = {}
    for asset, ac in assets_cfg.items():
        floor = ac.get("floor_usd") or 50
        flip_10 = sum(1 for d in dist_margins_fail if d is not None and -10 <= d < 0)
        flip_25 = sum(1 for d in dist_margins_fail if d is not None and -25 <= d < 0)
        whatif_floor[asset] = {"floor_usd": floor, "flip_if_plus_10": flip_10, "flip_if_plus_25": flip_25}

    # What-if: yes_max/no_max 99->98
    entry_99_stoploss = sum(1 for ep in entry_prices_stoploss if ep >= 99)
    whatif_entry_band = {
        "yes_max": yes_max,
        "no_max": no_max,
        "stoploss_with_entry_ge_99": entry_99_stoploss,
        "would_filter_if_max_98": entry_99_stoploss,
    }

    return {
        "counts": {
            "eval_count": eval_count,
            "entry_decisions": eval_count,
            "submitted": submitted,
            "failed": failed,
            "skipped": skipped,
            "exited": exited,
            "stoploss": stoploss,
        },
        "top_errors": dict(sorted(error_reasons.items(), key=lambda x: -x[1])[:5]),
        "top_guard_skips": dict(sorted(skip_reasons.items(), key=lambda x: -x[1])[:5]),
        "guard_tune_knob_by_reason": guard_tune_knob_by_reason,
        "near_miss": {
            "distance_fail_count": len(dist_margins_fail),
            "dist_margin_distribution": _dist_stats(dist_margins_fail),
            "persistence_fail_count": len(persist_margins_fail),
            "persist_margin_distribution": _dist_stats(persist_margins_fail),
            "cutoff_block_count": cutoff_block,
            "seconds_to_close_distribution": _dist_stats(sec_to_close_fail),
            "entry_price_submitted_buckets": dict(entry_bucket_submitted),
            "entry_price_stoploss_buckets": dict(entry_bucket_stoploss),
        },
        "stoploss_diagnostics": stoploss_rows[:20],
        "whatif_floor_usd": whatif_floor,
        "whatif_entry_band": whatif_entry_band,
    }


def _strategy_summary_from_ledger(
    ledger_rows: List[Dict],
    hour_start_ts: float,
    hour_end_ts: float,
) -> List[Dict]:
    """One row per (interval, asset) with tuning metrics."""
    by_ia = defaultdict(lambda: {
        "eval_count": 0,
        "trade_attempts": 0,
        "submitted_count": 0,
        "failed_count": 0,
        "skipped_count": 0,
        "exited_count": 0,
        "stoploss_count": 0,
        "skip_reasons": defaultdict(int),
        "error_reasons": defaultdict(int),
        "dist_margins": [],
        "persist_fail": 0,
        "persist_total": 0,
        "dist_fail": 0,
        "dist_total": 0,
        "cutoff_block": 0,
        "cap_block": 0,
        "entry_prices": [],
    })
    for r in ledger_rows:
        interval = r.get("interval", "hourly")
        asset = (r.get("asset") or "").lower()
        key = (interval, asset)
        by_ia[key]["trade_attempts"] += 1
        exec_status = r.get("exec_status", "")
        if exec_status in ("SUBMITTED", "OPEN", "EXITED"):
            by_ia[key]["submitted_count"] += 1
        if exec_status == "FAILED":
            by_ia[key]["failed_count"] += 1
            err = r.get("exec_reason") or "UNKNOWN"
            by_ia[key]["error_reasons"][err] += 1
        elif exec_status == "SKIPPED":
            by_ia[key]["skipped_count"] += 1
            skip = r.get("exec_reason") or "SKIPPED"
            by_ia[key]["skip_reasons"][skip] += 1
        elif exec_status == "EXITED":
            by_ia[key]["exited_count"] += 1
            if "STOPLOSS" in (r.get("exec_reason") or ""):
                by_ia[key]["stoploss_count"] += 1
        if r.get("dist_status") == "FAIL":
            by_ia[key]["dist_fail"] += 1
        if r.get("dist_status") in ("PASS", "FAIL"):
            by_ia[key]["dist_total"] += 1
            dm = r.get("dist_margin")
            if dm is not None and dm != "":
                try:
                    by_ia[key]["dist_margins"].append(float(dm))
                except (TypeError, ValueError):
                    pass
        if r.get("persist_status") == "FAIL":
            by_ia[key]["persist_fail"] += 1
        if r.get("persist_status") in ("PASS", "FAIL"):
            by_ia[key]["persist_total"] += 1
        if r.get("cutoff_status") == "BLOCK":
            by_ia[key]["cutoff_block"] += 1
        if (r.get("exec_reason") or "").startswith("SKIPPED_CAP") or "CAP" in (r.get("exec_reason") or ""):
            by_ia[key]["cap_block"] += 1
        ep = r.get("entry_price_cents")
        if ep is not None and ep != "":
            try:
                by_ia[key]["entry_prices"].append(float(ep))
            except (TypeError, ValueError):
                pass

    out = []
    for (interval, asset), v in sorted(by_ia.items()):
        attempts = v["trade_attempts"] or 1
        dist_margins = v["dist_margins"]
        dist_total = v["dist_total"] or 1
        persist_total = v["persist_total"] or 1
        top_3_skip = ",".join("%s:%d" % (r, c) for r, c in sorted(v["skip_reasons"].items(), key=lambda x: -x[1])[:3])
        top_1_error = max(v["error_reasons"], key=v["error_reasons"].get) if v["error_reasons"] else ""

        def _pct(n: int, d: int) -> float:
            if d <= 0:
                return 0.0
            return round(n / d, 4)

        def _avg(lst: list) -> Optional[float]:
            if not lst:
                return None
            return round(sum(lst) / len(lst), 2)

        def _quantile(lst: list, q: float) -> Optional[float]:
            if not lst:
                return None
            s = sorted(lst)
            i = max(0, min(len(s) - 1, int(len(s) * q)))
            return round(s[i], 2)

        out.append({
            "hour_start_ts": datetime.fromtimestamp(hour_start_ts, tz=timezone.utc).isoformat(),
            "hour_end_ts": datetime.fromtimestamp(hour_end_ts, tz=timezone.utc).isoformat(),
            "interval": interval,
            "asset": asset,
            "eval_count": v["trade_attempts"],
            "trade_attempts": v["trade_attempts"],
            "submitted_count": v["submitted_count"],
            "failed_count": v["failed_count"],
            "skipped_count": v["skipped_count"],
            "exited_count": v["exited_count"],
            "stoploss_count": v["stoploss_count"],
            "top_3_skip_reasons": top_3_skip,
            "top_1_error": top_1_error,
            "dist_fail_rate": _pct(v["dist_fail"], dist_total),
            "persist_fail_rate": _pct(v["persist_fail"], persist_total),
            "cutoff_block_rate": _pct(v["cutoff_block"], attempts),
            "cap_block_rate": _pct(v["cap_block"], attempts),
            "avg_dist_margin": _avg(dist_margins),
            "p10_dist_margin": _quantile(dist_margins, 0.10),
            "p50_dist_margin": _quantile(dist_margins, 0.50),
            "p90_dist_margin": _quantile(dist_margins, 0.90),
            "avg_entry_price_cents": _avg(v["entry_prices"]),
        })
    return out


def enrich_ledger_with_kalshi_outcomes(ledger_rows: List[Dict]) -> List[Dict]:
    """
    Fetch actual outcomes from Kalshi API for each unique ticker and add resolved_as column.
    Returns updated rows (yes/no/scalar or empty if not yet determined).
    """
    try:
        from bot.market import fetch_ticker_outcome
    except ImportError:
        return ledger_rows
    tickers = {r.get("ticker") or "" for r in ledger_rows if r.get("ticker")}
    tickers.discard("")
    cache: Dict[str, Optional[str]] = {}
    for t in tickers:
        cache[t] = fetch_ticker_outcome(t)
    for r in ledger_rows:
        t = r.get("ticker")
        r["resolved_as"] = (cache.get(t) or "") if t else ""
    return ledger_rows


def build_strategy_ledger_for_window(
    log_path: str,
    start_utc: float,
    end_utc: float,
    project_root: Optional[Path] = None,
    format: str = "full",
) -> Tuple[List[Dict], List[Dict], float, float]:
    """
    Build strategy ledger and summary for an arbitrary [start_utc, end_utc) window (epoch seconds).
    format: "full" | "concise" — full uses all columns; concise uses decision-useful minimal set.
    Returns (ledger_rows, summary_rows, start_utc, end_utc).
    """
    from tools.analyze_bot_log import parse_log_lines

    if project_root and not Path(log_path).is_absolute():
        log_file = project_root / log_path
    else:
        log_file = Path(log_path)
    if not log_file.exists():
        return [], [], start_utc, end_utc

    events = list(parse_log_lines(str(log_file)))
    filtered = _filter_events_in_window(events, start_utc, end_utc)
    lifecycles = _trade_lifecycles_enter_decision_only(filtered)

    use_concise = (format or "full").lower() == "concise"
    row_fn = _lifecycle_to_concise_ledger_row if use_concise else _lifecycle_to_strategy_ledger_row

    ledger_rows = []
    for key, evs in lifecycles.items():
        row = row_fn(key, evs)
        ledger_rows.append(row)
    ledger_rows.sort(key=lambda r: (r.get("ts_first_seen", ""), r.get("interval"), r.get("asset"), r.get("window_id"), r.get("ticker"), r.get("side")))

    enrich_ledger_with_kalshi_outcomes(ledger_rows)

    summary_rows = _strategy_summary_from_ledger(ledger_rows, start_utc, end_utc)
    return ledger_rows, summary_rows, start_utc, end_utc


def build_strategy_ledger_for_hour(
    log_path: str,
    report_hour_la: datetime,
    project_root: Optional[Path] = None,
    format: str = "full",
) -> Tuple[List[Dict], List[Dict], float, float]:
    """
    Build strategy ledger rows and summary rows for [report_hour_la - 1h, report_hour_la) PT.
    format: "full" | "concise". Returns (ledger_rows, summary_rows, start_utc_ts, end_utc_ts).
    Only includes trade attempts (at least one ENTER_DECISION per key).
    """
    start_utc, end_utc = _hour_window_pt(report_hour_la)
    return build_strategy_ledger_for_window(log_path, start_utc, end_utc, project_root, format=format)


INTERVALS_ORDER = ("fifteen_min", "hourly", "daily", "weekly")


def _partition_rows_by_interval(
    ledger_rows: List[Dict], summary_rows: List[Dict]
) -> Dict[str, Tuple[List[Dict], List[Dict]]]:
    """Partition ledger and summary rows by interval. Returns {interval: (ledger_rows, summary_rows)}."""
    by_iv: Dict[str, Tuple[List[Dict], List[Dict]]] = {
        iv: ([], []) for iv in INTERVALS_ORDER
    }
    for r in ledger_rows:
        iv = r.get("interval") or "hourly"
        if iv == "15min":
            iv = "fifteen_min"
        if iv not in by_iv:
            by_iv[iv] = ([], [])
        by_iv[iv][0].append(r)
    for r in summary_rows:
        iv = r.get("interval") or "hourly"
        if iv == "15min":
            iv = "fifteen_min"
        if iv not in by_iv:
            by_iv[iv] = ([], [])
        by_iv[iv][1].append(r)
    return by_iv


def write_strategy_artifacts(
    ledger_rows: List[Dict],
    summary_rows: List[Dict],
    out_dir: Path,
    window_key: str,
    format: str = "full",
) -> Tuple[Path, Path]:
    """Write strategy_ledger_<window>.tsv and strategy_summary_<window>.tsv; return (ledger_path, summary_path).
    format: "full" | "concise" — use corresponding columns for ledger.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    use_concise = (format or "full").lower() == "concise"
    ledger_cols = CONCISE_LEDGER_COLS if use_concise else STRATEGY_LEDGER_COLS
    ledger_path = out_dir / f"strategy_ledger_{window_key}.tsv"
    summary_path = out_dir / f"strategy_summary_{window_key}.tsv"
    with open(ledger_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(ledger_cols)
        for r in ledger_rows:
            w.writerow([r.get(c, "") for c in ledger_cols])
    with open(summary_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(STRATEGY_SUMMARY_COLS)
        for r in summary_rows:
            w.writerow([r.get(c, "") for c in STRATEGY_SUMMARY_COLS])
    return ledger_path, summary_path


def write_strategy_artifacts_by_interval(
    ledger_rows: List[Dict],
    summary_rows: List[Dict],
    out_dir: Path,
    window_key: str,
    format: str = "full",
) -> Dict[str, Tuple[Path, Path]]:
    """Write per-interval strategy_ledger and strategy_summary files. Returns {interval: (ledger_path, summary_path)}."""
    by_iv = _partition_rows_by_interval(ledger_rows, summary_rows)
    out = {}
    for iv in INTERVALS_ORDER:
        lrows, srows = by_iv.get(iv, ([], []))
        lpath, spath = write_strategy_artifacts(lrows, srows, out_dir, f"{iv}_{window_key}", format=format)
        out[iv] = (lpath, spath)
    return out


def write_tuning_summary_artifact(tuning_summary: Dict[str, Any], out_dir: Path, window_key: str) -> Path:
    """Write tuning_summary_<window>.json; return path."""
    import json
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"tuning_summary_{window_key}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(tuning_summary, f, default=str, indent=2)
    return path


def write_tuning_summary_artifacts_by_interval(
    ledger_rows: List[Dict],
    config: dict,
    out_dir: Path,
    window_key: str,
) -> Dict[str, Path]:
    """Write per-interval tuning_summary files. Returns {interval: path}."""
    by_iv = _partition_rows_by_interval(ledger_rows, [])
    out = {}
    for iv in INTERVALS_ORDER:
        lrows = by_iv.get(iv, ([], []))[0]
        tuning = build_tuning_summary(lrows, config)
        path = write_tuning_summary_artifact(tuning, out_dir, f"{iv}_{window_key}")
        out[iv] = path
    return out
