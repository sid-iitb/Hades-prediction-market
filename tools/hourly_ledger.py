"""
Hourly ledger: build transaction summary for the last 60 minutes (PT).
Uses logs/bot.log and analyzer schema. Output: ledger rows (1 per trade attempt) + window summary.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytz

LA_TZ = pytz.timezone("America/Los_Angeles")

# Status enum for ledger
STATUS_SKIPPED_GUARD = "SKIPPED_GUARD"
STATUS_SKIPPED_CAP = "SKIPPED_CAP"
STATUS_FAILED = "FAILED"
STATUS_OPEN = "OPEN"
STATUS_EXITED = "EXITED"

LEDGER_COLS = [
    "report_hour",
    "ts_first_seen",
    "ts_entry_decision",
    "ts_order_submitted",
    "ts_exit",
    "interval",
    "asset",
    "window_id",
    "ticker",
    "side",
    "status",
    "entry_price_cents",
    "exit_price_cents",
    "exit_reason_code",
    "pnl_pct",
    "cost_cents",
    "contracts",
    "exec_reason",
    "spot",
    "strike",
    "distance",
    "min_distance_required",
    "dist_status",
    "dist_margin",
    "persist_status",
    "persist_margin",
    "cutoff_seconds",
    "seconds_to_close",
    "cutoff_status",
    "cap_current_total",
    "cap_current_ticker",
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


def _trade_lifecycles(events: List[Dict]) -> Dict[Tuple, List[Dict]]:
    """Group by (interval, asset, window_id, ticker, side); sort by ts."""
    by_trade = defaultdict(list)
    for ev in events:
        ticker = ev.get("ticker")
        side = ev.get("side")
        if ticker is None or side is None:
            if ev.get("event_type") == "EXIT" and ev.get("ticker"):
                ticker = ev.get("ticker")
                side = ev.get("side")
            else:
                continue
        interval = ev.get("interval") or "hourly"
        if interval == "15min":
            interval = "fifteen_min"
        asset = (ev.get("asset") or "").lower()
        window_id = ev.get("window_id") or ""
        key = (interval, asset, window_id, ticker, str(side).lower())
        if ev.get("event_type") in (
            "EVAL", "SKIP", "ENTER", "ENTER_DECISION",
            "ORDER_SUBMITTED", "ORDER_FAILED", "ORDER_SKIPPED", "EXIT", "ROLL_AFTER_STOP",
        ):
            by_trade[key].append(ev)
    for key in by_trade:
        by_trade[key].sort(key=lambda e: e.get("ts") or "")
    return dict(by_trade)


def _ledger_status_and_reason(events: List[Dict], side: str) -> Tuple[str, str]:
    status = STATUS_SKIPPED_GUARD
    exec_reason = ""
    has_submit = False
    has_fail = False
    has_skip_cap = False
    skip_reason = ""
    for ev in events:
        et = ev.get("event_type") or ""
        if et == "ORDER_SUBMITTED":
            has_submit = True
        if et == "ORDER_FAILED":
            has_fail = True
            exec_reason = ev.get("reason_code") or ev.get("error_reason") or ev.get("reason") or "ORDER_FAILED"
        if et == "ORDER_SKIPPED":
            has_fail = True
            rr = ev.get("reason_code") or ev.get("skipped_reason") or ev.get("reason") or ""
            if "cap" in rr.lower() or "SKIPPED_CAP" in rr:
                has_skip_cap = True
            exec_reason = rr or "ORDER_SKIPPED"
        if et == "SKIP":
            skip_reason = ev.get("reason_code") or ""
            if "cap" in skip_reason.lower() or "SKIPPED_CAP" in skip_reason:
                has_skip_cap = True
        if et == "EXIT":
            status = STATUS_EXITED
            exec_reason = ev.get("reason_code") or "EXIT"
    if has_submit:
        if any(e.get("event_type") == "EXIT" for e in events):
            status = STATUS_EXITED
            if not exec_reason:
                for e in events:
                    if e.get("event_type") == "EXIT":
                        exec_reason = e.get("reason_code") or "EXIT"
                        break
        else:
            status = STATUS_OPEN
            exec_reason = ""  # OPEN = no exec_reason
    elif has_fail:
        status = STATUS_FAILED
        if not exec_reason:
            exec_reason = "ORDER_FAILED"
    elif has_skip_cap or ("cap" in (skip_reason or "").lower()):
        status = STATUS_SKIPPED_CAP
        exec_reason = skip_reason or "SKIPPED_CAP_REACHED"
    else:
        status = STATUS_SKIPPED_GUARD
        exec_reason = skip_reason or "SKIP"
    return status, exec_reason


def _lifecycle_to_ledger_row(
    key: Tuple,
    events: List[Dict],
    report_hour_str: str,
) -> Dict[str, Any]:
    interval, asset, window_id, ticker, side = key
    side_upper = side.upper()
    status, exec_reason = _ledger_status_and_reason(events, side)

    ts_first = ""
    ts_entry = ""
    ts_order = ""
    ts_exit = ""
    entry_price = None
    exit_price = None
    exit_reason_code = ""
    pnl_pct = None
    contracts = 1
    spot = None
    strike = None
    distance = None
    min_dist = None
    dist_status = "NA"
    dist_margin = ""
    persist_status = "NA"
    persist_margin = ""
    cutoff_seconds = None
    seconds_to_close = None
    cutoff_status = ""
    cap_current_total = None
    cap_current_ticker = None

    entry_ev = None
    for ev in events:
        et = ev.get("event_type") or ""
        ts = (ev.get("ts") or "")[:19]
        if not ts_first:
            ts_first = ts
        if et in ("ENTER", "ENTER_DECISION"):
            ts_entry = ts
            entry_ev = ev
        if et == "ORDER_SUBMITTED":
            ts_order = ts
            entry_price = ev.get("price_cents") or (ev.get("yes_price_cents") if side == "yes" else ev.get("no_price_cents"))
            if entry_price is None:
                entry_price = ev.get("yes_price_cents") if side == "yes" else ev.get("no_price_cents")
            contracts = ev.get("contracts") or 1
        if et == "SKIP" and entry_ev is None:
            entry_ev = ev
        if et == "EXIT":
            ts_exit = ts
            exit_reason_code = ev.get("reason_code") or ""
            pnl_pct = ev.get("pnl_pct")
            exit_price = ev.get("yes_price_cents") or ev.get("no_price_cents")

    if entry_ev:
        if entry_price is None:
            entry_price = entry_ev.get("yes_price_cents") if side == "yes" else entry_ev.get("no_price_cents")
        spot = entry_ev.get("spot")
        strike = entry_ev.get("strike")
        distance = entry_ev.get("distance")
        min_dist = entry_ev.get("min_distance_required")
        if distance is not None and min_dist is not None:
            dist_status = "PASS" if distance >= min_dist else "FAIL"
            dist_margin = distance - min_dist
        streak = entry_ev.get("persistence_streak") or entry_ev.get("streak")
        required = entry_ev.get("persistence_required_effective") or entry_ev.get("persistence_required") or entry_ev.get("required_polls")
        if streak is not None and required is not None:
            persist_status = "PASS" if streak >= required else "FAIL"
            persist_margin = streak - required
        cutoff_seconds = entry_ev.get("cutoff_seconds")
        seconds_to_close = entry_ev.get("seconds_to_close")
        cap = entry_ev.get("cap_state") or {}
        cap_current_total = cap.get("current_total_orders")
        cap_current_ticker = cap.get("current_ticker_orders")
        if cutoff_seconds is not None and seconds_to_close is not None and seconds_to_close <= cutoff_seconds:
            cutoff_status = "BLOCK"
        else:
            cutoff_status = "PASS"

    cost_cents = None
    if entry_price is not None and contracts is not None:
        cost_cents = entry_price * contracts

    return {
        "report_hour": report_hour_str,
        "ts_first_seen": ts_first,
        "ts_entry_decision": ts_entry or "",
        "ts_order_submitted": ts_order or "",
        "ts_exit": ts_exit or "",
        "interval": interval,
        "asset": asset,
        "window_id": window_id,
        "ticker": ticker,
        "side": side_upper,
        "status": status,
        "entry_price_cents": entry_price,
        "exit_price_cents": exit_price,
        "exit_reason_code": exit_reason_code,
        "pnl_pct": pnl_pct,
        "cost_cents": cost_cents,
        "contracts": contracts,
        "exec_reason": exec_reason,
        "spot": spot,
        "strike": strike,
        "distance": distance,
        "min_distance_required": min_dist,
        "dist_status": dist_status,
        "dist_margin": dist_margin,
        "persist_status": persist_status,
        "persist_margin": persist_margin,
        "cutoff_seconds": cutoff_seconds,
        "seconds_to_close": seconds_to_close,
        "cutoff_status": cutoff_status,
        "cap_current_total": cap_current_total,
        "cap_current_ticker": cap_current_ticker,
    }


def _window_summary_from_ledger(ledger_rows: List[Dict]) -> List[Dict]:
    """Aggregate ledger rows by (interval, asset, window_id)."""
    by_window = defaultdict(lambda: {"attempts": 0, "submitted": 0, "failed": 0, "skipped": 0, "exited": 0, "open": 0, "skipped_guard": 0, "skipped_cap": 0, "exits_stoploss": 0, "exits_takeprofit": 0, "exits_hardflip": 0, "realized_pnl": 0.0})
    for r in ledger_rows:
        key = (r.get("interval"), r.get("asset"), r.get("window_id"))
        by_window[key]["attempts"] += 1
        s = r.get("status", "")
        if s == STATUS_EXITED:
            by_window[key]["exited"] += 1
            rc = (r.get("exit_reason_code") or "").upper()
            if "STOPLOSS" in rc or "STOP_LOSS" in rc:
                by_window[key]["exits_stoploss"] += 1
            elif "TAKEPROFIT" in rc or "TAKE_PROFIT" in rc:
                by_window[key]["exits_takeprofit"] += 1
            elif "HARD" in rc or "FLIP" in rc:
                by_window[key]["exits_hardflip"] += 1
            pnl = r.get("pnl_pct")
            if pnl is not None:
                by_window[key]["realized_pnl"] += float(pnl)
        elif s == STATUS_OPEN:
            by_window[key]["open"] += 1
            by_window[key]["submitted"] += 1
        elif s == STATUS_FAILED:
            by_window[key]["failed"] += 1
        elif s == STATUS_SKIPPED_CAP:
            by_window[key]["skipped_cap"] += 1
            by_window[key]["skipped"] += 1
        else:
            by_window[key]["skipped_guard"] += 1
            by_window[key]["skipped"] += 1
    out = []
    for (interval, asset, window_id), v in sorted(by_window.items()):
        out.append({
            "interval": interval,
            "asset": asset,
            "window_id": window_id,
            "attempts": v["attempts"],
            "submitted": v["submitted"],
            "failed": v["failed"],
            "skipped": v["skipped"],
            "exited": v["exited"],
            "open": v["open"],
            "skipped_guard": v["skipped_guard"],
            "skipped_cap": v["skipped_cap"],
            "exits_stoploss": v["exits_stoploss"],
            "exits_takeprofit": v["exits_takeprofit"],
            "exits_hardflip": v["exits_hardflip"],
            "realized_pnl": v["realized_pnl"],
        })
    return out


def build_ledger_for_hour(
    log_path: str,
    report_hour_la: datetime,
    project_root: Optional[Path] = None,
) -> Tuple[List[Dict], List[Dict], Dict[str, Any]]:
    """
    Build ledger rows and window summary for [report_hour_la - 1h, report_hour_la) in LA time.
    Returns (ledger_rows, window_rows, summary_dict).
    """
    from tools.analyze_bot_log import parse_log_lines

    if project_root and not Path(log_path).is_absolute():
        log_file = project_root / log_path
    else:
        log_file = Path(log_path)
    if not log_file.exists():
        return [], [], _empty_summary()

    events = list(parse_log_lines(str(log_file)))
    start_utc, end_utc = _hour_window_pt(report_hour_la)
    filtered = _filter_events_in_window(events, start_utc, end_utc)
    lifecycles = _trade_lifecycles(filtered)

    report_hour_str = report_hour_la.strftime("%Y-%m-%d %H:%M PT")
    ledger_rows = []
    for key, evs in lifecycles.items():
        row = _lifecycle_to_ledger_row(key, evs, report_hour_str)
        ledger_rows.append(row)
    ledger_rows.sort(key=lambda r: (r.get("ts_first_seen", ""), r.get("interval"), r.get("asset"), r.get("window_id"), r.get("ticker"), r.get("side")))

    window_rows = _window_summary_from_ledger(ledger_rows)
    summary = _summary_from_ledger(ledger_rows)
    return ledger_rows, window_rows, summary


def _empty_summary() -> Dict[str, Any]:
    return {
        "total_attempts": 0,
        "submitted_count": 0,
        "failed_count": 0,
        "skipped_count": 0,
        "exits_count": 0,
        "open_count": 0,
        "realized_pnl": None,
        "top_error_reason": "",
        "top_skip_reasons": [],
        "exits_stoploss": 0,
        "exits_takeprofit": 0,
        "exits_hardflip": 0,
    }


def _summary_from_ledger(ledger_rows: List[Dict]) -> Dict[str, Any]:
    total = len(ledger_rows)
    submitted = sum(1 for r in ledger_rows if r.get("status") == STATUS_OPEN or (r.get("status") == STATUS_EXITED and r.get("ts_order_submitted")))
    failed = sum(1 for r in ledger_rows if r.get("status") == STATUS_FAILED)
    skipped = sum(1 for r in ledger_rows if r.get("status") in (STATUS_SKIPPED_GUARD, STATUS_SKIPPED_CAP))
    exited = sum(1 for r in ledger_rows if r.get("status") == STATUS_EXITED)
    open_count = sum(1 for r in ledger_rows if r.get("status") == STATUS_OPEN)

    realized = None
    pnls = [r.get("pnl_pct") for r in ledger_rows if r.get("status") == STATUS_EXITED and r.get("pnl_pct") is not None]
    if pnls:
        realized = sum(pnls)

    fail_reasons = defaultdict(int)
    for r in ledger_rows:
        if r.get("status") == STATUS_FAILED:
            fail_reasons[r.get("exec_reason") or "UNKNOWN"] += 1
    top_error = ""
    if fail_reasons:
        top_error = max(fail_reasons, key=fail_reasons.get)

    skip_reasons = defaultdict(int)
    for r in ledger_rows:
        if r.get("status") in (STATUS_SKIPPED_GUARD, STATUS_SKIPPED_CAP):
            skip_reasons[r.get("exec_reason") or "SKIP"] += 1
    top_skips = sorted(skip_reasons.items(), key=lambda x: -x[1])[:3]

    exits_sl = sum(1 for r in ledger_rows if r.get("status") == STATUS_EXITED and "STOPLOSS" in (r.get("exit_reason_code") or ""))
    exits_tp = sum(1 for r in ledger_rows if r.get("status") == STATUS_EXITED and "TAKEPROFIT" in (r.get("exit_reason_code") or ""))
    exits_hf = sum(1 for r in ledger_rows if r.get("status") == STATUS_EXITED and ("HARD" in (r.get("exit_reason_code") or "") or "FLIP" in (r.get("exit_reason_code") or "")))

    return {
        "total_attempts": total,
        "submitted_count": submitted,
        "failed_count": failed,
        "skipped_count": skipped,
        "exits_count": exited,
        "open_count": open_count,
        "realized_pnl": realized,
        "top_error_reason": top_error,
        "top_skip_reasons": top_skips,
        "exits_stoploss": exits_sl,
        "exits_takeprofit": exits_tp,
        "exits_hardflip": exits_hf,
    }
