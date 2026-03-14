#!/usr/bin/env python3
"""
Analysis script for bot log (logs/bot.log).
Reads JSON lines, groups by (interval, asset, window_id), prints per-window summaries.
Usage:
  python tools/analyze_bot_log.py [path/to/bot.log]
  python tools/analyze_bot_log.py --summary --format tsv logs/bot.log
  python tools/analyze_bot_log.py --detail logs/bot.log
Stdlib only.
"""
import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# Reason code normalization
REASON_NORMALIZE = {
    "cutoff_seconds": "SKIP_NO_NEW_ENTRY_CUTOFF",
    "SKIP_NO_NEW_ENTRY_CUTOFF": "SKIP_NO_NEW_ENTRY_CUTOFF",
    "disabled_until_expiry": "SKIP_DISABLED_UNTIL_EXPIRY",
    "persistence": "SKIP_PERSISTENCE",
    "distance_buffer": "SKIP_DISTANCE_BUFFER",
    "anchor_mismatch": "SKIP_ANCHOR_MISMATCH",
    "roll_exhausted": "SKIP_ROLL_EXHAUSTED",
    "roll_same_ticker_blocked": "SKIP_ROLL_SAME_TICKER_BLOCKED",
    "BLOCK_REVERSAL_CUTOFF": "BLOCK_REVERSAL_CUTOFF",
    "BLOCK_SAME_SIDE_AFTER_STOP": "BLOCK_SAME_SIDE_AFTER_STOP",
    "BLOCK_MAX_REVERSALS": "BLOCK_MAX_REVERSALS",
    "BLOCK_REVERSAL_WEAK_SIGNAL": "BLOCK_REVERSAL_WEAK_SIGNAL",
    "REVERSAL_ENTRY_ALLOWED": "REVERSAL_ENTRY_ALLOWED",
}
CAP_REASON = "SKIPPED_CAP_REACHED"
ERR_TOP_OF_BOOK = "ERR_TOP_OF_BOOK_MISSING"
ERR_API = "ERR_API"
ERR_UNKNOWN = "ERR_UNKNOWN"
EXIT_STOPLOSS = "EXIT_STOPLOSS"
EXIT_HARD_FLIP = "EXIT_HARD_FLIP"
EXIT_TAKE_PROFIT = "EXIT_TAKE_PROFIT"


def _load_cutoff_seconds(project_root: Path) -> dict:
    """Load no_new_entry_cutoff_seconds from config. Returns {fifteen_min: int, hourly: int}."""
    defaults = {"fifteen_min": 40, "hourly": 90}
    try:
        from bot.config_loader import load_config, resolve_config_path
        cfg_path = resolve_config_path(project_root)
        if not cfg_path.exists():
            return defaults
        config = load_config(str(cfg_path))
        fm = (config.get("fifteen_min") or {}).get("risk_guards") or {}
        hr = ((config.get("schedule") or {}).get("hourly_risk_guards") or {})
        return {
            "fifteen_min": int(fm.get("no_new_entry_cutoff_seconds", defaults["fifteen_min"])),
            "hourly": int(hr.get("no_new_entry_cutoff_seconds", defaults["hourly"])),
        }
    except Exception:
        return defaults


def _parse_ts(ts: str) -> float:
    """Parse ISO ts to epoch seconds, or 0 if invalid."""
    if not ts:
        return 0.0
    try:
        s = str(ts)[:26].replace("Z", "+00:00")
        if "+" not in s and "-" in s[10:]:
            pass
        elif "T" in s and "+" not in s:
            s = s + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def _filter_events_since(events, since_ts: float):
    """Filter events to those with ts >= since_ts."""
    if since_ts <= 0:
        return events
    return [e for e in events if _parse_ts(e.get("ts") or "") >= since_ts]


def parse_log_lines(path: str):
    """Yield parsed JSON dicts for lines that look like our analysis events."""
    p = Path(path)
    if not p.exists():
        return
    with open(p, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Log format: "YYYY-MM-DD HH:MM:SS | LEVEL | {json}"
            if " | " in line and "{" in line:
                idx = line.find("{")
                if idx >= 0:
                    line = line[idx:]
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            # Accept lines with event_type (canonical) or event + window_id (legacy) or ROLL_AFTER_STOP
            has_event_type = "event_type" in obj
            has_event_window = "event" in obj and "window_id" in obj
            has_roll = obj.get("ROLL_AFTER_STOP") is True and "rolled_ticker" in obj
            if has_event_type or has_event_window or has_roll:
                if has_roll:
                    obj["event_type"] = "ROLL_AFTER_STOP"
                    obj["ticker"] = obj.get("rolled_ticker")
                    obj["side"] = obj.get("side", "").lower()
                    obj["interval"] = "hourly"
                elif has_event_window and not has_event_type:
                    obj["event_type"] = obj.get("event", "EVAL")
                if "interval" in obj and obj["interval"] == "15min":
                    obj["interval"] = "fifteen_min"
                yield obj


def group_by_window(events):
    """Group events by (interval, asset, window_id)."""
    groups = defaultdict(list)
    for ev in events:
        interval = ev.get("interval") or "hourly"
        asset = ev.get("asset") or ""
        window_id = ev.get("window_id") or ""
        key = (interval, asset, window_id)
        groups[key].append(ev)
    return groups


def _normalize_reason(reason: str) -> str:
    """Map to tight reason set."""
    if not reason:
        return ""
    r = str(reason).strip()
    if r in REASON_NORMALIZE:
        return REASON_NORMALIZE[r]
    if r.startswith("EXIT_"):
        return r
    if r.startswith("SKIP_"):
        return r
    if r.startswith("ERR_"):
        return r
    if "cap" in r.lower() or "SKIPPED_CAP" in r:
        return CAP_REASON
    if "top-of-book" in r.lower() or "orderbook" in r.lower() or "top of book" in r.lower():
        return ERR_TOP_OF_BOOK
    if "api" in r.lower() or "network" in r.lower():
        return ERR_API
    return ERR_UNKNOWN


def _entry_reason_compact(ev: dict, interval: str) -> str:
    """Build compact token list: IN_BAND, PERSIST_OK, DIST_OK, CUTOFF_OK, CAP_OK, ANCHOR_OK, ROLL_AFTER_STOP."""
    tokens = []
    lo = ev.get("entry_band_min_cents")
    hi = ev.get("entry_band_max_cents")
    yes_p = ev.get("yes_price_cents")
    no_p = ev.get("no_price_cents")
    side = str(ev.get("side") or "").lower()
    side_price = yes_p if side == "yes" else no_p
    if lo is not None and hi is not None and side_price is not None:
        tokens.append("IN_BAND" if lo <= side_price <= hi else "NOT_IN_BAND")
    else:
        tokens.append("BAND_NA")
    streak = ev.get("persistence_streak")
    required = ev.get("persistence_required_effective") or ev.get("persistence_required")
    if streak is not None and required is not None:
        tokens.append("PERSIST_OK" if streak >= required else "PERSIST_FAIL")
    else:
        tokens.append("PERSIST_NA")
    dist = ev.get("distance")
    min_dist = ev.get("min_distance_required")
    if dist is not None and min_dist is not None:
        tokens.append("DIST_OK" if dist >= min_dist else "DIST_FAIL")
    else:
        tokens.append("DIST_NA")
    sec = ev.get("seconds_to_close")
    cutoff_block = (sec is not None and sec <= 40) if interval == "fifteen_min" else (sec is not None and sec <= 90)
    if ev.get("reason_code") == "REVERSAL_ENTRY_ALLOWED":
        tokens.append("CUTOFF_OK")
    elif sec is not None:
        tokens.append("CUTOFF_BLOCK" if cutoff_block else "CUTOFF_OK")
    else:
        tokens.append("CUTOFF_NA")
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
    if ev.get("ROLL_AFTER_STOP"):
        tokens.append("ROLL_AFTER_STOP")
    return "+".join(tokens)


def _stoploss_context(ev: dict) -> str:
    """Compact stop-loss explanation including MFE/MAE and dist_margin when available."""
    spot = ev.get("spot")
    strike = ev.get("strike")
    dist = ev.get("distance")
    min_dist = ev.get("min_distance_required")
    dist_margin = ev.get("dist_margin")
    pnl = ev.get("pnl_pct")
    mfe_pct = ev.get("mfe_pct")
    mae_pct = ev.get("mae_pct")
    sec = ev.get("seconds_to_close")
    ttc = (sec / 60.0) if sec is not None else None
    parts = []
    if spot is not None and strike is not None:
        parts.append("spot=%s" % spot)
        parts.append("strike=%s" % strike)
    if dist is not None:
        parts.append("dist=%s" % dist)
    if min_dist is not None:
        parts.append("min=%s" % min_dist)
    if dist_margin is not None:
        parts.append("margin=%.1f" % dist_margin)
    if pnl is not None:
        parts.append("pnl=%.2f%%" % (pnl * 100))
    if mfe_pct is not None:
        parts.append("mfe=%.2f%%" % (mfe_pct * 100))
    if mae_pct is not None:
        parts.append("mae=%.2f%%" % (mae_pct * 100))
    if ttc is not None:
        parts.append("ttc=%.1f" % ttc)
    if parts:
        return " ".join(parts)
    return "unknown" if not (spot or strike) else "pnl=%s ttc=%s" % (pnl, ttc)


def _build_trade_lifecycles(events):
    """Build per-trade lifecycle: key -> sorted events."""
    by_trade = defaultdict(list)
    for ev in events:
        ticker = ev.get("ticker")
        side = ev.get("side")
        if ticker is None or side is None:
            if ev.get("event_type") == "EXIT" and ev.get("ticker"):
                ticker = ev.get("ticker")
                side = ev.get("side")
            elif ev.get("event_type") == "STOPLOSS_COUNTERFACTUAL" and ev.get("stopped_ticker"):
                ticker = ev.get("stopped_ticker")
                side = ev.get("stopped_side", "")
            else:
                continue
        interval = ev.get("interval") or "hourly"
        asset = ev.get("asset") or ""
        window_id = ev.get("window_id") or ""
        key = (interval, asset, window_id, ticker, str(side).lower())
        if ev.get("event_type") in ("EVAL", "SKIP", "ENTER", "ENTER_DECISION", "ORDER_SUBMITTED", "ORDER_FAILED", "ORDER_SKIPPED", "EXIT", "ROLL_AFTER_STOP", "STOPLOSS_COUNTERFACTUAL"):
            by_trade[key].append(ev)
    for key in by_trade:
        by_trade[key].sort(key=lambda e: e.get("ts") or "")
    return by_trade


def _trade_row_from_lifecycle(key, events, interval: str) -> dict:
    """Convert lifecycle to trade row dict."""
    interval, asset, window_id, ticker, side = key
    ts_first = ""
    ts_entry = ""
    ts_order = ""
    ts_exit = ""
    status = "SKIPPED"
    entry_price = None
    exit_reason = None
    pnl_pct = None
    fail_or_skip = ""
    entry_reason = ""
    stoploss_ctx = ""
    has_order_id = False
    has_exit = False
    has_fail = False
    entry_ev = None
    for ev in events:
        et = ev.get("event_type") or ""
        ts = (ev.get("ts") or "")[:19]
        if not ts_first:
            ts_first = ts
        if et in ("ENTER", "ENTER_DECISION"):
            ts_entry = ts
            entry_ev = ev
        if et == "ORDER_SUBMITTED" and (ev.get("order_id") or ev.get("client_order_id")):
            ts_order = ts
            has_order_id = True
            entry_price = ev.get("price_cents") or ev.get("yes_price_cents") if side == "yes" else ev.get("no_price_cents")
            if entry_price is None:
                entry_price = ev.get("yes_price_cents") if side == "yes" else ev.get("no_price_cents")
        if et == "ORDER_FAILED":
            has_fail = True
            fail_or_skip = _normalize_reason(ev.get("reason_code") or ev.get("reason") or str(ev.get("error", "")))
            if not fail_or_skip:
                fail_or_skip = ERR_UNKNOWN
        if et == "ORDER_SKIPPED":
            has_fail = True
            fail_or_skip = _normalize_reason(ev.get("reason_code") or ev.get("reason") or CAP_REASON)
        if et == "SKIP":
            if not fail_or_skip:
                fail_or_skip = _normalize_reason(ev.get("reason_code") or "")
            if entry_ev is None:
                entry_ev = ev
        if et == "ROLL_AFTER_STOP" and entry_ev is not None:
            entry_ev = dict(entry_ev)
            entry_ev["ROLL_AFTER_STOP"] = True
        if et == "EXIT":
            ts_exit = ts
            has_exit = True
            exit_reason = _normalize_reason(ev.get("reason_code") or "")
            pnl_pct = ev.get("pnl_pct")
            if exit_reason == EXIT_STOPLOSS:
                stoploss_ctx = _stoploss_context(ev)
    if entry_ev:
        entry_reason = _entry_reason_compact(entry_ev, interval)
    if has_order_id:
        if has_exit:
            status = "EXITED"
        else:
            status = "OPEN"
    else:
        if has_fail:
            status = "FAILED"
        else:
            status = "SKIPPED"
        if has_exit:
            status = "EXITED"
    if entry_price is None and entry_ev:
        side_price = entry_ev.get("yes_price_cents") if side == "yes" else entry_ev.get("no_price_cents")
        entry_price = side_price

    # Derived columns from entry_ev
    strike_val = entry_ev.get("strike") if entry_ev else None
    strike_known = strike_val is not None and (strike_val or 0) > 0
    dist = entry_ev.get("distance") if entry_ev else None
    min_dist = entry_ev.get("min_distance_required") if entry_ev else None
    streak = (entry_ev.get("persistence_streak") if entry_ev else None) or (entry_ev.get("streak") if entry_ev else None)
    required = (entry_ev.get("persistence_required_effective") or entry_ev.get("persistence_required") or entry_ev.get("required_polls") if entry_ev else None)
    sec_close = entry_ev.get("seconds_to_close") if entry_ev else None
    cutoff_secs = entry_ev.get("cutoff_seconds") if entry_ev else (90 if interval == "hourly" else 40)
    rc = entry_ev.get("reason_code") if entry_ev else ""

    if dist is not None and min_dist is not None:
        dist_status = "PASS" if dist >= min_dist else "FAIL"
        dist_margin = dist - min_dist
    else:
        dist_status = "NA"
        dist_margin = ""

    if streak is not None and required is not None:
        persist_status = "PASS" if streak >= required else "FAIL"
        persist_margin = streak - required
    else:
        persist_status = "NA"
        persist_margin = ""

    if rc == "SKIP_NO_NEW_ENTRY_CUTOFF" or rc == "cutoff_seconds":
        cutoff_status = "BLOCK"
    elif cutoff_secs is not None and sec_close is not None and sec_close <= cutoff_secs:
        cutoff_status = "BLOCK"
    else:
        cutoff_status = "PASS"
    cutoff_margin = (sec_close - cutoff_secs) if (sec_close is not None and cutoff_secs is not None) else ""

    cap = entry_ev.get("cap_state") or {} if entry_ev else {}
    cap_current_total = cap.get("current_total_orders")
    cap_max_total = cap.get("max_total_orders_per_window")
    cap_margin_total = (cap_current_total - cap_max_total) if (cap_current_total is not None and cap_max_total is not None) else ""
    cap_current_ticker = cap.get("current_ticker_orders")
    cap_max_ticker = cap.get("max_orders_per_ticker")
    cap_margin_ticker = (cap_current_ticker - cap_max_ticker) if (cap_current_ticker is not None and cap_max_ticker is not None) else ""

    if has_order_id:
        if has_exit:
            exec_status = "EXITED"
            exec_reason = exit_reason or ""
        else:
            exec_status = "SUBMITTED"
            exec_reason = ""
    elif has_fail:
        exec_status = "FAILED"
        exec_reason = fail_or_skip or ERR_UNKNOWN
    elif CAP_REASON in (fail_or_skip or "") or "SKIPPED_CAP" in (fail_or_skip or ""):
        exec_status = "SKIPPED"
        exec_reason = "SKIPPED_CAP"
    else:
        exec_status = "SKIPPED"
        exec_reason = "SKIPPED_GUARD"

    block_reason = exec_reason if exec_status in ("SKIPPED", "FAILED") else ""

    return {
        "ts_first_seen": ts_first,
        "ts_entry_decision": ts_entry or "",
        "ts_order_submitted": ts_order or "",
        "ts_exit": ts_exit or "",
        "interval": interval,
        "asset": asset,
        "window_id": window_id,
        "ticker": ticker,
        "side": side.upper(),
        "status": status,
        "entry_price_cents": entry_price,
        "exit_reason_code": exit_reason or "",
        "pnl_pct": pnl_pct,
        "fail_or_skip_reason": fail_or_skip or "",
        "entry_reason_compact": entry_reason or "",
        "stoploss_context": stoploss_ctx or "",
        "strike": strike_val,
        "strike_known": strike_known,
        "block_reason": block_reason,
        "distance": dist,
        "min_distance_required": min_dist,
        "dist_margin": dist_margin,
        "dist_status": dist_status,
        "persistence_streak": streak,
        "persistence_required": required,
        "persist_margin": persist_margin,
        "persist_status": persist_status,
        "seconds_to_close": sec_close,
        "cutoff_seconds": cutoff_secs,
        "cutoff_margin": cutoff_margin,
        "cutoff_status": cutoff_status,
        "cap_current_total": cap_current_total,
        "cap_max_total": cap_max_total,
        "cap_margin_total": cap_margin_total,
        "cap_current_ticker": cap_current_ticker,
        "cap_max_ticker": cap_max_ticker,
        "cap_margin_ticker": cap_margin_ticker,
        "exec_status": exec_status,
        "exec_reason": exec_reason or "",
    }


def _collect_stoploss_trades(events):
    """
    Collect every stop-loss trade with full entry + exit + risk-guard details.
    Returns list of dicts: one per EXIT with reason_code EXIT_STOPLOSS.
    """
    lifecycles = _build_trade_lifecycles(events)
    out = []
    for key, evs in lifecycles.items():
        interval, asset, window_id, ticker, side = key
        exit_evs = [e for e in evs if e.get("event_type") == "EXIT" and
                    _normalize_reason(e.get("reason_code") or "") == EXIT_STOPLOSS]
        if not exit_evs:
            continue
        exit_ev = exit_evs[-1]
        ts_exit = exit_ev.get("ts") or ""
        # Entry: last ENTER_DECISION / ENTER and last ORDER_SUBMITTED before this exit
        pre_exit = [e for e in evs if (e.get("ts") or "") < ts_exit]
        entry_decision = None
        order_submitted = None
        for e in reversed(pre_exit):
            et = e.get("event_type") or ""
            if et in ("ENTER_DECISION", "ENTER") and entry_decision is None:
                entry_decision = e
            if et == "ORDER_SUBMITTED" and order_submitted is None:
                order_submitted = e
            if entry_decision is not None and order_submitted is not None:
                break
        # Prefer entry_decision for spot/distance/seconds_to_close; order_submitted for price/contracts
        entry_ev = entry_decision or order_submitted
        if not entry_ev:
            entry_ev = pre_exit[-1] if pre_exit else None
        ts_entry = (entry_ev.get("ts") or "") if entry_ev else ""
        entry_price = exit_ev.get("entry_price_cents")
        if entry_price is None and order_submitted is not None:
            entry_price = order_submitted.get("price_cents")
        if entry_price is None and entry_ev:
            side_l = (side or "").lower()
            entry_price = entry_ev.get("yes_price_cents") if side_l == "yes" else entry_ev.get("no_price_cents")

        # Collect all ORDER_SUBMITTED fills (pyramiding)
        fills = []
        total_contracts = 0
        ts_first_entry = ""
        ts_last_entry = ""
        side_l = (side or "").lower()
        for e in pre_exit:
            if e.get("event_type") == "ORDER_SUBMITTED" and (e.get("order_id") or e.get("client_order_id")):
                pc = e.get("price_cents") or (e.get("yes_price_cents") if side_l == "yes" else e.get("no_price_cents"))
                if pc is None:
                    pc = e.get("yes_price_cents") if side_l == "yes" else e.get("no_price_cents")
                cnt = e.get("contracts") or 1
                # Distance at the moment this individual order was filled (from that ORDER_SUBMITTED event)
                dist = e.get("distance")
                if dist is None and entry_ev:
                    dist = entry_ev.get("distance")
                fills.append({"price_cents": pc, "contracts": cnt, "distance": dist, "ts": (e.get("ts") or "")[:26]})
                total_contracts += int(cnt)
                if not ts_first_entry:
                    ts_first_entry = (e.get("ts") or "")[:26]
                ts_last_entry = (e.get("ts") or "")[:26]
        # seconds_held
        ts_exit_ts = (exit_ev.get("ts") or "")[:26]
        seconds_held = None
        if (ts_first_entry or ts_entry) and ts_exit_ts:
            t1 = _parse_ts(ts_first_entry or ts_entry)
            t2 = _parse_ts(ts_exit_ts)
            if t1 > 0 and t2 > 0:
                seconds_held = int(round(t2 - t1))
        # avg_entry_price (weighted by contracts)
        avg_entry_price = None
        if total_contracts and fills:
            total_cost = sum((f.get("price_cents") or 0) * (f.get("contracts") or 1) for f in fills)
            avg_entry_price = round(total_cost / total_contracts) if total_cost else entry_price
        elif entry_price is not None:
            avg_entry_price = entry_price

        # Risk-guard at entry
        persistence_streak = None
        persistence_required = None
        recent_cross_flag = None
        late_persistence_applied = None
        cutoff_seconds = None
        entry_band_min = None
        entry_band_max = None
        if entry_ev:
            persistence_streak = entry_ev.get("persistence_streak") or entry_ev.get("streak")
            persistence_required = (entry_ev.get("persistence_required_effective") or
                                   entry_ev.get("persistence_required") or entry_ev.get("required_polls"))
            recent_cross_flag = entry_ev.get("recent_cross_flag")
            late_persistence_applied = entry_ev.get("late_persistence_applied")
            cutoff_seconds = entry_ev.get("cutoff_seconds")
            entry_band_min = entry_ev.get("entry_band_min_cents")
            entry_band_max = entry_ev.get("entry_band_max_cents")

        # Exit TOB / slippage
        yes_bid = exit_ev.get("yes_bid_cents")
        yes_ask = exit_ev.get("yes_ask_cents")
        no_bid = exit_ev.get("no_bid_cents")
        no_ask = exit_ev.get("no_ask_cents")
        tob_spread_at_exit = exit_ev.get("tob_spread_cents")
        if tob_spread_at_exit is None and side_l == "yes" and yes_bid is not None and yes_ask is not None:
            tob_spread_at_exit = yes_ask - yes_bid
        elif tob_spread_at_exit is None and side_l == "no" and no_bid is not None and no_ask is not None:
            tob_spread_at_exit = no_ask - no_bid
        target_sl_price = exit_ev.get("target_sl_price_cents") or (yes_bid if side_l == "yes" else no_bid)
        actual_exit_price = exit_ev.get("exit_price_cents")
        slippage_cents = exit_ev.get("slippage_cents")
        if slippage_cents is None and target_sl_price is not None and actual_exit_price is not None:
            slippage_cents = target_sl_price - actual_exit_price
        # mfe_cents, mae_cents
        mfe_cents = None
        mae_cents = None
        ep = avg_entry_price or entry_price
        if ep is not None:
            mfe_pct = exit_ev.get("mfe_pct")
            mae_pct = exit_ev.get("mae_pct")
            if mfe_pct is not None:
                mfe_cents = round(float(ep) * float(mfe_pct))
            if mae_pct is not None:
                mae_cents = round(float(ep) * float(mae_pct))

        out.append({
            "interval": interval,
            "asset": asset or "",
            "window_id": window_id or "",
            "ticker": ticker or "",
            "side": (side or "").upper(),
            # Entry
            "ts_entry": ts_entry,
            "ts_first_entry": ts_first_entry or ts_entry,
            "ts_last_entry": ts_last_entry or ts_entry,
            "avg_entry_price_cents": avg_entry_price,
            "total_contracts": total_contracts or (order_submitted.get("contracts") if order_submitted else None),
            "entry_fill_count": len(fills) or 1,
            "fills": fills,
            "seconds_to_close_at_entry": entry_ev.get("seconds_to_close") if entry_ev else None,
            "spot_at_entry": entry_ev.get("spot") if entry_ev else None,
            "strike": (entry_ev.get("strike") if entry_ev else None) or exit_ev.get("strike"),
            "distance_at_entry": entry_ev.get("distance") if entry_ev else None,
            "min_distance_required_at_entry": entry_ev.get("min_distance_required") if entry_ev else None,
            "entry_price_cents": entry_price,
            "contracts": total_contracts or (order_submitted.get("contracts") if order_submitted else None),
            "persistence_streak_at_entry": persistence_streak,
            "persistence_required_at_entry": persistence_required,
            "recent_cross_flag_at_entry": recent_cross_flag,
            "late_persistence_applied_at_entry": late_persistence_applied,
            "cutoff_seconds_at_entry": cutoff_seconds,
            "entry_band_min_cents": entry_band_min,
            "entry_band_max_cents": entry_band_max,
            # Exit
            "ts_exit": ts_exit,
            "seconds_held": seconds_held,
            "seconds_to_close_at_exit": exit_ev.get("seconds_to_close"),
            "spot_at_exit": exit_ev.get("spot"),
            "distance_at_exit": exit_ev.get("distance"),
            "min_distance_required_at_exit": exit_ev.get("min_distance_required"),
            "exit_price_cents": exit_ev.get("exit_price_cents"),
            "target_sl_price_cents": target_sl_price,
            "actual_exit_price": actual_exit_price,
            "slippage_cents": slippage_cents,
            "tob_spread_at_exit": tob_spread_at_exit,
            "yes_bid_at_exit": yes_bid,
            "yes_ask_at_exit": yes_ask,
            "no_bid_at_exit": no_bid,
            "no_ask_at_exit": no_ask,
            "pnl_pct": exit_ev.get("pnl_pct"),
            "dist_margin_at_exit": exit_ev.get("dist_margin"),
            "mfe_pct": exit_ev.get("mfe_pct"),
            "mae_pct": exit_ev.get("mae_pct"),
            "mfe_cents": mfe_cents,
            "mae_cents": mae_cents,
        })
    out.sort(key=lambda r: (r["ts_exit"], r["interval"], r["asset"], r["window_id"], r["ticker"], r["side"]))
    return out


def _find_counterfactual_for_stoploss(events, window_id: str, ticker: str, side: str):
    """Return STOPLOSS_COUNTERFACTUAL event for this stopped trade if present (hourly)."""
    for ev in events:
        if ev.get("event_type") != "STOPLOSS_COUNTERFACTUAL":
            continue
        if ev.get("window_id") != window_id or ev.get("stopped_ticker") != ticker or ev.get("stopped_side", "").lower() != (side or "").lower():
            continue
        return ev
    return None


def _format_stoploss_section(
    stoploss_trades: list, events_by_window: dict, cutoff_map: dict, header: str = "=== STOP LOSS ==="
) -> str:
    """Build full STOP LOSS report text: side-by-side table + per-trade details + counterfactual."""
    lines = []
    lines.append(header)
    if header == "=== STOP LOSS ===":
        lines.append("(Most important for fine-tuning algorithms.)")
    lines.append("")

    if not stoploss_trades:
        lines.append("No stop-loss exits in this log.")
        return "\n".join(lines)

    # Side-by-side table (entry vs exit) with seconds_held and slippage
    lines.append("--- Side-by-side (entry vs exit) ---")
    col_headers = (
        "window_id", "interval", "entry_cents", "exit_cents", "seconds_held",
        "slippage", "pnl_pct", "spot (entry->exit)", "strike", "dist_margin"
    )
    header = "  ".join(col_headers)
    lines.append(header)
    lines.append("-" * min(120, len(header)))

    for r in stoploss_trades:
        entry_p = r.get("avg_entry_price_cents") or r.get("entry_price_cents")
        entry_str = "%s¢ (avg)" % _fmt_num(entry_p) if entry_p is not None else "n/a"
        exit_p = r.get("exit_price_cents")
        exit_str = "%s¢" % _fmt_num(exit_p) if exit_p is not None else "n/a"
        sec_held = r.get("seconds_held")
        sec_str = "%ss" % sec_held if sec_held is not None else "n/a"
        slippage = r.get("slippage_cents")
        slip_str = "%s¢" % slippage if slippage is not None else "n/a"
        pnl = r.get("pnl_pct")
        pnl_str = "%.1f%%" % (pnl * 100) if pnl is not None else "n/a"
        spot_e = r.get("spot_at_entry")
        spot_x = r.get("spot_at_exit")
        spot_str = "%s -> %s" % (_fmt_num(spot_e), _fmt_num(spot_x)) if (spot_e is not None or spot_x is not None) else "n/a"
        margin = r.get("dist_margin_at_exit")
        margin_str = _fmt_num(margin) if margin is not None else "n/a"
        row = (
            r.get("window_id", ""),
            r.get("interval", ""),
            entry_str,
            exit_str,
            sec_str,
            slip_str,
            pnl_str,
            spot_str,
            _fmt_num(r.get("strike")),
            margin_str,
        )
        lines.append("  ".join(str(x) for x in row))

    lines.append("")
    lines.append("--- Per-trade details (entry + exit + risk guards) ---")

    for i, r in enumerate(stoploss_trades, 1):
        lines.append("")
        interval_display = r.get("interval", "").replace("fifteen_min", "fifteen_min")
        lines.append("  [Stop-loss #%d] %s | %s | %s %s" % (
            i, interval_display, r.get("asset"), r.get("ticker"), r.get("side")))
        lines.append("    ENTRY SUMMARY:")
        ts_fe = r.get("ts_first_entry") or r.get("ts_entry")
        ts_le = r.get("ts_last_entry") or r.get("ts_entry")
        avg_ep = r.get("avg_entry_price_cents") or r.get("entry_price_cents")
        tot_c = r.get("total_contracts") or r.get("contracts")
        lines.append("      ts_first_entry=%s  ts_last_entry=%s" % (
            (ts_fe or "")[:19], (ts_le or "")[:19]))
        lines.append("      avg_entry_price=%s¢  total_contracts=%s" % (_fmt_num(avg_ep), _fmt_num(tot_c)))
        # FILLS breakdown
        fills = r.get("fills") or []
        if fills:
            lines.append("      FILLS:")
            for f in fills:
                pc = f.get("price_cents")
                cnt = f.get("contracts", 1)
                dist = f.get("distance")
                dist_str = " (dist: %s)" % _fmt_num(dist) if dist is not None else ""
                lines.append("        - %s @ %s¢%s" % (cnt, _fmt_num(pc), dist_str))
        lines.append("      seconds_to_close=%s  spot=%s  strike=%s  distance=%s  min_distance_required=%s" % (
            _fmt_num(r.get("seconds_to_close_at_entry")), _fmt_num(r.get("spot_at_entry")),
            _fmt_num(r.get("strike")), _fmt_num(r.get("distance_at_entry")), _fmt_num(r.get("min_distance_required_at_entry"))))
        lines.append("      persistence_streak=%s  persistence_required=%s  cutoff_seconds=%s  entry_band=%s-%s" % (
            _fmt_num(r.get("persistence_streak_at_entry")), _fmt_num(r.get("persistence_required_at_entry")),
            _fmt_num(r.get("cutoff_seconds_at_entry")),
            _fmt_num(r.get("entry_band_min_cents")), _fmt_num(r.get("entry_band_max_cents"))))
        # EXIT SUMMARY with Exit Environment (TOB at exit)
        lines.append("    EXIT SUMMARY:")
        pnl = r.get("pnl_pct")
        pnl_str = "%.2f%%" % (pnl * 100) if pnl is not None else "n/a"
        sec_held = r.get("seconds_held")
        sec_str = "%ss" % sec_held if sec_held is not None else "n/a"
        tgt_sl = r.get("target_sl_price_cents")
        actual = r.get("actual_exit_price") or r.get("exit_price_cents")
        slip = r.get("slippage_cents")
        slip_str = "%s¢" % slip if slip is not None else "n/a"
        lines.append("      ts_exit=%s  exit_price_cents=%s  pnl_pct=%s" % (
            (r.get("ts_exit") or "")[:19], _fmt_num(actual), pnl_str))
        lines.append("      seconds_held=%s  target_sl_price=%s  slippage_cents=%s" % (
            sec_str, "%s¢" % _fmt_num(tgt_sl) if tgt_sl is not None else "n/a", slip_str))
        # TOB at exit (Bid=47¢ | Ask=55¢ | Spread=8¢ for the side we sold)
        side_sold = (r.get("side") or "").lower()
        if side_sold == "yes":
            bid_exit = r.get("yes_bid_at_exit")
            ask_exit = r.get("yes_ask_at_exit")
        else:
            bid_exit = r.get("no_bid_at_exit")
            ask_exit = r.get("no_ask_at_exit")
        spread = r.get("tob_spread_at_exit")
        if bid_exit is not None or ask_exit is not None or spread is not None:
            tob_parts = []
            if bid_exit is not None:
                tob_parts.append("Bid=%s¢" % bid_exit)
            if ask_exit is not None:
                tob_parts.append("Ask=%s¢" % ask_exit)
            if spread is not None:
                tob_parts.append("Spread=%s¢" % spread)
            lines.append("      TOB_at_exit: %s" % " | ".join(tob_parts))
        if r.get("mfe_pct") is not None or r.get("mae_pct") is not None or r.get("mfe_cents") is not None or r.get("mae_cents") is not None:
            lines.append("      mfe_pct=%s  mae_pct=%s  mfe_cents=%s  mae_cents=%s" % (
                "%.2f%%" % (r.get("mfe_pct") * 100) if r.get("mfe_pct") is not None else "n/a",
                "%.2f%%" % (r.get("mae_pct") * 100) if r.get("mae_pct") is not None else "n/a",
                _fmt_num(r.get("mfe_cents")), _fmt_num(r.get("mae_cents"))))

        # Counterfactual (hourly)
        key = (r.get("interval"), (r.get("asset") or "").lower(), r.get("window_id"))
        evs_for_window = events_by_window.get(key) or []
        cf = _find_counterfactual_for_stoploss(
            evs_for_window, r.get("window_id"), r.get("ticker"), r.get("side"))
        if cf:
            cands = cf.get("candidates") or []
            pass_c = sum(1 for c in cands if c.get("decision") == "PASS")
            fail_c = sum(1 for c in cands if c.get("decision") == "FAIL")
            fail_reasons = defaultdict(int)
            for c in cands:
                if c.get("decision") == "FAIL":
                    fail_reasons[c.get("fail_reason") or "unknown"] += 1
            lines.append("    STOPLOSS_COUNTERFACTUAL (hourly): candidates=%d PASS=%d FAIL=%d  fail_reasons=%s" % (
                len(cands), pass_c, fail_c, dict(fail_reasons)))

    lines.append("")
    lines.append("--- End STOP LOSS ---")
    return "\n".join(lines)


def _fmt_num(val):
    """Format number for report; None -> 'n/a'."""
    if val is None:
        return "n/a"
    if isinstance(val, float):
        return "%.2f" % val if val == val else "n/a"
    return str(val)


def build_stoploss_report_for_window(
    log_path: str,
    start_utc: float,
    end_utc: float,
    project_root: Optional[Path] = None,
) -> str:
    """
    Build STOP LOSS section text for events in [start_utc, end_utc].
    For use by hourly_ledger_job and email report integration.
    """
    proj = project_root or Path(__file__).resolve().parent.parent
    events = list(parse_log_lines(log_path))
    events = [e for e in events if start_utc <= _parse_ts(e.get("ts") or "") <= end_utc]
    groups = group_by_window(events)
    cutoff_map = _load_cutoff_seconds(proj)
    stoploss_trades = _collect_stoploss_trades(events)
    return _format_stoploss_section(stoploss_trades, groups, cutoff_map)


def build_stoploss_reports_by_interval(
    log_path: str,
    start_utc: float,
    end_utc: float,
    project_root: Optional[Path] = None,
) -> dict:
    """
    Build STOP LOSS report text per interval. Returns {interval: report_text}.
    Intervals: fifteen_min, hourly, daily, weekly.
    """
    proj = project_root or Path(__file__).resolve().parent.parent
    events = list(parse_log_lines(log_path))
    events = [e for e in events if start_utc <= _parse_ts(e.get("ts") or "") <= end_utc]
    groups = group_by_window(events)
    cutoff_map = _load_cutoff_seconds(proj)
    stoploss_trades = _collect_stoploss_trades(events)
    out = {}
    for iv in ("fifteen_min", "hourly", "daily", "weekly"):
        filtered = [r for r in stoploss_trades if (r.get("interval") or "hourly") == iv]
        header = "--- STOP LOSS ---"  # sub-header when embedded in interval section
        out[iv] = _format_stoploss_section(filtered, groups, cutoff_map, header=header)
    return out


def _build_window_row(key, events, a: dict) -> dict:
    """Build window-level compact row."""
    interval, asset, window_id = key
    top3 = sorted(a.get("skip_reasons", {}).items(), key=lambda x: -x[1])[:3]
    top3_str = ",".join("%s:%d" % (r, c) for r, c in top3)
    denom = a.get("rate_denom", 1) or 1
    dist_total = a.get("dist_total", 0) or 1
    persist_total = a.get("persist_total", 0) or 1
    return {
        "interval": interval,
        "asset": asset,
        "window_id": window_id,
        "eval_count": a.get("eval_count", 0),
        "entry_decisions": a.get("entry_decisions", 0),
        "orders_submitted": a.get("orders_submitted", 0),
        "orders_failed": sum(a.get("order_failed_reasons", {}).values()),
        "orders_skipped": sum(a.get("order_skipped_reasons", {}).values()),
        "exits_total": a.get("counts", {}).get("EXIT", 0),
        "exits_stoploss": a.get("exit_reasons", {}).get(EXIT_STOPLOSS, 0),
        "exits_takeprofit": a.get("exit_reasons", {}).get(EXIT_TAKE_PROFIT, 0),
        "exits_hardflip": a.get("exit_reasons", {}).get(EXIT_HARD_FLIP, 0),
        "top_3_skip_reasons": top3_str,
        "strike_known_rate": round(a.get("strike_known_count", 0) / denom, 2) if denom else 0,
        "dist_fail_rate": round(a.get("dist_fail_count", 0) / dist_total, 2) if dist_total else 0,
        "persist_fail_rate": round(a.get("persist_fail_count", 0) / persist_total, 2) if persist_total else 0,
        "cutoff_block_rate": round(a.get("cutoff_block_count", 0) / denom, 2) if denom else 0,
        "cap_block_rate": round(a.get("cap_block_count", 0) / denom, 2) if denom else 0,
        "top_1_error": a.get("top_1_error", ""),
    }


TRADE_COLS = [
    "ts_first_seen", "ts_entry_decision", "ts_order_submitted", "ts_exit",
    "interval", "asset", "window_id", "ticker", "side", "status",
    "entry_price_cents", "exit_reason_code", "pnl_pct",
    "fail_or_skip_reason", "entry_reason_compact", "stoploss_context",
    "block_reason",
    "distance", "min_distance_required", "dist_margin", "dist_status",
    "persistence_streak", "persistence_required", "persist_margin", "persist_status",
    "seconds_to_close", "cutoff_seconds", "cutoff_margin", "cutoff_status",
    "cap_current_total", "cap_max_total", "cap_margin_total",
    "cap_current_ticker", "cap_max_ticker", "cap_margin_ticker",
    "strike", "strike_known",
    "exec_status", "exec_reason",
]
WINDOW_COLS = [
    "interval", "asset", "window_id", "eval_count", "entry_decisions",
    "orders_submitted", "orders_failed", "orders_skipped",
    "exits_total", "exits_stoploss", "exits_takeprofit", "exits_hardflip",
    "top_3_skip_reasons",
    "strike_known_rate", "dist_fail_rate", "persist_fail_rate",
    "cutoff_block_rate", "cap_block_rate", "top_1_error",
]


def _norm_key(k):
    """Normalize (interval, asset, window_id) for comparison (asset lowercase)."""
    return (k[0], (k[1] or "").lower(), k[2] or "")


def _bucket_margin(val, buckets=((-20, -10), (-10, -5), (-5, 0), (0, 5))):
    """Bucket a numeric margin value. Returns label like '-10..-5' or 'other'."""
    if val is None or val == "":
        return None
    try:
        v = float(val)
    except (TypeError, ValueError):
        return None
    for lo, hi in buckets:
        if lo <= v < hi:
            return "%d..%d" % (lo, hi)
    if v >= 5:
        return "5+"
    if v < -20:
        return "<-20"
    return "other"


def _compute_block_aggregations(trade_rows):
    """
    Per (interval, asset): top 5 block_reason, histograms for SKIP_DISTANCE_BUFFER,
    SKIP_PERSISTENCE, and cutoff blocks.
    """
    by_ia = defaultdict(lambda: {"block_reasons": defaultdict(int), "dist_margins": [], "persist_margins": [], "cutoff_margins": []})
    for r in trade_rows:
        interval = r.get("interval", "hourly")
        asset = (r.get("asset") or "").lower()
        key = (interval, asset)
        br = r.get("block_reason") or ""
        if br:
            by_ia[key]["block_reasons"][br] += 1
            if br == "SKIP_DISTANCE_BUFFER":
                dm = r.get("dist_margin")
                if dm is not None and dm != "":
                    try:
                        by_ia[key]["dist_margins"].append(float(dm))
                    except (TypeError, ValueError):
                        pass
            elif br == "SKIP_PERSISTENCE":
                pm = r.get("persist_margin")
                if pm is not None and pm != "":
                    try:
                        by_ia[key]["persist_margins"].append(float(pm))
                    except (TypeError, ValueError):
                        pass
            elif br in ("SKIP_NO_NEW_ENTRY_CUTOFF", "cutoff_seconds"):
                cm = r.get("cutoff_margin")
                if cm is not None and cm != "":
                    try:
                        by_ia[key]["cutoff_margins"].append(float(cm))
                    except (TypeError, ValueError):
                        pass

    out = {}
    for (interval, asset), data in by_ia.items():
        top5 = sorted(data["block_reasons"].items(), key=lambda x: -x[1])[:5]
        dist_buckets = defaultdict(int)
        for v in data["dist_margins"]:
            b = _bucket_margin(v)
            if b:
                dist_buckets[b] += 1
        persist_buckets = defaultdict(int)
        for v in data["persist_margins"]:
            b = _bucket_margin(v)
            if b:
                persist_buckets[b] += 1
        cutoff_buckets = defaultdict(int)
        for v in data["cutoff_margins"]:
            b = _bucket_margin(v)
            if b:
                cutoff_buckets[b] += 1
        ikey = interval
        if ikey not in out:
            out[ikey] = {}
        out[ikey][asset] = {
            "top_5_block_reason": [{"reason": r, "count": c} for r, c in top5],
            "SKIP_DISTANCE_BUFFER_dist_margin_buckets": dict(dist_buckets),
            "SKIP_PERSISTENCE_persist_margin_buckets": dict(persist_buckets),
            "cutoff_cutoff_margin_buckets": dict(cutoff_buckets),
        }
    return out


def build_summary_for_window_keys(events, window_keys):
    """
    Build trade rows, window rows, and totals for a given set of windows.
    window_keys: set of (interval, asset, window_id). Asset matching is case-insensitive.
    Returns (trade_rows, window_rows, totals_by_interval).
    totals_by_interval: {"hourly": {...}, "fifteen_min": {...}} with eval_count, entry_decisions,
    orders_submitted, orders_failed, orders_skipped, exits_stoploss.
    """
    groups = group_by_window(events)
    want = {_norm_key(k) for k in window_keys}
    filtered_groups = {k: groups[k] for k in groups if _norm_key(k) in want}
    filtered_events = []
    for k in filtered_groups:
        filtered_events.extend(filtered_groups[k])

    lifecycles = _build_trade_lifecycles(filtered_events)
    trade_rows = []
    for key, evs in lifecycles.items():
        if _norm_key(key) not in want:
            continue
        interval = key[0]
        row = _trade_row_from_lifecycle(key, evs, interval)
        trade_rows.append(row)
    trade_rows.sort(key=lambda r: (r["ts_first_seen"], r["interval"], r["asset"], r["window_id"], r["ticker"], r["side"]))

    window_rows = []
    for key in sorted(filtered_groups.keys(), key=lambda k: (k[1], k[2])):
        evs = filtered_groups[key]
        a = analyze_window(evs)
        window_rows.append(_build_window_row(key, evs, a))

    totals_by_interval = defaultdict(lambda: {
        "eval_count": 0, "entry_decisions": 0, "orders_submitted": 0,
        "orders_failed": 0, "orders_skipped": 0, "exits_stoploss": 0,
    })
    for r in window_rows:
        iv = r.get("interval", "hourly")
        totals_by_interval[iv]["eval_count"] += r.get("eval_count", 0)
        totals_by_interval[iv]["entry_decisions"] += r.get("entry_decisions", 0)
        totals_by_interval[iv]["orders_submitted"] += r.get("orders_submitted", 0)
        totals_by_interval[iv]["orders_failed"] += r.get("orders_failed", 0)
        totals_by_interval[iv]["orders_skipped"] += r.get("orders_skipped", 0)
        totals_by_interval[iv]["exits_stoploss"] += r.get("exits_stoploss", 0)

    block_aggregations = _compute_block_aggregations(trade_rows)
    return trade_rows, window_rows, dict(totals_by_interval), block_aggregations


def _print_summary_mode(events, groups, fmt: str, delimiter: str):
    """Print TRADE_ROWS and WINDOW_ROWS in TSV/CSV."""
    lifecycles = _build_trade_lifecycles(events)
    trade_rows = []
    for key, evs in lifecycles.items():
        interval = key[0]
        row = _trade_row_from_lifecycle(key, evs, interval)
        trade_rows.append(row)
    trade_rows.sort(key=lambda r: (r["ts_first_seen"], r["interval"], r["asset"], r["window_id"], r["ticker"], r["side"]))
    trade_cols = TRADE_COLS
    window_rows = []
    for key in sorted(groups.keys(), key=lambda k: (k[1], k[2])):
        evs = groups[key]
        a = analyze_window(evs)
        window_rows.append(_build_window_row(key, evs, a))
    window_cols = WINDOW_COLS
    if fmt == "text":
        print("\n=== TRADE_ROWS ===")
        for r in trade_rows:
            print(delimiter.join(str(r.get(c, "")) for c in trade_cols))
        print("\n=== WINDOW_ROWS ===")
        for r in window_rows:
            print(delimiter.join(str(r.get(c, "")) for c in window_cols))
    else:
        w = csv.writer(sys.stdout, delimiter=delimiter, lineterminator="\n")
        print("=== TRADE_ROWS ===")
        w.writerow(trade_cols)
        for r in trade_rows:
            w.writerow([r.get(c, "") for c in trade_cols])
        print("=== WINDOW_ROWS ===")
        w.writerow(window_cols)
        for r in window_rows:
            w.writerow([r.get(c, "") for c in window_cols])


def _collect_entries_to_explain(events, interval: str) -> list:
    """
    Identify entries to explain. Prefer ORDER_SUBMITTED; fallback to ENTER/ENTER_DECISION.
    Dedupe: unique order_id/client_order_id when present; else (ticker, side, ts_rounded).
    Returns list of merged entry dicts (ORDER_SUBMITTED augmented with ENTER data when available).
    """
    order_events = [e for e in events if e.get("event_type") == "ORDER_SUBMITTED" and (e.get("order_id") or e.get("client_order_id"))]
    enter_events = [e for e in events if e.get("event_type") in ("ENTER", "ENTER_DECISION")]
    seen_ids = set()
    seen_keys = set()  # (ticker, side, ts_rounded) - ENTERs already merged or added
    entries = []

    def _ts_rounded(ev):
        ts = ev.get("ts") or ""
        if "T" in ts and "." in ts:
            return ts.split(".")[0]
        return ts[:19] if len(ts) >= 19 else ts

    # Prefer ORDER_SUBMITTED - merge with latest ENTER for same ticker+side
    for oev in order_events:
        oid = oev.get("order_id") or oev.get("client_order_id")
        if oid and oid in seen_ids:
            continue
        if oid:
            seen_ids.add(oid)
        ticker = oev.get("ticker") or ""
        side = oev.get("side") or ""
        matches = [e for e in enter_events if e.get("ticker") == ticker and e.get("side") == side]
        base = dict(oev)
        merged_enter = None
        if matches:
            merged_enter = max(matches, key=lambda e: (e.get("ts") or "")[:19])
            for k, v in merged_enter.items():
                if k not in base or base[k] is None:
                    base[k] = v
        entries.append(base)
        if merged_enter:
            seen_keys.add((ticker, side, _ts_rounded(merged_enter)))

    # Add ENTER/ENTER_DECISION not covered by ORDER_SUBMITTED
    for eev in enter_events:
        key = (eev.get("ticker"), eev.get("side"), _ts_rounded(eev))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        entries.append(dict(eev))

    entries.sort(key=lambda e: e.get("ts") or "")
    return entries


def _format_entry_explanation_block(entry: dict, cutoff_seconds: int, interval: str, window_id: str) -> str:
    """Build compact explanation block for one entry (A through E)."""
    lines = []
    ts = (entry.get("ts") or "")[:19]
    ticker = entry.get("ticker") or "?"
    side = (entry.get("side") or "?").upper()
    order_id = entry.get("order_id") or entry.get("client_order_id")
    price_cents = entry.get("price_cents") or (entry.get("yes_price_cents") if side == "YES" else entry.get("no_price_cents"))
    contracts = entry.get("contracts")

    # A) Identity
    lines.append("    [Identity] ts=%s ticker=%s side=%s window_id=%s" % (ts, ticker, side, window_id))
    if order_id:
        lines.append("      order_id=%s client_order_id=%s" % (entry.get("order_id", ""), entry.get("client_order_id", "")))
    lines.append("      price_cents=%s contracts=%s" % (price_cents, contracts))

    # B) Signal + Band
    lo = entry.get("entry_band_min_cents")
    hi = entry.get("entry_band_max_cents")
    yes_p = entry.get("yes_price_cents")
    no_p = entry.get("no_price_cents")
    side_price = yes_p if side == "YES" else no_p
    in_band = (lo is not None and hi is not None and side_price is not None and lo <= side_price <= hi) if side_price is not None else None
    lines.append("    [Signal+Band] entry_band=%s-%s yes_price=%s no_price=%s in_band_side_price=%s" % (lo, hi, yes_p, no_p, in_band))

    # C) Time + Cutoff
    sec = entry.get("seconds_to_close")
    inside_cutoff = (sec is not None and sec <= cutoff_seconds) if sec is not None else None
    allowed_reversal = entry.get("reason_code") == "REVERSAL_ENTRY_ALLOWED"
    lines.append("    [Time+Cutoff] seconds_to_close=%s cutoff_seconds=%s inside_cutoff=%s allowed_due_to_reversal=%s" % (
        sec, cutoff_seconds, inside_cutoff, allowed_reversal))

    # D) Guardrails
    streak = entry.get("persistence_streak")
    required = entry.get("persistence_required_effective") or entry.get("persistence_required")
    recent_cross = entry.get("recent_cross_flag")
    spot = entry.get("spot")
    strike = entry.get("strike")
    distance = entry.get("distance")
    min_dist = entry.get("min_distance_required")
    disabled = entry.get("disabled_until_expiry")
    stopouts = entry.get("stopouts_count")
    cap = entry.get("cap_state") or {}
    lines.append("    [Guardrails] persistence_streak=%s persistence_required=%s recent_cross_flag=%s" % (streak, required, recent_cross))
    lines.append("      spot=%s strike=%s distance=%s min_distance_required=%s" % (spot, strike, distance, min_dist))
    lines.append("      disabled_until_expiry=%s stopouts_count=%s" % (disabled, stopouts))
    if interval == "fifteen_min":
        lines.append("      reversal_enabled=%s stopped_side=%s reversals_taken=%s" % (
            entry.get("reversal_enabled"), entry.get("stopped_side"), entry.get("reversals_taken")))
    else:
        lines.append("      anchor_yes=%s anchor_no=%s anchor_locked=%s" % (
            entry.get("anchor_yes_ticker"), entry.get("anchor_no_ticker"), entry.get("anchor_locked")))
    if cap:
        lines.append("      cap_state: current_total=%s current_ticker=%s max_total=%s max_per_ticker=%s" % (
            cap.get("current_total_orders"), cap.get("current_ticker_orders"),
            cap.get("max_total_orders_per_window"), cap.get("max_orders_per_ticker")))

    # E) Decision summary
    missing = []
    parts = []
    if lo is not None and hi is not None and side_price is not None:
        parts.append("in_band" if (lo <= side_price <= hi) else "NOT_in_band")
    else:
        missing.append("entry_band/side_price")
    if streak is not None and required is not None:
        parts.append("persistence_met" if streak >= required else "persistence_NOT_met")
    else:
        missing.append("persistence")
    if distance is not None and min_dist is not None:
        parts.append("distance_met" if distance >= min_dist else "distance_NOT_met")
    elif interval == "hourly" and strike:
        missing.append("distance")
    else:
        parts.append("distance_N/A")
    if disabled is not None:
        parts.append("not_disabled" if not disabled else "DISABLED")
    if cap:
        ct, mt = cap.get("current_total_orders"), cap.get("max_total_orders_per_window")
        parts.append("caps_ok" if (ct is not None and mt is not None and ct < mt) else "caps_check")
    if inside_cutoff is not None:
        parts.append("(not_in_cutoff OR reversal_allowed)" if (not inside_cutoff or allowed_reversal) else "INSIDE_CUTOFF_BLOCKED")
    else:
        missing.append("cutoff")
    summary = " + ".join(parts) if parts else "MISSING_FIELDS"
    if missing:
        summary += " | MISSING_FIELDS: %s" % ",".join(missing)
    lines.append("    [Decision] ENTRY allowed because: %s" % summary)
    return "\n".join(lines)


def _print_entry_explanations(events, interval: str, asset: str, window_id: str, cutoff_map: dict) -> None:
    """Print ENTRY EXPLANATIONS section for one window."""
    cutoff_key = "fifteen_min" if interval == "fifteen_min" else "hourly"
    cutoff_seconds = cutoff_map.get(cutoff_key, 40 if interval == "fifteen_min" else 90)
    entries = _collect_entries_to_explain(events, interval)
    if not entries:
        print("  ENTRY EXPLANATIONS: (none)")
        return

    print("  ENTRY EXPLANATIONS:")
    for i, entry in enumerate(entries, 1):
        print("  --- Entry %d ---" % i)
        print(_format_entry_explanation_block(entry, cutoff_seconds, interval, window_id))

    # Window-level rollup
    submitted_ids = set()
    by_side = defaultdict(int)
    by_ticker = defaultdict(int)
    ticker_side_counts = defaultdict(int)
    for e in entries:
        oid = e.get("order_id") or e.get("client_order_id")
        if oid and e.get("event_type") == "ORDER_SUBMITTED":
            submitted_ids.add(oid)
        ticker = e.get("ticker") or "?"
        side = (e.get("side") or "?").upper()
        by_side[side] += 1
        by_ticker[ticker] += 1
        ticker_side_counts[(ticker, side)] += 1

    entries_count = len(submitted_ids) if submitted_ids else len(entries)
    print("  [Rollup] entries_count=%d entries_by_side=%s entries_by_ticker=%s" % (
        entries_count,
        dict(by_side),
        dict(sorted(by_ticker.items(), key=lambda x: -x[1])[:3]),
    ))
    dups = [(k, v) for k, v in ticker_side_counts.items() if v > 1]
    if dups:
        print("  POTENTIAL_DUPLICATE_ENTRIES: %s" % dups)


def analyze_window(events):
    """Compute summary stats for one window's events."""
    counts = defaultdict(int)
    skip_reasons = defaultdict(int)
    exit_reasons = defaultdict(int)
    order_failed_reasons = defaultdict(int)
    order_skipped_reasons = defaultdict(int)
    order_submitted_ids = set()  # unique order_id or client_order_id for ORDER_SUBMITTED
    distance_buffer_skips = []
    reversal_sequence = []
    cutoff_attempts = 0
    cutoff_allowed_reversal = 0
    has_window_summary = False
    for ev in events:
        et = ev.get("event_type") or ev.get("event") or "EVAL"
        counts[et] += 1
        if et == "WINDOW_SUMMARY":
            has_window_summary = True
        reason = ev.get("reason_code")
        if et == "SKIP" and reason:
            skip_reasons[reason] += 1
            if reason == "SKIP_DISTANCE_BUFFER":
                d = ev.get("distance")
                m = ev.get("min_distance_required")
                if d is not None or m is not None:
                    distance_buffer_skips.append({"distance": d, "min_required": m})
            if reason == "SKIP_NO_NEW_ENTRY_CUTOFF":
                cutoff_attempts += 1
        if et == "EXIT" and reason:
            exit_reasons[reason] += 1
        if et in ("ENTER", "ENTER_DECISION") and reason == "REVERSAL_ENTRY_ALLOWED":
            cutoff_allowed_reversal += 1
            reversal_sequence.append(ev)
        if et == "EXIT" and reason == "EXIT_STOPLOSS":
            reversal_sequence.append(ev)
        if et == "ORDER_SUBMITTED":
            oid = ev.get("order_id") or ev.get("client_order_id")
            if oid:
                order_submitted_ids.add(oid)
        if et == "ORDER_FAILED" and reason:
            order_failed_reasons[reason] += 1
        if et == "ORDER_SKIPPED" and reason:
            order_skipped_reasons[reason] += 1

    # Derived rates (from EVAL/SKIP/ENTER/ENTER_DECISION with ticker+side)
    strike_known_count = 0
    dist_fail_count = 0
    dist_total = 0
    persist_fail_count = 0
    persist_total = 0
    cutoff_block_count = 0
    rate_denom = 0
    for ev in events:
        et = ev.get("event_type") or ev.get("event") or ""
        if et not in ("EVAL", "SKIP", "ENTER", "ENTER_DECISION"):
            continue
        if ev.get("ticker") is None and ev.get("side") is None:
            continue
        rate_denom += 1
        strike_val = ev.get("strike")
        if strike_val is not None and (strike_val or 0) > 0:
            strike_known_count += 1
        dist = ev.get("distance")
        min_dist = ev.get("min_distance_required")
        if dist is not None and min_dist is not None:
            dist_total += 1
            if dist < min_dist:
                dist_fail_count += 1
        streak = ev.get("persistence_streak") or ev.get("streak")
        required = ev.get("persistence_required_effective") or ev.get("persistence_required") or ev.get("required_polls")
        if streak is not None and required is not None:
            persist_total += 1
            if streak < required:
                persist_fail_count += 1

    cutoff_block_count = skip_reasons.get("SKIP_NO_NEW_ENTRY_CUTOFF", 0) + skip_reasons.get("cutoff_seconds", 0)
    cap_block_count = sum(order_skipped_reasons.values())
    top_1_error = ""
    if order_failed_reasons:
        top_1_error = max(order_failed_reasons.keys(), key=lambda k: order_failed_reasons[k])

    # Sort reversal sequence by ts
    reversal_sequence.sort(key=lambda x: x.get("ts") or "")
    # EVAL: use explicit count if present, else derive as SKIP + (ENTER or ENTER_DECISION)
    explicit_eval = counts.get("EVAL", 0)
    entry_decisions = counts.get("ENTER", 0) + counts.get("ENTER_DECISION", 0)
    derived_eval = counts.get("SKIP", 0) + entry_decisions
    eval_count = explicit_eval if explicit_eval > 0 else derived_eval
    return {
        "counts": dict(counts),
        "eval_count": eval_count,
        "entry_decisions": entry_decisions,
        "orders_submitted": len(order_submitted_ids),
        "order_failed_reasons": dict(order_failed_reasons),
        "order_skipped_reasons": dict(order_skipped_reasons),
        "window_status": "COMPLETE" if has_window_summary else "IN_PROGRESS",
        "skip_reasons": dict(skip_reasons),
        "exit_reasons": dict(exit_reasons),
        "distance_buffer_skips": distance_buffer_skips,
        "reversal_sequence": reversal_sequence,
        "cutoff_attempts": cutoff_attempts,
        "cutoff_allowed_reversal": cutoff_allowed_reversal,
        "strike_known_count": strike_known_count,
        "rate_denom": rate_denom or eval_count or 1,
        "dist_fail_count": dist_fail_count,
        "dist_total": dist_total,
        "persist_fail_count": persist_fail_count,
        "persist_total": persist_total,
        "cutoff_block_count": cutoff_block_count,
        "cap_block_count": cap_block_count,
        "top_1_error": top_1_error,
    }


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Analyze bot log (JSON lines). Summary mode: compact TSV/CSV. Detail mode: verbose per-window."
    )
    parser.add_argument("log_path", nargs="?", default="logs/bot.log", help="Path to bot log")
    parser.add_argument("--summary", action="store_true", help="Compact summary tables (TRADE_ROWS, WINDOW_ROWS)")
    parser.add_argument("--detail", action="store_true", help="Verbose per-window output (default when no mode)")
    parser.add_argument("--format", choices=("tsv", "csv", "text"), default="tsv", help="Output format (default: tsv)")
    parser.add_argument("--since-hours", type=float, metavar="N", help="Include events from last N hours")
    parser.add_argument("--since", metavar="ISO", help="Include events since YYYY-MM-DDTHH:MM:SS")
    return parser.parse_args()


def main():
    args = _parse_args()
    project_root = Path(__file__).resolve().parent.parent
    log_path = args.log_path
    if not Path(log_path).is_absolute():
        log_path = project_root / log_path

    events = list(parse_log_lines(str(log_path)))
    since_ts = 0.0
    if args.since:
        since_ts = _parse_ts(args.since)
    elif args.since_hours:
        delta = timedelta(hours=args.since_hours)
        since_ts = (datetime.now(timezone.utc) - delta).timestamp()
    if since_ts > 0:
        events = _filter_events_since(events, since_ts)

    groups = group_by_window(events)
    cutoff_map = _load_cutoff_seconds(project_root)

    use_summary = args.summary
    if not args.summary and not args.detail:
        use_summary = False  # default to detail for backward compat

    if use_summary:
        delimiter = "\t" if args.format == "tsv" else ","
        _print_summary_mode(events, groups, args.format, delimiter)
        return

    # Separate fifteen_min and hourly (detail mode)
    fifteen_min_keys = sorted([k for k in groups if k[0] == "fifteen_min"], key=lambda x: (x[1], x[2]))
    hourly_keys = sorted([k for k in groups if k[0] == "hourly"], key=lambda x: (x[1], x[2]))

    # ---------- STOP LOSS (first: most important for fine-tuning) ----------
    stoploss_trades = _collect_stoploss_trades(events)
    stoploss_report_text = _format_stoploss_section(stoploss_trades, groups, cutoff_map)
    print("\n" + "=" * 70)
    print("  STOP LOSS")
    print("=" * 70)
    print(stoploss_report_text)

    # Write stop-loss report to file for attachment / archival
    reports_dir = project_root / "reports"
    try:
        reports_dir.mkdir(parents=True, exist_ok=True)
        report_path = reports_dir / "stop_loss_report.txt"
        report_path.write_text(stoploss_report_text, encoding="utf-8")
        print("\n  [Written: %s]" % report_path)
    except Exception as e:
        print("\n  [Could not write stop_loss_report.txt: %s]" % e)

    def section_title(title):
        return "\n" + "=" * 70 + "\n  " + title + "\n" + "=" * 70

    def window_header(interval, asset, window_id):
        return "\n--- %s | %s | %s ---" % (interval, asset or "?", window_id or "?")

    # ---------- fifteen_min ----------
    print(section_title("FIFTEEN_MIN WINDOWS"))
    for key in fifteen_min_keys:
        interval, asset, window_id = key
        evs = groups[key]
        a = analyze_window(evs)
        print(window_header(interval, asset, window_id))
        print("  Window status: %s" % a["window_status"])
        print("  Counts: EVAL=%d SKIP=%d ENTER_DECISION=%d EXIT=%d WINDOW_SUMMARY=%d" % (
            a["eval_count"],
            a["counts"].get("SKIP", 0),
            a["entry_decisions"],
            a["counts"].get("EXIT", 0),
            a["counts"].get("WINDOW_SUMMARY", 0),
        ))
        print("  Reconciliation: Entry decisions=%d Orders submitted=%d Orders failed=%d Orders skipped=%d" % (
            a["entry_decisions"],
            a["orders_submitted"],
            sum(a["order_failed_reasons"].values()),
            sum(a["order_skipped_reasons"].values()),
        ))
        if a["order_failed_reasons"] or a["order_skipped_reasons"]:
            if a["order_failed_reasons"]:
                print("  Orders failed by reason:")
                for r, c in a["order_failed_reasons"].items():
                    print("    %s: %d" % (r, c))
            if a["order_skipped_reasons"]:
                print("  Orders skipped by reason:")
                for r, c in a["order_skipped_reasons"].items():
                    print("    %s: %d" % (r, c))
        if a["skip_reasons"]:
            print("  Top skip reasons (desc):")
            for r, c in sorted(a["skip_reasons"].items(), key=lambda x: -x[1]):
                print("    %s: %d" % (r, c))
        if a["exit_reasons"]:
            print("  Exit reasons:")
            for r, c in a["exit_reasons"].items():
                print("    %s: %d" % (r, c))
        if a["reversal_sequence"]:
            print("  Reversal sequence (STOPLOSS -> REVERSAL_ENTRY):")
            for ev in a["reversal_sequence"]:
                et = ev.get("event_type")
                reason = ev.get("reason_code")
                ts = (ev.get("ts") or "")[:19]
                print("    %s | %s | %s" % (ts, et, reason or ""))
        if a["distance_buffer_skips"]:
            dists = [x["distance"] for x in a["distance_buffer_skips"] if x["distance"] is not None]
            mins = [x["min_required"] for x in a["distance_buffer_skips"] if x["min_required"] is not None]
            print("  Distance buffer (SKIP_DISTANCE_BUFFER): count=%d" % len(a["distance_buffer_skips"]))
            if dists:
                print("    distance min=%.2f avg=%.2f max=%.2f" % (min(dists), sum(dists) / len(dists), max(dists)))
            if mins:
                print("    min_distance_required min=%.2f avg=%.2f max=%.2f" % (min(mins), sum(mins) / len(mins), max(mins)))
        if a["cutoff_attempts"] > 0 or a["cutoff_allowed_reversal"] > 0:
            print("  Cutoff: attempts_inside_cutoff=%d allowed_due_to_reversal=%d" % (
                a["cutoff_attempts"], a["cutoff_allowed_reversal"]))
        # WINDOW_SUMMARY line if present
        for ev in evs:
            if ev.get("event_type") == "WINDOW_SUMMARY":
                print("  [WINDOW_SUMMARY] total_evals=%s total_enters=%s total_exits=%s total_skips=%s stopouts=%s reversals_taken_total=%s" % (
                    ev.get("total_evals"), ev.get("total_enters"), ev.get("total_exits"),
                    ev.get("total_skips"), ev.get("stopouts_count_total"), ev.get("reversals_taken_total")))
        _print_entry_explanations(evs, "fifteen_min", asset, window_id, cutoff_map)

    # ---------- hourly ----------
    print(section_title("HOURLY WINDOWS"))
    for key in hourly_keys:
        interval, asset, window_id = key
        evs = groups[key]
        a = analyze_window(evs)
        print(window_header(interval, asset, window_id))
        print("  Window status: %s" % a["window_status"])
        print("  Counts: EVAL=%d SKIP=%d ENTER_DECISION=%d EXIT=%d WINDOW_SUMMARY=%d" % (
            a["eval_count"],
            a["counts"].get("SKIP", 0),
            a["entry_decisions"],
            a["counts"].get("EXIT", 0),
            a["counts"].get("WINDOW_SUMMARY", 0),
        ))
        print("  Reconciliation: Entry decisions=%d Orders submitted=%d Orders failed=%d Orders skipped=%d" % (
            a["entry_decisions"],
            a["orders_submitted"],
            sum(a["order_failed_reasons"].values()),
            sum(a["order_skipped_reasons"].values()),
        ))
        if a["order_failed_reasons"] or a["order_skipped_reasons"]:
            if a["order_failed_reasons"]:
                print("  Orders failed by reason:")
                for r, c in a["order_failed_reasons"].items():
                    print("    %s: %d" % (r, c))
            if a["order_skipped_reasons"]:
                print("  Orders skipped by reason:")
                for r, c in a["order_skipped_reasons"].items():
                    print("    %s: %d" % (r, c))
        if a["skip_reasons"]:
            print("  Top skip reasons (desc):")
            for r, c in sorted(a["skip_reasons"].items(), key=lambda x: -x[1]):
                print("    %s: %d" % (r, c))
        if a["exit_reasons"]:
            print("  Exit reasons:")
            for r, c in a["exit_reasons"].items():
                print("    %s: %d" % (r, c))
        if a["distance_buffer_skips"]:
            dists = [x["distance"] for x in a["distance_buffer_skips"] if x["distance"] is not None]
            mins = [x["min_required"] for x in a["distance_buffer_skips"] if x["min_required"] is not None]
            print("  Distance buffer (SKIP_DISTANCE_BUFFER): count=%d" % len(a["distance_buffer_skips"]))
            if dists:
                print("    distance min=%.2f avg=%.2f max=%.2f" % (min(dists), sum(dists) / len(dists), max(dists)))
        for ev in evs:
            if ev.get("event_type") == "WINDOW_SUMMARY":
                print("  [WINDOW_SUMMARY] total_evals=%s total_enters=%s total_exits=%s total_skips=%s stopouts=%s anchors_used=%s" % (
                    ev.get("total_evals"), ev.get("total_enters"), ev.get("total_exits"),
                    ev.get("total_skips"), ev.get("stopouts_count_total"), ev.get("anchors_used")))
        # STOPLOSS_COUNTERFACTUAL (hourly only)
        cf_evs = [e for e in evs if e.get("event_type") == "STOPLOSS_COUNTERFACTUAL"]
        for cf in cf_evs:
            cands = cf.get("candidates") or []
            pass_count = sum(1 for c in cands if c.get("decision") == "PASS")
            fail_count = sum(1 for c in cands if c.get("decision") == "FAIL")
            fail_reasons = defaultdict(int)
            for c in cands:
                fr = c.get("fail_reason") or "unknown"
                if c.get("decision") == "FAIL":
                    fail_reasons[fr] += 1
            print("  [STOPLOSS_COUNTERFACTUAL] stopped=%s side=%s pnl=%.2f%% candidates=%d PASS=%d FAIL=%d" % (
                cf.get("stopped_ticker", "?"), cf.get("stopped_side", "?"),
                (cf.get("pnl_pct") or 0) * 100, len(cands), pass_count, fail_count))
            if fail_reasons:
                print("    fail_reasons: %s" % dict(fail_reasons))
        _print_entry_explanations(evs, "hourly", asset, window_id, cutoff_map)

    print("\n" + "=" * 70)
    print("  Total windows: fifteen_min=%d hourly=%d" % (len(fifteen_min_keys), len(hourly_keys)))
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
