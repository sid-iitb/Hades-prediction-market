#!/usr/bin/env python3
"""
Generate a report from Last-90s strategy JSON logs (placements + stop-losses).

Reads bot log for LAST_90S_PLACEMENT_REPORT and LAST_90S_STOP_LOSS_REPORT events,
then prints (and optionally writes) a human-readable report for tuning
window_seconds, min_bid_cents, stop_loss_pct, etc.

By default, queries Kalshi for each placement and skip ticker and adds final_outcome
(yes/no/scalar/not_settled) to last_90s_placements.tsv and last_90s_skips.tsv. Use --no-fetch-final-outcomes to skip.

Usage:
  python tools/last_90s_report.py [path/to/bot.log]   # default: fetch final_outcome from Kalshi
  python tools/last_90s_report.py --dry-run
  python tools/last_90s_report.py --output reports/last_90s_report.txt
  python tools/last_90s_report.py --since-hours 24
  python tools/last_90s_report.py --no-fetch-final-outcomes  # skip Kalshi lookup (faster, final_outcome=n/a)
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterator
from zoneinfo import ZoneInfo

EVENT_PLACEMENT = "LAST_90S_PLACEMENT_REPORT"
EVENT_STOP_LOSS = "LAST_90S_STOP_LOSS_REPORT"

# Human-readable lines (when JSON report wasn't emitted, e.g. older runs)
RE_STOP_LOSS = re.compile(
    r"\[last_90s\]\s+\[(\w+)\]\s+STOP_LOSS\s+sell\s+(\w+)\s+x\s+(\d+)\s+@\s+(\d+)c\s+for\s+(\S+)\s+\(entry=(\d+)c\s+loss=([\d.]+)%\)"
)
RE_PLACED = re.compile(
    r"\[last_90s\]\s+\[(\w+)\]\s+PLACED\s+(limit|market)\s+(\w+)\s+@\s+(\d+)c\s+x\s+(\d+)\s+for\s+(\S+)\s+\((\d+)s\s+to\s+close\)"
)
RE_ORDER_ID = re.compile(r"order_id=([\w-]+)")
RE_YES_NO_BID = re.compile(r"yes_bid=(\d+)\s+no_bid=(\d+)")
# OUTCOME: [last_90s] OUTCOME [ASSET] market=MARKET order_id=UUID side=yes placed=1 executed=0|1 status=executed|canceled|resting result=? outcome=unfilled|executed_unknown_result|positive|negative
RE_OUTCOME = re.compile(
    r"\[last_90s\]\s+OUTCOME\s+\[\w+\]\s+market=\S+\s+order_id=([\w-]+)\s+side=\w+\s+placed=\d+\s+executed=(\d+)\s+status=(\w+)\s+result=\S+\s+outcome=(\w+)"
)
# SKIP (distance buffer): [last_90s] [ASSET] SKIP: distance_from_strike=X < or <= min_distance_at_placement=Y [for TICKER] (strike=... spot=...)
RE_SKIP_DISTANCE = re.compile(
    r"\[last_90s\]\s+\[(\w+)\]\s+SKIP:\s+distance_from_strike=([\d.]+|n/a)\s+<=?\s+min_distance_at_placement=([\d.]+)\s+(?:for\s+(\S+)\s+)?\(strike=(\S+)\s+spot=(\S+)\)"
)
# SKIP (min_bid): [last_90s] [ASSET] SKIP: yes|no bid=Xc < or <= min_bid_cents=Y for TICKER (too risky)...  (ticker optional in old logs)
RE_SKIP_MIN_BID = re.compile(
    r"\[last_90s\]\s+\[(\w+)\]\s+SKIP:\s+(yes|no)\s+bid=(\d+)c\s+<=?\s+min_bid_cents=(\d+)(?:\s+for\s+(\S+))?\s+\(too risky\)"
)
# SKIP (invalid ask for market-style): [last_90s] [ASSET] SKIP: bid=Xc < 99 but ask=Y invalid for market-style [for TICKER]
RE_SKIP_INVALID_ASK = re.compile(
    r"\[last_90s\]\s+\[(\w+)\]\s+SKIP:\s+bid=(\d+)c\s+<\s+99\s+but\s+ask=(\S+)\s+invalid\s+for\s+market-style(?:\s+for\s+(\S+))?"
)
# Generic: [last_90s] [ASSET] SKIP: <anything>
RE_SKIP_GENERIC = re.compile(
    r"\[last_90s\]\s+\[(\w+)\]\s+SKIP:\s+(.+)"
)


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def fetch_final_outcomes(tickers: set[str]) -> dict[str, str]:
    """
    Query Kalshi for each ticker's market result. Returns dict: ticker -> final_outcome.
    final_outcome is: yes | no | scalar | not_settled | n/a (on error or no data).
    """
    if not tickers:
        return {}
    root = _project_root()
    try:
        sys.path.insert(0, str(root))
        from dotenv import load_dotenv
        load_dotenv(root / ".env")
        from src.client.kalshi_client import KalshiClient
    except ImportError:
        return {t: "n/a" for t in tickers}

    try:
        client = KalshiClient()
    except Exception:
        return {t: "n/a" for t in tickers}

    out: dict[str, str] = {}
    for ticker in sorted(tickers):
        if not ticker or ticker == "n/a":
            continue
        try:
            m = client.get_market(ticker)
            if not m:
                out[ticker] = "n/a"
                continue
            status = (m.get("status") or "").strip().lower()
            result = (m.get("result") or "").strip().lower()
            if result in ("yes", "no", "scalar"):
                out[ticker] = result
            elif status in ("finalized", "determined"):
                out[ticker] = result if result else "not_settled"
            else:
                out[ticker] = status or "not_settled"
        except Exception:
            out[ticker] = "n/a"
        time.sleep(0.15)
    for t in tickers:
        if t and t != "n/a" and t not in out:
            out[t] = "n/a"
    return out


def _parse_ts(ts: str) -> float:
    """Parse ISO ts to epoch seconds; 0 if invalid."""
    if not ts:
        return 0.0
    try:
        s = str(ts)[:26].replace("Z", "+00:00")
        if "T" in s and "+" not in s and "Z" not in ts:
            s = s + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0.0


def _log_line_ts(line: str) -> str:
    """Extract ISO-like ts from log line 'YYYY-MM-DD HH:MM:SS | ...'. Bot logs UTC (see bot/logging.py)."""
    if len(line) >= 19 and line[4] == "-" and line[10] == " " and line[13] == ":":
        return line[:19].replace(" ", "T") + "+00:00"
    return ""


def parse_last_90s_text_lines(log_path: Path) -> Iterator[dict]:
    """Yield dicts from human-readable [last_90s] PLACED / STOP_LOSS lines (no JSON)."""
    if not log_path.exists():
        return
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if "[last_90s]" not in line:
                continue
            ts_utc = _log_line_ts(line)
            m = RE_STOP_LOSS.search(line)
            if m:
                asset, side, fill_count, sell_price, ticker, entry, loss_pct = m.groups()
                yield {
                    "event": EVENT_STOP_LOSS,
                    "ts_utc": ts_utc,
                    "asset": asset.upper(),
                    "ticker": ticker,
                    "side": side.lower(),
                    "entry_cents": int(entry),
                    "sell_price_cents": int(sell_price),
                    "fill_count": int(fill_count),
                    "loss_pct": float(loss_pct),
                    "stop_loss_pct": None,
                    "seconds_to_close": None,
                    "strike": None,
                    "spot": None,
                    "distance_from_strike": None,
                    "window_seconds": None,
                    "min_bid_cents": None,
                    "order_id": None,
                }
                continue
            m = RE_PLACED.search(line)
            if m:
                asset, order_type, side, price_cents, count, ticker, secs = m.groups()
                order_id = None
                oid = RE_ORDER_ID.search(line)
                if oid:
                    order_id = oid.group(1)
                yes_bid = no_bid = None
                bids = RE_YES_NO_BID.search(line)
                if bids:
                    yes_bid, no_bid = int(bids.group(1)), int(bids.group(2))
                yield {
                    "event": EVENT_PLACEMENT,
                    "ts_utc": ts_utc,
                    "asset": asset.upper(),
                    "ticker": ticker,
                    "side": side.lower(),
                    "order_type": order_type.lower(),
                    "price_cents": int(price_cents),
                    "count": int(count),
                    "seconds_to_close": float(secs),
                    "order_id": order_id,
                    "yes_bid": yes_bid,
                    "no_bid": no_bid,
                    "yes_ask": None,
                    "no_ask": None,
                    "strike": None,
                    "spot": None,
                    "distance_from_strike": None,
                    "window_seconds": None,
                    "min_bid_cents": None,
                    "limit_bid_cents": None,
                }
                continue


def parse_distance_context_lines(log_path: Path) -> list[dict]:
    """
    Parse log for JSON lines that contain ticker, distance, spot, strike (e.g. fifteen_min SKIP/EVAL).
    Returns list of {ts_utc, ticker, distance, spot, strike, min_distance_required} sorted by ts_utc.
    """
    out: list[dict] = []
    if not log_path.exists():
        return out
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if '"distance"' not in line or '"ticker"' not in line or "{" not in line:
                continue
            idx = line.find("{")
            if idx < 0:
                continue
            try:
                obj = json.loads(line[idx:])
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            ticker = obj.get("ticker")
            dist = obj.get("distance")
            spot = obj.get("spot")
            strike = obj.get("strike")
            if ticker is None or dist is None:
                continue
            try:
                dist_f = float(dist)
            except (TypeError, ValueError):
                continue
            ts = (obj.get("ts") or "")[:26]
            if not ts:
                ts = _log_line_ts(line)
            min_dist = obj.get("min_distance_required")
            if min_dist is not None:
                try:
                    min_dist = float(min_dist)
                except (TypeError, ValueError):
                    min_dist = None
            out.append({
                "ts_utc": ts,
                "ticker": str(ticker),
                "distance": dist_f,
                "spot": float(spot) if spot is not None else None,
                "strike": float(strike) if strike is not None else None,
                "min_distance_required": min_dist,
            })
    out.sort(key=lambda x: (x.get("ts_utc") or "", x.get("ticker") or ""))
    return out


def _find_distance_at_placement(
    stoplosses: list[dict],
    placements: list[dict],
    context_lines: list[dict],
) -> dict[tuple, dict]:
    """
    For each stop loss, find the placement (same asset+ticker, ts before stop loss) and the
    most recent context line (same ticker, context_ts <= placement_ts). Return (asset, ticker) -> context.
    Uses epoch seconds for comparison (log line ts may be local, JSON ts may be UTC).
    """
    result: dict[tuple, dict] = {}
    for sl in stoplosses:
        asset = (sl.get("asset") or "").upper()
        ticker = sl.get("ticker") or ""
        sl_epoch = _parse_ts(sl.get("ts_utc") or "")
        if not asset or not ticker:
            continue
        # Placement that led to this stop loss: same asset+ticker, placement_ts < sl_ts, latest
        cand_placements = [
            p for p in placements
            if (p.get("asset") or "").upper() == asset and (p.get("ticker") or "") == ticker
            and _parse_ts(p.get("ts_utc") or "") < sl_epoch
        ]
        if not cand_placements:
            continue
        placement = max(cand_placements, key=lambda p: _parse_ts(p.get("ts_utc") or ""))
        place_epoch = _parse_ts(placement.get("ts_utc") or "")
        # Context line: same ticker, context within 10 min of placement (allow 10h for log local vs JSON UTC)
        cand_ctx = [
            c for c in context_lines
            if (c.get("ticker") or "") == ticker
            and abs(_parse_ts(c.get("ts_utc") or "") - place_epoch) <= 36000
        ]
        if not cand_ctx:
            continue
        # Prefer the latest context in the window (closest to placement time)
        best = max(cand_ctx, key=lambda c: _parse_ts(c.get("ts_utc") or ""))
        result[(asset, ticker)] = {
            "distance_at_placement": best.get("distance"),
            "spot_at_placement": best.get("spot"),
            "strike_at_placement": best.get("strike"),
            "min_distance_required": best.get("min_distance_required"),
        }
    return result


def _safe_float(x) -> float | None:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def parse_last_90s_skips(log_path: Path) -> list[dict]:
    """
    Parse all [last_90s] SKIP lines. Returns list of dicts with:
    ts_utc, asset, ticker, skip_reason (distance_buffer|min_bid|invalid_ask|other),
    and reason-specific fields: distance_from_strike, min_distance_at_placement, strike, spot,
    side, bid_cents, min_bid_cents, ask_cents, raw_reason.
    """
    out: list[dict] = []
    if not log_path.exists():
        return out
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if "[last_90s]" not in line or "SKIP:" not in line:
                continue
            ts_utc = _log_line_ts(line)
            asset = None
            ticker = ""
            skip_reason = "other"
            extra = {}

            m = RE_SKIP_DISTANCE.search(line)
            if m:
                asset, dist_str, min_dist_str, ticker, strike_str, spot_str = m.groups()
                ticker = ticker or ""
                dist = None if (dist_str is None or dist_str == "n/a") else _safe_float(dist_str)
                min_dist = _safe_float(min_dist_str) if min_dist_str else None
                strike = None if (strike_str is None or strike_str == "n/a") else _safe_float(strike_str)
                spot = None if (spot_str is None or spot_str == "n/a") else _safe_float(spot_str)
                skip_reason = "distance_buffer"
                extra = {"distance_from_strike": dist, "min_distance_at_placement": min_dist, "strike": strike, "spot": spot}
            else:
                m = RE_SKIP_MIN_BID.search(line)
                if m:
                    asset, side, bid_cents, min_bid_cents, ticker_opt = m.groups()
                    ticker = ticker_opt or ""
                    skip_reason = "min_bid"
                    extra = {"side": side, "bid_cents": int(bid_cents) if bid_cents else None, "min_bid_cents": int(min_bid_cents) if min_bid_cents else None}
                else:
                    m = RE_SKIP_INVALID_ASK.search(line)
                    if m:
                        asset, bid_cents, ask_cents, ticker_opt = m.groups()
                        ticker = ticker_opt or ""
                        skip_reason = "invalid_ask"
                        extra = {"bid_cents": int(bid_cents) if bid_cents else None, "ask_cents": ask_cents}
                    else:
                        m = RE_SKIP_GENERIC.search(line)
                        if m:
                            asset, raw_reason = m.groups()
                            skip_reason = "other"
                            extra = {"raw_reason": (raw_reason or "").strip()}

            if asset is None:
                continue
            out.append({
                "ts_utc": ts_utc,
                "asset": (asset or "").upper(),
                "ticker": ticker or "",
                "skip_reason": skip_reason,
                **extra,
            })
    out.sort(key=lambda x: (x.get("ts_utc") or "", x.get("asset") or "", x.get("skip_reason") or "", x.get("ticker") or ""))
    return out


def parse_last_90s_outcomes(log_path: Path) -> dict[str, dict]:
    """
    Parse [last_90s] OUTCOME lines. Returns dict: order_id -> {status, outcome, executed, ts_utc}.
    status = executed | canceled | resting; outcome = unfilled | executed_unknown_result | positive | negative.
    """
    out: dict[str, dict] = {}
    if not log_path.exists():
        return out
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if "[last_90s] OUTCOME" not in line:
                continue
            m = RE_OUTCOME.search(line)
            if not m:
                continue
            order_id, executed, status, outcome = m.groups()
            if not order_id:
                continue
            ts_utc = _log_line_ts(line)
            out[order_id] = {
                "status": status.lower(),
                "outcome": outcome.lower(),
                "executed": int(executed),
                "ts_utc": ts_utc,
            }
    return out


def parse_last_90s_events(log_path: Path) -> Iterator[dict]:
    """Yield parsed JSON dicts for lines containing LAST_90S_PLACEMENT_REPORT or LAST_90S_STOP_LOSS_REPORT."""
    if not log_path.exists():
        return
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or EVENT_PLACEMENT not in line and EVENT_STOP_LOSS not in line:
                continue
            idx = line.find("{")
            if idx < 0:
                continue
            try:
                obj = json.loads(line[idx:])
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            ev = obj.get("event")
            if ev not in (EVENT_PLACEMENT, EVENT_STOP_LOSS):
                continue
            yield obj


def _fmt(v, num_fmt="%.2f"):
    if v is None:
        return "n/a"
    if isinstance(v, float):
        return num_fmt % v if v == v else "n/a"
    return str(v)


def _fmt_distance(v) -> str:
    """Format distance/min_distance for display; use 4 decimals when |v| < 20 (XRP/SOL) so 0.0075 is not shown as 0.01."""
    if v is None:
        return "n/a"
    try:
        x = float(v)
        if x != x:
            return "n/a"
        return "%.4f" % x if abs(x) < 20 else "%.2f" % x
    except (TypeError, ValueError):
        return "n/a"


def _resolved_in_favor(side: str, final_outcome: str, executed) -> str:
    """
    Return won | lost | n/a.
    won = placed YES and market resolved YES, or placed NO and market resolved NO.
    lost = placed YES and market resolved NO, or placed NO and market resolved YES (flipped).
    n/a = not filled, or final_outcome unknown (n/a, scalar, not_settled).
    """
    if executed is None or executed == 0 or (isinstance(executed, str) and executed in ("n/a", "0")):
        return "n/a"
    side = (side or "").strip().lower()
    fo = (final_outcome or "").strip().lower()
    if fo not in ("yes", "no"):
        return "n/a"
    if side == "yes" and fo == "yes":
        return "won"
    if side == "no" and fo == "no":
        return "won"
    if side in ("yes", "no"):
        return "lost"  # flipped
    return "n/a"


def build_placement_section(
    placements: list[dict],
    outcomes: dict[str, dict] | None = None,
    final_outcomes: dict[str, str] | None = None,
) -> str:
    """Format placements table for tuning (time, price, strike, spot, distance, window_seconds, min_bid_cents).
    If outcomes is provided (order_id -> {status, outcome, executed}), adds columns: status, outcome, executed.
    final_outcomes: ticker -> Kalshi market result (yes/no/scalar/not_settled).
    """
    lines = [
        "=== LAST-90S PLACEMENTS ===",
        "(side = YES/NO placed; final_outcome = Kalshi result; resolved_in_favor = won [same direction] | lost [flipped] | n/a.)",
        "",
    ]
    if not placements:
        lines.append("No placement events in log.")
        return "\n".join(lines)

    outcomes = outcomes or {}
    final_outcomes = final_outcomes or {}
    cols = [
        "ts_utc", "asset", "ticker", "side", "price_cents", "count", "seconds_to_close",
        "strike", "spot", "distance_from_strike", "window_seconds", "min_bid_cents", "limit_bid_cents",
        "yes_bid", "no_bid", "yes_ask", "no_ask", "order_type", "order_id",
        "status", "outcome", "executed", "final_outcome", "resolved_in_favor",
    ]
    lines.append("\t".join(cols))
    lines.append("-" * 80)

    for p in placements:
        oid = p.get("order_id")
        o = (outcomes.get(oid) if oid else None) or {}
        ticker = p.get("ticker") or ""
        fo = (final_outcomes.get(ticker) or "n/a") if ticker else "n/a"
        side = (p.get("side") or "").lower()
        ex = o.get("executed") if o.get("executed") is not None else None
        rif = _resolved_in_favor(side, fo, ex)
        row = [
            (p.get("ts_utc") or "")[:19],
            p.get("asset") or "",
            ticker,
            side,
            _fmt(p.get("price_cents"), "%d"),
            _fmt(p.get("count"), "%d"),
            _fmt(p.get("seconds_to_close")),
            _fmt(p.get("strike")),
            _fmt(p.get("spot")),
            _fmt(p.get("distance_from_strike")),
            _fmt(p.get("window_seconds"), "%d"),
            _fmt(p.get("min_bid_cents"), "%d"),
            _fmt(p.get("limit_bid_cents"), "%d"),
            _fmt(p.get("yes_bid"), "%d"),
            _fmt(p.get("no_bid"), "%d"),
            _fmt(p.get("yes_ask"), "%d"),
            _fmt(p.get("no_ask"), "%d"),
            p.get("order_type") or "",
            oid or "",
            o.get("status") or "n/a",
            o.get("outcome") or "n/a",
            _fmt(ex, "%d") if ex is not None else "n/a",
            fo,
            rif,
        ]
        lines.append("\t".join(str(x) for x in row))

    lines.append("")
    lines.append("Total placements: %d" % len(placements))
    return "\n".join(lines)


def build_stoploss_section(
    stoplosses: list[dict],
    placement_context: dict[tuple, dict] | None = None,
) -> str:
    """Format stop-loss table for tuning stop_loss_pct, window_seconds, min_bid_cents.
    placement_context = (asset, ticker) -> {distance_at_placement, spot_at_placement, strike_at_placement, min_distance_required}.
    """
    lines = [
        "=== LAST-90S STOP LOSSES ===",
        "(Use for tuning: stop_loss_pct, window_seconds, min_bid_cents. distance_at_placement = at entry, to avoid tight strikes.)",
        "",
    ]
    if not stoplosses:
        lines.append("No stop-loss events in log.")
        return "\n".join(lines)

    placement_context = placement_context or {}
    cols = [
        "ts_utc", "asset", "ticker", "side", "entry_cents", "sell_price_cents", "fill_count",
        "loss_pct", "stop_loss_pct", "seconds_to_close", "strike", "spot", "distance_from_strike",
        "distance_at_placement", "spot_at_placement", "strike_at_placement", "min_dist_required_at_placement",
        "window_seconds", "min_bid_cents", "order_id",
    ]
    lines.append("\t".join(cols))
    lines.append("-" * 80)

    for s in stoplosses:
        key = ((s.get("asset") or "").upper(), s.get("ticker") or "")
        ctx = placement_context.get(key) or {}
        row = [
            (s.get("ts_utc") or "")[:19],
            s.get("asset") or "",
            s.get("ticker") or "",
            (s.get("side") or "").lower(),
            _fmt(s.get("entry_cents"), "%d"),
            _fmt(s.get("sell_price_cents"), "%d"),
            _fmt(s.get("fill_count"), "%d"),
            _fmt(s.get("loss_pct")),
            _fmt(s.get("stop_loss_pct")),
            _fmt(s.get("seconds_to_close")),
            _fmt(s.get("strike")),
            _fmt(s.get("spot")),
            _fmt(s.get("distance_from_strike")),
            _fmt(ctx.get("distance_at_placement")),
            _fmt(ctx.get("spot_at_placement")),
            _fmt(ctx.get("strike_at_placement")),
            _fmt(ctx.get("min_distance_required")),
            _fmt(s.get("window_seconds"), "%d"),
            _fmt(s.get("min_bid_cents"), "%d"),
            s.get("order_id") or "",
        ]
        lines.append("\t".join(str(x) for x in row))

    lines.append("")
    lines.append("Total stop-losses: %d" % len(stoplosses))
    # Prevention note when we have distance_at_placement
    with_ctx = [s for s in stoplosses if placement_context.get(((s.get("asset") or "").upper(), s.get("ticker") or ""))]
    if with_ctx:
        lines.append("")
        lines.append("--- Distance at placement (could have prevented by requiring min distance) ---")
        for s in with_ctx:
            key = ((s.get("asset") or "").upper(), s.get("ticker") or "")
            ctx = placement_context.get(key) or {}
            dist = ctx.get("distance_at_placement")
            min_d = ctx.get("min_distance_required")
            ticker = s.get("ticker") or ""
            if dist is not None:
                spot_a = ctx.get("spot_at_placement") if "spot_at_placement" in ctx else ctx.get("spot")
                strike_a = ctx.get("strike_at_placement") if "strike_at_placement" in ctx else ctx.get("strike")
                lines.append(
                    "  %s: distance_at_placement=%.2f  spot=%.2f  strike=%.2f  min_distance_required_in_15m=%.2f  → consider requiring min_distance >= %.0f in last_90s to avoid"
                    % (ticker, dist, spot_a or 0, strike_a or 0, min_d or 0, min_d if min_d is not None else dist)
                )
    return "\n".join(lines)


def _skip_details(s: dict) -> str:
    """One-line details string for a skip record."""
    reason = s.get("skip_reason") or "other"
    if reason == "distance_buffer":
        d = s.get("distance_from_strike")
        m = s.get("min_distance_at_placement")
        return "distance=%s <= min_distance_at_placement=%s" % (_fmt_distance(d), _fmt_distance(m))
    if reason == "min_bid":
        side = s.get("side") or "?"
        bid = s.get("bid_cents")
        min_b = s.get("min_bid_cents")
        return "%s bid=%sc < min_bid_cents=%s" % (side, bid if bid is not None else "n/a", min_b if min_b is not None else "n/a")
    if reason == "invalid_ask":
        bid = s.get("bid_cents")
        ask = s.get("ask_cents")
        return "bid=%sc ask=%s invalid" % (bid if bid is not None else "n/a", ask if ask is not None else "n/a")
    return (s.get("raw_reason") or "")[:80]


def _ticker_to_window_id(ticker: str) -> str:
    """Derive 15-min window/market id from ticker (e.g. KXBTC15M-26FEB280530-30 -> KXBTC15M-26FEB280530)."""
    if not ticker or "-" not in ticker:
        return ticker or ""
    return ticker.rsplit("-", 1)[0]


# 15-min slug prefix by asset (must match Kalshi format)
_PREFIX_15M = {"BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M", "XRP": "KXXRP15M"}


# 15m contract suffix variants (strike/bucket); used to guess full ticker from window_id for fetch
_WINDOW_ID_SUFFIXES = ("-00", "-15", "-30", "-45")


def _window_id_to_ticker_candidates(window_id: str) -> list[str]:
    """Return possible full tickers for a window_id (e.g. KXBTC15M-26FEB271930 -> [KXBTC15M-26FEB271930-30, ...])."""
    if not (window_id or "").strip():
        return []
    return [window_id + s for s in _WINDOW_ID_SUFFIXES]


def _final_outcome_for_window_id(final_outcomes: dict[str, str], window_id: str) -> str:
    """Return final_outcome for a skip that has window_id but no ticker. Match by exact ticker or prefix."""
    if not window_id or not final_outcomes:
        return "n/a"
    if window_id in final_outcomes:
        return final_outcomes[window_id]
    prefix = window_id + "-"
    for k, v in final_outcomes.items():
        if k.startswith(prefix):
            return v
    return "n/a"


def _ts_utc_asset_to_window_id(ts_utc: str, asset: str) -> str:
    """Derive 15-min window/market id from ts_utc and asset when ticker is missing (e.g. min_bid skips). Returns '' on parse error."""
    if not ts_utc or not (asset or "").strip():
        return ""
    asset = (asset or "").strip().upper()
    prefix = _PREFIX_15M.get(asset)
    if not prefix:
        return ""
    try:
        # Parse as UTC, convert to Eastern
        if "T" in ts_utc:
            dt_utc = datetime.fromisoformat(ts_utc.replace("Z", "+00:00")[:19])
        else:
            dt_utc = datetime.strptime(ts_utc[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        et = ZoneInfo("America/New_York")
        dt_et = dt_utc.astimezone(et)
        # Current 15-min window = next close at :00, :15, :30, :45
        minute = dt_et.minute
        close_min = ((minute // 15) + 1) * 15
        if close_min >= 60:
            close_time = (dt_et.replace(second=0, microsecond=0) + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        else:
            close_time = dt_et.replace(minute=close_min, second=0, microsecond=0)
        # Format YYmmmDDHHMM (e.g. 26FEB280030)
        yy = close_time.strftime("%y")
        mmm = close_time.strftime("%b").upper()
        dd = close_time.strftime("%d")
        hh = close_time.strftime("%H")
        mm = close_time.strftime("%M")
        return f"{prefix}-{yy}{mmm}{dd}{hh}{mm}"
    except Exception:
        return ""


def build_skips_section(skips: list[dict], final_outcomes: dict[str, str] | None = None) -> str:
    """Format table of all last_90s skips (distance_buffer, min_bid, invalid_ask, other).
    final_outcomes: ticker -> Kalshi market result (yes/no/scalar/not_settled).
    """
    lines = [
        "=== LAST-90S SKIPPED ===",
        "All skip types: distance_buffer (distance < min_distance_at_placement), min_bid (bid < min_bid_cents), invalid_ask (market-style), other. final_outcome = Kalshi market result.",
        "",
    ]
    if not skips:
        lines.append("No skips in log.")
        return "\n".join(lines)

    final_outcomes = final_outcomes or {}
    cols = ["ts_utc", "asset", "ticker", "window_id", "skip_reason", "details", "final_outcome"]
    lines.append("\t".join(cols))
    lines.append("-" * 80)
    for s in skips:
        ticker = s.get("ticker") or ""
        window_id = s.get("window_id") or _ticker_to_window_id(ticker) or _ts_utc_asset_to_window_id(s.get("ts_utc") or "", s.get("asset") or "")
        if ticker:
            fo = final_outcomes.get(ticker) or "n/a"
        else:
            fo = _final_outcome_for_window_id(final_outcomes, window_id)
        details = _skip_details(s).replace("\t", " ")
        row = [
            (s.get("ts_utc") or "")[:19],
            s.get("asset") or "",
            ticker,
            window_id,
            s.get("skip_reason") or "other",
            details,
            fo,
        ]
        lines.append("\t".join(str(x) for x in row))
    by_reason = {}
    for s in skips:
        r = s.get("skip_reason") or "other"
        by_reason[r] = by_reason.get(r, 0) + 1
    lines.append("")
    lines.append("Total skipped: %d  (%s)" % (len(skips), ", ".join("%s=%d" % (k, v) for k, v in sorted(by_reason.items()))))
    return "\n".join(lines)


def build_skips_summary_grouped(
    skips: list[dict],
    max_rows: int = 8,
    max_per_asset: int = 2,
) -> str:
    """Summary of skips grouped by asset and skip_reason with window_ids. At most max_rows total, max_per_asset rows per asset. E.g. 'BTC skipped in KXBTC15M-26FEB280530, KXBTC15M-26FEB281000 due to distance_buffer'."""
    if not skips:
        return "Skips summary (by asset and reason): no skips."
    # (asset, skip_reason) -> set of window_ids (use placeholder when missing)
    groups: dict[tuple[str, str], set[str]] = {}
    for s in skips:
        asset = (s.get("asset") or "").strip().upper() or "?"
        reason = s.get("skip_reason") or "other"
        wid = s.get("window_id") or _ticker_to_window_id(s.get("ticker") or "") or _ts_utc_asset_to_window_id(s.get("ts_utc") or "", s.get("asset") or "")
        if not wid.strip():
            wid = "(no window)"
        key = (asset, reason)
        if key not in groups:
            groups[key] = set()
        groups[key].add(wid)
    # Build rows: (asset, reason, windows_str); sort by asset then reason
    rows: list[tuple[str, str, str]] = []
    for (asset, reason), wids in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
        w_sorted = sorted(w for w in wids if w and w != "(no window)")
        if not w_sorted and "(no window)" in wids:
            windows_str = "(no window)"
        else:
            windows_str = ", ".join(w_sorted) if w_sorted else "(no window)"
        rows.append((asset, reason, windows_str))
    # At most max_per_asset rows per asset, then take first max_rows
    by_asset: dict[str, list[tuple[str, str, str]]] = {}
    for r in rows:
        a = r[0]
        if a not in by_asset:
            by_asset[a] = []
        if len(by_asset[a]) < max_per_asset:
            by_asset[a].append(r)
    out_rows: list[tuple[str, str, str]] = []
    for asset in sorted(by_asset.keys()):
        out_rows.extend(by_asset[asset])
        if len(out_rows) >= max_rows:
            out_rows = out_rows[:max_rows]
            break
    lines = ["=== SKIPS SUMMARY (by asset and skip reason, max %d rows, max %d per asset) ===" % (max_rows, max_per_asset), ""]
    for asset, reason, windows_str in out_rows:
        lines.append("%s skipped in %s due to %s." % (asset, windows_str, reason))
    return "\n".join(lines)


def build_summary_section(
    placements: list[dict],
    stoplosses: list[dict],
    outcomes: dict[str, dict] | None = None,
    skips: list[dict] | None = None,
    report_hours: float | None = None,
) -> str:
    """One-line summary: total placements, executed (filled), canceled/unfilled, stop losses, skipped (distance buffer).
    If report_hours is set (e.g. from --since-hours), adds expected max placements = 4 assets * 4 windows/hour * hours.
    """
    total = len(placements)
    outcomes = outcomes or {}
    executed = 0
    canceled = 0
    resting = 0
    unknown = 0
    for p in placements:
        oid = p.get("order_id")
        o = (outcomes.get(oid) if oid else None) or {}
        ex = o.get("executed")
        if ex is not None and int(ex) > 0:
            executed += 1
        else:
            st = (o.get("status") or "").lower()
            if st == "canceled":
                canceled += 1
            elif st == "resting":
                resting += 1
            else:
                unknown += 1
    stop_loss_count = len(stoplosses)
    skip_count = len(skips) if skips else 0
    placements_plus_skips = total + skip_count
    lines = [
        "--- SUMMARY ---",
        "Total placements: %d  |  Executed (filled): %d  |  Canceled/unfilled: %d  |  Resting (unfilled): %d  |  No outcome yet: %d",
        "Stop losses: %d  |  Skipped: %d",
        "Placements + Skipped: %d  (each 15-min window: either we placed or we skipped)",
    ]
    if report_hours is not None and report_hours > 0:
        hrs = int(round(report_hours))
        expected_max = 4 * 4 * hrs  # 4 assets, 4 windows/hour
        lines.append("Expected max (4 assets x 4 windows/hour x %d h): %d  |  Actual placements: %d  |  Placements+Skips: %d  (close to expected = full coverage)" % (hrs, expected_max, total, placements_plus_skips))
    lines.append("")
    return "\n".join(lines) % (total, executed, canceled, resting, unknown, stop_loss_count, skip_count, placements_plus_skips)


def build_report(
    placements: list[dict],
    stoplosses: list[dict],
    fmt: str,
    outcomes: dict[str, dict] | None = None,
    placement_context: dict[tuple, dict] | None = None,
    skips: list[dict] | None = None,
    final_outcomes: dict[str, str] | None = None,
    report_hours: float | None = None,
) -> str:
    """Build full report text (tsv-style tables). outcomes = order_id -> {status, outcome, executed}. placement_context = (asset, ticker) -> distance/spot/strike at placement for stop losses. skips = skipped (distance buffer) list. final_outcomes = ticker -> Kalshi market result. report_hours = time window in hours for expected-placement note."""
    summary_txt = build_summary_section(placements, stoplosses, outcomes, skips, report_hours)
    placement_txt = build_placement_section(placements, outcomes, final_outcomes)
    stoploss_txt = build_stoploss_section(stoplosses, placement_context)
    skips_txt = build_skips_section(skips or [], final_outcomes)
    skips_summary_txt = build_skips_summary_grouped(skips or [], max_rows=8, max_per_asset=2)
    header = "LAST-90S STRATEGY REPORT (placements + stop-losses for tuning)"
    return "\n".join([header, "", summary_txt, placement_txt, "", stoploss_txt, "", skips_txt, "", skips_summary_txt])


def _tsv_cell(x) -> str:
    """Format a cell for TSV; avoid embedded tabs/newlines."""
    s = "" if x is None else str(x).replace("\t", " ").replace("\r", " ").replace("\n", " ")
    return s


def write_tsv(path: Path, cols: list[str], rows: list[list]) -> None:
    """Write a single TSV file with header row and data rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("\t".join(_tsv_cell(c) for c in cols) + "\n")
        for row in rows:
            f.write("\t".join(_tsv_cell(c) for c in row) + "\n")


def build_placement_tsv(
    placements: list[dict],
    outcomes: dict[str, dict] | None,
    final_outcomes: dict[str, str] | None = None,
) -> tuple[list[str], list[list]]:
    """Return (columns, list of row lists) for placements TSV. resolved_in_favor = won | lost | n/a (won = same direction, lost = flipped)."""
    outcomes = outcomes or {}
    final_outcomes = final_outcomes or {}
    cols = [
        "ts_utc", "asset", "ticker", "side", "price_cents", "count", "seconds_to_close",
        "strike", "spot", "distance_from_strike", "window_seconds", "min_bid_cents", "limit_bid_cents",
        "yes_bid", "no_bid", "yes_ask", "no_ask", "order_type", "order_id",
        "status", "outcome", "executed", "final_outcome", "resolved_in_favor",
    ]
    rows = []
    for p in placements:
        oid = p.get("order_id")
        o = (outcomes.get(oid) if oid else None) or {}
        ticker = p.get("ticker") or ""
        fo = (final_outcomes.get(ticker) or "n/a") if ticker else "n/a"
        side = (p.get("side") or "").lower()
        ex = o.get("executed") if o.get("executed") is not None else None
        rif = _resolved_in_favor(side, fo, ex)
        rows.append([
            (p.get("ts_utc") or "")[:19],
            p.get("asset") or "",
            ticker,
            side,
            _fmt(p.get("price_cents"), "%d"),
            _fmt(p.get("count"), "%d"),
            _fmt(p.get("seconds_to_close")),
            _fmt(p.get("strike")),
            _fmt(p.get("spot")),
            _fmt(p.get("distance_from_strike")),
            _fmt(p.get("window_seconds"), "%d"),
            _fmt(p.get("min_bid_cents"), "%d"),
            _fmt(p.get("limit_bid_cents"), "%d"),
            _fmt(p.get("yes_bid"), "%d"),
            _fmt(p.get("no_bid"), "%d"),
            _fmt(p.get("yes_ask"), "%d"),
            _fmt(p.get("no_ask"), "%d"),
            p.get("order_type") or "",
            oid or "",
            o.get("status") or "n/a",
            o.get("outcome") or "n/a",
            _fmt(ex, "%d") if ex is not None else "n/a",
            fo,
            rif,
        ])
    return cols, rows


def build_stoploss_tsv(
    stoplosses: list[dict],
    placement_context: dict[tuple, dict] | None,
) -> tuple[list[str], list[list]]:
    """Return (columns, list of row lists) for stop-losses TSV."""
    placement_context = placement_context or {}
    cols = [
        "ts_utc", "asset", "ticker", "side", "entry_cents", "sell_price_cents", "fill_count",
        "loss_pct", "stop_loss_pct", "seconds_to_close", "strike", "spot", "distance_from_strike",
        "distance_at_placement", "spot_at_placement", "strike_at_placement", "min_dist_required_at_placement",
        "window_seconds", "min_bid_cents", "order_id",
    ]
    rows = []
    for s in stoplosses:
        key = ((s.get("asset") or "").upper(), s.get("ticker") or "")
        ctx = placement_context.get(key) or {}
        rows.append([
            (s.get("ts_utc") or "")[:19],
            s.get("asset") or "",
            s.get("ticker") or "",
            (s.get("side") or "").lower(),
            _fmt(s.get("entry_cents"), "%d"),
            _fmt(s.get("sell_price_cents"), "%d"),
            _fmt(s.get("fill_count"), "%d"),
            _fmt(s.get("loss_pct")),
            _fmt(s.get("stop_loss_pct")),
            _fmt(s.get("seconds_to_close")),
            _fmt(s.get("strike")),
            _fmt(s.get("spot")),
            _fmt(s.get("distance_from_strike")),
            _fmt(ctx.get("distance_at_placement")),
            _fmt(ctx.get("spot_at_placement")),
            _fmt(ctx.get("strike_at_placement")),
            _fmt(ctx.get("min_distance_required")),
            _fmt(s.get("window_seconds"), "%d"),
            _fmt(s.get("min_bid_cents"), "%d"),
            s.get("order_id") or "",
        ])
    return cols, rows


def build_skips_tsv(skips: list[dict], final_outcomes: dict[str, str] | None = None) -> tuple[list[str], list[list]]:
    """Return (columns, list of row lists) for skips TSV. Columns: ts_utc, asset, ticker, window_id, skip_reason, details, final_outcome.
    final_outcome is from Kalshi (yes/no/scalar/not_settled) when report is run with default fetch; otherwise n/a."""
    final_outcomes = final_outcomes or {}
    cols = ["ts_utc", "asset", "ticker", "window_id", "skip_reason", "details", "final_outcome"]
    rows = []
    for s in skips:
        ticker = s.get("ticker") or ""
        window_id = s.get("window_id") or _ticker_to_window_id(ticker) or _ts_utc_asset_to_window_id(s.get("ts_utc") or "", s.get("asset") or "")
        if ticker:
            fo = final_outcomes.get(ticker) or "n/a"
        else:
            fo = _final_outcome_for_window_id(final_outcomes, window_id)
        details = _skip_details(s).replace("\t", " ")
        rows.append([
            (s.get("ts_utc") or "")[:19],
            s.get("asset") or "",
            ticker,
            window_id,
            s.get("skip_reason") or "other",
            details,
            fo,
        ])
    return cols, rows


def build_summary_tsv(
    placements: list[dict],
    stoplosses: list[dict],
    outcomes: dict[str, dict] | None,
    skips: list[dict] | None,
) -> tuple[list[str], list[list]]:
    """Return (columns, single summary row) for summary TSV."""
    total = len(placements)
    outcomes = outcomes or {}
    executed = canceled = resting = unknown = 0
    for p in placements:
        oid = p.get("order_id")
        o = (outcomes.get(oid) if oid else None) or {}
        ex = o.get("executed")
        if ex is not None and int(ex) > 0:
            executed += 1
        else:
            st = (o.get("status") or "").lower()
            if st == "canceled":
                canceled += 1
            elif st == "resting":
                resting += 1
            else:
                unknown += 1
    cols = [
        "total_placements", "executed", "canceled", "resting", "no_outcome_yet",
        "stop_losses", "skipped_distance_buffer",
    ]
    row = [total, executed, canceled, resting, unknown, len(stoplosses), len(skips or [])]
    return cols, [row]


def _parse_args():
    p = argparse.ArgumentParser(
        description="Generate Last-90s report from bot log (placements + stop-losses)."
    )
    p.add_argument(
        "log_path",
        nargs="?",
        default=None,
        help="Path to bot log (default: project_root/logs/bot.log)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print report to stdout only; do not write to file.",
    )
    p.add_argument(
        "--output",
        "-o",
        metavar="FILE",
        default=None,
        help="Write report to this file (default: reports/last_90s_report.txt unless --dry-run)",
    )
    p.add_argument(
        "--format",
        choices=("text", "tsv"),
        default="text",
        help="Table format (default: text with tabs)",
    )
    p.add_argument(
        "--since-hours",
        type=float,
        metavar="N",
        help="Include events from last N hours",
    )
    p.add_argument(
        "--since",
        metavar="ISO",
        help="Include events since YYYY-MM-DDTHH:MM:SS",
    )
    p.add_argument(
        "--no-fetch-final-outcomes",
        action="store_true",
        help="Skip Kalshi lookup for final outcome (default: fetch yes/no from Kalshi for placements/skips).",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    root = _project_root()
    log_path = args.log_path
    if not log_path:
        log_path = root / "logs" / "bot.log"
    else:
        log_path = Path(log_path)
    if not log_path.is_absolute():
        log_path = root / log_path

    since_ts = 0.0
    if args.since:
        since_ts = _parse_ts(args.since)
    elif args.since_hours:
        since_ts = (datetime.now(timezone.utc) - timedelta(hours=args.since_hours)).timestamp()

    json_events = list(parse_last_90s_events(log_path))
    text_events = list(parse_last_90s_text_lines(log_path))
    if since_ts > 0:
        json_events = [e for e in json_events if _parse_ts(e.get("ts_utc") or "") >= since_ts]
        text_events = [e for e in text_events if _parse_ts(e.get("ts_utc") or "") >= since_ts]

    # Prefer JSON over text for same event (by ts + asset + ticker)
    def key_placement(e):
        return ((e.get("ts_utc") or "")[:19], (e.get("asset") or "").upper(), e.get("ticker") or "")
    def key_stoploss(e):
        return ((e.get("ts_utc") or "")[:19], (e.get("asset") or "").upper(), e.get("ticker") or "")

    placements_by_key = {key_placement(e): e for e in json_events if e.get("event") == EVENT_PLACEMENT}
    for e in text_events:
        if e.get("event") == EVENT_PLACEMENT and key_placement(e) not in placements_by_key:
            placements_by_key[key_placement(e)] = e
    placements = list(placements_by_key.values())
    placements.sort(key=lambda e: (e.get("ts_utc") or "", e.get("asset") or "", e.get("ticker") or ""))

    stoplosses_by_key = {key_stoploss(e): e for e in json_events if e.get("event") == EVENT_STOP_LOSS}
    for e in text_events:
        if e.get("event") == EVENT_STOP_LOSS and key_stoploss(e) not in stoplosses_by_key:
            stoplosses_by_key[key_stoploss(e)] = e
    stoplosses = list(stoplosses_by_key.values())
    stoplosses.sort(key=lambda e: (e.get("ts_utc") or "", e.get("asset") or "", e.get("ticker") or ""))

    outcomes = parse_last_90s_outcomes(log_path)
    context_lines = parse_distance_context_lines(log_path)
    placement_context = _find_distance_at_placement(stoplosses, placements, context_lines)

    skips = parse_last_90s_skips(log_path)
    if since_ts > 0:
        skips = [s for s in skips if _parse_ts(s.get("ts_utc") or "") >= since_ts]

    # Collect unique tickers (placements + skips) and fetch final_outcome from Kalshi by default
    all_tickers = set()
    for p in placements:
        t = p.get("ticker") or ""
        if t and t != "n/a":
            all_tickers.add(t)
    for s in skips:
        t = s.get("ticker") or ""
        if t and t != "n/a":
            all_tickers.add(t)
        else:
            # Skip has no ticker (e.g. min_bid log "for n/a"); add window_id-based candidates so we can fetch outcome
            wid = s.get("window_id") or _ticker_to_window_id(t) or _ts_utc_asset_to_window_id(s.get("ts_utc") or "", s.get("asset") or "")
            for cand in _window_id_to_ticker_candidates(wid):
                all_tickers.add(cand)
    # Fetch final outcomes from Kalshi (default); used in placements TSV and skips TSV
    final_outcomes: dict[str, str] = {}
    if all_tickers and not args.no_fetch_final_outcomes and not args.dry_run:
        print("[Fetching final outcomes from Kalshi for %d tickers...]" % len(all_tickers), file=sys.stderr)
        final_outcomes = fetch_final_outcomes(all_tickers)

    report_hours = float(args.since_hours) if getattr(args, "since_hours", None) else None
    report = build_report(placements, stoplosses, args.format, outcomes, placement_context, skips, final_outcomes, report_hours)

    print(report)

    if args.dry_run:
        print("\n[Dry run: no file written]")
        return 0

    out_path = args.output
    if not out_path:
        out_path = root / "reports" / "last_90s_report.txt"
    else:
        out_path = Path(out_path)
    if not out_path.is_absolute():
        out_path = root / out_path

    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report, encoding="utf-8")
        print("\n[Written: %s]" % out_path)
    except Exception as e:
        print("\n[Could not write report: %s]" % e, file=sys.stderr)
        return 1

    # Write TSV files for analysis (last_90s_limit_99)
    tsv_dir = out_path.parent
    try:
        cols, rows = build_placement_tsv(placements, outcomes, final_outcomes)
        write_tsv(tsv_dir / "last_90s_placements.tsv", cols, rows)
        print("[Written: %s]" % (tsv_dir / "last_90s_placements.tsv"))
        cols, rows = build_stoploss_tsv(stoplosses, placement_context)
        write_tsv(tsv_dir / "last_90s_stoplosses.tsv", cols, rows)
        print("[Written: %s]" % (tsv_dir / "last_90s_stoplosses.tsv"))
        cols, rows = build_skips_tsv(skips, final_outcomes)  # final_outcome from Kalshi (default fetch)
        write_tsv(tsv_dir / "last_90s_skips.tsv", cols, rows)
        print("[Written: %s]" % (tsv_dir / "last_90s_skips.tsv"))
        skips_summary_txt = build_skips_summary_grouped(skips, max_rows=8, max_per_asset=2)
        (tsv_dir / "last_90s_skips_summary.txt").write_text(skips_summary_txt, encoding="utf-8")
        print("[Written: %s]" % (tsv_dir / "last_90s_skips_summary.txt"))
        cols, rows = build_summary_tsv(placements, stoplosses, outcomes, skips)
        write_tsv(tsv_dir / "last_90s_summary.tsv", cols, rows)
        print("[Written: %s]" % (tsv_dir / "last_90s_summary.tsv"))
    except Exception as e:
        print("\n[Could not write TSV files: %s]" % e, file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
