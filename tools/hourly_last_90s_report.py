#!/usr/bin/env python3
"""
Generate a report from Hourly last-90s strategy logs (placements + skips + stop-losses).

Same structure as 15-min last_90s_report: summary, placements table, stop-losses table,
skips table, skips summary. Window ID = hour_market_id (e.g. KXBTCD-26FEB2814).

Reads bot log for:
  - [hourly_last_90s] PLACED ... and JSON HOURLY_LAST_90S_PLACEMENT
  - [hourly_last_90s] SKIP: ... (min_bid, distance_from_strike <= min_distance_at_placement, other)
  - [hourly_last_90s] STOP_LOSS ... (when implemented)
  - [hourly_last_90s] OUTCOME ...

Writes:
  - reports/hourly_last_90s_report.txt
  - reports/hourly_last_90s_placements.tsv
  - reports/hourly_last_90s_skips.tsv
  - reports/hourly_last_90s_stoplosses.tsv
  - reports/hourly_last_90s_skips_summary.txt

Usage:
  python tools/hourly_last_90s_report.py [path/to/bot.log]
  python tools/hourly_last_90s_report.py --since-hours 1
  python tools/hourly_last_90s_report.py --window-id-suffix 26FEB2816   # only 4PM ET Feb 28, 2026
  python tools/hourly_last_90s_report.py --output reports/hourly_last_90s_report.txt
  python tools/hourly_last_90s_report.py --no-fetch-final-outcomes
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

PREFIX = "hourly_last_90s"
EVENT_PLACEMENT = "HOURLY_LAST_90S_PLACEMENT"

RE_PLACED = re.compile(
    r"\[hourly_last_90s\]\s+\[(\w+)\]\s+PLACED\s+(\w+)\s+(\S+)\s+@\s+(\d+)c\s+x\s+(\d+)\s+for\s+(\S+)\s+\((\d+)s\s+to\s+close\)\s+order_id=(\S+)"
)
RE_SKIP_MIN_BID = re.compile(
    r"\[hourly_last_90s\]\s+\[(\w+)\]\s+SKIP:\s+(yes|no)\s+bid=(\d+)c\s+<\s+min_bid_cents=(\d+)(?:\s+for\s+(\S+))?\s+\(too risky\)"
)
# distance_from_strike <= min_distance_at_placement
RE_SKIP_DISTANCE = re.compile(
    r"\[hourly_last_90s\]\s+\[(\w+)\]\s+SKIP:\s+distance_from_strike=([\d.]+)\s+<=\s+min_distance_at_placement=([\d.]+)\s+for\s+(\S+)\s+\(strike=([\d.]+)\s+spot=([\d.]+)\)"
)
RE_SKIP_GENERIC = re.compile(
    r"\[hourly_last_90s\]\s+\[(\w+)\]\s+SKIP:\s+(.+)"
)
RE_OUTCOME = re.compile(
    r"\[hourly_last_90s\]\s+OUTCOME\s+\[\w+\]\s+market=(\S+)\s+ticker=(\S+)\s+side=(\w+)\s+order_id=([\w-]+)\s+executed=(\d+)\s+status=(\w+)\s+outcome=(\w+)"
)
# Future: when hourly last_90s has stop-loss, log e.g. [hourly_last_90s] [ASSET] STOP_LOSS sell yes x N @ Nc for TICKER (entry=Nc loss=N%)
RE_STOP_LOSS = re.compile(
    r"\[hourly_last_90s\]\s+\[(\w+)\]\s+STOP_LOSS\s+sell\s+(\w+)\s+x\s+(\d+)\s+@\s+(\d+)c\s+for\s+(\S+)\s+\(entry=(\d+)c\s+loss=([\d.]+)%\)"
)
# No eligible tickers: [hourly_last_90s] [DOGE] No eligible tickers for KXDOGE-26FEB2816
RE_NO_ELIGIBLE_TICKERS = re.compile(
    r"\[hourly_last_90s\]\s+\[(\w+)\]\s+No eligible tickers for\s+(\S+)"
)


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _log_line_ts(line: str) -> str:
    if len(line) >= 19 and line[4] == "-" and line[10] == " " and line[13] == ":":
        return line[:19].replace(" ", "T") + "+00:00"
    return ""


def _parse_ts(ts: str) -> float:
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


def parse_placements(log_path: Path) -> list[dict]:
    out = []
    if not log_path.exists():
        return out
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if "[hourly_last_90s]" not in line or "PLACED" not in line:
                if EVENT_PLACEMENT in line and "{" in line:
                    idx = line.find("{")
                    if idx >= 0:
                        try:
                            obj = json.loads(line[idx:])
                            if isinstance(obj, dict) and obj.get("event") == EVENT_PLACEMENT:
                                ts = obj.get("ts_utc") or _log_line_ts(line)
                                out.append({
                                    "ts_utc": ts[:19] if ts else "",
                                    "asset": (obj.get("asset") or "").upper(),
                                    "hour_market_id": obj.get("hour_market_id") or "",
                                    "ticker": obj.get("ticker") or "",
                                    "side": (obj.get("side") or "yes").lower(),
                                    "price_cents": obj.get("price_cents"),
                                    "count": obj.get("count"),
                                    "seconds_to_close": obj.get("seconds_to_close"),
                                    "order_id": obj.get("order_id") or "",
                                })
                        except json.JSONDecodeError:
                            pass
                continue
            m = RE_PLACED.search(line)
            if m:
                asset, side, _, price, count, ticker, secs, order_id = m.groups()
                ts = _log_line_ts(line)
                out.append({
                    "ts_utc": ts[:19] if ts else "",
                    "asset": asset.upper(),
                    "hour_market_id": "",  # not in text line
                    "ticker": ticker,
                    "side": side.lower(),
                    "price_cents": int(price),
                    "count": int(count),
                    "seconds_to_close": float(secs),
                    "order_id": order_id,
                })
    out.sort(key=lambda x: (x.get("ts_utc") or "", x.get("asset") or "", x.get("ticker") or ""))
    return out


def parse_skips(log_path: Path) -> list[dict]:
    out = []
    if not log_path.exists():
        return out
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if "[hourly_last_90s]" not in line or "SKIP:" not in line:
                continue
            ts = _log_line_ts(line)
            asset = None
            ticker = ""
            reason = "other"
            extra = {}
            m = RE_SKIP_DISTANCE.search(line)
            if m:
                asset, dist, min_d, ticker, strike, spot = m.groups()
                reason = "distance_buffer"
                try:
                    extra = {
                        "distance_from_strike": float(dist),
                        "min_distance_at_placement": float(min_d),
                        "strike": float(strike),
                        "spot": float(spot),
                    }
                except (ValueError, TypeError):
                    pass
            else:
                m = RE_SKIP_MIN_BID.search(line)
                if m:
                    g = m.groups()
                    asset, side, bid, min_bid = g[0], g[1], g[2], g[3]
                    ticker = g[4] if len(g) > 4 else ""
                    reason = "min_bid"
                    try:
                        extra = {
                            "side": (side or "").lower(),
                            "bid_cents": int(bid) if bid is not None else None,
                            "min_bid_cents": int(min_bid) if min_bid is not None else None,
                        }
                    except (ValueError, TypeError):
                        pass
                else:
                    m = RE_SKIP_GENERIC.search(line)
                    if m:
                        asset, raw = m.groups()
                        reason = "other"
                        extra = {"raw_reason": (raw or "").strip()}
            if asset is None:
                continue
            rec = {
                "ts_utc": ts[:19] if ts else "",
                "asset": asset.upper(),
                "ticker": ticker or "",
                "window_id": _ticker_to_window_id_hourly(ticker) if ticker else "",
                "skip_reason": reason,
            }
            rec.update(extra)
            out.append(rec)
    out.sort(key=lambda x: (x.get("ts_utc") or "", x.get("asset") or "", x.get("skip_reason") or "", x.get("ticker") or ""))
    return out


def parse_no_eligible_tickers(log_path: Path) -> list[dict]:
    """Parse [hourly_last_90s] [ASSET] No eligible tickers for HOUR_MARKET_ID. Returns list of {ts_utc, asset, hour_market_id}."""
    out = []
    if not log_path.exists():
        return out
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if "[hourly_last_90s]" not in line or "No eligible tickers" not in line:
                continue
            m = RE_NO_ELIGIBLE_TICKERS.search(line)
            if not m:
                continue
            ts = _log_line_ts(line)
            asset, hour_market_id = m.groups()
            out.append({
                "ts_utc": ts[:19] if ts else "",
                "asset": (asset or "").upper(),
                "hour_market_id": hour_market_id or "",
            })
    out.sort(key=lambda x: (x.get("ts_utc") or "", x.get("asset") or "", x.get("hour_market_id") or ""))
    return out


def parse_stoplosses(log_path: Path) -> list[dict]:
    """Parse [hourly_last_90s] STOP_LOSS lines. Returns list of dicts (empty until strategy logs stop-loss)."""
    out = []
    if not log_path.exists():
        return out
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if "[hourly_last_90s]" not in line or "STOP_LOSS" not in line:
                continue
            m = RE_STOP_LOSS.search(line)
            if not m:
                continue
            ts = _log_line_ts(line)
            asset, side, count, sell_cents, ticker, entry_cents, loss_pct = m.groups()
            out.append({
                "ts_utc": ts[:19] if ts else "",
                "asset": asset.upper(),
                "ticker": ticker or "",
                "side": (side or "yes").lower(),
                "count": int(count),
                "sell_price_cents": int(sell_cents),
                "entry_price_cents": int(entry_cents),
                "loss_pct": float(loss_pct),
            })
    out.sort(key=lambda x: (x.get("ts_utc") or "", x.get("asset") or "", x.get("ticker") or ""))
    return out


def parse_outcomes(log_path: Path) -> dict[str, dict]:
    """order_id -> {status, outcome, executed, hour_market_id, side}."""
    out = {}
    if not log_path.exists():
        return out
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if "[hourly_last_90s] OUTCOME" not in line:
                continue
            m = RE_OUTCOME.search(line)
            if m:
                hour_market_id, ticker, side, order_id, executed, status, outcome = m.groups()
                out[order_id] = {
                    "status": status.lower(),
                    "outcome": outcome.lower(),
                    "executed": int(executed),
                    "hour_market_id": hour_market_id,
                    "side": side.lower(),
                    "ticker": ticker,
                }
    return out


def fetch_final_outcomes(tickers: set[str]) -> dict[str, str]:
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
    out = {}
    for ticker in sorted(tickers):
        if not ticker or ticker == "n/a":
            continue
        try:
            m = client.get_market(ticker)
            if not m:
                out[ticker] = "n/a"
                continue
            result = (m.get("result") or "").strip().lower()
            out[ticker] = result if result in ("yes", "no", "scalar") else (m.get("status") or "n/a")
        except Exception:
            out[ticker] = "n/a"
        time.sleep(0.15)
    for t in tickers:
        if t and t != "n/a" and t not in out:
            out[t] = "n/a"
    return out


def _fmt(v, num_fmt="%.2f"):
    if v is None:
        return "n/a"
    if isinstance(v, float):
        return num_fmt % v if v == v else "n/a"
    return str(v)


def _fmt_distance(v) -> str:
    """Format distance/min_distance; 4 decimals when |v| < 20 (XRP/SOL) so 0.0075 is not shown as 0.01."""
    if v is None:
        return "n/a"
    try:
        x = float(v)
        if x != x:
            return "n/a"
        return "%.4f" % x if abs(x) < 20 else "%.2f" % x
    except (TypeError, ValueError):
        return "n/a"


def _ticker_to_window_id_hourly(ticker: str) -> str:
    """Derive hourly window/market id from ticker (e.g. KXBTC-26FEB2816-B66625 -> KXBTC-26FEB2816)."""
    if not ticker or "-" not in ticker:
        return ticker or ""
    return ticker.rsplit("-", 1)[0]


def _window_suffix(id_or_ticker: str) -> str:
    """Extract YYmmmDDHH window suffix from hour_market_id or ticker (e.g. KXBTCD-26FEB2816 -> 26FEB2816)."""
    parts = (id_or_ticker or "").split("-")
    return parts[1] if len(parts) >= 2 else ""


def _skip_details(s: dict) -> str:
    """One-line details for a skip record (same style as last_90s_report)."""
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
    if reason == "other" and s.get("raw_reason"):
        return (s.get("raw_reason") or "")[:80]
    return ""


def _build_summary_section(
    placements: list[dict],
    stoplosses: list[dict],
    outcomes: dict[str, dict],
    skips: list[dict],
    no_eligible_tickers: list[dict] | None = None,
) -> str:
    no_eligible_tickers = no_eligible_tickers or []
    # Count unique order_ids (placements can have duplicate rows per order with/without hour_market_id)
    unique_order_ids = set()
    for p in placements:
        oid = p.get("order_id")
        if oid and str(oid).strip() and str(oid) != "n/a":
            unique_order_ids.add(oid)
    total_orders = len(unique_order_ids)
    executed = canceled = resting = unknown = 0
    for oid in unique_order_ids:
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
    lines = [
        "--- SUMMARY ---",
        "Total orders placed (unique): %d  |  Executed (filled): %d  |  Canceled/unfilled: %d  |  Resting (unfilled): %d  |  No outcome yet: %d" % (total_orders, executed, canceled, resting, unknown),
        "Stop losses: %d  |  Skipped: %d  |  No eligible tickers: %d" % (len(stoplosses), len(skips), len(no_eligible_tickers)),
        "Placements + Skipped: %d" % (total_orders + len(skips)),
        "",
    ]
    return "\n".join(lines)


def _build_stoploss_section(stoplosses: list[dict]) -> str:
    lines = [
        "=== STOP-LOSSES ===",
        "(Hourly last_90s does not implement stop-loss yet; section present for parity with 15-min report.)",
        "ts_utc\tasset\tticker\tside\tcount\tsell_price_cents\tentry_price_cents\tloss_pct",
        "-" * 80,
    ]
    for s in stoplosses:
        lines.append("\t".join([
            (s.get("ts_utc") or "")[:19],
            s.get("asset") or "",
            s.get("ticker") or "",
            s.get("side") or "",
            _fmt(s.get("count"), "%d"),
            _fmt(s.get("sell_price_cents"), "%d"),
            _fmt(s.get("entry_price_cents"), "%d"),
            _fmt(s.get("loss_pct")),
        ]))
    lines.append("")
    lines.append("Total stop-losses: %d" % len(stoplosses))
    return "\n".join(lines)


def _build_skips_summary_grouped(skips: list[dict], max_rows: int = 12, max_per_asset: int = 3) -> str:
    """Summary of skips grouped by asset and skip_reason with window_ids. Same format as last_90s: 'ASSET skipped in W1, W2, ... due to REASON.'"""
    if not skips:
        return "=== SKIPS SUMMARY (by asset and skip reason, max %d rows, max %d per asset) ===\n\nSkips summary: no skips." % (max_rows, max_per_asset)
    from collections import defaultdict
    grouped = defaultdict(set)  # (asset, reason) -> set of window_id
    for s in skips:
        asset = (s.get("asset") or "").strip().upper() or "?"
        reason = s.get("skip_reason") or "other"
        wid = (s.get("window_id") or _ticker_to_window_id_hourly(s.get("ticker") or "") or "").strip()
        if not wid:
            wid = "(no window)"
        grouped[(asset, reason)].add(wid)
    lines = [
        "=== SKIPS SUMMARY (by asset and skip reason, max %d rows, max %d per asset) ===" % (max_rows, max_per_asset),
        "",
    ]
    count = 0
    seen_asset = defaultdict(int)
    for (asset, reason), windows in sorted(grouped.items(), key=lambda x: (x[0][0], x[0][1])):
        if count >= max_rows:
            break
        if seen_asset[asset] >= max_per_asset:
            continue
        seen_asset[asset] += 1
        count += 1
        w_sorted = sorted(w for w in windows if w and w != "(no window)")
        if not w_sorted and "(no window)" in windows:
            windows_str = "(no window)"
        else:
            windows_str = ", ".join(w_sorted) if w_sorted else "(no window)"
        lines.append("%s skipped in %s due to %s." % (asset, windows_str, reason))
    return "\n".join(lines)


def _build_no_eligible_tickers_section(no_tickers: list[dict]) -> str:
    """Section for assets/markets that had no eligible tickers (no placement or skip possible)."""
    if not no_tickers:
        return ""
    lines = [
        "",
        "=== NO ELIGIBLE TICKERS ===",
        "For these asset/hour_market_id the strategy found no tickers in spot window (no placement or skip logged).",
        "",
        "ts_utc\tasset\thour_market_id",
        "-" * 60,
    ]
    for r in no_tickers:
        lines.append("\t".join([
            (r.get("ts_utc") or "")[:19],
            r.get("asset") or "",
            r.get("hour_market_id") or "",
        ]))
    lines.append("")
    lines.append("Total: %d" % len(no_tickers))
    return "\n".join(lines)


def build_report_text(
    placements: list[dict],
    skips: list[dict],
    outcomes: dict[str, dict],
    final_outcomes: dict[str, str],
    stoplosses: list[dict] | None = None,
    no_eligible_tickers: list[dict] | None = None,
) -> str:
    stoplosses = stoplosses or []
    no_eligible_tickers = no_eligible_tickers or []
    lines = [
        "HOURLY LAST-90S LIMIT-99 REPORT (placements + skips + stop-losses)",
        "",
        _build_summary_section(placements, stoplosses, outcomes, skips, no_eligible_tickers),
        "=== PLACEMENTS ===",
        "ts_utc\tasset\thour_market_id\tticker\tside\tprice_cents\tcount\tseconds_to_close\torder_id\tstatus\toutcome\texecuted\tfinal_outcome",
        "-" * 80,
    ]
    for p in placements:
        oid = p.get("order_id")
        o = (outcomes.get(oid) if oid else None) or {}
        ticker = p.get("ticker") or ""
        fo = (final_outcomes.get(ticker) or "n/a") if ticker else "n/a"
        row = [
            (p.get("ts_utc") or "")[:19],
            p.get("asset") or "",
            p.get("hour_market_id") or "",
            ticker,
            (p.get("side") or "").lower(),
            _fmt(p.get("price_cents"), "%d"),
            _fmt(p.get("count"), "%d"),
            _fmt(p.get("seconds_to_close")),
            oid or "",
            o.get("status") or "n/a",
            o.get("outcome") or "n/a",
            _fmt(o.get("executed"), "%d") if o.get("executed") is not None else "n/a",
            fo,
        ]
        lines.append("\t".join(str(x) for x in row))
    lines.append("")
    lines.append("Total placements: %d" % len(placements))
    lines.append("")
    lines.append(_build_stoploss_section(stoplosses))
    lines.append("")
    lines.append("=== SKIPS ===")
    lines.append("All skip types: distance_buffer (distance <= min_distance_at_placement), min_bid (bid < min_bid_cents), other. final_outcome = Kalshi market result.")
    lines.append("")
    lines.append("ts_utc\tasset\tticker\twindow_id\tskip_reason\tdetails\tfinal_outcome")
    lines.append("-" * 80)
    for s in skips:
        details = _skip_details(s).replace("\t", " ")
        ticker = s.get("ticker") or ""
        window_id = s.get("window_id") or _ticker_to_window_id_hourly(ticker)
        fo = (final_outcomes.get(ticker) or "n/a") if ticker else "n/a"
        lines.append("\t".join([
            (s.get("ts_utc") or "")[:19],
            s.get("asset") or "",
            ticker,
            window_id,
            s.get("skip_reason") or "other",
            details,
            fo,
        ]))
    lines.append("")
    lines.append("Total skips: %d" % len(skips))
    by_reason = {}
    for s in skips:
        r = s.get("skip_reason") or "other"
        by_reason[r] = by_reason.get(r, 0) + 1
    if by_reason:
        lines.append("  (%s)" % ", ".join("%s=%d" % (k, v) for k, v in sorted(by_reason.items())))
    lines.append("")
    lines.append(_build_skips_summary_grouped(skips))
    lines.append(_build_no_eligible_tickers_section(no_eligible_tickers))
    return "\n".join(lines)


def write_tsv(path: Path, cols: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    def cell(x):
        return "" if x is None else str(x).replace("\t", " ").replace("\r", " ").replace("\n", " ")
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("\t".join(cell(c) for c in cols) + "\n")
        for row in rows:
            f.write("\t".join(cell(c) for c in row) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description="Hourly last-90s report from bot log (placements, skips, stop-losses)")
    ap.add_argument("log_path", nargs="?", default=None, help="Path to bot log (default: project logs/bot.log)")
    ap.add_argument("--output", default=None, help="Output report path (default: reports/hourly_last_90s_report.txt)")
    ap.add_argument("--since-hours", type=float, metavar="N", help="Include events from last N hours")
    ap.add_argument(
        "--window-id-suffix",
        metavar="SUFFIX",
        help="Only include the hourly market with this close-hour suffix (ET). E.g. 26FEB2816 = 4PM ET Feb 28, 2026.",
    )
    ap.add_argument("--no-fetch-final-outcomes", action="store_true", help="Do not query Kalshi for final outcomes")
    args = ap.parse_args()
    root = _project_root()
    log_path = Path(args.log_path) if args.log_path else root / "logs" / "bot.log"
    if not log_path.is_absolute():
        log_path = root / log_path
    since_ts = 0.0
    if getattr(args, "since_hours", None) is not None and args.since_hours > 0:
        since_ts = (datetime.now(timezone.utc) - timedelta(hours=args.since_hours)).timestamp()
    placements = parse_placements(log_path)
    skips = parse_skips(log_path)
    stoplosses = parse_stoplosses(log_path)
    no_eligible_tickers = parse_no_eligible_tickers(log_path)
    outcomes = parse_outcomes(log_path)
    if since_ts > 0:
        placements = [p for p in placements if _parse_ts(p.get("ts_utc") or "") >= since_ts]
        skips = [s for s in skips if _parse_ts(s.get("ts_utc") or "") >= since_ts]
        stoplosses = [s for s in stoplosses if _parse_ts(s.get("ts_utc") or "") >= since_ts]
        no_eligible_tickers = [n for n in no_eligible_tickers if _parse_ts(n.get("ts_utc") or "") >= since_ts]
    window_suffix = (getattr(args, "window_id_suffix", None) or "").strip().upper()
    if window_suffix:
        placements = [p for p in placements if _window_suffix(p.get("hour_market_id") or p.get("ticker") or "") == window_suffix]
        skips = [s for s in skips if _window_suffix(s.get("window_id") or s.get("ticker") or "") == window_suffix]
        stoplosses = [s for s in stoplosses if _window_suffix(s.get("ticker") or "") == window_suffix]
        no_eligible_tickers = [n for n in no_eligible_tickers if _window_suffix(n.get("hour_market_id") or "") == window_suffix]
    all_tickers = set(p.get("ticker") for p in placements if p.get("ticker") and p.get("ticker") != "n/a")
    for s in skips:
        t = s.get("ticker") or ""
        if t and t != "n/a":
            all_tickers.add(t)
    final_outcomes = {}
    if all_tickers and not args.no_fetch_final_outcomes:
        print("[Fetching final outcomes from Kalshi for %d tickers...]" % len(all_tickers), file=sys.stderr)
        final_outcomes = fetch_final_outcomes(all_tickers)
    report = build_report_text(placements, skips, outcomes, final_outcomes, stoplosses=stoplosses, no_eligible_tickers=no_eligible_tickers)
    print(report)
    out_path = Path(args.output) if args.output else root / "reports" / "hourly_last_90s_report.txt"
    if not out_path.is_absolute():
        out_path = root / out_path
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report, encoding="utf-8")
        print("\n[Written: %s]" % out_path)
    except Exception as e:
        print("\n[Could not write report: %s]" % e, file=sys.stderr)
        return 1
    tsv_dir = out_path.parent
    try:
        cols = ["ts_utc", "asset", "hour_market_id", "ticker", "side", "price_cents", "count", "seconds_to_close", "order_id", "status", "outcome", "executed", "final_outcome"]
        rows = []
        for p in placements:
            oid = p.get("order_id")
            o = (outcomes.get(oid) if oid else None) or {}
            ticker = p.get("ticker") or ""
            fo = (final_outcomes.get(ticker) or "n/a") if ticker else "n/a"
            rows.append([
                (p.get("ts_utc") or "")[:19], p.get("asset") or "", p.get("hour_market_id") or "", ticker,
                (p.get("side") or "").lower(), _fmt(p.get("price_cents"), "%d"), _fmt(p.get("count"), "%d"),
                _fmt(p.get("seconds_to_close")), oid or "", o.get("status") or "n/a", o.get("outcome") or "n/a",
                _fmt(o.get("executed"), "%d") if o.get("executed") is not None else "n/a", fo,
            ])
        write_tsv(tsv_dir / "hourly_last_90s_placements.tsv", cols, rows)
        print("[Written: %s]" % (tsv_dir / "hourly_last_90s_placements.tsv"))
        cols = ["ts_utc", "asset", "ticker", "window_id", "skip_reason", "details", "final_outcome"]
        rows = []
        for s in skips:
            details = _skip_details(s).replace("\t", " ")
            ticker = s.get("ticker") or ""
            window_id = s.get("window_id") or _ticker_to_window_id_hourly(ticker)
            fo = (final_outcomes.get(ticker) or "n/a") if ticker else "n/a"
            rows.append([
                (s.get("ts_utc") or "")[:19], s.get("asset") or "", ticker, window_id,
                s.get("skip_reason") or "other", details, fo,
            ])
        write_tsv(tsv_dir / "hourly_last_90s_skips.tsv", cols, rows)
        print("[Written: %s]" % (tsv_dir / "hourly_last_90s_skips.tsv"))
        cols_sl = ["ts_utc", "asset", "ticker", "side", "count", "sell_price_cents", "entry_price_cents", "loss_pct"]
        rows_sl = [[
            (s.get("ts_utc") or "")[:19], s.get("asset") or "", s.get("ticker") or "", s.get("side") or "",
            _fmt(s.get("count"), "%d"), _fmt(s.get("sell_price_cents"), "%d"), _fmt(s.get("entry_price_cents"), "%d"), _fmt(s.get("loss_pct")),
        ] for s in stoplosses]
        write_tsv(tsv_dir / "hourly_last_90s_stoplosses.tsv", cols_sl, rows_sl)
        print("[Written: %s]" % (tsv_dir / "hourly_last_90s_stoplosses.tsv"))
        cols_ne = ["ts_utc", "asset", "hour_market_id"]
        rows_ne = [[(n.get("ts_utc") or "")[:19], n.get("asset") or "", n.get("hour_market_id") or ""] for n in no_eligible_tickers]
        write_tsv(tsv_dir / "hourly_last_90s_no_eligible.tsv", cols_ne, rows_ne)
        print("[Written: %s]" % (tsv_dir / "hourly_last_90s_no_eligible.tsv"))
        skips_summary_txt = _build_skips_summary_grouped(skips, max_rows=12, max_per_asset=3)
        (tsv_dir / "hourly_last_90s_skips_summary.txt").write_text(skips_summary_txt, encoding="utf-8")
        print("[Written: %s]" % (tsv_dir / "hourly_last_90s_skips_summary.txt"))
    except Exception as e:
        print("\n[Could not write TSVs: %s]" % e, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
