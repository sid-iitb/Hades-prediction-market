#!/usr/bin/env python3
"""
Verify last_90s_limit_99 filled count against Kalshi account fills.

Fetches portfolio fills from Kalshi API, filters to 15-min market buys
(KXBTC15M, KXETH15M, KXSOL15M, KXXRP15M), and reports:
  - Total buy fills (rows)
  - Distinct orders with at least one buy fill (order count)
  - Optional: same restricted to a time range to match report log window.

Usage:
  python tools/verify_last_90s_fills.py
  python tools/verify_last_90s_fills.py --since-days 7
  python tools/verify_last_90s_fills.py --min-ts 1730000000 --max-ts 1730200000
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# 15-min ticker prefixes used by last_90s_limit_99 (from config assets)
LAST_90S_TICKER_PREFIXES = ("KXBTC15M-", "KXETH15M-", "KXSOL15M-", "KXXRP15M-")


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description="Verify last_90s fills from Kalshi account")
    parser.add_argument("--since-days", type=float, default=None, help="Only fills in the last N days")
    parser.add_argument("--min-ts", type=int, default=None, help="Only fills with ts >= this (Unix seconds)")
    parser.add_argument("--max-ts", type=int, default=None, help="Only fills with ts <= this (Unix seconds)")
    parser.add_argument("--verbose", action="store_true", help="Print each fill line")
    parser.add_argument("--cross-check", metavar="TSV", default=None, help="Cross-check: read placements TSV, count report 'executed=1' order_ids that have a fill in Kalshi (path to last_90s_placements.tsv)")
    args = parser.parse_args()

    root = _project_root()
    sys.path.insert(0, str(root))
    try:
        from dotenv import load_dotenv
        load_dotenv(root / ".env")
        from src.client.kalshi_client import KalshiClient
    except ImportError as e:
        print("Error importing:", e, file=sys.stderr)
        sys.exit(1)

    min_ts = args.min_ts
    max_ts = args.max_ts
    if args.since_days is not None:
        import time
        now = int(time.time())
        min_ts = now - int(args.since_days * 86400)
        if max_ts is None:
            max_ts = now

    try:
        client = KalshiClient()
    except Exception as e:
        print("Kalshi client init failed:", e, file=sys.stderr)
        sys.exit(1)

    print("Fetching portfolio fills from Kalshi...", file=sys.stderr)
    try:
        all_fills = client.get_all_fills(min_ts=min_ts, max_ts=max_ts, limit=200, max_pages=100)
    except Exception as e:
        print("Failed to fetch fills:", e, file=sys.stderr)
        sys.exit(1)

    # Filter: 15-min tickers only, buy only (we're counting entry placements, not sells)
    last_90s_buys = []
    for f in all_fills:
        ticker = (f.get("ticker") or f.get("market_ticker") or "").strip()
        action = (f.get("action") or "").strip().lower()
        if action != "buy":
            continue
        if not any(ticker.startswith(p) for p in LAST_90S_TICKER_PREFIXES):
            continue
        last_90s_buys.append(f)

    order_ids = {f.get("order_id") for f in last_90s_buys if f.get("order_id")}
    total_contracts = sum(int(f.get("count", 0) or 0) for f in last_90s_buys)

    if min_ts or max_ts:
        print("Time filter: min_ts=%s max_ts=%s" % (min_ts, max_ts), file=sys.stderr)
    print("Total fills from API (all tickers): %d" % len(all_fills), file=sys.stderr)
    print("", file=sys.stderr)

    print("=== Last-90s (15-min) BUY fills (KXBTC15M, KXETH15M, KXSOL15M, KXXRP15M) ===")
    print("Fill rows (buy, 15-min): %d" % len(last_90s_buys))
    print("Distinct orders with ≥1 buy fill: %d" % len(order_ids))
    print("Total contracts bought: %d" % total_contracts)
    print("")
    print("Compare with report: 'Executed (filled): 84' = number of placement orders that had executed=1 in the log.")
    print("So 'Distinct orders' above should match that count if the report window and API window align.")

    if args.verbose and last_90s_buys:
        print("")
        print("Fills (order_id, ticker, side, count, created_time):")
        for f in sorted(last_90s_buys, key=lambda x: (x.get("created_time") or "", x.get("order_id") or "")):
            print("  %s  %s  %s  %s  %s" % (
                f.get("order_id", ""),
                f.get("ticker", f.get("market_ticker", "")),
                f.get("side", ""),
                f.get("count", 0),
                f.get("created_time", ""),
            ))

    # Cross-check: report says 84 executed; how many of those order_ids have a fill in Kalshi?
    if args.cross_check:
        tsv_path = Path(args.cross_check)
        if not tsv_path.is_absolute():
            tsv_path = root / tsv_path
        if not tsv_path.exists():
            print("Cross-check TSV not found: %s" % tsv_path, file=sys.stderr)
        else:
            with open(tsv_path) as f:
                header = f.readline().strip().split("\t")
                try:
                    idx_order_id = header.index("order_id")
                    idx_executed = header.index("executed")
                except ValueError:
                    print("Cross-check: missing order_id or executed column", file=sys.stderr)
                else:
                    report_executed_ids = set()
                    for line in f:
                        parts = line.strip().split("\t")
                        if len(parts) <= max(idx_order_id, idx_executed):
                            continue
                        try:
                            ex = parts[idx_executed].strip()
                            oid = parts[idx_order_id].strip()
                        except IndexError:
                            continue
                        if ex == "1" and oid and oid != "n/a":
                            report_executed_ids.add(oid)
                    kalshi_filled_order_ids = {f.get("order_id") for f in last_90s_buys if f.get("order_id")}
                    in_both = report_executed_ids & kalshi_filled_order_ids
                    only_report = report_executed_ids - kalshi_filled_order_ids
                    only_kalshi = kalshi_filled_order_ids - report_executed_ids
                    print("")
                    print("=== Cross-check (report executed=1 vs Kalshi fills) ===")
                    print("Report placements with executed=1 (order count): %d" % len(report_executed_ids))
                    print("Of those, present in Kalshi fills (buy, 15-min): %d" % len(in_both))
                    print("In report only (no fill in Kalshi): %d" % len(only_report))
                    if only_report and len(only_report) <= 20:
                        print("  Order IDs: %s" % ", ".join(sorted(only_report)))
                    elif only_report:
                        print("  (First 10 order IDs: %s)" % ", ".join(sorted(only_report)[:10]))


if __name__ == "__main__":
    main()
