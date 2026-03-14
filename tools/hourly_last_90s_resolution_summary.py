#!/usr/bin/env python3
"""
Resolution summary for hourly_last_90s_limit_99 filled orders: how many resolved
in our favor, how many had stop loss. Reads hourly_last_90s_placements.tsv and
hourly_last_90s_stoplosses.tsv, fetches Kalshi market results, and reports
won/lost/stopped out.

Usage:
  python tools/hourly_last_90s_resolution_summary.py
  python tools/hourly_last_90s_resolution_summary.py --placements reports/hourly_last_90s_placements.tsv --stoplosses reports/hourly_last_90s_stoplosses.tsv
  python tools/hourly_last_90s_resolution_summary.py --no-fetch  # use final_outcome from TSV if present
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def fetch_final_outcomes(tickers: set[str]) -> dict[str, str]:
    """Query Kalshi for each ticker's market result. Returns ticker -> yes|no|scalar|not_settled|n/a."""
    if not tickers:
        return {}
    root = _project_root()
    sys.path.insert(0, str(root))
    try:
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


def main():
    parser = argparse.ArgumentParser(description="Hourly last-90s filled orders: resolved in favor vs stop loss")
    parser.add_argument("--placements", default="reports/hourly_last_90s_placements.tsv", help="Placements TSV path")
    parser.add_argument("--stoplosses", default="reports/hourly_last_90s_stoplosses.tsv", help="Stop losses TSV path")
    parser.add_argument("--no-fetch", action="store_true", help="Do not fetch Kalshi; use final_outcome from TSV if present")
    args = parser.parse_args()

    root = _project_root()
    placements_path = Path(args.placements) if Path(args.placements).is_absolute() else root / args.placements
    stoplosses_path = Path(args.stoplosses) if Path(args.stoplosses).is_absolute() else root / args.stoplosses

    if not placements_path.exists():
        print("Placements TSV not found:", placements_path, file=sys.stderr)
        sys.exit(1)

    # Hourly placements TSV: ts_utc, asset, hour_market_id, ticker, side, price_cents, count, seconds_to_close, order_id, status, outcome, executed, final_outcome
    with open(placements_path) as f:
        header = f.readline().strip().split("\t")
        try:
            idx_order_id = header.index("order_id")
            idx_executed = header.index("executed")
            idx_ticker = header.index("ticker")
            idx_asset = header.index("asset")
            idx_side = header.index("side")
            idx_final_outcome = header.index("final_outcome") if "final_outcome" in header else None
        except ValueError as e:
            print("Missing column in placements TSV:", e, file=sys.stderr)
            sys.exit(1)

    filled: list[dict] = []
    seen_oid: set[str] = set()
    with open(placements_path) as f:
        f.readline()
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) <= max(idx_order_id, idx_executed):
                continue
            if parts[idx_executed].strip() != "1":
                continue
            oid = parts[idx_order_id].strip()
            if not oid or oid == "n/a" or oid in seen_oid:
                continue
            seen_oid.add(oid)
            ticker = parts[idx_ticker].strip() if idx_ticker < len(parts) else ""
            asset = parts[idx_asset].strip() if idx_asset < len(parts) else ""
            side = (parts[idx_side].strip() or "").lower() if idx_side < len(parts) else ""
            fo_tsv = parts[idx_final_outcome].strip() if idx_final_outcome is not None and idx_final_outcome < len(parts) else ""
            filled.append({
                "order_id": oid,
                "ticker": ticker,
                "asset": asset,
                "side": side,
                "final_outcome_tsv": fo_tsv or None,
            })

    # Hourly stoplosses TSV: ts_utc, asset, ticker, side, count, sell_price_cents, entry_price_cents, loss_pct (no order_id)
    stop_tickers: set[tuple[str, str]] = set()
    if stoplosses_path.exists():
        with open(stoplosses_path) as f:
            sl_header = f.readline().strip().split("\t")
            if sl_header and "asset" in sl_header and "ticker" in sl_header:
                sl_asset = sl_header.index("asset")
                sl_ticker = sl_header.index("ticker")
                for line in f:
                    p = line.strip().split("\t")
                    if len(p) > max(sl_asset, sl_ticker):
                        a, t = (p[sl_asset].strip() or "").upper(), (p[sl_ticker].strip() or "")
                        if a and t:
                            stop_tickers.add((a, t))

    # Fetch final outcomes from Kalshi unless --no-fetch
    tickers_to_fetch = {row["ticker"] for row in filled if row["ticker"] and row["ticker"] != "n/a"}
    if args.no_fetch or not tickers_to_fetch:
        final_outcomes: dict[str, str] = {}
    else:
        print("Fetching market results from Kalshi for %d tickers..." % len(tickers_to_fetch), file=sys.stderr)
        final_outcomes = fetch_final_outcomes(tickers_to_fetch)

    # Classify each filled order
    stop_loss_count = 0
    won = 0
    lost = 0
    unknown = 0
    rows_out: list[tuple[str, str, str, str, str, str]] = []

    for row in filled:
        ticker = row["ticker"] or ""
        asset = (row["asset"] or "").upper()
        side = row["side"] or ""
        fo = final_outcomes.get(ticker) or row.get("final_outcome_tsv") or "n/a"
        fo = (fo or "").strip().lower()

        is_stop = (asset, ticker) in stop_tickers
        if is_stop:
            stop_loss_count += 1

        if fo in ("yes", "no"):
            if (side == "yes" and fo == "yes") or (side == "no" and fo == "no"):
                res = "won"
                won += 1
            else:
                res = "lost"
                lost += 1
        else:
            res = "unknown"
            unknown += 1

        rows_out.append((
            ticker,
            asset,
            side,
            "stop" if is_stop else "-",
            fo,
            res,
        ))

    # Print report
    print("")
    print("=== Hourly last-90s limit-99 filled orders: resolution summary ===")
    print("(Based on %d unique filled orders from hourly_last_90s_placements.tsv with executed=1)" % len(filled))
    print("")
    print("--- Counts ---")
    print("  Total filled (unique orders):     %d" % len(filled))
    print("  Had stop loss:                    %d" % stop_loss_count)
    print("  Resolved in our favor (won):      %d" % won)
    print("  Resolved against us (lost):       %d" % lost)
    print("  Unknown (market not yes/no):      %d" % unknown)
    print("")
    print("  (Won + Lost + Unknown = %d; stop loss is a subset of these.)" % (won + lost + unknown))
    print("")
    print("--- Per-order detail (ticker, asset, side, stop_loss, final_outcome, result) ---")
    print("%s\t%s\t%s\t%s\t%s\t%s" % ("ticker", "asset", "side", "stop_loss", "final_outcome", "result"))
    for r in rows_out:
        print("%s\t%s\t%s\t%s\t%s\t%s" % r)
    print("")
    print("Done.")


if __name__ == "__main__":
    main()
