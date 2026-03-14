#!/usr/bin/env python3
"""
Live DB verification for last_90s strategy (bypasses hourly TSV).

Queries data/bot_state.db → strategy_report_last_90s for the last 30 minutes.
Prints:
  - Last 5 rows where limit_or_market == 'limit' OR placed == 1 (seconds_to_close,
    min_distance_threshold, price_cents; time-decay check).
  - Last 5 rows where placed == 0 (skip_reason, skip_details; waiting_for_dip check).
  - Whether min_distance_threshold matches time-decay from config bases.

Usage:
  python tools/verify_last_90s_live_db.py
  python tools/verify_last_90s_live_db.py --db /path/to/bot_state.db
  python tools/verify_last_90s_live_db.py --out verification_live.txt
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Config bases (fifteen_min.last_90s_limit_99.min_distance_at_placement)
BASE_MIN_DISTANCE = {"btc": 35.0, "eth": 2.5, "sol": 0.10, "xrp": 0.003}


def time_decay_multiplier(seconds_to_close: float) -> float:
    """Match bot/last_90s_strategy._get_time_decay_multiplier."""
    try:
        s = float(seconds_to_close)
    except (TypeError, ValueError):
        return 1.0
    if s >= 55:
        return 1.0
    if s >= 45:
        return 0.8
    if s >= 35:
        return 0.6
    if s >= 25:
        return 0.4
    return 0.2


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify last_90s from live SQLite DB (last 30 min)")
    ap.add_argument("--db", default=None, help="Path to bot_state.db (default: data/bot_state.db)")
    ap.add_argument("--minutes", type=int, default=30, help="Look back window in minutes (default 30)")
    ap.add_argument("--out", default=None, help="Also write output to this file")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent.parent
    db_path = Path(args.db) if args.db else root / "data" / "bot_state.db"
    if not db_path.is_file():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 1

    since = (datetime.now(timezone.utc) - timedelta(minutes=args.minutes)).strftime("%Y-%m-%dT%H:%M:%S")
    out_lines: list[str] = []

    def log(s: str = "") -> None:
        print(s)
        out_lines.append(s)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Check table exists
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='strategy_report_last_90s'"
    )
    if not cur.fetchone():
        log("Table strategy_report_last_90s not found in DB.")
        conn.close()
        if args.out:
            Path(args.out).write_text("\n".join(out_lines), encoding="utf-8")
        return 1

    log("=" * 72)
    log("  LIVE DB VERIFICATION — strategy_report_last_90s (last %d minutes)" % args.minutes)
    log("  DB: %s" % db_path)
    log("  Since: %s UTC" % since)
    log("=" * 72)

    # --- Last 5 rows where limit_or_market == 'limit' OR placed == 1 ---
    cur.execute(
        """
        SELECT ts_utc, window_id, asset, ticker, side, seconds_to_close, distance,
               min_distance_threshold, bid, limit_or_market, price_cents, placed
        FROM strategy_report_last_90s
        WHERE ts_utc >= ?
          AND (limit_or_market = 'limit' OR placed = 1)
        ORDER BY ts_utc DESC
        LIMIT 5
        """,
        (since,),
    )
    placed_rows = cur.fetchall()

    log("")
    log("--- Last 5 rows (limit_or_market = 'limit' OR placed = 1) ---")
    if not placed_rows:
        log("  (none in last %d minutes)" % args.minutes)
    else:
        for r in placed_rows:
            log("  ts_utc: %s" % (r["ts_utc"] or ""))
            log("  window_id: %s  asset: %s  ticker: %s  side: %s" % (
                r["window_id"] or "", r["asset"] or "", r["ticker"] or "", r["side"] or ""
            ))
            log("  seconds_to_close: %s  min_distance_threshold: %s  price_cents: %s  limit_or_market: %s  placed: %s" % (
                r["seconds_to_close"],
                r["min_distance_threshold"],
                r["price_cents"],
                r["limit_or_market"] or "",
                r["placed"],
            ))
            log("  distance: %s  bid: %s" % (r["distance"], r["bid"]))
            log("")

    # --- Last 5 rows where placed == 0 ---
    cur.execute(
        """
        SELECT ts_utc, window_id, asset, ticker, skip_reason, skip_details, placed
        FROM strategy_report_last_90s
        WHERE ts_utc >= ? AND (placed = 0 OR placed IS NULL)
        ORDER BY ts_utc DESC
        LIMIT 5
        """,
        (since,),
    )
    skip_rows = cur.fetchall()

    log("--- Last 5 rows (placed = 0) — skip_reason / skip_details ---")
    if not skip_rows:
        log("  (none in last %d minutes)" % args.minutes)
    else:
        for r in skip_rows:
            log("  ts_utc: %s  window_id: %s  asset: %s" % (
                r["ts_utc"] or "", r["window_id"] or "", r["asset"] or ""
            ))
            log("  skip_reason: %s" % (r["skip_reason"] or "(null)"))
            log("  skip_details: %s" % (r["skip_details"] or "(null)")[:200])
            if r["skip_details"] and len((r["skip_details"] or "")) > 200:
                log("    ... (truncated)")
            log("")

    # --- Time-decay check: does min_distance_threshold = base[asset] * multiplier(seconds_to_close)? ---
    log("--- Time-decay verification (config bases: btc=35, eth=2.5, sol=0.10, xrp=0.003) ---")
    cur.execute(
        """
        SELECT ts_utc, window_id, asset, seconds_to_close, min_distance_threshold, placed
        FROM strategy_report_last_90s
        WHERE ts_utc >= ? AND placed = 1 AND min_distance_threshold IS NOT NULL
        ORDER BY ts_utc DESC
        LIMIT 20
        """,
        (since,),
    )
    decay_rows = cur.fetchall()
    if not decay_rows:
        log("  No placed rows with min_distance_threshold in last %d minutes." % args.minutes)
    else:
        log("  Multipliers: >=55s→1.0, >=45s→0.8, >=35s→0.6, >=25s→0.4, <25s→0.2")
        log("")
        for r in decay_rows:
            asset = (r["asset"] or "").strip().lower()
            base = BASE_MIN_DISTANCE.get(asset)
            secs = r["seconds_to_close"]
            try:
                secs_f = float(secs) if secs is not None else None
            except (TypeError, ValueError):
                secs_f = None
            mult = time_decay_multiplier(secs_f) if secs_f is not None else None
            stored = r["min_distance_threshold"]
            try:
                stored_f = float(stored) if stored is not None else None
            except (TypeError, ValueError):
                stored_f = None
            if base is not None and mult is not None:
                expected = base * mult
                match = "OK" if stored_f is not None and abs(stored_f - expected) < 0.01 else "MISMATCH"
                log("  %s  asset=%s  seconds_to_close=%s  multiplier=%s  base=%s  expected=%.4f  stored=%s  [%s]" % (
                    (r["ts_utc"] or "")[:19],
                    asset,
                    secs,
                    mult,
                    base,
                    expected,
                    stored,
                    match,
                ))
            else:
                log("  %s  asset=%s  seconds_to_close=%s  stored=%s  (base unknown or invalid secs)" % (
                    (r["ts_utc"] or "")[:19], asset, secs, stored
                ))

    # --- waiting_for_dip in skip_reason? ---
    cur.execute(
        """
        SELECT COUNT(*) FROM strategy_report_last_90s
        WHERE ts_utc >= ? AND (skip_reason = 'waiting_for_dip' OR skip_details LIKE '%waiting_for_dip%')
        """,
        (since,),
    )
    waiting_count = cur.fetchone()[0]
    log("")
    log("--- waiting_for_dip in DB (last %d min) ---" % args.minutes)
    log("  Rows with skip_reason='waiting_for_dip' or skip_details containing 'waiting_for_dip': %d" % waiting_count)

    conn.close()

    log("")
    log("=" * 72)
    log("  Done.")
    log("=" * 72)

    if args.out:
        out_path = Path(args.out)
        out_path.write_text("\n".join(out_lines), encoding="utf-8")
        print("(also written to %s)" % out_path, file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
