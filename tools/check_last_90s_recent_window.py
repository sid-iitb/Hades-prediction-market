#!/usr/bin/env python3
"""
Check the most recent completed 15-min window for last_90s_limit_99.

Computes previous_15min_market_id (the window that just closed), queries
strategy_report_last_90s for that window, and prints row count + next steps
per docs/LAST_90S_MISSING_ROW_INVESTIGATION.md.

Usage:
  python -m tools.check_last_90s_recent_window [--db PATH]

Default DB: data/bot_state.db
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TABLE = "strategy_report_last_90s"
DEFAULT_DB = PROJECT_ROOT / "data" / "bot_state.db"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check last_90s DB for the most recent completed 15-min window (see docs/LAST_90S_MISSING_ROW_INVESTIGATION.md)"
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to bot_state.db")
    args = parser.parse_args()

    # Use same logic as strategy: previous 15-min market (ET)
    try:
        from bot.market import get_previous_15min_market_id
    except Exception as e:
        print(f"Cannot import bot.market: {e}", file=sys.stderr)
        return 1

    prev_id = get_previous_15min_market_id(asset="btc")
    suffix = prev_id.split("-")[-1] if "-" in prev_id else prev_id

    print(f"Most recent completed window: {suffix}")
    print(f"  (full previous_15min_market_id: {prev_id})")

    if not args.db.exists():
        print(f"  DB not found: {args.db}", file=sys.stderr)
        print("  Next: create/start bot so DB exists, then re-run.", file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(args.db))
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,))
        if not cur.fetchone():
            print(f"  Table {TABLE} not found in {args.db}. Run bot once to create it.")
            conn.close()
            return 0
        cur.execute(
            "SELECT COUNT(*) FROM " + TABLE + " WHERE window_id LIKE ?",
            (f"%{suffix}%",),
        )
        n = cur.fetchone()[0]
    finally:
        conn.close()

    print(f"  Rows in DB for this window: {n}")

    if n == 0:
        print()
        print("No row (placed=0 or placed=1) for this window → follow docs/LAST_90S_MISSING_ROW_INVESTIGATION.md:")
        print("  1. Search logs for: Entering main while loop, Resolve pass (previous_15min_market=..." + suffix + "),")
        print("     DEBUG_WS_1/2/3/4, Writing aggregated skip row / placement row.")
        print("  2. Identify where the pipeline stopped; apply proposed fixes (timing, config, DB, WS).")
        print("  3. If no log file: run bot with logs (e.g. python -u bot/main.py 2>&1 | tee bot.log).")
        return 0

    print("  At least one row present; no debug needed for this window.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
