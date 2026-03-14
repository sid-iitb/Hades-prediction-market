#!/usr/bin/env python3
"""
Backfill strategy_report_last_90s with fallback (placed=0) rows for one 15-min window.
Use when the bot was not running when that window was "previous", so resolve never ran.

Usage:
  python -m tools.backfill_last_90s_window 26MAR060000 [--db PATH]

Default DB: data/bot_state.db
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = PROJECT_ROOT / "data" / "bot_state.db"

ASSETS = ["btc", "eth", "sol", "xrp"]
# Must match generate_all_kalshi_urls.ASSET_PREFIXES_15M (uppercase)
PREFIXES = {"btc": "KXBTC15M", "eth": "KXETH15M", "sol": "KXSOL15M", "xrp": "KXXRP15M"}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill last_90s strategy_report_last_90s with fallback rows for one window (e.g. 26MAR060000)"
    )
    parser.add_argument("window_suffix", help="Window suffix, e.g. 26MAR060000")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to bot_state.db")
    args = parser.parse_args()
    suffix = args.window_suffix.strip().upper()
    if not suffix:
        print("window_suffix required", file=sys.stderr)
        return 1
    if not args.db.exists():
        print(f"DB not found: {args.db}", file=sys.stderr)
        return 1

    sys.path.insert(0, str(PROJECT_ROOT))
    from bot.strategy_report_db import ensure_report_db, write_row_last_90s

    ensure_report_db(str(args.db))
    written = 0
    for asset in ASSETS:
        window_id = f"{PREFIXES.get(asset, 'KXBTC15M')}-{suffix}"
        import sqlite3
        conn = sqlite3.connect(str(args.db))
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM strategy_report_last_90s WHERE window_id = ? AND asset = ? LIMIT 1",
            (window_id, asset),
        )
        if cur.fetchone():
            conn.close()
            continue
        conn.close()
        try:
            write_row_last_90s(
                str(args.db),
                window_id,
                asset,
                placed=0,
                skip_reason="aggregated_skips",
                skip_details="Backfill: no row (window missed; bot not running when previous was this window).",
            )
            written += 1
            print(f"Wrote fallback row window_id={window_id} asset={asset}")
        except Exception as e:
            print(f"Write failed {window_id} {asset}: {e}", file=sys.stderr)
    print(f"Backfill done: {written} row(s) written for window {suffix}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
