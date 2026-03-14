#!/usr/bin/env python3
"""
Summarize V2 dry-run capture and pipeline health.

Reads data/v2_state.db (v2_telemetry_last_90s, v2_order_registry) and prints:
- Telemetry summary: counts by reason and asset for last_90s_limit_99.
- Recent telemetry rows (optional).
- Registry row count (in dry run usually 0 new orders).
- Short checklist of what to verify in logs.

Usage (from project root):
  python tools/verify_v2_dry_run.py
  python tools/verify_v2_dry_run.py --recent 30   # show last 30 telemetry rows
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def _db_path() -> Path:
    root = Path(__file__).resolve().parent.parent
    return root / "data" / "v2_state.db"


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify V2 dry-run data and pipeline")
    parser.add_argument("--recent", type=int, default=0, help="Show N most recent telemetry rows (0 = skip)")
    args = parser.parse_args()

    db = _db_path()
    if not db.exists():
        print(f"DB not found: {db}")
        print("Run 'python -m bot.v2_main' in dry run first so the pipeline creates and uses v2_state.db.")
        return 1

    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row

    # --- Telemetry summary ---
    try:
        cur = conn.execute(
            """
            SELECT reason, placed, asset, COUNT(*) AS n
            FROM v2_telemetry_last_90s
            GROUP BY reason, placed, asset
            ORDER BY reason, placed, asset
            """
        )
        rows = cur.fetchall()
    except sqlite3.OperationalError as e:
        if "no such table" in str(e).lower():
            print("Table v2_telemetry_last_90s not found. Run the pipeline at least through one 15m active window (last ~90s).")
        else:
            print("Telemetry query failed:", e)
        conn.close()
        return 1

    print("=== V2 dry-run verification ===\n")
    print("--- last_90s_limit_99 telemetry (v2_telemetry_last_90s) ---")
    if not rows:
        print("No rows yet. Run the bot until at least one 15m window is in its last ~90 seconds.")
    else:
        for r in rows:
            print(f"  reason={r['reason']} placed={r['placed']} asset={r['asset']} count={r['n']}")
        print()

    # --- Recent telemetry ---
    if args.recent > 0:
        cur = conn.execute(
            """
            SELECT window_id, asset, placed, reason, seconds_to_close, bid, distance, timestamp
            FROM v2_telemetry_last_90s
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            (args.recent,),
        )
        recs = cur.fetchall()
        print(f"--- Last {args.recent} telemetry rows ---")
        for r in recs:
            print(dict(r))
        print()

    # --- Registry (dry run typically has 0 new orders) ---
    try:
        cur = conn.execute("SELECT COUNT(*) AS n FROM v2_order_registry")
        reg_count = cur.fetchone()["n"]
        print("--- v2_order_registry ---")
        print(f"  Total orders: {reg_count} (in dry run expect 0 new; may have leftovers from a prior live run)")
        print()
    except sqlite3.OperationalError:
        pass

    conn.close()

    print("--- What to check in logs ---")
    print("  1. Startup: 'V2 dry_run=True', 'V2 DB initialized', 'Started fifteen_min pipeline thread'")
    print("  2. Every cycle: '[V2 DATA] Asset: ... | Quote: WS|REST | Spot: WS|REST | Distance: <number>'")
    print("  3. When conditions met: '[DRY RUN] Would place OrderIntent' or 'Would execute ExitAction'")
    print("  4. No [EXECUTION FATAL] lines (those are for live execution only)")
    print("\nSee docs/V2_DRY_RUN_VERIFICATION.md for full checklist.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
