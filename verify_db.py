#!/usr/bin/env python3
"""
Quick verification of V2 state DB: telemetry and order registry.
Run from project root: python verify_db.py
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parent
    db_path = root / "data" / "v2_state.db"
    if not db_path.exists():
        print(f"DB not found: {db_path}")
        return 1

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Last 10 rows from v2_telemetry_last_90s
    print("--- Last 10 rows from v2_telemetry_last_90s ---")
    try:
        cur = conn.execute(
            """
            SELECT id, window_id, asset, placed, seconds_to_close, bid, distance, reason, timestamp
            FROM v2_telemetry_last_90s
            ORDER BY id DESC
            LIMIT 10
            """
        )
        rows = cur.fetchall()
        if not rows:
            print("(no rows)")
        else:
            for r in rows:
                print(
                    f"  id={r['id']} window_id={r['window_id']} asset={r['asset']} placed={r['placed']} "
                    f"seconds_to_close={r['seconds_to_close']} bid={r['bid']} distance={r['distance']} "
                    f"reason={r['reason']} timestamp={r['timestamp']}"
                )
    except sqlite3.OperationalError as e:
        print(f"Error: {e}")

    # v2_order_registry row count
    print("\n--- v2_order_registry ---")
    try:
        cur = conn.execute("SELECT COUNT(*) AS n FROM v2_order_registry")
        n = cur.fetchone()["n"]
        print(f"  Total rows: {n}")
        if n == 0:
            print("  dry_run protected us: no real orders were placed.")
        else:
            print("  (registry has orders from a prior live run or non–dry-run)")
    except sqlite3.OperationalError as e:
        print(f"Error: {e}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
