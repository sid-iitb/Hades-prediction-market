#!/usr/bin/env python3
"""
Export last_90s_limit_99 rows for the last N windows to a single CSV.
Column names match strategy_report_last_90s table exactly.

Usage:
  python -m tools.export_last_90s_last_windows_csv [--windows N] [--out PATH]

Default: 4 windows, output: data/last_90s_last_windows.csv
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TABLE = "strategy_report_last_90s"


def export_last_windows_csv(
    db_path: Path,
    out_path: Path,
    num_windows: int = 4,
) -> int:
    """Query strategy_report_last_90s for last num_windows (by ts_utc), write CSV. Returns row count."""
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return -1
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({TABLE})")
    columns = [row[1] for row in cur.fetchall()]
    # Last N windows by suffix (e.g. 26MAR060100); each window has 4 rows (btc, eth, sol, xrp)
    cur.execute(
        f"""
        SELECT * FROM {TABLE}
        WHERE SUBSTR(window_id, INSTR(window_id, '-') + 1) IN (
            SELECT suffix FROM (
                SELECT SUBSTR(window_id, INSTR(window_id, '-') + 1) AS suffix,
                       MAX(ts_utc) AS mt
                FROM {TABLE}
                GROUP BY suffix
                ORDER BY mt DESC
                LIMIT ?
            )
        )
        ORDER BY window_id DESC, asset, ts_utc
        """,
        (num_windows,),
    )
    rows = cur.fetchall()
    conn.close()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for row in rows:
            w.writerow([row[c] if row[c] is not None else "" for c in columns])
    print(f"Wrote {len(rows)} rows to {out_path} (last {num_windows} windows, table: {TABLE})")
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export last_90s strategy_report_last_90s last N windows to one CSV (columns = table columns)"
    )
    parser.add_argument("--windows", type=int, default=4, help="Number of most recent windows (default: 4)")
    parser.add_argument("--db", type=Path, default=PROJECT_ROOT / "data" / "bot_state.db", help="Path to bot_state.db")
    parser.add_argument("--out", type=Path, default=PROJECT_ROOT / "data" / "last_90s_last_windows.csv", help="Output CSV path")
    args = parser.parse_args()
    n = export_last_windows_csv(args.db, args.out, args.windows)
    return 0 if n >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
