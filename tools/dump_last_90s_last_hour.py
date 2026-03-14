#!/usr/bin/env python3
"""
Dump strategy_report_last_90s (last_90s_limit_99) rows from the last N hours to CSV.
Column names match the table. Usage: python tools/dump_last_90s_last_hour.py [hours=1] [output.csv]
"""
from __future__ import annotations

import csv
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = PROJECT_ROOT / "data" / "bot_state.db"
TABLE = "strategy_report_last_90s"


def main() -> int:
    hours = 1.0
    out_path = PROJECT_ROOT / "last_90s_limit_99_last1h.csv"
    if len(sys.argv) >= 2:
        try:
            hours = float(sys.argv[1])
        except ValueError:
            pass
    if len(sys.argv) >= 3:
        out_path = Path(sys.argv[2])
    db_path = PROJECT_ROOT / "data" / "bot_state.db"
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 1
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({TABLE})")
    columns = [row[1] for row in cur.fetchall()]
    cur.execute(
        f"SELECT * FROM {TABLE} WHERE ts_utc >= datetime('now', ?) ORDER BY ts_utc, window_id, asset",
        (f"-{hours} hours",),
    )
    rows = cur.fetchall()
    conn.close()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for row in rows:
            w.writerow([row[c] for c in columns])
    print(f"Wrote {len(rows)} rows to {out_path} (last {hours}h, table {TABLE})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
