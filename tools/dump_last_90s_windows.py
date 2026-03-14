#!/usr/bin/env python3
"""
Dump strategy_report_last_90s rows for specific windows (all assets).
Column names match the table.
Usage: python tools/dump_last_90s_windows.py [output.csv]
Windows (suffixes): 26MAR051615, 26MAR051630, 26MAR051645, 26MAR051700
"""
from __future__ import annotations

import csv
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TABLE = "strategy_report_last_90s"

# Window suffixes requested: 1615, 1630, 1645, 1700 (26MAR05 = Mar 5, 2026)
WINDOW_SUFFIXES = ("26MAR051615", "26MAR051630", "26MAR051645", "26MAR051700")


def main() -> int:
    out_path = PROJECT_ROOT / "last_90s_limit_99_last1h.csv"
    if len(sys.argv) >= 2:
        out_path = Path(sys.argv[1])
    db_path = PROJECT_ROOT / "data" / "bot_state.db"
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 1
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({TABLE})")
    columns = [row[1] for row in cur.fetchall()]
    # Match window_id ending with any of the four suffixes (e.g. KXBTC15M-26MAR051615)
    placeholders = " OR ".join([f"window_id LIKE ?" for _ in WINDOW_SUFFIXES])
    params = [f"%{s}" for s in WINDOW_SUFFIXES]
    cur.execute(
        f"SELECT * FROM {TABLE} WHERE {placeholders} ORDER BY window_id, asset, ts_utc",
        params,
    )
    rows = cur.fetchall()
    conn.close()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for row in rows:
            w.writerow([row[c] for c in columns])
    print(f"Wrote {len(rows)} rows to {out_path} (windows: {', '.join(WINDOW_SUFFIXES)}, table {TABLE})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
