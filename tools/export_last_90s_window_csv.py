#!/usr/bin/env python3
"""
Export last_90s_limit_99 rows for a single 15-min window to CSV.
Window is matched by suffix (e.g. 26MAR051800 matches KXBTC15M-26MAR051800, KXETH15M-26MAR051800, ...).

Usage:
  python -m tools.export_last_90s_window_csv 26MAR051800 [--db PATH] [--out PATH]

Default DB: data/bot_state.db
Default out: last_90s_limit_99_26MAR051800.csv (in repo root)
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TABLE = "strategy_report_last_90s"


def export_window_csv(
    window_suffix: str,
    db_path: Path,
    out_path: Path,
) -> int:
    """Query strategy_report_last_90s for window_id LIKE %window_suffix%, write CSV. Returns row count."""
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return -1
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({TABLE})")
    columns = [row[1] for row in cur.fetchall()]
    pattern = f"%{window_suffix}%"
    cur.execute(
        f"SELECT * FROM {TABLE} WHERE window_id LIKE ? ORDER BY window_id, asset, ts_utc",
        (pattern,),
    )
    rows = cur.fetchall()
    conn.close()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for row in rows:
            w.writerow([row[c] for c in columns])
    print(f"Wrote {len(rows)} rows to {out_path} (window suffix: {window_suffix}, table: {TABLE})")
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export last_90s_limit_99 strategy_report_last_90s for one window to CSV")
    parser.add_argument("window_suffix", help="Window suffix, e.g. 26MAR051800")
    parser.add_argument("--db", type=Path, default=PROJECT_ROOT / "data" / "bot_state.db", help="Path to bot_state.db")
    parser.add_argument("--out", type=Path, default=None, help="Output CSV path (default: last_90s_limit_99_<window_suffix>.csv)")
    args = parser.parse_args()
    out_path = args.out or (PROJECT_ROOT / f"last_90s_limit_99_{args.window_suffix}.csv")
    n = export_window_csv(args.window_suffix, args.db, out_path)
    return 0 if n >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
