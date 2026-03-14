#!/usr/bin/env python3
"""
Export all last_90s_limit_99 rows for windows at or after a given cutoff to CSV.
Column names match the strategy_report_last_90s table exactly.

Window cutoff: window_id suffix (e.g. 26MAR051830) — includes that window and all later ones
(e.g. 26MAR051845, 26MAR051900, ...). window_id format is ASSET-26MAR051830.

Usage:
  python -m tools.export_last_90s_after_csv 26MAR051830 [--db PATH] [--out PATH]

Default DB: data/bot_state.db
Default out: last_90s_limit_99_after_26MAR051830.csv (in repo root)
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TABLE = "strategy_report_last_90s"


def export_after_csv(
    cutoff_suffix: str,
    db_path: Path,
    out_path: Path,
) -> int:
    """Query strategy_report_last_90s for window_id suffix >= cutoff, write CSV. Returns row count."""
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return -1
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({TABLE})")
    columns = [row[1] for row in cur.fetchall()]
    # window_id is like KXBTC15M-26MAR051830; suffix after last '-' is the date-time
    cur.execute(
        f"""
        SELECT * FROM {TABLE}
        WHERE SUBSTR(window_id, INSTR(window_id, '-') + 1) >= ?
        ORDER BY window_id, asset, ts_utc
        """,
        (cutoff_suffix.strip().upper(),),
    )
    rows = cur.fetchall()
    conn.close()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for row in rows:
            w.writerow([row[c] for c in columns])
    print(f"Wrote {len(rows)} rows to {out_path} (window_id suffix >= {cutoff_suffix}, table: {TABLE})")
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export last_90s_limit_99 rows for all windows at or after cutoff to CSV (same column names as table)"
    )
    parser.add_argument("cutoff_suffix", help="Window suffix cutoff, e.g. 26MAR051830 (inclusive)")
    parser.add_argument("--db", type=Path, default=PROJECT_ROOT / "data" / "bot_state.db", help="Path to bot_state.db")
    parser.add_argument("--out", type=Path, default=None, help="Output CSV path (default: last_90s_limit_99_after_<cutoff>.csv)")
    args = parser.parse_args()
    out_path = args.out or (PROJECT_ROOT / f"last_90s_limit_99_after_{args.cutoff_suffix}.csv")
    n = export_after_csv(args.cutoff_suffix, args.db, out_path)
    return 0 if n >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
