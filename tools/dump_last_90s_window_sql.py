#!/usr/bin/env python3
"""
Dump strategy_report_last_90s rows for a single 15-min window to a .sql file.
Output: schema (CREATE TABLE) + INSERT statements for rows matching window_id LIKE %window_suffix%.
Column names in the dump match the table.

Usage:
  python -m tools.dump_last_90s_window_sql 26MAR052245 [--db PATH] [--out PATH]

Default DB: data/bot_state.db
Default out: last_90s_limit_99_26MAR052245.sql (in repo root)
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TABLE = "strategy_report_last_90s"


def _escape_sql(s: str) -> str:
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def dump_window_sql(window_suffix: str, db_path: Path, out_path: Path) -> int:
    """Dump schema + INSERTs for window_id LIKE %window_suffix%. Returns row count."""
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return -1
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({TABLE})")
    info = cur.fetchall()
    columns = [row[1] for row in info]
    # Build CREATE TABLE from current schema so it matches INSERT columns
    col_defs = []
    for row in info:
        cid, name, ctype, notnull, default, pk = row[0], row[1], row[2] or "TEXT", row[3], row[4], row[5]
        col_defs.append(f"  {name} {ctype}" + (" PRIMARY KEY" if pk else ""))
    create_sql = f"CREATE TABLE IF NOT EXISTS {TABLE} (\n" + ",\n".join(col_defs) + "\n)"
    pattern = f"%{window_suffix}%"
    cur.execute(
        f"SELECT * FROM {TABLE} WHERE window_id LIKE ? ORDER BY window_id, asset, ts_utc",
        (pattern,),
    )
    rows = cur.fetchall()
    conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("-- strategy_report_last_90s dump for window_id LIKE %" + window_suffix + "%\n")
        f.write(create_sql + ";\n")
        col_list = ", ".join(columns)
        for row in rows:
            vals = []
            for c in columns:
                v = row[c]
                if v is None:
                    vals.append("NULL")
                elif isinstance(v, (int, float)) and not isinstance(v, bool):
                    vals.append(str(v))
                else:
                    vals.append(_escape_sql(v))
            f.write(f"INSERT INTO {TABLE} ({col_list}) VALUES ({', '.join(vals)});\n")
    print(f"Wrote {len(rows)} row(s) to {out_path} (window suffix: {window_suffix}, table: {TABLE})")
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="SQL dump of strategy_report_last_90s for one window")
    parser.add_argument("window_suffix", help="Window suffix, e.g. 26MAR052245")
    parser.add_argument("--db", type=Path, default=PROJECT_ROOT / "data" / "bot_state.db", help="Path to bot_state.db")
    parser.add_argument("--out", type=Path, default=None, help="Output .sql path")
    args = parser.parse_args()
    out_path = args.out or (PROJECT_ROOT / f"last_90s_limit_99_{args.window_suffix}.sql")
    n = dump_window_sql(args.window_suffix, args.db, out_path)
    return 0 if n >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
