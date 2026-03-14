#!/usr/bin/env python3
"""
Export last_90s_limit_99 rows from strategy_report_last_90s to CSV with column names matching the table.

Usage:
  python tools/export_last_90s_csv.py --window 26MAR061215 --asset btc
  python tools/export_last_90s_csv.py --window 26MAR061215 --asset btc --output last_90s_limit_99_btc_26MAR061215.csv
  python tools/export_last_90s_csv.py --window 26MAR061215 --asset btc --db data/bot_state.db

Matches rows where window_id contains the given window string (e.g. KXBTC15M-26MAR061215) and asset = btc.
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bot.strategy_report_db import LAST_90S_TABLE, ensure_report_db


def get_db_path(project_root: Path, config_path: str | None) -> str:
    if config_path and Path(config_path).exists():
        try:
            from bot.config_loader import load_config
            config = load_config(config_path)
            return (config.get("state") or {}).get("db_path", str(project_root / "data" / "bot_state.db"))
        except Exception:
            pass
    return str(project_root / "data" / "bot_state.db")


def export_last_90s_to_csv(
    db_path: str,
    window_pattern: str,
    asset: str,
    output_path: Path,
) -> int:
    """Query strategy_report_last_90s for window_id LIKE %window_pattern% and asset; write CSV with table column names. Returns row count."""
    ensure_report_db(db_path)
    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        # Match window_id containing the pattern (e.g. 26MAR061215 -> KXBTC15M-26MAR061215)
        cur.execute(
            f"SELECT * FROM {LAST_90S_TABLE} WHERE (window_id LIKE ? OR ticker LIKE ?) AND LOWER(asset) = LOWER(?) ORDER BY ts_utc ASC",
            (f"%{window_pattern}%", f"%{window_pattern}%", asset.strip()),
        )
        rows = cur.fetchall()
        col_names = [d[0] for d in cur.description]
    finally:
        conn.close()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(col_names)
        for r in rows:
            w.writerow(r)
    return len(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Export last_90s_limit_99 rows to CSV (columns = table columns)")
    ap.add_argument("--window", required=True, help="Window/ticker pattern, e.g. 26MAR061215")
    ap.add_argument("--asset", default="btc", help="Asset filter (default: btc)")
    ap.add_argument("--output", "-o", type=Path, default=None, help="Output CSV path (default: last_90s_limit_99_<asset>_<window>.csv in project root)")
    ap.add_argument("--db", type=str, default=None, help="DB path (default: from config state.db_path)")
    ap.add_argument("--config", type=str, default=None, help="Config file to read db_path from")
    args = ap.parse_args()

    db_path = args.db or get_db_path(PROJECT_ROOT, args.config or str(PROJECT_ROOT / "config" / "common.yaml"))
    out = args.output or (PROJECT_ROOT / f"last_90s_limit_99_{args.asset.lower()}_{args.window}.csv")

    n = export_last_90s_to_csv(db_path, args.window.strip(), args.asset, out)
    print(f"Exported {n} row(s) to {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
