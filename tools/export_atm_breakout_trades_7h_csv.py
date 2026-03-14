#!/usr/bin/env python3
"""
Export ATM breakout rows from the last N hours to CSV for what-if analysis.
Column names match strategy_report_atm_breakout exactly.

Usage:
  python tools/export_atm_breakout_trades_7h_csv.py
  python tools/export_atm_breakout_trades_7h_csv.py --hours 7 --out data/atm_breakout_trades_7h.csv
  python tools/export_atm_breakout_trades_7h_csv.py --hours 24 --trades-only
"""
from __future__ import annotations

import csv
import sqlite3
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bot.state import ensure_state_db, get_default_db_path


def export_atm_breakout_last_hours(
    db_path: str,
    out_path: Path,
    hours: float = 7.0,
    trades_only: bool = False,
) -> None:
    """
    Export strategy_report_atm_breakout rows from the last `hours` hours to CSV.
    Column names and order match the table. If trades_only=True, exclude exit_reason='NoTrade'.
    """
    ensure_state_db(db_path)
    # ts_utc is stored as ISO (e.g. 2026-03-13T00:32:08.123+00:00). Use cutoff timestamp for comparison.
    cutoff_ts = time.time() - (hours * 3600.0)
    cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(cutoff_ts))
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        where = "ts_utc >= ?"
        params: list = [cutoff_iso]
        if trades_only:
            where += " AND exit_reason IN ('TakeProfit','StopLoss','WindowEnd')"
        cur = conn.execute(
            f"SELECT * FROM strategy_report_atm_breakout WHERE {where} ORDER BY ts_utc ASC",
            params,
        )
        rows = [dict(r) for r in cur.fetchall()]
        col_names = [d[0] for d in cur.description] if cur.description else []
    finally:
        conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=col_names, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in col_names})

    print(f"Wrote {len(rows)} rows to {out_path} (last {hours}h, trades_only={trades_only})")


def export_atm_breakout_all_trades(
    db_path: str,
    out_path: Path,
    trades_only: bool = False,
) -> None:
    """Export all strategy_report_atm_breakout rows (optionally trades only) to CSV."""
    ensure_state_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if trades_only:
            cur = conn.execute(
                "SELECT * FROM strategy_report_atm_breakout WHERE exit_reason IN ('TakeProfit','StopLoss','WindowEnd') ORDER BY ts_utc ASC",
            )
        else:
            cur = conn.execute("SELECT * FROM strategy_report_atm_breakout ORDER BY ts_utc ASC")
        rows = [dict(r) for r in cur.fetchall()]
        col_names = [d[0] for d in cur.description] if cur.description else []
    finally:
        conn.close()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=col_names, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in col_names})

    print(f"Wrote {len(rows)} rows to {out_path} (all, trades_only={trades_only})")


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Export ATM breakout last N hours to CSV (table columns).")
    ap.add_argument("--hours", type=float, default=7.0, help="Hours of history (default 7)")
    ap.add_argument("--out", type=Path, default=None, help="Output CSV (default: data/atm_breakout_trades_7h.csv)")
    ap.add_argument("--db", default=None, help="Path to bot_state.db (default: data/bot_state.db)")
    ap.add_argument("--trades-only", action="store_true", help="Exclude NoTrade rows (only TP/SL/WindowEnd)")
    ap.add_argument("--all", action="store_true", help="Export all trades (no time filter)")
    args = ap.parse_args()

    db_path = args.db or get_default_db_path()
    if not Path(db_path).exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 1

    out_path = args.out or (PROJECT_ROOT / "data" / "atm_breakout_trades_7h.csv")
    if args.all:
        export_atm_breakout_all_trades(db_path, out_path, trades_only=args.trades_only)
    else:
        export_atm_breakout_last_hours(db_path, out_path, hours=args.hours, trades_only=args.trades_only)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
