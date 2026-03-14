import argparse
import csv
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bot.state import get_default_db_path


def export_last_90s_csv(db_path: str, out_path: Path, hours: float = 24) -> None:
    """
    Dump last N hours of rows for last_90s_limit_99 from strategy_report_last_90s
    into a CSV file. The CSV header matches the table's column order exactly.
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cutoff_dt = datetime.now(timezone.utc) - timedelta(hours=hours)
        cutoff = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%S")

        # Ensure table exists; if not, nothing to export
        cur.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name = 'strategy_report_last_90s'
            """
        )
        if not cur.fetchone():
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open("w", newline="", encoding="utf-8") as f:
                pass
            return

        cur.execute(
            """
            SELECT * FROM strategy_report_last_90s
            WHERE ts_utc >= ?
            ORDER BY ts_utc ASC
            """,
            (cutoff,),
        )
        rows = cur.fetchall()
        col_names = [col[0] for col in cur.description] if cur.description else []

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if col_names:
                writer.writerow(col_names)
            writer.writerows(rows)
    finally:
        conn.close()


def main() -> None:
    """
    Usage:
        python -m tools.export_last_90s_24h_csv [--hours HOURS] [optional_db_path]

    If db_path is omitted, uses bot.state.get_default_db_path().
    CSV is written to repo_root/last_90s_limit_99_last{N}h.csv.
    """
    parser = argparse.ArgumentParser(description="Export last_90s_limit_99 strategy_report_last_90s to CSV")
    parser.add_argument("--hours", type=float, default=24, help="Export rows from last N hours (default: 24)")
    parser.add_argument("db_path", nargs="?", default=None, help="Path to bot_state.db (default: from config)")
    args = parser.parse_args()
    db_path = args.db_path or get_default_db_path()
    hours = args.hours

    repo_root = Path(__file__).resolve().parents[1]
    out_path = repo_root / f"last_90s_limit_99_last{int(hours) if hours == int(hours) else hours}h.csv"

    export_last_90s_csv(db_path, out_path, hours=hours)
    print(f"Exported last {hours}h of strategy_report_last_90s to {out_path} from {db_path}")


if __name__ == "__main__":
    main()

