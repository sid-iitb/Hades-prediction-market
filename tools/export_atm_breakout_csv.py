import csv
import sqlite3
import sys
from pathlib import Path

from bot.state import ensure_state_db, get_default_db_path


def export_atm_breakout_csv(db_path: str, out_path: Path) -> None:
    """
    Dump all rows from strategy_report_atm_breakout into a CSV file.
    The CSV header matches the table's column order exactly.
    """
    ensure_state_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM strategy_report_atm_breakout")
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
        python -m tools.export_atm_breakout_csv [optional_db_path]

    If db_path is omitted, uses bot.state.get_default_db_path().
    CSV is written to repo_root/atm_breakout_strategy.csv.
    """
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    else:
        db_path = get_default_db_path()

    repo_root = Path(__file__).resolve().parents[1]
    out_path = repo_root / "atm_breakout_strategy.csv"

    export_atm_breakout_csv(db_path, out_path)
    print(f"Exported strategy_report_atm_breakout to {out_path} from {db_path}")


if __name__ == "__main__":
    main()

