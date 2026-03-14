#!/usr/bin/env python3
"""
Generate canonical strategy report TSV files from the strategy_report DB.

Reads the SQLite report DB (same as state db_path), outputs:
  reports/strategy_report_last_90s_limit_99.tsv
  reports/strategy_report_hourly_last_90s_limit_99.tsv

Each row = one candidate signal with: signal_identified, skip_reason, placed, filled,
final_outcome (WIN|LOSS|STOP_LOSS), pnl_cents, etc.

Usage:
  python tools/strategy_report_generator.py [--config config/config.yaml]
  python tools/strategy_report_generator.py --since-hours 1   # only rows from last 1 hour
  python tools/strategy_report_generator.py --strategy last_90s_limit_99
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bot.strategy_report_db import COLUMNS, get_all_rows


def _db_path(project_root: Path, config_path: str) -> str:
    """Resolve state db_path from config."""
    try:
        from bot.config_loader import load_config, resolve_config_path
        cfg_path = Path(config_path) if config_path and Path(config_path).exists() else resolve_config_path(project_root)
        if not cfg_path.exists():
            return str(project_root / "data" / "bot_state.db")
        config = load_config(str(cfg_path))
        return (config.get("state") or {}).get("db_path", str(project_root / "data" / "bot_state.db"))
    except Exception:
        return str(project_root / "data" / "bot_state.db")


def _format_val(v) -> str:
    if v is None or v == "":
        return ""
    if isinstance(v, float):
        return str(v)
    return str(v)


def write_tsv(rows: list[dict], path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\t".join(columns) + "\n")
        for r in rows:
            line = "\t".join(_format_val(r.get(c)) for c in columns)
            f.write(line + "\n")


def run(
    db_path: str,
    reports_dir: Path,
    strategy_name: str | None = None,
    since_ts_utc: str | None = None,
) -> tuple[Path, Path]:
    """Generate TSV files. Returns (path_last_90s, path_hourly)."""
    all_rows = get_all_rows(db_path, strategy_name=strategy_name, since_ts_utc=since_ts_utc)
    # Use DB column order for output (table has these columns)
    out_columns = [
        "ts_utc", "strategy_name", "window_id", "asset", "market", "ticker", "market_type", "side",
        "spot", "strike", "range_low", "range_high", "distance", "min_distance_threshold", "bid",
        "limit_or_market", "price_cents", "seconds_to_close", "signal_identified", "skip_reason",
        "placed", "order_id", "filled", "fill_price", "resolution_price", "final_outcome", "pnl_cents",
    ]
    last_90s = [r for r in all_rows if (r.get("strategy_name") or "").strip() == "last_90s_limit_99"]
    hourly = [r for r in all_rows if (r.get("strategy_name") or "").strip() == "hourly_last_90s_limit_99"]
    p1 = reports_dir / "strategy_report_last_90s_limit_99.tsv"
    p2 = reports_dir / "strategy_report_hourly_last_90s_limit_99.tsv"
    write_tsv(last_90s, p1, out_columns)
    write_tsv(hourly, p2, out_columns)
    return (p1, p2)


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate canonical strategy report TSVs from DB")
    ap.add_argument("--config", default="", help="Config path (default: config/config.yaml)")
    ap.add_argument("--db-path", default="", help="Override state DB path")
    ap.add_argument("--reports-dir", default="", help="Output directory (default: reports/)")
    ap.add_argument("--since-hours", type=float, default=0, help="Only include rows with ts_utc in last N hours (0 = all)")
    ap.add_argument("--strategy", default="", help="Filter: last_90s_limit_99 or hourly_last_90s_limit_99 (default: both)")
    args = ap.parse_args()
    project_root = PROJECT_ROOT
    config_path = args.config or str(project_root / "config" / "config.yaml")
    db_path = args.db_path or _db_path(project_root, config_path)
    reports_dir = Path(args.reports_dir) if args.reports_dir else project_root / "reports"
    since_ts_utc = None
    if args.since_hours and args.since_hours > 0:
        since = datetime.now(timezone.utc) - timedelta(hours=args.since_hours)
        since_ts_utc = since.strftime("%Y-%m-%dT%H:%M:%S")
    strategy = (args.strategy.strip() or None) if args.strategy else None
    p1, p2 = run(db_path, reports_dir, strategy_name=strategy, since_ts_utc=since_ts_utc)
    print(f"Wrote {p1}")
    print(f"Wrote {p2}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
