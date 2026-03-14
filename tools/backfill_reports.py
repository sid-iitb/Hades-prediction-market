#!/usr/bin/env python3
"""
Backfill bot_reports_hourly with reports from the last N hours of bot log data.
Usage:
  python tools/backfill_reports.py [--hours 4] [--config config.yaml]
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.offline_processing.generate_all_kalshi_urls import generate_kalshi_slug

from bot.hourly_report import write_hourly_report


def get_past_hour_market_ids(asset: str, hours: int = 4):
    """Return the last N hourly market IDs (most recent first)."""
    import pytz
    from datetime import datetime, timedelta

    et = pytz.timezone("US/Eastern")
    now_et = datetime.now(et).replace(minute=0, second=0, microsecond=0)
    ids = []
    for i in range(1, hours + 1):
        target = now_et - timedelta(hours=i)
        slug = generate_kalshi_slug(target, asset=asset)
        ids.append(slug.upper())
    return ids


def main():
    parser = argparse.ArgumentParser(description="Backfill hourly reports from bot log")
    parser.add_argument("--hours", type=int, default=4, help="Number of hours to backfill (default: 4)")
    parser.add_argument("--config", default="config/config.yaml", help="Config file (config/config.yaml for split)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    config_path = project_root / args.config
    if not config_path.exists():
        fallback = project_root / "config.yaml"
        if fallback.exists():
            config_path = fallback
        else:
            print(f"Config not found: {config_path}")
            sys.exit(1)

    from bot.config_loader import load_config
    config = load_config(str(config_path))

    log_cfg = config.get("logging", {}) or {}
    log_path = log_cfg.get("file", "logs/bot.log")
    reports_cfg = config.get("reports", {}) or {}
    db_path = reports_cfg.get("db_path", "data/kalshi_ingest.db")
    assets = config.get("assets", ["btc"])
    if isinstance(assets, str):
        assets = [assets]

    # Collect unique hour market IDs across assets (btc and eth may share hours)
    seen = set()
    hour_ids = []
    for asset in assets:
        if str(asset).lower() not in ("btc", "eth", "sol", "xrp"):
            continue
        for hid in get_past_hour_market_ids(asset, args.hours):
            if hid not in seen:
                seen.add(hid)
                hour_ids.append(hid)

    log_file = project_root / log_path
    if not log_file.exists():
        print(f"Log file not found: {log_file}")
        sys.exit(1)

    print(f"Backfilling {len(hour_ids)} hourly reports from {log_file}")
    for hid in hour_ids:
        try:
            rid = write_hourly_report(str(hid), str(log_file), project_root, db_path=db_path)
            if rid:
                print(f"  OK: {rid}")
            else:
                print(f"  SKIP: {hid} (no data or failed)")
        except Exception as e:
            print(f"  ERROR: {hid}: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
