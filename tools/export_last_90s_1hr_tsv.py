#!/usr/bin/env python3
"""
Export last_90s_limit_99 rows for the four 15-min runs in a given hour (e.g. 1pm–2pm UTC or PT),
after refreshing filled/resolution from Kalshi API. Output: 16 rows (4 windows × 4 assets) when all present.

Usage:
  # 1pm–2pm Pacific (recommended for 15-min):
  python tools/export_last_90s_1hr_tsv.py --date 2026-03-01 --hour-pt 13 --config config/fifteen_min.yaml
  # 1pm–2pm UTC:
  python tools/export_last_90s_1hr_tsv.py --date 2026-03-01 --hour 13
  python tools/export_last_90s_1hr_tsv.py --date 2026-03-01 --hour 13 --no-refresh

Writes: reports/last_90s_1pm_2pm_<date>_<hour>UTC.tsv (canonical LAST_90S_COLUMNS, Kalshi resolution fields filled).
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _db_path(project_root: Path, config_path: str) -> str:
    try:
        from bot.config_loader import load_config, resolve_config_path
        cfg_path = Path(config_path) if config_path and (project_root / config_path.strip()).exists() else resolve_config_path(project_root)
        if not cfg_path.exists():
            out = project_root / "data" / "bot_state.db"
            return str(out.resolve())
        config = load_config(str(cfg_path))
        raw = (config.get("state") or {}).get("db_path", str(project_root / "data" / "bot_state.db"))
        p = Path(raw)
        if not p.is_absolute():
            p = project_root / p
        return str(p.resolve())
    except Exception:
        return str((project_root / "data" / "bot_state.db").resolve())


def _window_suffixes_for_hour(date_str: str, hour_utc: int) -> list[str]:
    """Return window_id suffixes for the 4 quarter hours in the given UTC hour. date_str = YYYY-MM-DD."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        year = dt.strftime("%y")
        month = dt.strftime("%b").upper()  # MAR
        day = dt.strftime("%d")
    except ValueError:
        return []
    return [
        f"{year}{month}{day}{hour_utc:02d}00",
        f"{year}{month}{day}{hour_utc:02d}15",
        f"{year}{month}{day}{hour_utc:02d}30",
        f"{year}{month}{day}{hour_utc:02d}45",
    ]


def _utc_hour_for_pt_hour(date_str: str, hour_pt: int) -> int:
    """Return UTC hour for the given local hour in America/Los_Angeles on date_str (YYYY-MM-DD)."""
    try:
        import pytz
        tz = pytz.timezone("America/Los_Angeles")
        dt_naive = datetime.strptime(f"{date_str} {hour_pt:02d}:00:00", "%Y-%m-%d %H:%M:%S")
        dt_local = tz.localize(dt_naive)
        return dt_local.astimezone(timezone.utc).hour
    except Exception:
        # PST = UTC-8, PDT = UTC-7; assume PST if no pytz
        return (hour_pt + 8) % 24

def main() -> int:
    ap = argparse.ArgumentParser(description="Export last_90s TSV for 4 runs in one hour (1pm–2pm) with Kalshi refresh")
    ap.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"), help="Date YYYY-MM-DD")
    ap.add_argument("--hour", type=int, default=None, help="Start hour in UTC (e.g. 13 = 1pm UTC). Ignored if --hour-pt set.")
    ap.add_argument("--hour-pt", type=int, default=None, help="Start hour in Pacific (e.g. 13 = 1pm PT). Converts to UTC for window lookup.")
    ap.add_argument("--config", default="", help="Config path for db_path")
    ap.add_argument("--no-refresh", action="store_true", help="Skip Kalshi API refresh")
    ap.add_argument("--out", default="", help="Output TSV path (default: reports/last_90s_1pm_2pm_<date>_<hour>UTC.tsv)")
    args = ap.parse_args()

    if args.hour_pt is not None:
        hour_utc = _utc_hour_for_pt_hour(args.date, args.hour_pt)
    else:
        hour_utc = args.hour if args.hour is not None else 13

    root = PROJECT_ROOT
    db_path = _db_path(root, args.config or str(root / "config" / "config.yaml"))
    since_ts = f"{args.date}T{hour_utc:02d}:00:00"
    to_ts = f"{args.date}T{hour_utc + 1:02d}:00:00"

    if not args.no_refresh:
        from tools.strategy_report_hourly_email import refresh_report_from_kalshi
        n = refresh_report_from_kalshi(db_path, since_ts)
        print(f"[export_last_90s_1hr] Refreshed {n} rows from Kalshi API")

    from bot.strategy_report_db import get_all_rows_last_90s, LAST_90S_COLUMNS
    # Fetch all rows from the day (ts_utc >= start of day) so we get any window that was written today
    day_start = f"{args.date}T00:00:00"
    all_last_90s = get_all_rows_last_90s(db_path, since_ts_utc=day_start)
    suffixes = _window_suffixes_for_hour(args.date, hour_utc)
    if not suffixes:
        print(f"[export_last_90s_1hr] Invalid date: {args.date}", file=sys.stderr)
        return 1
    # Keep only rows whose window_id ends with one of the 4 suffixes (4 windows in this hour)
    rows = [r for r in all_last_90s if any((r.get("window_id") or "").endswith(s) for s in suffixes)]
    rows.sort(key=lambda r: (r.get("window_id") or "", r.get("asset") or ""))

    def _fmt(v):
        if v is None or v == "":
            return ""
        return str(v)

    out_path = args.out.strip()
    if not out_path:
        out_path = str(root / "reports" / f"last_90s_1pm_2pm_{args.date}_{hour_utc}UTC.tsv")
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\t".join(LAST_90S_COLUMNS) + "\n")
        for r in rows:
            f.write("\t".join(_fmt(r.get(c)) for c in LAST_90S_COLUMNS) + "\n")

    print(f"[export_last_90s_1hr] Wrote {len(rows)} rows to {out_path} (expected 16 for 4 windows × 4 assets)")
    if args.hour_pt is not None:
        print(f"[export_last_90s_1hr] 1pm–2pm PT → hour {hour_utc} UTC for date {args.date}")
    if len(rows) != 16:
        print(f"[export_last_90s_1hr] Note: expected 16 entries; missing rows may be skipped/not-run windows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
