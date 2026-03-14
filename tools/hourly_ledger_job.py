#!/usr/bin/env python3
"""
Hourly ledger job: build transaction summary for the last 60 minutes (PT), write files, optionally email.
Usage:
  python tools/hourly_ledger_job.py --lookback-min 60 --format tsv --send-email
  python tools/hourly_ledger_job.py --lookback-min 60 --force

Idempotent: skips writing/sending if ledger file already exists unless --force.

Cron (run at minute 0 past every hour, LA time or adjust TZ):
  0 * * * * cd /path/to/project && .venv/bin/python tools/hourly_ledger_job.py --lookback-min 60 --send-email
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pytz

LA_TZ = pytz.timezone("America/Los_Angeles")

# Project root
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load .env so SMTP_* (and other vars) are available when run from cron or CLI
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

from tools.hourly_ledger import (
    LEDGER_COLS,
    build_ledger_for_hour,
)
from tools.strategy_ledger import (
    build_strategy_ledger_for_hour,
    build_strategy_ledger_for_window,
    build_tuning_summary,
    write_strategy_artifacts_by_interval,
    write_tuning_summary_artifacts_by_interval,
)


def _report_hour_key(report_hour_la: datetime) -> str:
    """e.g. 20260217_0200PT"""
    return report_hour_la.strftime("%Y%m%d_%H00PT")


def _report_hour_display(report_hour_la: datetime) -> str:
    """e.g. 2026-02-17 02:00 PT"""
    return report_hour_la.strftime("%Y-%m-%d %H:%M PT")


def _load_config(project_root: Path) -> dict:
    """Load config (split config/config.yaml or single config.yaml)."""
    from bot.config_loader import load_config, resolve_config_path
    cfg_path = resolve_config_path(project_root)
    if not cfg_path.exists():
        return {}
    return load_config(str(cfg_path))


def run(
    log_path: str = "logs/bot.log",
    lookback_minutes: int = 60,
    lookback_hours: int = 1,
    send_email: bool = False,
    force: bool = False,
    format_tsv: bool = True,
    report_format: str = "concise",
    project_root: Optional[Path] = None,
) -> int:
    project_root = project_root or PROJECT_ROOT
    now_la = datetime.now(LA_TZ)
    report_hour_la = now_la.replace(minute=0, second=0, microsecond=0)
    key = _report_hour_key(report_hour_la)

    # 12-hour (or N-hour) one-off report: per-interval strategy ledger + email
    if lookback_hours and lookback_hours > 1:
        import time
        from datetime import timezone as tz
        from tools.analyze_bot_log import build_stoploss_reports_by_interval

        end_utc = time.time()
        start_utc = end_utc - (lookback_hours * 3600)
        strategy_ledger_rows, strategy_summary_rows, _, _ = build_strategy_ledger_for_window(
            log_path, start_utc, end_utc, project_root=project_root, format=report_format
        )
        window_key = "12h_%s" % report_hour_la.strftime("%Y%m%d_%H00PT") if lookback_hours == 12 else "%dh_%s" % (lookback_hours, report_hour_la.strftime("%Y%m%d_%H00PT"))
        strategy_out_dir = project_root / "reports" / "hourly" / ("12h" if lookback_hours == 12 else "%dh" % lookback_hours)
        strategy_out_dir.mkdir(parents=True, exist_ok=True)

        # Per-interval strategy ledger and summary
        artifacts = write_strategy_artifacts_by_interval(
            strategy_ledger_rows, strategy_summary_rows, strategy_out_dir, window_key, format=report_format
        )
        for iv, (lp, sp) in artifacts.items():
            lcount = sum(1 for r in strategy_ledger_rows if (r.get("interval") or "hourly").replace("15min", "fifteen_min") == iv)
            print(f"Wrote {lp} ({lcount} rows), {sp}")

        config = _load_config(project_root)
        tuning_paths = write_tuning_summary_artifacts_by_interval(
            strategy_ledger_rows, config, strategy_out_dir, window_key
        )
        for iv, tp in tuning_paths.items():
            print(f"Wrote {tp}")

        # Per-interval stop-loss reports
        abs_log = project_root / log_path if not Path(log_path).is_absolute() else Path(log_path)
        stoploss_by_interval = build_stoploss_reports_by_interval(
            str(abs_log), start_utc, end_utc, project_root=project_root
        )
        stop_loss_paths = []
        for iv, txt in stoploss_by_interval.items():
            p = strategy_out_dir / f"stop_loss_{iv}_{window_key}.txt"
            p.write_text(txt, encoding="utf-8")
            stop_loss_paths.append(p)
            print(f"Wrote {p}")

        # Per-interval tuning for email
        tuning_by_interval = {}
        for iv in ("fifteen_min", "hourly", "daily", "weekly"):
            rows = [r for r in strategy_ledger_rows if (r.get("interval") or "hourly").replace("15min", "fifteen_min") == iv]
            if rows:
                tuning_by_interval[iv] = build_tuning_summary(rows, config)

        if send_email:
            to_email = __import__("os").environ.get("SMTP_TO", "").strip()
            if not to_email:
                print("[hourly_ledger] SMTP_TO not set; skipping email")
            else:
                from bot.email_reporter import (
                    build_hourly_strategy_email_body,
                    fetch_portfolio_snapshot,
                    send_ledger_email,
                )
                start_pt = datetime.fromtimestamp(start_utc, tz=LA_TZ).strftime("%Y-%m-%d %H:%M PT")
                end_pt = datetime.fromtimestamp(end_utc, tz=LA_TZ).strftime("%Y-%m-%d %H:%M PT")
                subject = "[Kalshi Bot] Strategy Report — Last %d hours (%s → %s)" % (lookback_hours, start_pt, end_pt)
                portfolio_snapshot = fetch_portfolio_snapshot()
                realized_pnl = sum(float(r.get("pnl_pct") or 0) for r in strategy_ledger_rows if r.get("exec_status") == "EXITED" and r.get("pnl_pct") is not None)
                stoploss_count = sum(1 for r in strategy_ledger_rows if r.get("exec_status") == "EXITED" and "STOPLOSS" in (r.get("exec_reason") or ""))
                portfolio_snapshot["realized_pnl_last_hour"] = realized_pnl if realized_pnl else None
                portfolio_snapshot["stoploss_count_last_hour"] = stoploss_count
                hour_end_ts_iso = datetime.fromtimestamp(end_utc, tz=tz.utc).isoformat()
                body_text = build_hourly_strategy_email_body(
                    time_start_pt=start_pt,
                    time_end_pt=end_pt,
                    portfolio_snapshot=portfolio_snapshot,
                    summary_rows=strategy_summary_rows,
                    ledger_rows=strategy_ledger_rows,
                    hour_end_ts_iso=hour_end_ts_iso,
                    stoploss_by_interval=stoploss_by_interval,
                    tuning_by_interval=tuning_by_interval,
                )
                attachments = []
                for iv, (lp, sp) in artifacts.items():
                    attachments.extend([lp, sp])
                attachments.extend(tuning_paths.values())
                attachments.extend(stop_loss_paths)
                send_ledger_email(to_email=to_email, subject=subject, body_text=body_text, attachments=attachments)
                print(f"Email sent to {to_email}")
        return 0

    # Standard 1-hour report
    date_dir = report_hour_la.strftime("%Y-%m-%d")
    out_dir = project_root / "reports" / "hourly_ledger" / date_dir
    ledger_tsv = out_dir / f"ledger_{key}.tsv"
    ledger_json = out_dir / f"ledger_{key}.json"
    windows_tsv = out_dir / f"windows_{key}.tsv"

    if not force and ledger_tsv.exists():
        print(f"[hourly_ledger] Skip (exists): {ledger_tsv}")
        return 0

    ledger_rows, window_rows, summary = build_ledger_for_hour(
        log_path, report_hour_la, project_root=project_root
    )

    out_dir.mkdir(parents=True, exist_ok=True)

    # Ledger TSV
    with open(ledger_tsv, "w", encoding="utf-8", newline="") as f:
        import csv
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(LEDGER_COLS)
        for r in ledger_rows:
            w.writerow([r.get(c, "") for c in LEDGER_COLS])
    print(f"Wrote {ledger_tsv} ({len(ledger_rows)} rows)")

    # Ledger JSON
    with open(ledger_json, "w", encoding="utf-8") as f:
        json.dump({"report_hour": _report_hour_display(report_hour_la), "ledger_rows": ledger_rows, "summary": summary}, f, default=str, indent=2)
    print(f"Wrote {ledger_json}")

    # Windows TSV
    WINDOW_COLS = ["interval", "asset", "window_id", "attempts", "submitted", "failed", "skipped", "exited", "open", "skipped_guard", "skipped_cap", "exits_stoploss", "exits_takeprofit", "exits_hardflip", "realized_pnl"]
    with open(windows_tsv, "w", encoding="utf-8", newline="") as f:
        import csv
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(WINDOW_COLS)
        for r in window_rows:
            w.writerow([r.get(c, "") for c in WINDOW_COLS])
    print(f"Wrote {windows_tsv} ({len(window_rows)} windows)")

    # Strategy Tuning Ledger: reports/hourly/<YYYYMMDD>/<HH>/ per-interval
    hour_str = report_hour_la.strftime("%H")
    strategy_out_dir = project_root / "reports" / "hourly" / report_hour_la.strftime("%Y%m%d") / hour_str
    strategy_ledger_rows, strategy_summary_rows, start_utc, end_utc = build_strategy_ledger_for_hour(
        log_path, report_hour_la, project_root=project_root, format=report_format
    )

    artifacts = write_strategy_artifacts_by_interval(
        strategy_ledger_rows, strategy_summary_rows, strategy_out_dir, key, format=report_format
    )
    for iv, (lp, sp) in artifacts.items():
        lcount = sum(1 for r in strategy_ledger_rows if (r.get("interval") or "hourly").replace("15min", "fifteen_min") == iv)
        print(f"Wrote {lp} ({lcount} rows), {sp}")

    config = _load_config(project_root)
    tuning_paths = write_tuning_summary_artifacts_by_interval(strategy_ledger_rows, config, strategy_out_dir, key)
    for iv, tp in tuning_paths.items():
        print(f"Wrote {tp}")

    # Per-interval stop-loss reports (always write, not only when sending email)
    from tools.analyze_bot_log import build_stoploss_reports_by_interval
    abs_log = project_root / log_path if not Path(log_path).is_absolute() else Path(log_path)
    stoploss_by_interval = build_stoploss_reports_by_interval(
        str(abs_log), start_utc, end_utc, project_root=project_root
    )
    stop_loss_paths = []
    for iv, txt in stoploss_by_interval.items():
        p = strategy_out_dir / f"stop_loss_{iv}_{key}.txt"
        p.write_text(txt, encoding="utf-8")
        stop_loss_paths.append(p)
    print(f"Wrote {len(stop_loss_paths)} stop-loss reports")

    if send_email:
        to_email = __import__("os").environ.get("SMTP_TO", "").strip()
        if not to_email:
            print("[hourly_ledger] SMTP_TO not set; skipping email")
        else:
            from datetime import timezone as tz
            from bot.email_reporter import (
                build_hourly_strategy_email_body,
                fetch_portfolio_snapshot,
                send_ledger_email,
            )

            tuning_by_interval = {}
            for iv in ("fifteen_min", "hourly", "daily", "weekly"):
                rows = [r for r in strategy_ledger_rows if (r.get("interval") or "hourly").replace("15min", "fifteen_min") == iv]
                if rows:
                    tuning_by_interval[iv] = build_tuning_summary(rows, config)

            subject = "[Kalshi Bot] Hourly Strategy Report — %s" % _report_hour_display(report_hour_la)
            portfolio_snapshot = fetch_portfolio_snapshot()
            realized_pnl = sum(float(r.get("pnl_pct") or 0) for r in strategy_ledger_rows if r.get("exec_status") == "EXITED" and r.get("pnl_pct") is not None)
            stoploss_count = sum(1 for r in strategy_ledger_rows if r.get("exec_status") == "EXITED" and "STOPLOSS" in (r.get("exec_reason") or ""))
            portfolio_snapshot["realized_pnl_last_hour"] = realized_pnl if realized_pnl else None
            portfolio_snapshot["stoploss_count_last_hour"] = stoploss_count
            hour_end_ts_iso = datetime.fromtimestamp(end_utc, tz=tz.utc).isoformat()
            time_start_pt = (report_hour_la - __import__("datetime").timedelta(hours=1)).strftime("%Y-%m-%d %H:00 PT")
            time_end_pt = _report_hour_display(report_hour_la)
            body_text = build_hourly_strategy_email_body(
                time_start_pt=time_start_pt,
                time_end_pt=time_end_pt,
                portfolio_snapshot=portfolio_snapshot,
                summary_rows=strategy_summary_rows,
                ledger_rows=strategy_ledger_rows,
                hour_end_ts_iso=hour_end_ts_iso,
                stoploss_by_interval=stoploss_by_interval,
                tuning_by_interval=tuning_by_interval,
            )
            attachments = []
            for iv, (lp, sp) in artifacts.items():
                attachments.extend([lp, sp])
            attachments.extend(tuning_paths.values())
            attachments.extend(stop_loss_paths)
            send_ledger_email(to_email=to_email, subject=subject, body_text=body_text, attachments=attachments)
            print(f"Email sent to {to_email}")

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hourly ledger job (last 60m PT)")
    parser.add_argument("--log-path", default="logs/bot.log", help="Path to bot.log")
    parser.add_argument("--lookback-min", type=int, default=60, help="Window length in minutes (default 60)")
    parser.add_argument("--format", default="tsv", choices=("tsv", "json"), help="Primary format")
    parser.add_argument("--send-email", action="store_true", help="Send email with attachments")
    parser.add_argument("--force", action="store_true", help="Overwrite existing report and resend if --send-email")
    parser.add_argument("--lookback-hours", type=int, default=1, help="Report window in hours (e.g. 12 for last 12h); strategy + email only, no legacy 1h files")
    parser.add_argument("--report-format", default="concise", choices=("concise", "full"), help="Ledger format: concise (default, decision-useful) or full (debugging)")
    args = parser.parse_args()
    sys.exit(run(
        log_path=args.log_path,
        lookback_minutes=args.lookback_min,
        lookback_hours=args.lookback_hours,
        send_email=args.send_email,
        force=args.force,
        format_tsv=(args.format == "tsv"),
        report_format=args.report_format,
    ))
