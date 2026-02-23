"""
Generate hourly summary reports (analyzer summary mode) when the hourly window rolls over.
Persists reports to SQLite (bot_reports_hourly) for API/UI. Optionally writes TSV to disk.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Optional

from bot.market import get_15min_window_ids_for_hour


def _asset_from_hour_market_id(hour_market_id: str) -> str:
    """Infer asset from hour market id (KXBTCD vs KXETHD). Returns lowercase."""
    mid = (hour_market_id or "").upper()
    return "eth" if "KXETHD" in mid or mid.startswith("KXETHD") else "btc"


def _report_id_for_hour(hour_market_id: str) -> str:
    """Build report ID: 2026-02-16_16_KXBTCD-26FEB1616 (no extension)."""
    return _filename_for_hour(hour_market_id).replace(".tsv", "")


def _filename_for_hour(hour_market_id: str) -> str:
    """Build report filename: 2026-02-16_16_KXBTCD-26FEB1616.tsv from hour market id."""
    slug = (hour_market_id or "").strip().upper()
    dash = slug.find("-")
    if dash < 0 or len(slug) < dash + 10:
        return f"hour_{hour_market_id}.tsv".replace("/", "_")
    rest = slug[dash + 1:]
    yy = rest[:2]
    month_abbr = rest[2:5].upper()
    dd = rest[5:7]
    hh = rest[7:9]
    months = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
              "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"}
    mm = months.get(month_abbr, "01")
    year = "20" + yy
    return f"{year}-{mm}-{dd}_{hh}_{hour_market_id}.tsv"


def _ensure_reports_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_reports_hourly (
            id TEXT PRIMARY KEY,
            generated_at TEXT,
            hour_bucket_utc TEXT,
            hour_window_id TEXT,
            report_json TEXT,
            report_tsv TEXT,
            eval_hourly INTEGER,
            entry_decisions_hourly INTEGER,
            submitted_hourly INTEGER,
            failed_hourly INTEGER,
            skipped_hourly INTEGER,
            stoploss_hourly INTEGER,
            eval_fifteen INTEGER,
            entry_decisions_fifteen INTEGER,
            submitted_fifteen INTEGER,
            failed_fifteen INTEGER,
            skipped_fifteen INTEGER,
            stoploss_fifteen INTEGER
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reports_hour_bucket ON bot_reports_hourly(hour_bucket_utc)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reports_window_id ON bot_reports_hourly(hour_window_id)")
    conn.commit()


def write_hourly_report(
    previous_hour_market_id: str,
    log_path: str,
    project_root: Path,
    db_path: Optional[str] = None,
) -> str:
    """
    Generate the hourly report for the window that just finished and persist to SQLite.
    Includes the one hourly window and the four 15-min windows inside that hour.
    Returns the report ID if successful, empty string on failure.
    """
    import csv
    import io
    from datetime import datetime, timezone, timedelta

    from tools.analyze_bot_log import (
        TRADE_COLS,
        WINDOW_COLS,
        build_summary_for_window_keys,
        parse_log_lines,
    )

    asset = _asset_from_hour_market_id(previous_hour_market_id)
    window_keys = {("hourly", asset, previous_hour_market_id)}
    for w in get_15min_window_ids_for_hour(previous_hour_market_id):
        window_keys.add(("fifteen_min", asset, w))

    log_file = Path(log_path)
    if not log_file.is_absolute():
        log_file = project_root / log_path
    if not log_file.exists():
        return ""

    events = list(parse_log_lines(str(log_file)))

    def _parse_ts(ts_str):
        try:
            s = str(ts_str)[:26].replace("Z", "+00:00")
            if "T" in s and "+" not in s and "-" not in s[10:]:
                s = s + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            return 0.0

    # Add daily/weekly window_keys from events in this hour bucket
    report_id = _report_id_for_hour(previous_hour_market_id)
    parts = report_id.split("_")
    if len(parts) >= 2:
        try:
            hour_start = datetime.fromisoformat(f"{parts[0]}T{parts[1]}:00:00+00:00")
            hour_end = hour_start + timedelta(hours=1)
            start_ts = hour_start.timestamp()
            end_ts = hour_end.timestamp()
            for e in events:
                if str(e.get("interval", "")).lower() not in ("daily", "weekly"):
                    continue
                ts = _parse_ts(e.get("ts") or "")
                if start_ts <= ts < end_ts:
                    iv = e.get("interval", "")
                    a = e.get("asset", asset)
                    wid = e.get("window_id") or e.get("ticker") or ""
                    if wid:
                        window_keys.add((iv, a or asset, wid))
        except Exception:
            pass
    trade_rows, window_rows, totals_by_interval, block_aggregations = build_summary_for_window_keys(events, window_keys)

    report_id = _report_id_for_hour(previous_hour_market_id)
    generated_at = datetime.now(timezone.utc).isoformat()
    # hour_bucket_utc: infer from report_id (YYYY-MM-DD_HH_...) -> YYYY-MM-DDTHH:00:00+00:00
    parts = report_id.split("_")
    hour_bucket_utc = f"{parts[0]}T{parts[1]}:00:00+00:00" if len(parts) >= 2 else generated_at[:19] + "+00:00"

    report_json = {
        "trade_rows": trade_rows,
        "window_rows": window_rows,
        "totals_by_interval": totals_by_interval,
        "block_aggregations": block_aggregations,
    }

    buf = io.StringIO()
    w = csv.writer(buf, delimiter="\t", lineterminator="\n")
    buf.write("=== TRADE_ROWS ===\n")
    w.writerow(TRADE_COLS)
    for r in trade_rows:
        w.writerow([r.get(c, "") for c in TRADE_COLS])
    buf.write("=== WINDOW_ROWS ===\n")
    w.writerow(WINDOW_COLS)
    for r in window_rows:
        w.writerow([r.get(c, "") for c in WINDOW_COLS])
    buf.write("=== HOUR TOTALS ===\n")
    for interval in ("hourly", "fifteen_min", "daily", "weekly"):
        t = totals_by_interval.get(interval, {})
        if not t and interval in ("daily", "weekly"):
            continue
        buf.write(
            f"interval={interval}\teval_count={t.get('eval_count', 0)}\t"
            f"entry_decisions={t.get('entry_decisions', 0)}\tsubmitted={t.get('orders_submitted', 0)}\t"
            f"failed={t.get('orders_failed', 0)}\tskipped={t.get('orders_skipped', 0)}\t"
            f"exits_stoploss={t.get('exits_stoploss', 0)}\n"
        )
    report_tsv = buf.getvalue()

    h = totals_by_interval.get("hourly", {})
    f = totals_by_interval.get("fifteen_min", {})

    if db_path:
        abs_db = Path(db_path)
        if not abs_db.is_absolute():
            abs_db = project_root / db_path
        abs_db.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(abs_db))
        conn.row_factory = sqlite3.Row
        try:
            _ensure_reports_table(conn)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO bot_reports_hourly (
                    id, generated_at, hour_bucket_utc, hour_window_id, report_json, report_tsv,
                    eval_hourly, entry_decisions_hourly, submitted_hourly, failed_hourly, skipped_hourly, stoploss_hourly,
                    eval_fifteen, entry_decisions_fifteen, submitted_fifteen, failed_fifteen, skipped_fifteen, stoploss_fifteen
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    generated_at=excluded.generated_at, hour_bucket_utc=excluded.hour_bucket_utc,
                    hour_window_id=excluded.hour_window_id, report_json=excluded.report_json, report_tsv=excluded.report_tsv,
                    eval_hourly=excluded.eval_hourly, entry_decisions_hourly=excluded.entry_decisions_hourly,
                    submitted_hourly=excluded.submitted_hourly, failed_hourly=excluded.failed_hourly,
                    skipped_hourly=excluded.skipped_hourly, stoploss_hourly=excluded.stoploss_hourly,
                    eval_fifteen=excluded.eval_fifteen, entry_decisions_fifteen=excluded.entry_decisions_fifteen,
                    submitted_fifteen=excluded.submitted_fifteen, failed_fifteen=excluded.failed_fifteen,
                    skipped_fifteen=excluded.skipped_fifteen, stoploss_fifteen=excluded.stoploss_fifteen
                """,
                (
                    report_id,
                    generated_at,
                    hour_bucket_utc,
                    previous_hour_market_id,
                    json.dumps(report_json, default=str),
                    report_tsv,
                    h.get("eval_count", 0),
                    h.get("entry_decisions", 0),
                    h.get("orders_submitted", 0),
                    h.get("orders_failed", 0),
                    h.get("orders_skipped", 0),
                    h.get("exits_stoploss", 0),
                    f.get("eval_count", 0),
                    f.get("entry_decisions", 0),
                    f.get("orders_submitted", 0),
                    f.get("orders_failed", 0),
                    f.get("orders_skipped", 0),
                    f.get("exits_stoploss", 0),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    return report_id


def maybe_emit_hourly_report(
    previous_hour_market_id: str,
    log_path: str,
    project_root: Path,
    logger,
    db_path: Optional[str] = None,
) -> None:
    """Generate hourly report and persist to SQLite (if db_path given)."""
    if not previous_hour_market_id:
        return
    try:
        report_id = write_hourly_report(
            previous_hour_market_id,
            log_path,
            project_root,
            db_path=db_path,
        )
        if report_id:
            logger.info("[hourly_report] Persisted report %s to DB", report_id)
    except Exception as e:
        if logger:
            logger.warning("[hourly_report] Failed to write report: %s", e)
