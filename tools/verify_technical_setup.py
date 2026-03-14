#!/usr/bin/env python3
"""
360° technical verification for last_90s strategy (DB + logs, 3–4 cycles).

Queries strategy_report_last_90s and scans logs/bot.log for the last 60 minutes.
Outputs technical_verification_report.txt with 5 sections:
  1. Time-decay placement check (min_distance_threshold saved correctly)
  2. Aggregated skip analysis (full skip_details strings)
  3. Dual-strike / multi-order check (windows with 2+ orders)
  4. Stop-loss & resolution integrity
  5. System health (log exceptions / DB warnings for [last_90s])

Usage:
  python tools/verify_technical_setup.py
  python tools/verify_technical_setup.py --minutes 60 --out technical_verification_report.txt
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path


def _since_ts(minutes: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S")


def _log_line_in_window(line: str, since_dt: datetime) -> bool:
    """Return True if log line timestamp (UTC) is >= since_dt. Expect format: 2026-03-02 16:13:31 | ..."""
    m = re.match(r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+\|", line)
    if not m:
        return False
    try:
        ts = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%Y-%m-%d %H:%M:%S")
        ts = ts.replace(tzinfo=timezone.utc)
        return ts >= since_dt
    except ValueError:
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="360° technical verification for last_90s (DB + log, last N min)")
    ap.add_argument("--db", default=None, help="Path to bot_state.db (default: data/bot_state.db)")
    ap.add_argument("--log-file", default=None, help="Path to bot log (default: logs/bot.log)")
    ap.add_argument("--minutes", type=int, default=60, help="Look back window in minutes (default 60)")
    ap.add_argument("--out", default="technical_verification_report.txt", help="Output report path")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent.parent
    db_path = Path(args.db) if args.db else root / "data" / "bot_state.db"
    log_path = Path(args.log_file) if args.log_file else root / "logs" / "bot.log"
    out_path = root / args.out if not Path(args.out).is_absolute() else Path(args.out)

    since = _since_ts(args.minutes)
    since_dt = datetime.now(timezone.utc) - timedelta(minutes=args.minutes)
    out_lines: list[str] = []

    def log(s: str = "") -> None:
        out_lines.append(s)

    log("=" * 72)
    log("  360° TECHNICAL VERIFICATION — last_90s strategy")
    log("  Window: last %d minutes (since %s UTC)" % (args.minutes, since))
    log("  DB: %s" % db_path)
    log("  Log: %s" % log_path)
    log("=" * 72)

    if not db_path.is_file():
        log("ERROR: DB not found: %s" % db_path)
        out_path.write_text("\n".join(out_lines), encoding="utf-8")
        return 1

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='strategy_report_last_90s'"
    )
    if not cur.fetchone():
        log("ERROR: Table strategy_report_last_90s not found.")
        conn.close()
        out_path.write_text("\n".join(out_lines), encoding="utf-8")
        return 1

    # --- 1. Time-Decay Placement Check ---
    log("")
    log("--- 1. Time-Decay Placement Check (placed == 1) ---")
    cur.execute(
        """
        SELECT window_id, asset, limit_or_market, seconds_to_close, distance, min_distance_threshold, ts_utc
        FROM strategy_report_last_90s
        WHERE ts_utc >= ? AND placed = 1
        ORDER BY ts_utc DESC
        """,
        (since,),
    )
    placed_rows = cur.fetchall()
    if not placed_rows:
        log("  No placed rows in the last %d minutes." % args.minutes)
    else:
        for r in placed_rows:
            log("  window_id=%s asset=%s limit_or_market=%s seconds_to_close=%s distance=%s min_distance_threshold=%s" % (
                r["window_id"] or "",
                r["asset"] or "",
                r["limit_or_market"] or "",
                r["seconds_to_close"],
                r["distance"],
                r["min_distance_threshold"],
            ))
            log("    ts_utc=%s" % (r["ts_utc"] or ""))

    # --- 2. Aggregated Skip Analysis ---
    log("")
    log("--- 2. Aggregated Skip Analysis (placed == 0) — full skip_details ---")
    cur.execute(
        """
        SELECT window_id, asset, skip_reason, skip_details, ts_utc
        FROM strategy_report_last_90s
        WHERE ts_utc >= ? AND (placed = 0 OR placed IS NULL)
        ORDER BY ts_utc DESC
        """,
        (since,),
    )
    skip_rows = cur.fetchall()
    if not skip_rows:
        log("  No skip rows in the last %d minutes." % args.minutes)
    else:
        # Print 4–5 full skip_details strings
        for i, r in enumerate(skip_rows[:5]):
            details = (r["skip_details"] or "(null)")
            log("  [%d] window_id=%s asset=%s skip_reason=%s" % (i + 1, r["window_id"] or "", r["asset"] or "", r["skip_reason"] or ""))
            log("      skip_details (full): %s" % details)
            log("")

    # --- 3. Dual-Strike (Multi-Order) Check ---
    log("--- 3. Dual-Strike (Multi-Order) Check ---")
    cur.execute(
        """
        SELECT window_id, asset, COUNT(*) AS cnt
        FROM strategy_report_last_90s
        WHERE ts_utc >= ? AND placed = 1
        GROUP BY window_id, asset
        HAVING COUNT(*) > 1
        """,
        (since,),
    )
    multi = cur.fetchall()
    if not multi:
        log("  Windows with 2+ orders in last %d min: 0 (normal for calm markets)." % args.minutes)
    else:
        for g in multi:
            log("  window_id=%s asset=%s count=%d" % (g["window_id"], g["asset"], g["cnt"]))
            cur.execute(
                "SELECT order_id, limit_or_market, price_cents, ts_utc FROM strategy_report_last_90s WHERE window_id = ? AND asset = ? AND ts_utc >= ? AND placed = 1 ORDER BY ts_utc",
                (g["window_id"], g["asset"], since),
            )
            for row in cur.fetchall():
                log("    order_id=%s limit_or_market=%s price_cents=%s ts_utc=%s" % (
                    row["order_id"], row["limit_or_market"], row["price_cents"], row["ts_utc"],
                ))

    # --- 4. Stop-Loss & Resolution Integrity ---
    log("")
    log("--- 4. Stop-Loss & Resolution Integrity ---")
    cur.execute(
        """
        SELECT order_id, window_id, asset, is_stop_loss, stop_loss_sell_price, final_outcome, pnl_cents, ts_utc
        FROM strategy_report_last_90s
        WHERE ts_utc >= ? AND is_stop_loss = 1
        ORDER BY ts_utc DESC
        """,
        (since,),
    )
    sl_rows = cur.fetchall()
    if not sl_rows:
        log("  Rows with is_stop_loss == 1: none in last %d min." % args.minutes)
    else:
        for r in sl_rows:
            log("  order_id=%s window_id=%s asset=%s stop_loss_sell_price=%s final_outcome=%s pnl_cents=%s ts_utc=%s" % (
                r["order_id"], r["window_id"], r["asset"], r["stop_loss_sell_price"],
                r["final_outcome"], r["pnl_cents"], r["ts_utc"],
            ))
    log("  Completed windows (final_outcome / pnl_cents) in window:")
    cur.execute(
        """
        SELECT window_id, asset, order_id, final_outcome, pnl_cents, placed, ts_utc
        FROM strategy_report_last_90s
        WHERE ts_utc >= ? AND placed = 1 AND (final_outcome IS NOT NULL OR pnl_cents IS NOT NULL)
        ORDER BY ts_utc DESC
        """,
        (since,),
    )
    resolved = cur.fetchall()
    if not resolved:
        log("  No resolved rows (final_outcome/pnl_cents) in last %d min." % args.minutes)
    else:
        for r in resolved[:20]:
            log("  window_id=%s asset=%s order_id=%s final_outcome=%s pnl_cents=%s ts_utc=%s" % (
                r["window_id"], r["asset"], r["order_id"] or "", r["final_outcome"], r["pnl_cents"], r["ts_utc"],
            ))

    conn.close()

    # --- 5. System Health (Log Exceptions) ---
    log("")
    log("--- 5. System Health (Log Exceptions — last %d min) ---" % args.minutes)
    if not log_path.is_file():
        log("  Log file not found: %s" % log_path)
    else:
        err_pattern = re.compile(
            r"(Traceback|Exception|Error|database|write_row_last_90s failed|NameError|KeyError|TypeError)",
            re.IGNORECASE,
        )
        last_90s_pattern = re.compile(r"\[last_90s\]", re.IGNORECASE)
        matching: list[str] = []
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if not _log_line_in_window(line, since_dt):
                    continue
                if last_90s_pattern.search(line) and err_pattern.search(line):
                    matching.append(line.rstrip())
        if not matching:
            log("  No Traceback/Exception/Error/DB warnings tied to [last_90s] in last %d min." % args.minutes)
        else:
            log("  Found %d matching line(s):" % len(matching))
            for line in matching[-30:]:  # last 30 to avoid huge output
                log("    %s" % line[:200])

    log("")
    log("=" * 72)
    log("  End of report.")
    log("=" * 72)

    out_path.write_text("\n".join(out_lines), encoding="utf-8")
    print("Report written to %s" % out_path, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
