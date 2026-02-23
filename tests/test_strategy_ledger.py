"""
Minimal tests for strategy tuning ledger: reconciliation and summary counts.
Uses a small in-memory log excerpt to validate ledger rows and summary aggregates.
"""
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pytz

# Project root
TESTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TESTS_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.strategy_ledger import (
    STRATEGY_LEDGER_COLS,
    STRATEGY_SUMMARY_COLS,
    build_strategy_ledger_for_hour,
    write_strategy_artifacts,
)

LA_TZ = pytz.timezone("America/Los_Angeles")


def test_strategy_ledger_columns_stable():
    """Columns are defined and non-empty."""
    assert len(STRATEGY_LEDGER_COLS) >= 20
    assert "ts_first_seen" in STRATEGY_LEDGER_COLS
    assert "exec_status" in STRATEGY_LEDGER_COLS
    assert "exec_reason" in STRATEGY_LEDGER_COLS
    assert len(STRATEGY_SUMMARY_COLS) >= 15
    assert "trade_attempts" in STRATEGY_SUMMARY_COLS
    assert "dist_fail_rate" in STRATEGY_SUMMARY_COLS


def test_strategy_ledger_from_mini_log():
    """
    Build ledger and summary from a tiny bot.log excerpt.
    Validates: one row per trade attempt (ENTER_DECISION key), counts match.
    """
    # Hour window: 2026-02-19 13:00 PT -> 14:00 PT
    # Use timestamps inside that window (ISO UTC)
    base_ts = "2026-02-19T21:30:00.000Z"  # 13:30 PT

    lines = [
        # Trade 1: ENTER_DECISION -> ORDER_SUBMITTED -> EXIT (stoploss)
        '{"ts":"%s","event_type":"ENTER_DECISION","interval":"hourly","asset":"btc","window_id":"KXBTCD-26FEB1914","ticker":"KXBTCD-26FEB1914","side":"yes","yes_price_cents":85,"no_price_cents":15,"persistence_streak":2,"persistence_required":2,"distance":120,"min_distance_required":100,"seconds_to_close":300,"cutoff_seconds":90}' % base_ts,
        '{"ts":"2026-02-19T21:31:00.000Z","event_type":"ORDER_SUBMITTED","interval":"hourly","asset":"btc","window_id":"KXBTCD-26FEB1914","ticker":"KXBTCD-26FEB1914","side":"yes","order_id":"ord-1"}',
        '{"ts":"2026-02-19T21:35:00.000Z","event_type":"EXIT","interval":"hourly","asset":"btc","window_id":"KXBTCD-26FEB1914","ticker":"KXBTCD-26FEB1914","side":"yes","reason_code":"EXIT_STOPLOSS","pnl_pct":-0.05}',
        # Trade 2: ENTER_DECISION -> ORDER_FAILED
        '{"ts":"2026-02-19T21:40:00.000Z","event_type":"ENTER_DECISION","interval":"hourly","asset":"btc","window_id":"KXBTCD-26FEB1914","ticker":"KXBTCD-26FEB1915","side":"no","yes_price_cents":20,"no_price_cents":80}',
        '{"ts":"2026-02-19T21:41:00.000Z","event_type":"ORDER_FAILED","interval":"hourly","asset":"btc","window_id":"KXBTCD-26FEB1914","ticker":"KXBTCD-26FEB1915","side":"no","error_reason":"No top-of-book data available"}',
        # Trade 3: ENTER_DECISION only (no order) -> SKIPPED
        '{"ts":"2026-02-19T21:50:00.000Z","event_type":"ENTER_DECISION","interval":"fifteen_min","asset":"eth","window_id":"W1","ticker":"KXETH15M-26FEB191455","side":"yes"}',
    ]
    with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
        for line in lines:
            f.write(line + "\n")
        log_path = f.name

    try:
        report_hour_la = LA_TZ.localize(datetime(2026, 2, 19, 14, 0, 0))
        ledger_rows, summary_rows, start_utc, end_utc = build_strategy_ledger_for_hour(
            log_path, report_hour_la, project_root=PROJECT_ROOT
        )

        # Exactly 3 trade attempts (3 unique keys with ENTER_DECISION)
        assert len(ledger_rows) == 3, "expected 3 ledger rows, got %d" % len(ledger_rows)

        keys = set((r["interval"], r["asset"], r["window_id"], r["ticker"], r["side"]) for r in ledger_rows)
        assert len(keys) == 3, "expected 3 unique keys, got %d" % len(keys)

        # Execution status: one EXITED, one FAILED, one SKIPPED
        statuses = [r["exec_status"] for r in ledger_rows]
        assert "EXITED" in statuses
        assert "FAILED" in statuses
        assert "SKIPPED" in statuses

        # EXITED row has exec_reason EXIT_STOPLOSS
        exited = [r for r in ledger_rows if r["exec_status"] == "EXITED"]
        assert len(exited) == 1
        assert "STOPLOSS" in (exited[0].get("exec_reason") or "")

        # FAILED row has ERR_TOP_OF_BOOK_MISSING (from "No top-of-book" message)
        failed = [r for r in ledger_rows if r["exec_status"] == "FAILED"]
        assert len(failed) == 1
        assert "TOP_OF_BOOK" in (failed[0].get("exec_reason") or "") or "top-of-book" in (failed[0].get("exec_reason") or "").lower()

        # Summary: per (interval, asset) - hourly btc: 2 attempts, fifteen_min eth: 1
        assert len(summary_rows) >= 1
        total_attempts = sum(r["trade_attempts"] for r in summary_rows)
        assert total_attempts == 3
        total_exited = sum(r["exited_count"] for r in summary_rows)
        assert total_exited == 1
        total_failed = sum(r["failed_count"] for r in summary_rows)
        assert total_failed == 1
        total_skipped = sum(r["skipped_count"] for r in summary_rows)
        assert total_skipped == 1

        # Write artifacts and ensure files exist
        out_dir = Path(tempfile.mkdtemp())
        ledger_path, summary_path = write_strategy_artifacts(ledger_rows, summary_rows, out_dir, "20260219_1400PT")
        assert ledger_path.exists()
        assert summary_path.exists()
        content = ledger_path.read_text()
        assert "ts_first_seen" in content
        assert "exec_status" in content
    finally:
        Path(log_path).unlink(missing_ok=True)


if __name__ == "__main__":
    import unittest
    unittest.main(module="test_strategy_ledger", argv=sys.argv)
