#!/usr/bin/env python3
"""Test reports DB, API endpoints, and bot writer."""
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

# Add project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_bot_reports_table_and_upsert():
    """Test bot creates table and upserts reports."""
    from bot.hourly_report import (
        _ensure_reports_table,
        _report_id_for_hour,
        write_hourly_report,
    )

    with tempfile.TemporaryDirectory() as tmp:
        project_root = Path(tmp)
        db_path = str(project_root / "test_reports.db")
        log_file = project_root / "logs" / "bot.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        # Write minimal log with one analysis event so parser yields something
        log_file.write_text(
            '2026-02-14 14:00:00 | INFO | {"event_type":"ENTRY_DECISION","interval":"hourly",'
            '"asset":"btc","window_id":"KXBTCD-26FEB1414","ticker":"KXBTCD-26FEB1414-A","side":"yes"}\n'
        )

        # Write report (creates table, inserts)
        report_id = write_hourly_report(
            "KXBTCD-26FEB1414",
            str(log_file),
            project_root,
            db_path=db_path,
        )
        assert report_id, "Expected non-empty report_id"
        assert "KXBTCD" in report_id or "26FEB1414" in report_id

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT id, hour_window_id, report_json, report_tsv, eval_hourly FROM bot_reports_hourly")
        row = cur.fetchone()
        conn.close()

        assert row is not None, "Expected one row"
        assert row["id"] == report_id
        assert row["hour_window_id"] == "KXBTCD-26FEB1414"
        assert row["report_json"]
        data = json.loads(row["report_json"])
        assert "trade_rows" in data
        assert "window_rows" in data
        assert "totals_by_interval" in data
        assert row["report_tsv"] and "TRADE_ROWS" in row["report_tsv"]

        # Upsert same ID (overwrite)
        write_hourly_report("KXBTCD-26FEB1414", str(log_file), project_root, db_path=db_path)
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS n FROM bot_reports_hourly")
        assert cur.fetchone()[0] == 1
        conn.close()

    print("PASS: bot reports table and upsert")


def test_api_endpoints():
    """Test API list, detail, and TSV endpoints."""
    from fastapi.testclient import TestClient
    import tempfile

    # Use temp DB for API test
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test_api.db")
        os.environ["KALSHI_DB_PATH"] = db_path

        from src.api import app

        client = TestClient(app)

        # List reports (empty)
        r = client.get("/api/reports/hourly?limit=10&offset=0")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        # May be empty

        # Insert a row directly for testing
        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            INSERT INTO bot_reports_hourly (
                id, generated_at, hour_bucket_utc, hour_window_id, report_json, report_tsv,
                eval_hourly, entry_decisions_hourly, submitted_hourly, failed_hourly, skipped_hourly, stoploss_hourly,
                eval_fifteen, entry_decisions_fifteen, submitted_fifteen, failed_fifteen, skipped_fifteen, stoploss_fifteen
            ) VALUES (
                '2026-02-14_14_KXBTCD-26FEB1414', '2026-02-14T14:05:00+00:00', '2026-02-14T14:00:00+00:00',
                'KXBTCD-26FEB1414', '{"trade_rows":[],"window_rows":[],"totals_by_interval":{"hourly":{"eval_count":1}}}',
                '=== TRADE_ROWS ===\\n', 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            )
            """
        )
        conn.commit()
        conn.close()

        # List reports
        r = client.get("/api/reports/hourly?limit=10&offset=0")
        assert r.status_code == 200
        data = r.json()
        assert len(data) >= 1
        row = data[0]
        assert "id" in row
        assert "metrics" in row
        assert "hourly" in row["metrics"]
        assert row["metrics"]["hourly"]["eval"] == 1

        # Get one report
        r = client.get("/api/reports/hourly/2026-02-14_14_KXBTCD-26FEB1414")
        assert r.status_code == 200
        d = r.json()
        assert d["id"] == "2026-02-14_14_KXBTCD-26FEB1414"
        assert "report" in d
        assert "totals_by_interval" in d["report"]

        # TSV
        r = client.get("/api/reports/hourly/2026-02-14_14_KXBTCD-26FEB1414/tsv")
        assert r.status_code == 200
        assert "TRADE_ROWS" in r.text

        # 404 for missing
        r = client.get("/api/reports/hourly/nonexistent-id")
        assert r.status_code == 404

        r = client.get("/api/reports/hourly/nonexistent-id/tsv")
        assert r.status_code == 404

        del os.environ["KALSHI_DB_PATH"]

    print("PASS: API endpoints")


def test_reports_page_serves():
    """Test /reports page returns 200."""
    from fastapi.testclient import TestClient
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test_ui.db")
        os.environ["KALSHI_DB_PATH"] = db_path

        from src.api import app

        client = TestClient(app)
        r = client.get("/reports")
        assert r.status_code == 200
        assert b"Hourly Reports" in r.content
        assert b"Report Detail" in r.content

        del os.environ["KALSHI_DB_PATH"]

    print("PASS: reports page serves")


if __name__ == "__main__":
    test_bot_reports_table_and_upsert()
    test_api_endpoints()
    test_reports_page_serves()
    print("\nAll tests passed.")
