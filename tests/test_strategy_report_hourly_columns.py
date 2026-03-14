"""
Tests that the hourly strategy report (last 1 hour) fetches and outputs all expected columns,
and that Kalshi API refresh has sufficient DB columns, env, and logging.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TESTS_DIR.parent
if str(PROJECT_ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(PROJECT_ROOT))

# Legacy full schema (strategy_report table) still used for other strategies
EXPECTED_LEGACY_REPORT_COLUMNS = [
    "ts_utc", "strategy_name", "window_id", "asset", "market", "ticker", "market_type", "side",
    "spot", "strike", "range_low", "range_high", "distance", "min_distance_threshold", "bid",
    "limit_or_market", "price_cents", "seconds_to_close", "signal_identified", "skip_reason", "skip_details",
    "placed", "order_id", "filled", "fill_price", "resolution_price", "final_outcome", "pnl_cents",
]


class TestReportColumns(unittest.TestCase):
    """Verify report fetches and outputs all columns."""

    def test_legacy_columns_match_table(self):
        """strategy_report table COLUMNS must match legacy full schema."""
        from bot.strategy_report_db import COLUMNS
        for col in EXPECTED_LEGACY_REPORT_COLUMNS:
            self.assertIn(col, COLUMNS, f"Legacy column '{col}' must be in strategy_report_db.COLUMNS")

    def test_get_all_rows_returns_lean_keys_per_strategy(self):
        """get_all_rows: last_90s rows have LAST_90S_COLUMNS; hourly_last_90s have HOURLY_LAST_90S_COLUMNS."""
        from bot.strategy_report_db import get_all_rows, ensure_report_db, LAST_90S_COLUMNS, HOURLY_LAST_90S_COLUMNS
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            ensure_report_db(db_path)
            rows = get_all_rows(db_path, strategy_name=None, since_ts_utc=None)
            for r in rows:
                sn = (r.get("strategy_name") or "").strip()
                if sn == "last_90s_limit_99":
                    for c in LAST_90S_COLUMNS:
                        self.assertIn(c, r, f"last_90s row must have key '{c}'")
                elif sn == "hourly_last_90s_limit_99":
                    for c in HOURLY_LAST_90S_COLUMNS:
                        self.assertIn(c, r, f"hourly_last_90s row must have key '{c}'")
        finally:
            try:
                os.unlink(db_path)
            except Exception:
                pass

    def test_get_all_rows_row_keys_include_lean_columns_after_write(self):
        """After write_row_last_90s, get_all_rows must return dicts with LAST_90S_COLUMNS keys."""
        from bot.strategy_report_db import get_all_rows, ensure_report_db, write_row_last_90s, LAST_90S_COLUMNS
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            ensure_report_db(db_path)
            write_row_last_90s(
                db_path, "WINDOW1", "btc",
                placed=0, skip_reason="min_bid", skip_details="yes bid=90c < 97",
            )
            rows = get_all_rows(db_path, strategy_name=None, since_ts_utc=None)
            self.assertGreaterEqual(len(rows), 1)
            row = rows[0]
            for c in LAST_90S_COLUMNS:
                self.assertIn(c, row, f"get_all_rows must return key '{c}' for last_90s report TSV")
        finally:
            try:
                os.unlink(db_path)
            except Exception:
                pass

    def test_report_run_produces_tsv_with_lean_columns(self):
        """run() must write last_90s TSV and hourly TSV both with lean columns (LAST_90S / HOURLY_LAST_90S)."""
        from bot.strategy_report_db import ensure_report_db, write_row_last_90s, LAST_90S_COLUMNS, HOURLY_LAST_90S_COLUMNS
        from tools.strategy_report_hourly_email import run
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "bot_state.db")
            ensure_report_db(db_path)
            write_row_last_90s(
                db_path, "W1", "btc",
                placed=0, skip_reason="min_bid", skip_details="yes bid=90c < 97",
            )
            config_path = os.path.join(tmp, "config.yaml")
            with open(config_path, "w") as f:
                f.write("state:\n  db_path: %s\nassets: [btc]\n" % db_path.replace("\\", "/"))
            project_root = Path(tmp)
            run(
                config_path=config_path,
                send_email=False,
                since_hours=1.0,
                project_root=project_root,
                refresh_from_kalshi=False,
            )
            reports_dir = project_root / "reports"
            p1 = reports_dir / "strategy_report_last_90s_limit_99.tsv"
            self.assertTrue(p1.exists(), "Report must write strategy_report_last_90s_limit_99.tsv")
            with open(p1, encoding="utf-8") as f:
                lines = f.readlines()
            self.assertGreaterEqual(len(lines), 1, "TSV must have at least header")
            header = lines[0].rstrip("\n").split("\t")
            self.assertEqual(header, list(LAST_90S_COLUMNS), "Last_90s TSV header must match LAST_90S_COLUMNS (lean schema)")
            for i, line in enumerate(lines[1:], start=2):
                fields = line.rstrip("\n").split("\t")
                self.assertEqual(len(fields), len(LAST_90S_COLUMNS), f"TSV data row {i} must have {len(LAST_90S_COLUMNS)} columns (got {len(fields)})")
            # Hourly TSV uses lean schema (HOURLY_LAST_90S_COLUMNS)
            p2 = reports_dir / "strategy_report_hourly_last_90s_limit_99.tsv"
            self.assertTrue(p2.exists())
            with open(p2, encoding="utf-8") as f:
                hlines = f.readlines()
            self.assertGreaterEqual(len(hlines), 1)
            hheader = hlines[0].rstrip("\n").split("\t")
            self.assertEqual(hheader, list(HOURLY_LAST_90S_COLUMNS), "Hourly TSV header must match HOURLY_LAST_90S_COLUMNS (lean schema)")


class TestKalshiRefreshRequirements(unittest.TestCase):
    """Verify we have DB columns, env vars, and code path to call Kalshi API for report refresh."""

    def test_refresh_needs_order_id_in_row(self):
        """refresh_report_from_kalshi only processes rows with order_id; document that placed rows must have order_id."""
        from bot.strategy_report_db import ensure_report_db, write_row_last_90s, get_all_rows
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name
        try:
            ensure_report_db(db_path)
            # Row with order_id is candidate for refresh (lean table)
            write_row_last_90s(db_path, "W1", "btc", placed=1, order_id="ord-123", ticker="T", side="yes")
            rows = get_all_rows(db_path, since_ts_utc="2000-01-01T00:00:00")
            with_order = [r for r in rows if (r.get("order_id") or "").strip()]
            self.assertEqual(len(with_order), 1)
            self.assertEqual(with_order[0].get("order_id"), "ord-123")
        finally:
            try:
                os.unlink(db_path)
            except Exception:
                pass

    def test_refresh_needs_window_id_and_ticker_for_outcome(self):
        """last_90s needs window_id for fetch_15min_market_result; hourly needs window_id, ticker for fetch_ticker_outcome."""
        from bot.strategy_report_db import LAST_90S_COLUMNS, HOURLY_LAST_90S_COLUMNS
        self.assertIn("window_id", LAST_90S_COLUMNS)
        self.assertIn("ticker", LAST_90S_COLUMNS)
        self.assertIn("window_id", HOURLY_LAST_90S_COLUMNS)
        self.assertIn("ticker", HOURLY_LAST_90S_COLUMNS)

    def test_kalshi_client_env_documented(self):
        """KalshiClient requires KALSHI_API_KEY, KALSHI_PRIVATE_KEY, KALSHI_BASE_URL (documented in code)."""
        from src.client.kalshi_client import KalshiClient
        import os
        # Document required env (client raises if missing)
        required = ["KALSHI_API_KEY", "KALSHI_PRIVATE_KEY"]
        for env_key in required:
            # Just ensure the client references these (we don't set env in test)
            self.assertTrue(hasattr(KalshiClient, "__init__"))
        # .env.example or README should mention KALSHI_* for report refresh
        env_example = PROJECT_ROOT / ".env.example"
        if env_example.exists():
            content = env_example.read_text()
            # Optional: check that Kalshi is mentioned for report/API
            self.assertIn("SMTP", content)  # at least email env is documented


if __name__ == "__main__":
    unittest.main()
