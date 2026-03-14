"""
Tests for Last-90s Limit-99c strategy (15-min only). Ensures no impact on hourly or existing 15-min.
Includes concurrency tests: db_lock, ThreadPoolExecutor usage, and parallel execution.
"""
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bot.state import (
    ensure_state_db,
    get_last_90s_placed,
    get_last_90s_placement,
    get_last_90s_stop_loss_triggered,
    set_last_90s_placed,
    set_last_90s_stop_loss_triggered,
)
from bot.strategy_report_db import ensure_report_db
from bot.last_90s_strategy import (
    _get_cfg,
    _assets,
    _max_cost_cents,
    _choose_side,
    _get_bids,
    _limit_price_from_quote,
    run_last_90s_once,
)


class TestLast90sConfig(unittest.TestCase):
    def test_get_cfg_disabled(self):
        cfg = _get_cfg({"fifteen_min": {"last_90s_limit_99": {"enabled": False}}})
        self.assertIsNotNone(cfg)
        self.assertFalse(cfg.get("enabled"))

    def test_get_cfg_missing(self):
        self.assertIsNone(_get_cfg({}))
        self.assertIsNone(_get_cfg({"fifteen_min": {}}))

    def test_get_cfg_enabled(self):
        cfg = _get_cfg({"fifteen_min": {"last_90s_limit_99": {"enabled": True, "window_seconds": 90}}})
        self.assertEqual(cfg.get("window_seconds"), 90)

    def test_assets_default(self):
        cfg = {"enabled": True}
        self.assertIn("btc", _assets(cfg, {"assets": ["btc", "eth"]}))
        self.assertIn("eth", _assets(cfg, {"assets": ["btc", "eth"]}))

    def test_assets_configured(self):
        cfg = {"enabled": True, "assets": ["btc", "eth", "sol", "xrp"]}
        out = _assets(cfg, {})
        self.assertEqual(set(out), {"btc", "eth", "sol", "xrp"})

    def test_max_cost_cents(self):
        cfg = {"max_cost_cents": 500, "max_cost_cents_by_asset": {"btc": 1000, "eth": 500}}
        self.assertEqual(_max_cost_cents(cfg, {}, "btc"), 1000)
        self.assertEqual(_max_cost_cents(cfg, {}, "eth"), 500)
        self.assertEqual(_max_cost_cents(cfg, {}, "sol"), 500)


class TestLast90sState(unittest.TestCase):
    def setUp(self):
        self.fd = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.fd.name
        self.fd.close()
        ensure_state_db(self.db_path)

    def tearDown(self):
        try:
            os.unlink(self.db_path)
        except Exception:
            pass

    def test_placed_roundtrip(self):
        self.assertFalse(get_last_90s_placed(self.db_path, "MKT1", "btc"))
        set_last_90s_placed(self.db_path, "MKT1", "btc")
        self.assertTrue(get_last_90s_placed(self.db_path, "MKT1", "btc"))
        self.assertFalse(get_last_90s_placed(self.db_path, "MKT2", "btc"))

    def test_placement_and_stop_loss_triggered(self):
        self.assertIsNone(get_last_90s_placement(self.db_path, "MKT1", "btc"))
        set_last_90s_placed(
            self.db_path, "MKT1", "btc",
            order_id="ord-1", ticker="TICK-00", count=2, limit_price_cents=99, side="yes",
        )
        placement = get_last_90s_placement(self.db_path, "MKT1", "btc")
        self.assertIsNotNone(placement)
        self.assertEqual(placement[0], "ord-1")
        self.assertEqual(placement[1], "TICK-00")
        self.assertEqual(placement[2], 2)
        self.assertEqual(placement[3], 99)
        self.assertEqual(placement[4], "yes")
        self.assertFalse(get_last_90s_stop_loss_triggered(self.db_path, "MKT1", "btc"))
        set_last_90s_stop_loss_triggered(self.db_path, "MKT1", "btc")
        self.assertTrue(get_last_90s_stop_loss_triggered(self.db_path, "MKT1", "btc"))


class TestLast90sRunOnce(unittest.TestCase):
    def setUp(self):
        self.fd = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.fd.name
        self.fd.close()
        ensure_state_db(self.db_path)

    def tearDown(self):
        try:
            os.unlink(self.db_path)
        except Exception:
            pass

    def test_run_once_disabled(self):
        config = {"fifteen_min": {"last_90s_limit_99": {"enabled": False}}}
        logger = MagicMock()
        run_last_90s_once(config, logger, None, self.db_path)
        logger.info.assert_not_called()

    def test_run_once_no_cfg(self):
        config = {}
        logger = MagicMock()
        run_last_90s_once(config, logger, None, self.db_path)
        logger.info.assert_not_called()

    @patch("bot.last_90s_strategy.get_current_15min_market_id")
    @patch("bot.last_90s_strategy.get_minutes_to_close_15min")
    @patch("bot.last_90s_strategy.fetch_15min_market")
    @patch("bot.last_90s_strategy.in_no_trade_window")
    def test_run_once_observe_logs_only(self, no_trade, fetch_market, get_mins, get_mkt):
        no_trade.return_value = False
        get_mkt.return_value = "KXBTC15M-TEST"
        get_mins.return_value = 1.0  # 60 sec to close
        fetch_market.return_value = {"ticker": "KXBTC15M-TEST-00"}
        config = {
            "mode": "OBSERVE",
            "fifteen_min": {
                "last_90s_limit_99": {
                    "enabled": True,
                    "window_seconds": 90,
                    "limit_price_cents": 99,
                    "max_cost_cents": 1000,
                    "assets": ["btc"],
                }
            },
        }
        logger = MagicMock()
        run_last_90s_once(config, logger, MagicMock(), self.db_path)
        self.assertFalse(get_last_90s_placed(self.db_path, "KXBTC15M-TEST", "btc"))
        logger.info.assert_called()


class TestLast90sHelpers(unittest.TestCase):
    def test_get_bids_none(self):
        self.assertEqual(_get_bids(None), (0, 0))

    def test_get_bids_from_obj(self):
        class Q:
            yes_bid = 95
            no_bid = 5
        self.assertEqual(_get_bids(Q()), (95, 5))

    def test_choose_side_yes_config(self):
        logger = MagicMock()
        self.assertEqual(_choose_side({"side": "yes"}, None, logger, "btc"), "yes")
        self.assertEqual(_choose_side({"side": "no"}, None, logger, "btc"), "no")

    def test_choose_side_auto_higher_yes(self):
        logger = MagicMock()
        quote = type("Q", (), {"yes_bid": 98, "no_bid": 2})()
        self.assertEqual(_choose_side({"side": "auto"}, quote, logger, "btc"), "yes")

    def test_choose_side_auto_higher_no(self):
        logger = MagicMock()
        quote = type("Q", (), {"yes_bid": 2, "no_bid": 98})()
        self.assertEqual(_choose_side({"side": "auto"}, quote, logger, "btc"), "no")

    def test_limit_price_from_quote_uses_bid(self):
        quote = type("Q", (), {"yes_bid": 97, "no_bid": 3})()
        self.assertEqual(_limit_price_from_quote(quote, "yes", 99), 97)
        self.assertEqual(_limit_price_from_quote(quote, "no", 99), 3)

    def test_limit_price_from_quote_100_defaults_99(self):
        quote = type("Q", (), {"yes_bid": 100, "no_bid": 0})()
        self.assertEqual(_limit_price_from_quote(quote, "yes", 99), 99)


class TestLast90sConcurrency(unittest.TestCase):
    """Concurrency: db_lock, ThreadPoolExecutor, and parallel execution."""

    def setUp(self):
        self.fd = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.fd.name
        self.fd.close()
        ensure_state_db(self.db_path)

    def tearDown(self):
        try:
            os.unlink(self.db_path)
        except Exception:
            pass

    @patch("bot.last_90s_strategy.wait")
    @patch("bot.last_90s_strategy.ThreadPoolExecutor")
    @patch("bot.last_90s_strategy.get_previous_15min_market_id")
    @patch("bot.last_90s_strategy.in_no_trade_window")
    def test_run_once_with_db_lock_uses_thread_pool(
        self, no_trade, get_prev_market, executor_cls, wait_mock
    ):
        """With db_lock, run_last_90s_once uses ThreadPoolExecutor and submits one task per asset."""
        no_trade.return_value = False
        get_prev_market.return_value = None  # no previous market to resolve
        wait_mock.return_value = None  # avoid blocking on mock futures
        mock_executor = MagicMock()
        mock_executor.__enter__ = MagicMock(return_value=mock_executor)
        mock_executor.__exit__ = MagicMock(return_value=False)
        mock_future = MagicMock()
        mock_future.result = MagicMock(return_value=None)
        mock_executor.submit = MagicMock(return_value=mock_future)
        executor_cls.return_value = mock_executor

        config = {
            "mode": "OBSERVE",
            "fifteen_min": {
                "last_90s_limit_99": {
                    "enabled": True,
                    "window_seconds": 90,
                    "limit_price_cents": 99,
                    "assets": ["btc", "eth", "sol"],
                }
            },
        }
        logger = MagicMock()
        db_lock = threading.Lock()
        run_last_90s_once(config, logger, MagicMock(), self.db_path, db_lock=db_lock)

        executor_cls.assert_called_once()
        call_kw = executor_cls.call_args[1]
        self.assertGreaterEqual(call_kw["max_workers"], 2)
        self.assertLessEqual(call_kw["max_workers"], 8)
        self.assertEqual(mock_executor.submit.call_count, 3)

    @patch("bot.last_90s_strategy.get_current_15min_market_id")
    @patch("bot.last_90s_strategy.get_minutes_to_close_15min")
    @patch("bot.last_90s_strategy.fetch_15min_market")
    @patch("bot.last_90s_strategy.fetch_15min_quote")
    @patch("bot.last_90s_strategy._get_spot_price")
    @patch("bot.last_90s_strategy.get_previous_15min_market_id")
    @patch("bot.last_90s_strategy.in_no_trade_window")
    def test_run_once_with_db_lock_multiple_assets_db_safe(
        self, no_trade, get_prev, get_spot, fetch_quote, fetch_market, get_mins, get_mkt
    ):
        """With db_lock and multiple assets, DB writes are serialized; no exception, state consistent."""
        no_trade.return_value = False
        get_prev.return_value = None
        get_mkt.side_effect = lambda asset: f"KX{asset.upper()}15M-TEST"
        get_mins.return_value = 0.5  # 30 sec to close, inside window
        fetch_market.side_effect = lambda mkt: {"ticker": mkt + "-00"}
        quote = type("Q", (), {"yes_bid": 99, "no_bid": 1, "yes_ask": 99, "no_ask": 1})()
        fetch_quote.return_value = quote
        get_spot.return_value = 1000.0  # so distance is large

        config = {
            "mode": "OBSERVE",
            "fifteen_min": {
                "last_90s_limit_99": {
                    "enabled": True,
                    "window_seconds": 90,
                    "limit_price_cents": 99,
                    "min_bid_cents": 90,
                    "min_distance_at_placement": {"btc": 1, "eth": 1},
                    "assets": ["btc", "eth"],
                }
            },
        }
        logger = MagicMock()
        db_lock = threading.Lock()
        run_last_90s_once(config, logger, MagicMock(), self.db_path, db_lock=db_lock)

        # No placement (OBSERVE); both assets should have been considered
        self.assertFalse(get_last_90s_placed(self.db_path, "KXBTC15M-TEST", "btc"))
        self.assertFalse(get_last_90s_placed(self.db_path, "KXETH15M-TEST", "eth"))
        logger.info.assert_called()

    @patch("bot.last_90s_strategy.get_current_15min_market_id")
    @patch("bot.last_90s_strategy.get_minutes_to_close_15min")
    @patch("bot.last_90s_strategy.fetch_15min_market")
    @patch("bot.last_90s_strategy.fetch_15min_quote")
    @patch("bot.last_90s_strategy._get_spot_price")
    @patch("bot.last_90s_strategy.get_previous_15min_market_id")
    @patch("bot.last_90s_strategy.in_no_trade_window")
    def test_parallel_execution_reduces_elapsed_time(
        self, no_trade, get_prev, get_spot, fetch_quote, fetch_market, get_mins, get_mkt
    ):
        """With db_lock, asset work runs in parallel; total elapsed < 2 * single-asset delay."""
        no_trade.return_value = False
        get_prev.return_value = None
        get_mkt.side_effect = lambda asset: f"KX{asset.upper()}15M-TEST"
        get_mins.return_value = 0.5
        fetch_market.side_effect = lambda mkt: {"ticker": mkt + "-00"}
        delay_s = 0.04

        def slow_quote(*args, **kwargs):
            time.sleep(delay_s)
            return type("Q", (), {"yes_bid": 99, "no_bid": 1, "yes_ask": 99, "no_ask": 1})()

        fetch_quote.side_effect = slow_quote
        get_spot.return_value = 1000.0

        config = {
            "mode": "OBSERVE",
            "fifteen_min": {
                "last_90s_limit_99": {
                    "enabled": True,
                    "window_seconds": 90,
                    "limit_price_cents": 99,
                    "min_bid_cents": 90,
                    "min_distance_at_placement": {"btc": 1, "eth": 1},
                    "assets": ["btc", "eth"],
                }
            },
        }
        logger = MagicMock()
        db_lock = threading.Lock()
        start = time.perf_counter()
        run_last_90s_once(config, logger, MagicMock(), self.db_path, db_lock=db_lock)
        elapsed = time.perf_counter() - start

        # If parallel: ~delay_s. If sequential: ~2*delay_s. Allow some overhead.
        self.assertLess(elapsed, delay_s * 2 * 0.9, "Expected parallel execution (elapsed < 2*delay)")

    @patch("bot.last_90s_strategy.fetch_15min_market")
    @patch("bot.last_90s_strategy.fetch_15min_quote")
    @patch("bot.last_90s_strategy._get_spot_price")
    @patch("bot.last_90s_strategy.get_current_15min_market_id")
    @patch("bot.last_90s_strategy.get_minutes_to_close_15min")
    @patch("bot.last_90s_strategy._db_section")
    @patch("bot.last_90s_strategy.get_previous_15min_market_id")
    @patch("bot.last_90s_strategy.in_no_trade_window")
    def test_db_lock_used_for_db_section(
        self, no_trade, get_prev, db_section_mock, get_mins, get_mkt, get_spot, fetch_quote, fetch_market
    ):
        """When db_lock is provided, _db_section is invoked (lock used around DB access)."""
        from contextlib import contextmanager

        no_trade.return_value = False
        get_prev.return_value = None
        get_mkt.side_effect = lambda asset: f"KX{asset.upper()}15M-TEST"
        get_mins.return_value = 0.5  # in window (30 sec to close)
        fetch_market.side_effect = lambda mkt: {"ticker": mkt + "-00"}
        fetch_quote.return_value = type("Q", (), {"yes_bid": 99, "no_bid": 1, "yes_ask": 99, "no_ask": 1})()
        get_spot.return_value = 1000.0
        enter_count = [0]

        @contextmanager
        def count_enters(*args, **kwargs):
            enter_count[0] += 1
            yield

        db_section_mock.side_effect = count_enters

        config = {
            "mode": "OBSERVE",
            "fifteen_min": {
                "last_90s_limit_99": {
                    "enabled": True,
                    "window_seconds": 90,
                    "min_bid_cents": 90,
                    "min_distance_at_placement": {"btc": 1, "eth": 1},
                    "assets": ["btc", "eth"],
                }
            },
        }
        logger = MagicMock()
        db_lock = threading.Lock()
        run_last_90s_once(config, logger, MagicMock(), self.db_path, db_lock=db_lock)

        # Each asset enters _db_section at least once (get_last_90s_placed); and again for write_row_last_90s.
        self.assertGreater(enter_count[0], 0, "_db_section (lock) should be used when db_lock is set")


class TestLast90sAggregatedSkips(unittest.TestCase):
    """Test that aggregated_skips rows get skip_details from persisted aggregator (window_seconds=90, run_interval=1, always skip)."""

    def setUp(self):
        self.fd = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.fd.name
        self.fd.close()
        ensure_state_db(self.db_path)
        ensure_report_db(self.db_path)

    def tearDown(self):
        try:
            os.unlink(self.db_path)
        except Exception:
            pass

    @patch("bot.last_90s_strategy.get_current_15min_market_id")
    @patch("bot.last_90s_strategy.get_minutes_to_close_15min")
    @patch("bot.last_90s_strategy.fetch_15min_market")
    @patch("bot.last_90s_strategy.fetch_15min_quote")
    @patch("bot.last_90s_strategy._get_spot_price")
    @patch("bot.last_90s_strategy.get_previous_15min_market_id")
    @patch("bot.last_90s_strategy.in_no_trade_window")
    def test_aggregated_skips_has_skip_history_when_always_skip(
        self, no_trade, get_prev, get_spot, fetch_quote, fetch_market, get_mins, get_mkt
    ):
        """Run with window_seconds=90, run_interval_seconds=1; force skips (bid < min_bid, distance <= min_distance). Then resolve; skip_details must contain Aggregated skips and Checks:, not 'No skip history recorded'."""
        market_id = "KXBTC15M-26MAR021500"
        no_trade.return_value = False
        get_mkt.return_value = market_id
        get_mins.return_value = 0.5  # 30s to close, inside 90s window
        fetch_market.return_value = {"ticker": market_id + "-00"}
        # Low bid so we skip (current_bid <= min_bid_cents). yes_bid=50, no_bid=50 -> choose yes, current_bid=50; min_bid_cents=94 -> skip.
        quote = type("Q", (), {"yes_bid": 50, "no_bid": 50, "yes_ask": 99, "no_ask": 1})()
        fetch_quote.return_value = quote
        get_spot.return_value = 100_000.0
        # First 3 runs: no previous market (so resolve does nothing for btc). 4th run: previous = market_id so we resolve and write aggregated row.
        get_prev.side_effect = [None, None, None, market_id]

        config = {
            "mode": "TRADE",
            "fifteen_min": {
                "last_90s_limit_99": {
                    "enabled": True,
                    "window_seconds": 90,
                    "run_interval_seconds": 1,
                    "limit_price_cents": 99,
                    "min_bid_cents": 94,
                    "min_distance_at_placement": {"btc": 10},
                    "assets": ["btc"],
                }
            },
        }
        logger = MagicMock()
        db_lock = threading.Lock()

        # Run 3 times "in window" (each run: resolve first with get_prev=None, then in-window with current=market_id; we skip and persist).
        for _ in range(3):
            run_last_90s_once(config, logger, MagicMock(), self.db_path, db_lock=db_lock)

        # 4th run: resolve sees previous market = market_id, placed_prev = False -> get_and_clear from DB (or memory), write aggregated row.
        run_last_90s_once(config, logger, MagicMock(), self.db_path, db_lock=db_lock)

        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT skip_reason, skip_details FROM strategy_report_last_90s WHERE window_id = ? AND asset = ? AND placed = 0",
                (market_id, "btc"),
            )
            row = cur.fetchone()
        finally:
            conn.close()

        self.assertIsNotNone(row, "Expected one aggregated_skips row for (market_id, btc)")
        skip_reason, skip_details = row
        self.assertEqual(skip_reason, "aggregated_skips")
        self.assertIn("Aggregated skips", skip_details, "skip_details should contain 'Aggregated skips'")
        self.assertIn("Checks:", skip_details, "skip_details should contain 'Checks:'")
        self.assertNotIn("No skip history recorded", skip_details or "", "Bug: skip_details must not be 'No skip history recorded'")
