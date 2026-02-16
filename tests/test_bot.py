"""
Unit tests for the bot module.
"""
import unittest
from datetime import datetime
from unittest.mock import patch

import pytz

# Time computations
from bot.market import (
    get_current_15min_market_id,
    get_minutes_to_close,
    get_minutes_to_close_15min,
    get_market_context,
)
from bot.scheduler import get_minutes_since_hour_start, should_run
from bot.strategy import generate_signals, _in_range
from bot.market import TickerQuote
from bot.state import get_ticker_order_count, get_total_order_count, increment_order_count
import tempfile
import os


class TestTimeComputations(unittest.TestCase):
    def test_should_run_after_offset(self):
        with patch("bot.scheduler.datetime") as mock_dt:
            la = pytz.timezone("America/Los_Angeles")
            # 13:02:00 - 2 minutes past hour -> should run
            now = la.localize(datetime(2026, 2, 14, 13, 2, 0))
            mock_dt.now.return_value = now
            self.assertTrue(should_run(start_offset_minutes=1))

    def test_should_not_run_before_offset(self):
        with patch("bot.scheduler.datetime") as mock_dt:
            la = pytz.timezone("America/Los_Angeles")
            # 13:00:30 - 0.5 minutes past hour -> should not run
            now = la.localize(datetime(2026, 2, 14, 13, 0, 30))
            mock_dt.now.return_value = now
            self.assertFalse(should_run(start_offset_minutes=1))

    def test_get_minutes_to_close(self):
        # KXBTCD-26FEB1414 = Feb 14, 2026 14:00 ET
        mins = get_minutes_to_close("KXBTCD-26FEB1414")
        self.assertIsInstance(mins, (int, float))
        self.assertGreaterEqual(mins, 0)

    def test_get_minutes_to_close_eth(self):
        # KXETHD-26FEB1414 = Feb 14, 2026 14:00 ET (Ethereum)
        mins = get_minutes_to_close("KXETHD-26FEB1414")
        self.assertIsInstance(mins, (int, float))
        self.assertGreaterEqual(mins, 0)

    def test_get_minutes_to_close_15min_btc(self):
        mins = get_minutes_to_close_15min("KXBTC15M-26FEB141430")
        self.assertIsInstance(mins, (int, float))
        self.assertGreaterEqual(mins, 0)
        self.assertLessEqual(mins, 15)

    def test_get_minutes_to_close_15min_eth(self):
        mins = get_minutes_to_close_15min("KXETH15M-26FEB141430")
        self.assertIsInstance(mins, (int, float))
        self.assertGreaterEqual(mins, 0)
        self.assertLessEqual(mins, 15)

    def test_get_minutes_to_close_15min_sol(self):
        mins = get_minutes_to_close_15min("KXSOL15M-26FEB141430")
        self.assertIsInstance(mins, (int, float))
        self.assertGreaterEqual(mins, 0)
        self.assertLessEqual(mins, 15)

    def test_get_minutes_to_close_15min_xrp(self):
        mins = get_minutes_to_close_15min("KXXRP15M-26FEB141430")
        self.assertIsInstance(mins, (int, float))
        self.assertGreaterEqual(mins, 0)
        self.assertLessEqual(mins, 15)

    def test_get_current_15min_market_id_sol(self):
        et = pytz.timezone("US/Eastern")
        now_et = et.localize(datetime(2026, 2, 14, 14, 10, 0))
        with patch("bot.market.datetime") as mock_dt:
            mock_dt.now.return_value = now_et.astimezone(pytz.utc)
            mkt_id = get_current_15min_market_id(asset="sol")
            self.assertTrue(mkt_id.upper().startswith("KXSOL15M-"))
            self.assertIn("26FEB14", mkt_id.upper())

    def test_get_current_15min_market_id_xrp(self):
        et = pytz.timezone("US/Eastern")
        now_et = et.localize(datetime(2026, 2, 14, 14, 10, 0))
        with patch("bot.market.datetime") as mock_dt:
            mock_dt.now.return_value = now_et.astimezone(pytz.utc)
            mkt_id = get_current_15min_market_id(asset="xrp")
            self.assertTrue(mkt_id.upper().startswith("KXXRP15M-"))
            self.assertIn("26FEB14", mkt_id.upper())

    def test_market_context_late_window(self):
        ctx = get_market_context("KXBTCD-26FEB1414", late_window_minutes=10)
        self.assertIsNotNone(ctx.minutes_to_close)
        self.assertIsInstance(ctx.is_late_window, bool)


class TestThresholdChecks(unittest.TestCase):
    def test_in_range(self):
        self.assertTrue(_in_range(95, 93, 98))
        self.assertTrue(_in_range(93, 93, 98))
        self.assertTrue(_in_range(98, 93, 98))
        self.assertFalse(_in_range(92, 93, 98))
        self.assertFalse(_in_range(99, 93, 98))
        self.assertFalse(_in_range(None, 93, 98))

    def test_generate_signals_yes_only(self):
        quotes = [
            TickerQuote("T1", 69000, yes_ask=95, no_ask=5, yes_bid=94, no_bid=4, subtitle="$69,000"),
        ]
        signals = generate_signals(quotes, False, {
            "normal": {"yes_min": 93, "yes_max": 98, "no_min": 93, "no_max": 98},
        })
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].side, "yes")
        self.assertEqual(signals[0].reason, "YES_BUY")

    def test_generate_signals_no_only(self):
        quotes = [
            TickerQuote("T1", 69000, yes_ask=5, no_ask=95, yes_bid=4, no_bid=94, subtitle="$69,000"),
        ]
        signals = generate_signals(quotes, False, {
            "normal": {"yes_min": 93, "yes_max": 98, "no_min": 93, "no_max": 98},
        })
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].side, "no")

    def test_generate_signals_both_qualify_cheaper_wins(self):
        quotes = [
            TickerQuote("T1", 69000, yes_ask=94, no_ask=96, yes_bid=93, no_bid=95, subtitle="$69,000"),
        ]
        signals = generate_signals(quotes, False, {
            "normal": {"yes_min": 93, "yes_max": 98, "no_min": 93, "no_max": 98},
        })
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].side, "yes")  # 94 < 96

    def test_generate_signals_ambiguous_skip(self):
        quotes = [
            TickerQuote("T1", 69000, yes_ask=95, no_ask=95, yes_bid=94, no_bid=94, subtitle="$69,000"),
        ]
        signals = generate_signals(quotes, False, {
            "normal": {"yes_min": 93, "yes_max": 98, "no_min": 93, "no_max": 98},
        })
        self.assertEqual(len(signals), 0)


class TestCapEnforcement(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.db_path = self.tmp.name
        from bot.state import ensure_state_db
        ensure_state_db(self.db_path)

    def tearDown(self):
        os.unlink(self.db_path)

    def test_increment_and_count(self):
        increment_order_count(self.db_path, "H1", "T1", "yes", "ord1")
        increment_order_count(self.db_path, "H1", "T1", "yes", "ord2")
        count = get_ticker_order_count(self.db_path, "H1", "T1", "combined")
        self.assertEqual(count, 2)
        total = get_total_order_count(self.db_path, "H1")
        self.assertEqual(total, 2)

    def test_cap_combined(self):
        for i in range(6):
            increment_order_count(self.db_path, "H1", "T1", "yes" if i % 2 == 0 else "no")
        count = get_ticker_order_count(self.db_path, "H1", "T1", "combined")
        self.assertEqual(count, 6)

    def test_cap_per_side(self):
        increment_order_count(self.db_path, "H1", "T1", "yes", "o1")
        increment_order_count(self.db_path, "H1", "T1", "yes", "o2")
        increment_order_count(self.db_path, "H1", "T1", "no", "o3")
        yes_count = get_ticker_order_count(self.db_path, "H1", "T1", "per_side", side="yes")
        no_count = get_ticker_order_count(self.db_path, "H1", "T1", "per_side", side="no")
        self.assertEqual(yes_count, 2)
        self.assertEqual(no_count, 1)
