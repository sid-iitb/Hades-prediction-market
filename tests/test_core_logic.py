"""
Core logic tests: entry, exit, stop-loss, risk guards, schedule, analysis events.
Ensures all intervals (fifteen_min, hourly, daily, weekly) behave correctly for analysis and tuning.
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytz
from datetime import datetime

from bot.market import TickerQuote
from bot.strategy import generate_signals_farthest
from bot.strategy_15min import generate_signals_15min, get_no_signal_reason
from bot.exit_criteria import _check_hard_flip, evaluate_positions
from bot.risk_guards import gate_allow_entry, apply_guards_filter, reset_window_on_expiry
from bot.analysis_log import build_analysis_event, build_exit_analysis_event, EXIT_STOPLOSS, EXIT_TAKE_PROFIT


# ---------------------------------------------------------------------------
# Entry: generate_signals_farthest (hourly, daily, weekly)
# ---------------------------------------------------------------------------

class TestEntrySignalsFarthest(unittest.TestCase):
    """Farthest strategy used by hourly, daily, weekly."""

    def test_normal_window_uses_normal_thresholds(self):
        quotes = [TickerQuote("T1", 69000, yes_ask=95, no_ask=5, yes_bid=94, no_bid=4, subtitle="$69k")]
        signals = generate_signals_farthest(quotes, spot_price=69100, ctx_late_window=False, thresholds={
            "normal": {"yes_min": 90, "yes_max": 98, "no_min": 90, "no_max": 98},
            "late": {"yes_min": 94, "yes_max": 99, "no_min": 94, "no_max": 99},
        })
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].reason, "YES_BUY")
        self.assertFalse(signals[0].late_window)

    def test_late_window_uses_late_thresholds(self):
        quotes = [TickerQuote("T1", 69000, yes_ask=95, no_ask=5, yes_bid=94, no_bid=4, subtitle="$69k")]
        signals = generate_signals_farthest(quotes, spot_price=69100, ctx_late_window=True, thresholds={
            "normal": {"yes_min": 90, "yes_max": 98, "no_min": 90, "no_max": 98},
            "late": {"yes_min": 94, "yes_max": 99, "no_min": 94, "no_max": 99},
        })
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].reason, "YES_BUY_LATE")
        self.assertTrue(signals[0].late_window)

    def test_farthest_strike_picked(self):
        quotes = [
            TickerQuote("NEAR", 69000, yes_ask=95, no_ask=5, yes_bid=94, no_bid=4, subtitle="$69k"),
            TickerQuote("FAR", 68500, yes_ask=95, no_ask=5, yes_bid=94, no_bid=4, subtitle="$68.5k"),
        ]
        signals = generate_signals_farthest(quotes, spot_price=69100, ctx_late_window=False, thresholds={
            "normal": {"yes_min": 94, "yes_max": 99, "no_min": 94, "no_max": 99},
        })
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].ticker, "FAR")

    def test_strike_above_spot_bets_no(self):
        quotes = [TickerQuote("T1", 70000, yes_ask=10, no_ask=90, yes_bid=9, no_bid=89, subtitle="$70k")]
        signals = generate_signals_farthest(quotes, spot_price=69000, ctx_late_window=False, thresholds={
            "normal": {"yes_min": 90, "yes_max": 98, "no_min": 90, "no_max": 98},
        })
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].side, "no")

    def test_out_of_band_returns_empty(self):
        quotes = [TickerQuote("T1", 69000, yes_ask=50, no_ask=50, yes_bid=49, no_bid=49, subtitle="$69k")]
        signals = generate_signals_farthest(quotes, spot_price=69100, ctx_late_window=False, thresholds={
            "normal": {"yes_min": 94, "yes_max": 99, "no_min": 94, "no_max": 99},
        })
        self.assertEqual(len(signals), 0)


# ---------------------------------------------------------------------------
# Entry: generate_signals_15min
# ---------------------------------------------------------------------------

class TestEntrySignals15min(unittest.TestCase):
    """15-min strategy: only signals when seconds_to_close <= late_window_seconds."""

    def test_outside_late_window_returns_empty(self):
        quote = TickerQuote("KXBTC15M-X", 69000, yes_ask=95, no_ask=5, yes_bid=94, no_bid=4, subtitle="")
        config = {"fifteen_min": {"late_window_seconds": 140, "yes_min": 94, "yes_max": 99, "no_min": 94, "no_max": 99}}
        minutes_to_close = 5.0  # 300 sec > 140
        signals = generate_signals_15min(quote, minutes_to_close, config)
        self.assertEqual(len(signals), 0)

    def test_inside_late_window_returns_signal(self):
        quote = TickerQuote("KXBTC15M-X", 69000, yes_ask=95, no_ask=5, yes_bid=94, no_bid=4, subtitle="")
        config = {"fifteen_min": {"late_window_seconds": 140, "yes_min": 94, "yes_max": 99, "no_min": 94, "no_max": 99}}
        minutes_to_close = 2.0  # 120 sec <= 140
        signals = generate_signals_15min(quote, minutes_to_close, config)
        self.assertEqual(len(signals), 1)
        self.assertIn("15M", signals[0].reason)


# ---------------------------------------------------------------------------
# Exit: _check_hard_flip, evaluate_positions (stop_loss, take_profit, persistence)
# ---------------------------------------------------------------------------

class TestExitHardFlip(unittest.TestCase):
    """Hard-flip exit: YES + spot < strike, or NO + spot > strike."""

    def test_yes_spot_below_strike_flips(self):
        self.assertTrue(_check_hard_flip("yes", spot=68000, strike=69000))

    def test_no_spot_above_strike_flips(self):
        self.assertTrue(_check_hard_flip("no", spot=70000, strike=69000))

    def test_yes_spot_above_strike_holds(self):
        self.assertFalse(_check_hard_flip("yes", spot=70000, strike=69000))

    def test_no_spot_below_strike_holds(self):
        self.assertFalse(_check_hard_flip("no", spot=68000, strike=69000))


class TestExitCriteriaEvaluate(unittest.TestCase):
    """Exit criteria: stop-loss, take-profit, persistence, panic stop."""

    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_client.get_top_of_book.return_value = {
            "yes_bid": 80, "yes_ask": 81,
            "no_bid": 19, "no_ask": 20,
        }

    def _pos(self, ticker, side, entry_cents, count=1):
        # _normalize_position expects average_price (0-1 or cents) or avg_price
        return {
            "ticker": ticker,
            "side": side,
            "count": count,
            "average_price": entry_cents / 100.0,  # 70 -> 0.70 for _parse_cents
        }

    def test_take_profit_triggers(self):
        # Entry 70, bid 80 -> pnl = (80-70)/70 = 14.3% >= 10%
        pos = self._pos("KXBTCD-26FEB1414-T1", "yes", 70)
        self.mock_client.get_top_of_book.return_value = {"yes_bid": 80, "yes_ask": 81, "no_bid": 19, "no_ask": 20}
        results = evaluate_positions(
            positions=[pos],
            client=self.mock_client,
            stop_loss_pct=0.30,
            profit_target_pct=0.10,
            mode="OBSERVE",
            hour_market_id="KXBTCD-26FEB1414",
            sl_persistence_polls=2,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].action, "TAKE_PROFIT")
        self.assertTrue(results[0].exit_criteria_evaluated.get("take_profit_triggered"))

    def test_stop_loss_persistence_one_poll_holds(self):
        pos = self._pos("KXBTCD-26FEB1414-T1", "yes", 95)
        self.mock_client.get_top_of_book.return_value = {"yes_bid": 60, "yes_ask": 61, "no_bid": 39, "no_ask": 40}
        # pnl = (60-95)/95 = -36.8% <= -30% -> stop loss territory
        results = evaluate_positions(
            positions=[pos],
            client=self.mock_client,
            stop_loss_pct=0.30,
            profit_target_pct=None,
            mode="OBSERVE",
            hour_market_id="KXBTCD-26FEB1414",
            sl_persistence_polls=2,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].action, "SL_WARNING")
        self.assertFalse(results[0].exit_criteria_evaluated.get("stop_loss_triggered"))

    def test_stop_loss_persistence_one_poll_fires_with_polls_1(self):
        pos = self._pos("KXBTCD-26FEB1414-T1", "yes", 95)
        self.mock_client.get_top_of_book.return_value = {"yes_bid": 60, "yes_ask": 61, "no_bid": 39, "no_ask": 40}
        results = evaluate_positions(
            positions=[pos],
            client=self.mock_client,
            stop_loss_pct=0.30,
            profit_target_pct=None,
            mode="OBSERVE",
            hour_market_id="KXBTCD-26FEB1414",
            sl_persistence_polls=1,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].action, "STOP_LOSS")
        self.assertTrue(results[0].exit_criteria_evaluated.get("stop_loss_triggered"))

    def test_holding_above_thresholds(self):
        pos = self._pos("KXBTCD-26FEB1414-T1", "yes", 90)
        self.mock_client.get_top_of_book.return_value = {"yes_bid": 92, "yes_ask": 93, "no_bid": 7, "no_ask": 8}
        results = evaluate_positions(
            positions=[pos],
            client=self.mock_client,
            stop_loss_pct=0.30,
            profit_target_pct=0.10,
            mode="OBSERVE",
            hour_market_id="KXBTCD-26FEB1414",
            sl_persistence_polls=2,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].action, "HOLD")
        self.assertFalse(results[0].exit_criteria_evaluated.get("stop_loss_triggered"))
        self.assertFalse(results[0].exit_criteria_evaluated.get("take_profit_triggered"))


# ---------------------------------------------------------------------------
# Exit: _exit_check_due for daily/weekly
# ---------------------------------------------------------------------------

class TestExitCheckDue(unittest.TestCase):
    """_exit_check_due returns correct overrides for daily/weekly; uses DB for last-run persistence."""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.tmp.name
        from bot.state import ensure_state_db
        ensure_state_db(self.db_path)

    def tearDown(self):
        import os
        if hasattr(self, "tmp") and self.tmp:
            try:
                os.unlink(self.db_path)
            except Exception:
                pass

    def test_daily_returns_stop_profit_overrides(self):
        from bot.main import _exit_check_due
        config = {
            "exit_criteria": {
                "daily": {
                    "stop_loss_pct": 0.30,
                    "profit_target_pct": 0.10,
                    "evaluation_interval_minutes": 60,
                    "stop_loss_persistence_polls": 1,
                },
            },
        }
        with patch("bot.main.time") as mock_time:
            mock_time.time.return_value = 5000.0  # must be >= interval (60*60=3600) for run_now
            run_now, stop_o, profit_o = _exit_check_due("daily", "KXBTCD-26FEB2117", config, self.db_path)
        self.assertTrue(run_now)
        self.assertEqual(stop_o, 0.30)
        self.assertEqual(profit_o, 0.10)

    def test_weekly_returns_stop_profit_overrides(self):
        from bot.main import _exit_check_due
        config = {
            "exit_criteria": {
                "weekly": {
                    "stop_loss_pct": 0.30,
                    "profit_target_pct": 0.10,
                    "evaluation_interval_minutes": 360,
                    "stop_loss_persistence_polls": 1,
                },
            },
        }
        with patch("bot.main.time") as mock_time:
            mock_time.time.return_value = 25000.0  # must be >= interval (360*60=21600) for run_now
            run_now, stop_o, profit_o = _exit_check_due("weekly", "KXBTCD-26FEB2717", config, self.db_path)
        self.assertTrue(run_now)
        self.assertEqual(stop_o, 0.30)
        self.assertEqual(profit_o, 0.10)


# ---------------------------------------------------------------------------
# Schedule: late window gating
# ---------------------------------------------------------------------------

class TestScheduleState(unittest.TestCase):
    """Hourly and 15-min only run in late window."""

    def test_hourly_outside_late_window_should_not_run(self):
        from bot.market import get_hourly_schedule_state, get_minutes_to_close
        with patch("bot.market.get_minutes_to_close") as mock_mins:
            mock_mins.return_value = 30.0  # 30 min to close > 15
            run, sleep = get_hourly_schedule_state(["btc"], late_window_minutes=15)
        self.assertFalse(run)
        self.assertGreater(sleep, 0)

    def test_hourly_inside_late_window_should_run(self):
        with patch("bot.market.get_minutes_to_close") as mock_mins:
            mock_mins.return_value = 10.0
            from bot.market import get_hourly_schedule_state
            run, _ = get_hourly_schedule_state(["btc"], late_window_minutes=15)
        self.assertTrue(run)

    def test_15min_outside_late_window_should_not_run(self):
        with patch("bot.market.get_minutes_to_close_15min") as mock_mins:
            mock_mins.return_value = 5.0  # 300 sec > 140
            from bot.market import get_15min_schedule_state
            run, sleep = get_15min_schedule_state(["btc"], late_window_seconds=140)
        self.assertFalse(run)
        self.assertGreater(sleep, 0)

    def test_15min_inside_late_window_should_run(self):
        with patch("bot.market.get_minutes_to_close_15min") as mock_mins:
            mock_mins.return_value = 2.0  # 120 sec <= 140
            from bot.market import get_15min_schedule_state
            run, _ = get_15min_schedule_state(["btc"], late_window_seconds=140)
        self.assertTrue(run)


# ---------------------------------------------------------------------------
# Risk guards: daily/weekly no_new_entry_cutoff
# ---------------------------------------------------------------------------

class TestRiskGuardsDailyWeekly(unittest.TestCase):
    """Daily 4h and weekly 1d cutoff."""

    def setUp(self):
        self.window_id = "TEST_DW_%s" % id(self)
        self.ticker = "TEST_TICKER"

    def tearDown(self):
        reset_window_on_expiry("daily", self.window_id)
        reset_window_on_expiry("weekly", self.window_id)

    def _daily_guards(self, cutoff_secs=14400):
        return {
            "enabled": True,
            "no_new_entry_cutoff_seconds": cutoff_secs,
            "persistence_polls": 1,
            "stop_once_disable": {"enabled": False},
            "recent_cross": {"enabled": False},
            "distance_buffer": {"enabled": False},
        }

    def test_daily_blocks_when_inside_4h_cutoff(self):
        # 3 hours = 10800 sec < 14400
        allowed, reason, _ = gate_allow_entry(
            interval="daily",
            window_id=self.window_id,
            asset="btc",
            ticker=self.ticker,
            side="yes",
            yes_price=95,
            no_price=5,
            spot=100000.0,
            strike=99900.0,
            seconds_to_close=10800.0,  # 3h
            entry_band="94-99",
            guards_cfg=self._daily_guards(14400),
            is_hourly=False,
        )
        self.assertFalse(allowed)
        self.assertIn("cutoff", reason.lower())

    def test_daily_allows_when_outside_4h_cutoff(self):
        allowed, _, payload = gate_allow_entry(
            interval="daily",
            window_id=self.window_id,
            asset="btc",
            ticker=self.ticker + "_OK",
            side="yes",
            yes_price=95,
            no_price=5,
            spot=100000.0,
            strike=99900.0,
            seconds_to_close=20000.0,  # > 4h
            entry_band="94-99",
            guards_cfg=self._daily_guards(14400),
            is_hourly=False,
        )
        self.assertTrue(allowed)
        self.assertEqual(payload.get("persistence_required_effective"), 1)

    def test_weekly_blocks_when_inside_1d_cutoff(self):
        allowed, reason, _ = gate_allow_entry(
            interval="weekly",
            window_id=self.window_id,
            asset="btc",
            ticker=self.ticker + "_W",
            side="yes",
            yes_price=95,
            no_price=5,
            spot=100000.0,
            strike=99900.0,
            seconds_to_close=43200.0,  # 12h < 86400 (1 day)
            entry_band="94-99",
            guards_cfg=self._daily_guards(86400),
            is_hourly=False,
        )
        self.assertFalse(allowed)
        self.assertIn("cutoff", reason.lower())


# ---------------------------------------------------------------------------
# Analysis events: required fields for logging/instrumentation
# ---------------------------------------------------------------------------

class TestAnalysisEvents(unittest.TestCase):
    """Ensure analysis events have fields needed for strategy tuning."""

    def test_build_analysis_event_has_required_fields(self):
        ev = build_analysis_event({
            "event": "ENTER_DECISION",
            "interval": "hourly",
            "window_id": "KXBTCD-26FEB1414",
            "asset": "btc",
            "ticker": "KXBTCD-26FEB1414-T69000",
            "side": "yes",
            "yes_price": 95,
            "no_price": 5,
            "spot": 69100,
            "strike": 69000,
            "distance": 100,
            "min_distance_required": 50,
            "streak": 2,
            "required_polls": 2,
            "seconds_to_close": 600,
        })
        self.assertEqual(ev["event_type"], "ENTER_DECISION")
        self.assertEqual(ev["interval"], "hourly")
        self.assertEqual(ev["window_id"], "KXBTCD-26FEB1414")
        self.assertEqual(ev["ticker"], "KXBTCD-26FEB1414-T69000")
        self.assertEqual(ev["side"], "yes")
        self.assertIn("ts", ev)
        self.assertIsNotNone(ev.get("yes_price_cents"))
        self.assertIsNotNone(ev.get("spot"))
        self.assertIsNotNone(ev.get("strike"))
        self.assertIsNotNone(ev.get("distance"))

    def test_build_exit_analysis_event_has_pnl_and_reason(self):
        ev = build_exit_analysis_event(
            interval="hourly",
            window_id="KXBTCD-26FEB1414",
            asset="btc",
            action="STOP_LOSS",
            ticker="KXBTCD-26FEB1414-T69000",
            side="yes",
            pnl_pct=-0.35,
            entry_price_cents=95,
            exit_price_cents=60,
        )
        self.assertEqual(ev["event_type"], "EXIT")
        self.assertEqual(ev["reason_code"], EXIT_STOPLOSS)
        self.assertEqual(ev["pnl_pct"], -0.35)
        self.assertEqual(ev["entry_price_cents"], 95)
        self.assertEqual(ev["exit_price_cents"], 60)

    def test_build_exit_analysis_event_take_profit(self):
        ev = build_exit_analysis_event(
            interval="daily",
            window_id="KXBTCD-26FEB2117",
            asset="btc",
            action="TAKE_PROFIT",
            ticker="KXBTCD-26FEB2117-T69000",
            side="yes",
            pnl_pct=0.12,
        )
        self.assertEqual(ev["reason_code"], EXIT_TAKE_PROFIT)
        self.assertEqual(ev["interval"], "daily")


# ---------------------------------------------------------------------------
# Daily/Weekly: event ticker generation
# ---------------------------------------------------------------------------

class TestDailyWeeklyEventTickers(unittest.TestCase):
    """Event ticker format for 5 PM ET closes."""

    def test_generate_daily_event_tickers_format(self):
        from bot.market_daily_weekly import _generate_daily_event_tickers
        et = pytz.timezone("US/Eastern")
        with patch("bot.market_daily_weekly.datetime") as mock_dt:
            mock_dt.now.return_value = et.localize(datetime(2026, 2, 16, 10, 0, 0))  # 10am ET
            evts = _generate_daily_event_tickers(["btc", "eth"], lookahead_hours=48)
        self.assertGreater(len(evts), 0)
        for evt, asset in evts:
            self.assertTrue(evt.startswith("KXBTCD-") or evt.startswith("KXETHD-"))
            self.assertIn("17", evt)  # 5 PM = hour 17

    def test_generate_weekly_event_tickers_fridays_only(self):
        from bot.market_daily_weekly import _generate_weekly_event_tickers
        et = pytz.timezone("US/Eastern")
        with patch("bot.market_daily_weekly.datetime") as mock_dt:
            mock_dt.now.return_value = et.localize(datetime(2026, 2, 16, 10, 0, 0))  # Mon Feb 16
            evts = _generate_weekly_event_tickers(["btc"], lookahead_days=14)
        self.assertGreater(len(evts), 0)
        for evt, _ in evts:
            self.assertTrue(evt.startswith("KXBTCD-"))


# ---------------------------------------------------------------------------
# apply_guards_filter for daily/weekly (integration-style)
# ---------------------------------------------------------------------------

class TestApplyGuardsFilterDailyWeekly(unittest.TestCase):
    """apply_guards_filter emits EVAL/SKIP/ENTER_DECISION with correct payload."""

    def setUp(self):
        self.window_id = "TEST_AG_%s" % id(self)

    def tearDown(self):
        reset_window_on_expiry("daily", self.window_id)

    def test_apply_guards_emit_enter_decision_when_all_pass(self):
        from bot.strategy import Signal
        signals = [Signal("T1", "yes", 95, "YES_BUY", False)]
        guards = {
            "enabled": True,
            "no_new_entry_cutoff_seconds": 20000,  # far from close
            "persistence_polls": 1,
            "stop_once_disable": {"enabled": False},
            "recent_cross": {"enabled": False},
            "distance_buffer": {"enabled": False},
        }
        allowed, logs = apply_guards_filter(
            signals, "daily", self.window_id, "btc", 100000.0,
            seconds_to_close=25000,
            ticker_to_strike={"T1": 99900.0},
            guards_cfg=guards,
            is_hourly=False,
            yes_price_all=95,
            no_price_all=5,
            entry_band="94-99",
        )
        self.assertTrue(allowed)
        self.assertGreater(len(logs), 0)
        enter_logs = [l for l in logs if l.get("event") == "ENTER_DECISION"]
        self.assertGreater(len(enter_logs), 0)
        self.assertEqual(enter_logs[0].get("interval"), "daily")


# ---------------------------------------------------------------------------
# Paper positions (daily/weekly OBSERVE exit evaluation)
# ---------------------------------------------------------------------------

class TestPaperPositions(unittest.TestCase):
    """Paper ledger for OBSERVE-mode daily/weekly exit evaluation."""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.tmp.name
        from bot.state import ensure_state_db
        ensure_state_db(self.db_path)

    def tearDown(self):
        import os
        try:
            os.unlink(self.db_path)
        except Exception:
            pass

    def test_add_get_remove_paper_position(self):
        from bot.state import add_paper_position, get_paper_positions, remove_paper_position
        add_paper_position(self.db_path, "KXBTCD-26FEB2117", "yes", 95, "daily", "btc")
        positions = get_paper_positions(self.db_path, "daily", "KXBTCD-26FEB2117")
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0]["ticker"], "KXBTCD-26FEB2117")
        self.assertEqual(positions[0]["side"], "yes")
        self.assertEqual(positions[0]["average_price"], 0.95)
        remove_paper_position(self.db_path, "KXBTCD-26FEB2117")
        positions = get_paper_positions(self.db_path, "daily", "KXBTCD-26FEB2117")
        self.assertEqual(len(positions), 0)

    def test_prune_stale_paper_positions(self):
        from bot.state import add_paper_position, get_paper_positions, prune_stale_paper_positions
        add_paper_position(self.db_path, "T-OLD", "yes", 90, "daily", "btc")
        add_paper_position(self.db_path, "T-NEW", "no", 10, "daily", "btc")
        prune_stale_paper_positions(self.db_path, "daily", ["T-NEW"])
        self.assertEqual(len(get_paper_positions(self.db_path, "daily", "T-OLD")), 0)
        self.assertEqual(len(get_paper_positions(self.db_path, "daily", "T-NEW")), 1)


if __name__ == "__main__":
    unittest.main()
