"""
Unit tests for risk guards, including late_persistence_override.
"""
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bot.risk_guards import gate_allow_entry, reset_window_on_expiry


def _minimal_hourly_guards(late_override: bool = True):
    """Minimal hourly guards with late_persistence_override."""
    cfg = {
        "enabled": True,
        "no_new_entry_cutoff_seconds": 10,
        "persistence_polls": 2,
        "stop_once_disable": {"enabled": False},
        "recent_cross": {"enabled": False},
        "distance_buffer": {"enabled": False},
        "anchor_one_per_side": {"enabled": False},
    }
    if late_override:
        cfg["late_persistence_override"] = {
            "enabled": True,
            "seconds_to_close_lte": 300,
            "persistence_polls": 1,
        }
    return cfg


def _minimal_15min_guards(late_override: bool = True):
    """Minimal 15-min guards with late_persistence_override."""
    cfg = {
        "enabled": True,
        "no_new_entry_cutoff_seconds": 40,
        "persistence_polls": 2,
        "stop_once_disable": {"enabled": False},
        "recent_cross": {"enabled": False},
        "distance_buffer": {"enabled": False},
    }
    if late_override:
        cfg["late_persistence_override"] = {
            "enabled": True,
            "seconds_to_close_lte": 60,
            "persistence_polls": 1,
        }
    return cfg


class TestLatePersistenceOverride(unittest.TestCase):
    """Verify persistence_required_effective toggles at boundary."""

    def setUp(self):
        self.window_id = "TEST_WINDOW_%s" % id(self)
        self.ticker = "TEST_TICKER"
        self.entry_band = "94-99"

    def tearDown(self):
        reset_window_on_expiry("hourly", self.window_id)

    def test_hourly_at_boundary_300_passes(self):
        """Exactly seconds_to_close == 300: late override applies, effective=1, streak 1 >= 1 -> PASS."""
        guards = _minimal_hourly_guards()
        allowed, reason, payload = gate_allow_entry(
            interval="hourly",
            window_id=self.window_id,
            asset="btc",
            ticker=self.ticker,
            side="yes",
            yes_price=95,
            no_price=5,
            spot=100000.0,
            strike=99900.0,
            seconds_to_close=300.0,
            entry_band=self.entry_band,
            guards_cfg=guards,
            is_hourly=True,
        )
        self.assertTrue(allowed, "Expected PASS at boundary 300")
        self.assertEqual(payload.get("persistence_required_base"), 2)
        self.assertEqual(payload.get("persistence_required_effective"), 1)
        self.assertTrue(payload.get("late_persistence_applied"))

    def test_hourly_just_outside_boundary_skips(self):
        """seconds_to_close == 301: late override does NOT apply, effective=2, streak 1 < 2 -> SKIP_PERSISTENCE."""
        guards = _minimal_hourly_guards()
        allowed, reason, payload = gate_allow_entry(
            interval="hourly",
            window_id=self.window_id,
            asset="btc",
            ticker=self.ticker + "_B",
            side="yes",
            yes_price=95,
            no_price=5,
            spot=100000.0,
            strike=99900.0,
            seconds_to_close=301.0,
            entry_band=self.entry_band,
            guards_cfg=guards,
            is_hourly=True,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "persistence")
        self.assertEqual(payload.get("persistence_required_base"), 2)
        self.assertEqual(payload.get("persistence_required_effective"), 2)
        self.assertFalse(payload.get("late_persistence_applied"))

    def test_hourly_trade_skipped_outside_becomes_ok_inside(self):
        """Trade SKIP_PERSISTENCE at 301 becomes OK at 299 (late window)."""
        guards = _minimal_hourly_guards()
        # At 301: fails persistence (streak 1 < required 2)
        reset_window_on_expiry("hourly", self.window_id)
        allowed_301, _, _ = gate_allow_entry(
            interval="hourly",
            window_id=self.window_id,
            asset="btc",
            ticker=self.ticker + "_C",
            side="yes",
            yes_price=95,
            no_price=5,
            spot=100000.0,
            strike=99900.0,
            seconds_to_close=301.0,
            entry_band=self.entry_band,
            guards_cfg=guards,
            is_hourly=True,
        )
        self.assertFalse(allowed_301)
        # Same ticker now at 299: late override applies, effective=1, streak 1 >= 1 -> OK
        allowed_299, _, payload = gate_allow_entry(
            interval="hourly",
            window_id=self.window_id,
            asset="btc",
            ticker=self.ticker + "_C",
            side="yes",
            yes_price=95,
            no_price=5,
            spot=100000.0,
            strike=99900.0,
            seconds_to_close=299.0,
            entry_band=self.entry_band,
            guards_cfg=guards,
            is_hourly=True,
        )
        self.assertTrue(allowed_299, "Trade that was SKIP_PERSISTENCE at 301 should become OK at 299")
        self.assertEqual(payload.get("persistence_required_effective"), 1)
        self.assertTrue(payload.get("late_persistence_applied"))

    def test_fifteen_min_boundary_60(self):
        """15-min: exactly seconds_to_close == 60 -> late override, effective=1."""
        reset_window_on_expiry("15min", self.window_id)
        guards = _minimal_15min_guards()
        allowed, _, payload = gate_allow_entry(
            interval="15min",
            window_id=self.window_id,
            asset="eth",
            ticker=self.ticker + "_15m",
            side="yes",
            yes_price=95,
            no_price=5,
            spot=4000.0,
            strike=3990.0,
            seconds_to_close=60.0,
            entry_band=self.entry_band,
            guards_cfg=guards,
            is_hourly=False,
        )
        self.assertTrue(allowed)
        self.assertEqual(payload.get("persistence_required_effective"), 1)
        self.assertTrue(payload.get("late_persistence_applied"))

    def test_fifteen_min_61_no_override(self):
        """15-min: seconds_to_close == 61 -> no override, effective=2, streak 1 -> SKIP."""
        reset_window_on_expiry("15min", self.window_id)
        guards = _minimal_15min_guards()
        allowed, reason, payload = gate_allow_entry(
            interval="15min",
            window_id=self.window_id,
            asset="eth",
            ticker=self.ticker + "_15m61",
            side="yes",
            yes_price=95,
            no_price=5,
            spot=4000.0,
            strike=3990.0,
            seconds_to_close=61.0,
            entry_band=self.entry_band,
            guards_cfg=guards,
            is_hourly=False,
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "persistence")
        self.assertEqual(payload.get("persistence_required_effective"), 2)
        self.assertFalse(payload.get("late_persistence_applied"))


if __name__ == "__main__":
    unittest.main()
