"""
Tests for strategy report summary consistency.

Ensures: filled <= placed, wins+losses+stop_losses <= filled,
and (for canonical data) skipped + placed == total signals.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

# Project root
TESTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TESTS_DIR.parent
if str(PROJECT_ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(PROJECT_ROOT))


def _summary_for_rows(rows: list) -> dict:
    """Mirror of strategy_report_hourly_email._summary_for_rows."""
    n = len(rows)
    if n == 0:
        return {
            "signals": 0,
            "skipped": 0,
            "placed": 0,
            "filled": 0,
            "wins": 0,
            "losses": 0,
            "stop_losses": 0,
        }
    skipped = sum(1 for r in rows if (r.get("skip_reason") or "").strip())
    placed = sum(1 for r in rows if r.get("placed"))
    filled = sum(1 for r in rows if r.get("filled"))
    wins = sum(1 for r in rows if (r.get("final_outcome") or "").strip().upper() == "WIN")
    losses = sum(1 for r in rows if (r.get("final_outcome") or "").strip().upper() == "LOSS")
    stop_losses = sum(1 for r in rows if (r.get("final_outcome") or "").strip().upper() == "STOP_LOSS")
    return {
        "signals": n,
        "skipped": skipped,
        "placed": placed,
        "filled": filled,
        "wins": wins,
        "losses": losses,
        "stop_losses": stop_losses,
    }


class TestStrategyReportSummaryConsistency(unittest.TestCase):
    def test_filled_le_placed(self):
        from bot.strategy_report_db import get_all_rows
        db_path = os.environ.get("STRATEGY_REPORT_TEST_DB") or str(PROJECT_ROOT / "data" / "bot_state.db")
        if not Path(db_path).exists():
            self.skipTest("bot_state.db not found")
        for name in ("last_90s_limit_99", "hourly_last_90s_limit_99"):
            rows = get_all_rows(db_path, strategy_name=name, since_ts_utc=None)
            s = _summary_for_rows(rows)
            self.assertLessEqual(
                s["filled"],
                s["placed"],
                f"{name}: filled ({s['filled']}) must be <= placed ({s['placed']})",
            )

    def test_resolved_le_filled(self):
        from bot.strategy_report_db import get_all_rows
        db_path = os.environ.get("STRATEGY_REPORT_TEST_DB") or str(PROJECT_ROOT / "data" / "bot_state.db")
        if not Path(db_path).exists():
            self.skipTest("bot_state.db not found")
        for name in ("last_90s_limit_99", "hourly_last_90s_limit_99"):
            rows = get_all_rows(db_path, strategy_name=name, since_ts_utc=None)
            s = _summary_for_rows(rows)
            resolved = s["wins"] + s["losses"] + s["stop_losses"]
            self.assertLessEqual(
                resolved,
                s["filled"],
                f"{name}: wins+losses+stop_losses ({resolved}) must be <= filled ({s['filled']})",
            )

    def test_skipped_plus_placed_equals_total(self):
        """Each row should be either skipped or placed (canonical bot behavior)."""
        from bot.strategy_report_db import get_all_rows
        db_path = os.environ.get("STRATEGY_REPORT_TEST_DB") or str(PROJECT_ROOT / "data" / "bot_state.db")
        if not Path(db_path).exists():
            self.skipTest("bot_state.db not found")
        for name in ("last_90s_limit_99", "hourly_last_90s_limit_99"):
            rows = get_all_rows(db_path, strategy_name=name, since_ts_utc=None)
            if not rows:
                continue
            s = _summary_for_rows(rows)
            # Each row has either skip_reason set (skipped) or placed=1
            self.assertEqual(
                s["skipped"] + s["placed"],
                s["signals"],
                f"{name}: skipped + placed should equal total signals (skipped={s['skipped']}, placed={s['placed']}, signals={s['signals']})",
            )

    def test_summary_consistency_synthetic(self):
        """Consistency rules on synthetic rows."""
        # Filled <= placed
        rows = [
            {"placed": 1, "filled": 1, "skip_reason": None, "final_outcome": "WIN"},
            {"placed": 1, "filled": 0, "skip_reason": None, "final_outcome": None},
        ]
        s = _summary_for_rows(rows)
        self.assertEqual(s["signals"], 2)
        self.assertEqual(s["placed"], 2)
        self.assertEqual(s["filled"], 1)
        self.assertLessEqual(s["filled"], s["placed"])
        self.assertEqual(s["wins"], 1)
        self.assertLessEqual(s["wins"] + s["losses"] + s["stop_losses"], s["filled"])

        # Skipped + placed = total
        rows2 = [
            {"placed": 0, "filled": 0, "skip_reason": "min_bid", "final_outcome": None},
            {"placed": 1, "filled": 0, "skip_reason": None, "final_outcome": None},
        ]
        s2 = _summary_for_rows(rows2)
        self.assertEqual(s2["skipped"] + s2["placed"], s2["signals"])


if __name__ == "__main__":
    unittest.main()
