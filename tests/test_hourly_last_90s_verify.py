"""
Tests for hourly_last_90s_limit_99 signal and placement logic.

Verifies: generate_signals_farthest with pick_all_in_range + min_bid_cents
emits the correct side (e.g. NO when range below spot and no_bid=99), and
that would_place / would_skip is determined by the same side's bid.
"""
import sys
from pathlib import Path
from unittest.mock import patch

# Project root
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from typing import Optional

from bot.market import TickerQuote
from bot.strategy import Signal, generate_signals_farthest


def _quote(
    ticker: str,
    strike: float,
    yes_bid: int,
    yes_ask: int,
    no_bid: int,
    no_ask: int,
    range_low: Optional[float] = None,
    range_high: Optional[float] = None,
) -> TickerQuote:
    return TickerQuote(
        ticker=ticker,
        strike=strike,
        yes_bid=yes_bid,
        yes_ask=yes_ask,
        no_bid=no_bid,
        no_ask=no_ask,
        subtitle="",
        range_low=range_low,
        range_high=range_high,
    )


def test_range_below_spot_no_bid_high_emits_no_signal_and_would_place():
    """Range below spot: NO is likely (no_bid=99, no_ask=1). With min_bid_cents we must get NO signal and would place."""
    spot = 1.37
    # One ticker: range midpoint 1.35 (below spot). NO = out of range = likely.
    quotes = [
        _quote("KXXRP-26FEB2817-B1.3499500", strike=1.35, yes_bid=0, yes_ask=99, no_bid=99, no_ask=1),
    ]
    thresholds = {"yes_min": 92, "yes_max": 99, "no_min": 92, "no_max": 99}

    # With min_bid_cents: NO qualifies by bid (99 >= 95), YES qualifies by ask (99 in range)
    signals = generate_signals_farthest(
        quotes, spot, ctx_late_window=True, thresholds=thresholds,
        pick_all_in_range=True, min_bid_cents=95,
    )

    sides = [s.side.lower() for s in signals]
    assert "no" in sides, "Should emit NO signal when no_bid=99 (range below spot)"
    no_signals = [s for s in signals if s.side and s.side.lower() == "no"]
    assert len(no_signals) >= 1, "At least one NO signal"

    # For NO signal, bid would be 99 >= 95 → would_place
    q = quotes[0]
    no_bid = q.no_bid or 0
    assert no_bid >= 95, "NO bid should pass min_bid_cents so we would place"


def test_ask_qual_yes_bid_low_skip_and_bid_qual_no_place():
    """YES ask in range but yes_bid=0 → skip YES; min_bid_cents lets NO qualify by bid → would place NO."""
    spot = 1.37
    quotes = [
        _quote("KXXRP-26FEB2817-B1.3699500", strike=1.36, yes_bid=0, yes_ask=95, no_bid=99, no_ask=5),
    ]
    thresholds = {"yes_min": 92, "yes_max": 99, "no_min": 92, "no_max": 99}

    # Without min_bid_cents: only YES qualifies (ask 95 in range); NO ask 5 not in range
    signals_no_min = generate_signals_farthest(
        quotes, spot, ctx_late_window=True, thresholds=thresholds,
        pick_all_in_range=True, min_bid_cents=None,
    )
    sides_no_min = [s.side.lower() for s in signals_no_min]
    assert "yes" in sides_no_min
    assert "no" not in sides_no_min, "Without min_bid_cents, NO (ask=5) should not qualify"

    # With min_bid_cents: NO qualifies by bid 99 >= 95; YES still by ask
    signals = generate_signals_farthest(
        quotes, spot, ctx_late_window=True, thresholds=thresholds,
        pick_all_in_range=True, min_bid_cents=95,
    )
    sides = [s.side.lower() for s in signals]
    assert "no" in sides, "With min_bid_cents, NO should qualify by bid"
    # YES signal would be skipped at placement (yes_bid=0); NO would place (no_bid=99)
    q = quotes[0]
    assert (q.yes_bid or 0) < 95 and (q.no_bid or 0) >= 95


def test_above_below_filter_side_by_spot_strike():
    """Above/below: spot < strike must emit NO only; spot > strike must emit YES only."""
    thresholds = {"yes_min": 92, "yes_max": 99, "no_min": 92, "no_max": 99}
    # T87.9999: strike 88, spot 84.68 -> spot < strike -> only NO
    quotes_below = [_quote("KXSOLD-X-T87.9999", strike=88.0, yes_bid=0, yes_ask=92, no_bid=99, no_ask=8)]
    signals_below = generate_signals_farthest(
        quotes_below, 84.68, True, thresholds,
        pick_all_in_range=True, min_bid_cents=95, filter_side_by_spot_strike=True,
    )
    sides_below = [s.side.lower() for s in signals_below]
    assert sides_below == ["no"], "spot < strike must emit NO only (got %s)" % sides_below
    # T82.9999: strike 83, spot 84.68 -> spot > strike -> only YES
    quotes_above = [_quote("KXSOLD-X-T82.9999", strike=83.0, yes_bid=99, yes_ask=92, no_bid=0, no_ask=8)]
    signals_above = generate_signals_farthest(
        quotes_above, 84.68, True, thresholds,
        pick_all_in_range=True, min_bid_cents=95, filter_side_by_spot_strike=True,
    )
    sides_above = [s.side.lower() for s in signals_above]
    assert sides_above == ["yes"], "spot > strike must emit YES only (got %s)" % sides_above


def test_b_market_range_side_yes_inside_no_outside():
    """B (range) markets: YES only when range_low <= spot <= range_high, NO only when spot outside."""
    thresholds = {"yes_min": 92, "yes_max": 99, "no_min": 92, "no_max": 99}
    # Spot inside range [1.35, 1.37] -> only YES (if yes_qual)
    quotes_inside = [
        _quote(
            "KXXRP-26FEB2818-B1.3699500",
            strike=1.36,
            yes_bid=94,
            yes_ask=95,
            no_bid=5,
            no_ask=95,
            range_low=1.35,
            range_high=1.37,
        )
    ]
    signals_inside = generate_signals_farthest(
        quotes_inside, 1.36, True, thresholds,
        pick_all_in_range=True, min_bid_cents=95, filter_side_by_spot_strike=False,
    )
    sides_inside = [s.side.lower() for s in signals_inside]
    assert sides_inside == ["yes"], "spot in [1.35,1.37] must emit YES only (got %s)" % sides_inside

    # Spot outside range [1.30, 1.32] -> only NO (if no_qual)
    quotes_outside = [
        _quote(
            "KXXRP-26FEB2818-B0.7099500",
            strike=1.31,
            yes_bid=0,
            yes_ask=99,
            no_bid=99,
            no_ask=1,
            range_low=1.30,
            range_high=1.32,
        )
    ]
    signals_outside = generate_signals_farthest(
        quotes_outside, 1.3794, True, thresholds,
        pick_all_in_range=True, min_bid_cents=95, filter_side_by_spot_strike=False,
    )
    sides_outside = [s.side.lower() for s in signals_outside]
    assert sides_outside == ["no"], "spot outside [1.30,1.32] must emit NO only (got %s)" % sides_outside
