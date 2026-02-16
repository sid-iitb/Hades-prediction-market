"""
15-min crypto strategy. Trade only in the last N minutes.
- hourly_style: same as hourly - buy YES if 94-99c, NO if 94-99c (pick cheaper if both).
- lopsided_2min: buy cheap side (1-11c) when opposite is expensive (90-99c).
"""
from dataclasses import dataclass
from typing import Dict, List, Optional

from bot.market import TickerQuote


@dataclass
class Signal15min:
    ticker: str
    side: str  # yes | no
    price: int
    reason: str  # YES_BUY_15M_LOPSIDED, NO_BUY_15M_LOPSIDED
    late_window: bool  # True when in configured late window


def _in_range(price: Optional[int], lo: int, hi: int) -> bool:
    if price is None:
        return False
    return lo <= price <= hi


def _generate_signals_hourly_style(
    quote: TickerQuote,
    fm: Dict,
) -> List[Signal15min]:
    """Hourly-style: YES/NO 94-99c, pick cheaper if both qualify."""
    yes_lo = fm.get("yes_min", 94)
    yes_hi = fm.get("yes_max", 99)
    no_lo = fm.get("no_min", 94)
    no_hi = fm.get("no_max", 99)

    yes_qual = _in_range(quote.yes_ask, yes_lo, yes_hi)
    no_qual = _in_range(quote.no_ask, no_lo, no_hi)

    if yes_qual and no_qual:
        if quote.yes_ask == quote.no_ask:
            return []
        if (quote.yes_ask or 999) < (quote.no_ask or 999):
            return [Signal15min(
                ticker=quote.ticker, side="yes", price=quote.yes_ask or 0,
                reason="YES_BUY_15M", late_window=True,
            )]
        return [Signal15min(
            ticker=quote.ticker, side="no", price=quote.no_ask or 0,
            reason="NO_BUY_15M", late_window=True,
        )]
    elif yes_qual:
        return [Signal15min(
            ticker=quote.ticker, side="yes", price=quote.yes_ask or 0,
            reason="YES_BUY_15M", late_window=True,
        )]
    elif no_qual:
        return [Signal15min(
            ticker=quote.ticker, side="no", price=quote.no_ask or 0,
            reason="NO_BUY_15M", late_window=True,
        )]
    return []


def _generate_signals_lopsided(
    quote: TickerQuote,
    fm: Dict,
) -> List[Signal15min]:
    """Lopsided: buy cheap (1-11c) when opposite is expensive (90-99c)."""
    cheap_min = fm.get("cheap_side_min_cents", 1)
    cheap_max = fm.get("cheap_side_max_cents", 11)
    expensive_min = fm.get("expensive_side_min_cents", 90)
    expensive_max = fm.get("expensive_side_max_cents", 99)

    if _in_range(quote.yes_ask, cheap_min, cheap_max) and _in_range(quote.no_ask, expensive_min, expensive_max):
        return [Signal15min(
            ticker=quote.ticker, side="yes", price=quote.yes_ask or 0,
            reason="YES_BUY_15M_LOPSIDED", late_window=True,
        )]
    if _in_range(quote.no_ask, cheap_min, cheap_max) and _in_range(quote.yes_ask, expensive_min, expensive_max):
        return [Signal15min(
            ticker=quote.ticker, side="no", price=quote.no_ask or 0,
            reason="NO_BUY_15M_LOPSIDED", late_window=True,
        )]
    return []


def generate_signals_15min(
    quote: Optional[TickerQuote],
    minutes_to_close: float,
    config: Dict,
) -> List[Signal15min]:
    """
    Generate signals for 15-min binary market.
    Only trades when seconds_to_close <= late_window_seconds.
    """
    fm = config.get("fifteen_min", {}) or {}
    late_window_seconds = fm.get("late_window_seconds", 140)
    strategy = fm.get("strategy", "hourly_style")

    if quote is None:
        return []

    seconds_to_close = minutes_to_close * 60.0
    if seconds_to_close > late_window_seconds:
        return []

    if str(strategy).lower() == "lopsided_2min":
        return _generate_signals_lopsided(quote, fm)
    return _generate_signals_hourly_style(quote, fm)


def get_no_signal_reason(
    quote: Optional[TickerQuote],
    minutes_to_close: float,
    config: Dict,
) -> str:
    """Explain why no signal was generated (for logging)."""
    fm = config.get("fifteen_min", {}) or {}
    late_window_seconds = fm.get("late_window_seconds", 140)
    strategy = fm.get("strategy", "hourly_style")

    yes_str = f"{quote.yes_ask}c" if quote and quote.yes_ask is not None else "None"
    no_str = f"{quote.no_ask}c" if quote and quote.no_ask is not None else "None"

    if quote is None:
        return f"YES={yes_str} NO={no_str} | No market data"
    seconds_to_close = minutes_to_close * 60.0
    if seconds_to_close > late_window_seconds:
        return f"YES={yes_str} NO={no_str} | Outside late window (%.0fs > %ds)" % (seconds_to_close, late_window_seconds)
    if str(strategy).lower() == "lopsided_2min":
        cheap_min = fm.get("cheap_side_min_cents", 1)
        cheap_max = fm.get("cheap_side_max_cents", 11)
        expensive_min = fm.get("expensive_side_min_cents", 90)
        expensive_max = fm.get("expensive_side_max_cents", 99)
        return f"YES={yes_str} NO={no_str} | In late window but not lopsided (need cheap %d-%dc, expensive %d-%dc)" % (
            cheap_min, cheap_max, expensive_min, expensive_max
        )
    yes_lo = fm.get("yes_min", 94)
    yes_hi = fm.get("yes_max", 99)
    no_lo = fm.get("no_min", 94)
    no_hi = fm.get("no_max", 99)
    return f"YES={yes_str} NO={no_str} | In late window but neither in range (need yes %d-%dc or no %d-%dc)" % (
        yes_lo, yes_hi, no_lo, no_hi
    )
