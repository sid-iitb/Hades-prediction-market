"""
Signal generation logic for the bot.
Generates YES_BUY, NO_BUY, YES_BUY_LATE, NO_BUY_LATE signals.
Farthest strategy: pick single ticker farthest from spot with YES or NO in threshold.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional

from bot.market import TickerQuote


@dataclass
class Signal:
    ticker: str
    side: str  # yes | no
    price: int
    reason: str  # YES_BUY, NO_BUY, YES_BUY_LATE, NO_BUY_LATE
    late_window: bool


def _in_range(price: Optional[int], lo: int, hi: int) -> bool:
    if price is None:
        return False
    return lo <= price <= hi


def generate_signals_farthest(
    quotes: List[TickerQuote],
    spot_price: Optional[float],
    ctx_late_window: bool,
    thresholds: Dict,
    pick_all_in_range: bool = False,
    min_bid_cents: Optional[int] = None,
    filter_side_by_spot_strike: bool = False,
) -> List[Signal]:
    """
    If pick_all_in_range is False (default): pick the single FARTHEST ticker with YES or NO in
    threshold; if both in range, choose by spot vs strike. Returns at most one Signal.

    If pick_all_in_range is True: return all tickers that have YES and/or NO in range. Each
    ticker can contribute one YES and/or one NO signal. Risk guards will filter later.
    When min_bid_cents is set, also qualify a side if its bid >= min_bid_cents (so we can
    place limit at 99 on the likely side for range markets where ask is low, e.g. NO when
    range is below spot). When filter_side_by_spot_strike is True (above/below markets only),
    emit only the direction-consistent side: strike > spot -> NO only, strike < spot -> YES only.
    Signals are ordered by distance descending (farthest first).
    """
    # Support both { normal: {...}, late: {...} } and flat { yes_min, yes_max, no_min, no_max } for both.
    band = thresholds.get("late") if ctx_late_window else thresholds.get("normal")
    if band is None:
        band = thresholds
    yes_lo = band.get("yes_min", 94)
    yes_hi = band.get("yes_max", 99)
    no_lo = band.get("no_min", 94)
    no_hi = band.get("no_max", 99)
    if ctx_late_window:
        yes_reason = "YES_BUY_LATE"
        no_reason = "NO_BUY_LATE"
    else:
        yes_reason = "YES_BUY"
        no_reason = "NO_BUY"

    def _bid_ok(side: str, q: TickerQuote) -> bool:
        if min_bid_cents is None or min_bid_cents <= 0:
            return False
        bid = (q.yes_bid if side == "yes" else q.no_bid) or 0
        return min_bid_cents <= bid <= 99

    spot = float(spot_price or 0.0)
    candidates = []
    for q in quotes:
        yes_qual = _in_range(q.yes_ask, yes_lo, yes_hi) or _bid_ok("yes", q)
        no_qual = _in_range(q.no_ask, no_lo, no_hi) or _bid_ok("no", q)
        if not yes_qual and not no_qual:
            continue
        distance = abs(float(q.strike or 0) - spot)
        candidates.append({
            "quote": q,
            "distance": distance,
            "yes_qual": yes_qual,
            "no_qual": no_qual,
        })

    if not candidates:
        return []

    # Sort by distance descending (farthest first)
    candidates.sort(key=lambda c: c["distance"], reverse=True)

    if pick_all_in_range:
        signals = []
        for c in candidates:
            q = c["quote"]
            strike = float(q.strike or 0)
            if filter_side_by_spot_strike:
                # Above/below markets only: emit only the side that matches spot vs strike.
                # strike < spot -> bet price stays above -> YES.  strike > spot -> bet price stays below -> NO.
                if strike > spot:
                    if c["no_qual"]:
                        price = (q.no_ask or 0) or 99
                        signals.append(Signal(ticker=q.ticker, side="no", price=price, reason=no_reason, late_window=ctx_late_window))
                elif strike < spot:
                    if c["yes_qual"]:
                        price = (q.yes_ask or 0) or 99
                        signals.append(Signal(ticker=q.ticker, side="yes", price=price, reason=yes_reason, late_window=ctx_late_window))
                else:
                    if c["yes_qual"] and (not c["no_qual"] or (q.yes_ask or 999) <= (q.no_ask or 999)):
                        price = (q.yes_ask or 0) or 99
                        signals.append(Signal(ticker=q.ticker, side="yes", price=price, reason=yes_reason, late_window=ctx_late_window))
                    elif c["no_qual"]:
                        price = (q.no_ask or 0) or 99
                        signals.append(Signal(ticker=q.ticker, side="no", price=price, reason=no_reason, late_window=ctx_late_window))
            else:
                # Range (B) markets: YES if spot in [range_low, range_high], NO otherwise.
                range_low = getattr(q, "range_low", None)
                range_high = getattr(q, "range_high", None)
                if range_low is not None and range_high is not None:
                    spot_in_range = range_low <= spot <= range_high
                    if spot_in_range and c["yes_qual"]:
                        price = (q.yes_ask or 0) or 99
                        signals.append(Signal(ticker=q.ticker, side="yes", price=price, reason=yes_reason, late_window=ctx_late_window))
                    elif not spot_in_range and c["no_qual"]:
                        price = (q.no_ask or 0) or 99
                        signals.append(Signal(ticker=q.ticker, side="no", price=price, reason=no_reason, late_window=ctx_late_window))
                else:
                    # No range bounds: emit all qualified sides (e.g. legacy or non-B tickers).
                    if c["yes_qual"]:
                        price = (q.yes_ask or 0) or 99
                        signals.append(Signal(ticker=q.ticker, side="yes", price=price, reason=yes_reason, late_window=ctx_late_window))
                    if c["no_qual"]:
                        price = (q.no_ask or 0) or 99
                        signals.append(Signal(ticker=q.ticker, side="no", price=price, reason=no_reason, late_window=ctx_late_window))
        return signals

    # Original behavior: single farthest signal
    best = candidates[0]
    q = best["quote"]
    yes_qual = best["yes_qual"]
    no_qual = best["no_qual"]

    if yes_qual and no_qual:
        if q.yes_ask == q.no_ask:
            return []
        # Use spot vs strike for directional choice: strike > spot -> NO, strike < spot -> YES
        strike = float(q.strike or 0)
        if strike > spot:
            side = "no"   # Spot below strike: bet price stays below (NO)
        elif strike < spot:
            side = "yes"  # Spot above strike: bet price stays above (YES)
        else:
            side = "yes" if (q.yes_ask or 999) < (q.no_ask or 999) else "no"
    elif yes_qual:
        side = "yes"
    else:
        side = "no"

    price = (q.yes_ask if side == "yes" else q.no_ask) or 0
    reason = yes_reason if side == "yes" else no_reason
    return [Signal(ticker=q.ticker, side=side, price=price, reason=reason, late_window=ctx_late_window)]


def generate_signals(
    quotes: List[TickerQuote],
    ctx_late_window: bool,
    thresholds: Dict,
) -> List[Signal]:
    """
    Generate signals for all quotes.
    If both YES and NO qualify, pick the cheaper side. If equal, skip (AMBIGUOUS_BOTH_SIDES).
    """
    band = thresholds.get("late") if ctx_late_window else thresholds.get("normal")
    if band is None:
        band = thresholds
    yes_lo = band.get("yes_min", 94)
    yes_hi = band.get("yes_max", 99)
    no_lo = band.get("no_min", 94)
    no_hi = band.get("no_max", 99)
    if ctx_late_window:
        yes_reason = "YES_BUY_LATE"
        no_reason = "NO_BUY_LATE"
    else:
        yes_reason = "YES_BUY"
        no_reason = "NO_BUY"

    signals = []
    for q in quotes:
        yes_qual = _in_range(q.yes_ask, yes_lo, yes_hi)
        no_qual = _in_range(q.no_ask, no_lo, no_hi)
        if yes_qual and no_qual:
            if q.yes_ask == q.no_ask:
                continue  # AMBIGUOUS_BOTH_SIDES - skip, don't add to signals
            if (q.yes_ask or 999) < (q.no_ask or 999):
                signals.append(Signal(
                    ticker=q.ticker,
                    side="yes",
                    price=q.yes_ask or 0,
                    reason=yes_reason,
                    late_window=ctx_late_window,
                ))
            else:
                signals.append(Signal(
                    ticker=q.ticker,
                    side="no",
                    price=q.no_ask or 0,
                    reason=no_reason,
                    late_window=ctx_late_window,
                ))
        elif yes_qual:
            signals.append(Signal(
                ticker=q.ticker,
                side="yes",
                price=q.yes_ask or 0,
                reason=yes_reason,
                late_window=ctx_late_window,
            ))
        elif no_qual:
            signals.append(Signal(
                ticker=q.ticker,
                side="no",
                price=q.no_ask or 0,
                reason=no_reason,
                late_window=ctx_late_window,
            ))
    return signals
