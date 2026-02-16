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
) -> List[Signal]:
    """
    Pick the FARTHEST ticker (from spot) that has YES or NO in threshold.
    Trade YES or NO depending on what's in range; if both, pick cheaper.
    Returns at most one Signal.
    """
    if ctx_late_window:
        yes_lo = thresholds.get("late", {}).get("yes_min", 94)
        yes_hi = thresholds.get("late", {}).get("yes_max", 99)
        no_lo = thresholds.get("late", {}).get("no_min", 94)
        no_hi = thresholds.get("late", {}).get("no_max", 99)
        yes_reason = "YES_BUY_LATE"
        no_reason = "NO_BUY_LATE"
    else:
        yes_lo = thresholds.get("normal", {}).get("yes_min", 93)
        yes_hi = thresholds.get("normal", {}).get("yes_max", 98)
        no_lo = thresholds.get("normal", {}).get("no_min", 93)
        no_hi = thresholds.get("normal", {}).get("no_max", 98)
        yes_reason = "YES_BUY"
        no_reason = "NO_BUY"

    spot = float(spot_price or 0.0)
    candidates = []
    for q in quotes:
        yes_qual = _in_range(q.yes_ask, yes_lo, yes_hi)
        no_qual = _in_range(q.no_ask, no_lo, no_hi)
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
    best = candidates[0]
    q = best["quote"]
    yes_qual = best["yes_qual"]
    no_qual = best["no_qual"]

    if yes_qual and no_qual:
        if q.yes_ask == q.no_ask:
            return []
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
    if ctx_late_window:
        yes_lo = thresholds.get("late", {}).get("yes_min", 94)
        yes_hi = thresholds.get("late", {}).get("yes_max", 99)
        no_lo = thresholds.get("late", {}).get("no_min", 94)
        no_hi = thresholds.get("late", {}).get("no_max", 99)
        yes_reason = "YES_BUY_LATE"
        no_reason = "NO_BUY_LATE"
    else:
        yes_lo = thresholds.get("normal", {}).get("yes_min", 93)
        yes_hi = thresholds.get("normal", {}).get("yes_max", 98)
        no_lo = thresholds.get("normal", {}).get("no_min", 93)
        no_hi = thresholds.get("normal", {}).get("no_max", 98)
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
