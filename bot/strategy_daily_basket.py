"""
Daily basket (Iron Condor) strategy: find YES+NO pairs per window_id for simultaneous entry.
Profit zone: 65-80¢ both legs. Exit on combined 10¢ profit or individual leg -35% stop loss.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from bot.market import TickerQuote


@dataclass
class BasketSignal:
    """Paired YES + NO signals for one window_id (Iron Condor)."""
    window_id: str
    asset: str
    yes_ticker: str
    no_ticker: str
    yes_strike: float
    no_strike: float
    yes_price: int
    no_price: int
    yes_bid: Optional[int]
    no_bid: Optional[int]
    minutes_to_close: float
    spot_price: Optional[float]
    yes_distance: Optional[float]
    no_distance: Optional[float]
    combined_spread_at_entry: Optional[float]


def _in_range(price: Optional[int], lo: int, hi: int) -> bool:
    if price is None:
        return False
    return lo <= price <= hi


def generate_basket_signals(
    markets: List[Dict[str, Any]],
    spot_price: Optional[float],
    thresholds: Dict,
    is_late: bool,
    kalshi_client=None,
) -> List[BasketSignal]:
    """
    Find YES (lower strike) + NO (higher strike) pairs per window_id in 65-80¢ zone.
    Farthest selection: YES = lowest strike below spot, NO = highest strike above spot.
    Both legs must be in range. Returns at most one basket per window_id.
    """
    band = thresholds.get("late") if is_late else thresholds.get("normal")
    if band is None:
        band = thresholds
    yes_lo = band.get("yes_min", 65)
    yes_hi = band.get("yes_max", 80)
    no_lo = band.get("no_min", 65)
    no_hi = band.get("no_max", 80)

    by_window: Dict[str, List[Dict]] = {}
    for m in markets:
        wid = m.get("window_id") or m.get("event_ticker") or ""
        if not wid:
            continue
        if wid not in by_window:
            by_window[wid] = []
        by_window[wid].append(m)

    signals: List[BasketSignal] = []
    spot = float(spot_price or 0.0)

    for window_id, ms in by_window.items():
        if not ms:
            continue
        asset = str(ms[0].get("asset", "")).lower()

        # Sort by strike: lower strike = YES candidate (bet above), higher = NO (bet below)
        ms_sorted = sorted(ms, key=lambda x: float(x.get("strike") or 0))

        yes_cand = None
        no_cand = None
        for m in ms_sorted:
            strike = float(m.get("strike") or 0)
            yes_ask = m.get("yes_ask")
            no_ask = m.get("no_ask")
            try:
                yes_ask = int(yes_ask) if yes_ask is not None else None
            except (TypeError, ValueError):
                yes_ask = None
            try:
                no_ask = int(no_ask) if no_ask is not None else None
            except (TypeError, ValueError):
                no_ask = None

            if strike > 0 and spot > 0:
                # Farthest: YES = lowest strike below spot, NO = highest strike above spot
                if strike < spot and _in_range(yes_ask, yes_lo, yes_hi):
                    if yes_cand is None or strike < float(yes_cand.get("strike") or 999999):
                        yes_cand = {**m, "yes_ask": yes_ask, "no_ask": no_ask}
                elif strike > spot and _in_range(no_ask, no_lo, no_hi):
                    if no_cand is None or strike > float(no_cand.get("strike") or 0):
                        no_cand = {**m, "yes_ask": yes_ask, "no_ask": no_ask}

        if yes_cand is None or no_cand is None:
            continue

        yes_strike = float(yes_cand.get("strike") or 0)
        no_strike = float(no_cand.get("strike") or 0)
        if yes_strike >= no_strike:
            continue

        yes_price = yes_cand.get("yes_ask") or 0
        no_price = no_cand.get("no_ask") or 0
        yes_bid = yes_cand.get("yes_bid")
        no_bid = no_cand.get("no_bid")
        try:
            yes_bid = int(yes_bid) if yes_bid is not None else None
        except (TypeError, ValueError):
            yes_bid = None
        try:
            no_bid = int(no_bid) if no_bid is not None else None
        except (TypeError, ValueError):
            no_bid = None

        yes_dist = spot - yes_strike if spot > 0 else None
        no_dist = no_strike - spot if spot > 0 else None

        yes_ask_impl = 100 - no_bid if no_bid is not None else None
        no_ask_impl = 100 - yes_bid if yes_bid is not None else None
        spread_yes = (yes_ask_impl - yes_bid) if (yes_ask_impl is not None and yes_bid is not None) else 0
        spread_no = (no_ask_impl - no_bid) if (no_ask_impl is not None and no_bid is not None) else 0
        combined_spread = spread_yes + spread_no if (yes_bid is not None and no_bid is not None) else None

        mins = float(yes_cand.get("minutes_to_close") or 999)
        signals.append(BasketSignal(
            window_id=window_id,
            asset=asset,
            yes_ticker=yes_cand.get("ticker", ""),
            no_ticker=no_cand.get("ticker", ""),
            yes_strike=yes_strike,
            no_strike=no_strike,
            yes_price=int(yes_price),
            no_price=int(no_price),
            yes_bid=yes_bid,
            no_bid=no_bid,
            minutes_to_close=mins,
            spot_price=spot_price,
            yes_distance=yes_dist,
            no_distance=no_dist,
            combined_spread_at_entry=combined_spread,
        ))

    return signals


def gate_allow_basket_entry(
    basket: BasketSignal,
    seconds_to_close: float,
    guards_cfg: dict,
) -> Tuple[bool, str]:
    """
    Check if basket entry passes risk guards. Returns (allowed, reason_code).
    """
    if not guards_cfg.get("enabled", True):
        return (True, "PASS")
    cutoff_secs = guards_cfg.get("no_new_entry_cutoff_seconds")
    if cutoff_secs is not None and seconds_to_close <= cutoff_secs:
        return (False, "SKIP_NO_NEW_ENTRY_CUTOFF")
    dist_cfg = guards_cfg.get("distance_buffer") or {}
    if dist_cfg.get("enabled"):
        assets_cfg = dist_cfg.get("assets") or {}
        ac = assets_cfg.get(basket.asset.lower()) or {}
        pct = ac.get("pct") or 0.0075
        floor = ac.get("floor_usd") or 500
        min_dist = max(floor, (basket.spot_price or 0) * pct)
        yes_dist = basket.yes_distance if basket.yes_distance is not None and basket.yes_distance >= 0 else 0
        no_dist = basket.no_distance if basket.no_distance is not None and basket.no_distance >= 0 else 0
        if yes_dist < min_dist or no_dist < min_dist:
            return (False, "SKIP_DISTANCE_BUFFER")
    return (True, "PASS")


def evaluate_basket_exit(
    yes_ticker: str,
    no_ticker: str,
    yes_entry_cents: int,
    no_entry_cents: int,
    yes_bid: Optional[int],
    no_bid: Optional[int],
    take_profit_pct: float,
    stop_loss_pct: float,
    yes_position_pnl_pct: Optional[float],
    no_position_pnl_pct: Optional[float],
    panic_stop_loss_pct: Optional[float] = None,
) -> Tuple[str, bool]:
    """
    Returns (action, sell_both).
    action: "BASKET_TAKE_PROFIT" | "YES_LEG_STOPLOSS" | "NO_LEG_STOPLOSS" | "HOLD"
    sell_both: True if both legs should be sold.
    """
    panic = panic_stop_loss_pct or 0.60
    entry_cost = yes_entry_cents + no_entry_cents
    exit_value = (yes_bid or 0) + (no_bid or 0)
    net_cents = exit_value - entry_cost
    net_profit_pct = (net_cents / entry_cost) if entry_cost > 0 else 0.0

    if net_profit_pct >= take_profit_pct:
        return ("BASKET_TAKE_PROFIT", True)

    yes_sl = (yes_position_pnl_pct or 0) <= -stop_loss_pct
    no_sl = (no_position_pnl_pct or 0) <= -stop_loss_pct
    yes_panic = (yes_position_pnl_pct or 0) <= -panic
    no_panic = (no_position_pnl_pct or 0) <= -panic

    if yes_panic or no_panic:
        return ("YES_LEG_STOPLOSS" if yes_panic else "NO_LEG_STOPLOSS", True)
    if yes_sl:
        return ("YES_LEG_STOPLOSS", False)
    if no_sl:
        return ("NO_LEG_STOPLOSS", False)
    return ("HOLD", False)
