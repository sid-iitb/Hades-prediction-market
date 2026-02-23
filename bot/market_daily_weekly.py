"""
Daily and weekly market discovery via Kalshi API (getMarkets).
Uses event-ticker-based discovery: Kalshi daily closes at 5 PM ET, weekly on Friday 5 PM ET.
Event format: KXBTCD-YYMMMDDHH, KXETHD-YYMMMDDHH (HH=17 for 5 PM ET).
The close-window API returns non-crypto markets first, so we generate expected event tickers
and fetch by event_ticker directly.
"""
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz

from bot.market import (
    extract_strike_from_market,
    fetch_markets_for_event,
)

ET_TZ = pytz.timezone("US/Eastern")
UTC = pytz.UTC

# Kalshi daily/weekly: 5 PM ET close. Event format: KXBTCD-YYMMMDDHH (26FEB1617 = Feb 16, 2026, 5 PM ET)
DAILY_CLOSE_HOUR_ET = 17  # 5 PM ET
EVENT_PREFIX_BY_ASSET = {"btc": "KXBTCD", "eth": "KXETHD"}


def _ts_from_close_time(close_time: str) -> Optional[float]:
    """Parse close_time ISO string to epoch seconds. Returns None if invalid."""
    if not close_time:
        return None
    try:
        s = str(close_time)[:26].replace("Z", "+00:00")
        if "T" in s and "+" not in s and "-" not in s[10:]:
            s = s + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.timestamp()
    except Exception:
        return None


def _minutes_to_close_from_market(m: Dict[str, Any]) -> float:
    """Minutes until market close from market close_time."""
    close_time = m.get("close_time")
    ts = _ts_from_close_time(close_time) if close_time else None
    if ts is None:
        return 999.0
    now = datetime.now(UTC).timestamp()
    delta = ts - now
    return max(0.0, min(999.0, delta / 60.0))


def _generate_event_ticker(close_dt: datetime, asset: str) -> str:
    """Generate Kalshi event ticker for a 5 PM ET close. Format: KXBTCD-YYMMMDDHH."""
    et_dt = close_dt.astimezone(ET_TZ) if close_dt.tzinfo else ET_TZ.localize(close_dt)
    fmt = et_dt.strftime("%y%b%d%H").upper()  # e.g. 26FEB1617
    prefix = EVENT_PREFIX_BY_ASSET.get(str(asset).lower())
    return f"{prefix}-{fmt}" if prefix else ""


def _generate_daily_event_tickers(
    assets: List[str], lookahead_hours: float
) -> List[Tuple[str, str]]:
    """Generate (event_ticker, asset) for daily markets closing at 5 PM ET in the lookahead window."""
    now_et = datetime.now(ET_TZ)
    end_et = now_et + timedelta(hours=lookahead_hours)
    result: List[Tuple[str, str]] = []
    seen: set = set()
    # Walk each day; market closes at 5 PM ET
    dt = now_et.replace(hour=DAILY_CLOSE_HOUR_ET, minute=0, second=0, microsecond=0)
    if dt <= now_et:
        dt += timedelta(days=1)
    while dt <= end_et:
        for asset in assets:
            if str(asset).lower() not in ("btc", "eth"):
                continue
            evt = _generate_event_ticker(dt, asset)
            if evt and evt not in seen:
                seen.add(evt)
                result.append((evt, str(asset).lower()))
        dt += timedelta(days=1)
    return result


def _generate_weekly_event_tickers(
    assets: List[str], lookahead_days: float
) -> List[Tuple[str, str]]:
    """Generate (event_ticker, asset) for weekly markets (Friday 5 PM ET) in the lookahead window."""
    now_et = datetime.now(ET_TZ)
    end_et = now_et + timedelta(days=lookahead_days)
    result: List[Tuple[str, str]] = []
    seen: set = set()
    # Find next Friday at 5 PM ET
    dt = now_et.replace(hour=DAILY_CLOSE_HOUR_ET, minute=0, second=0, microsecond=0)
    days_until_friday = (4 - now_et.weekday()) % 7
    if days_until_friday == 0 and dt <= now_et:
        days_until_friday = 7
    dt += timedelta(days=days_until_friday)
    while dt <= end_et:
        for asset in assets:
            if str(asset).lower() not in ("btc", "eth"):
                continue
            evt = _generate_event_ticker(dt, asset)
            if evt and evt not in seen:
                seen.add(evt)
                result.append((evt, str(asset).lower()))
        dt += timedelta(days=7)
    return result


def _discover_markets_via_event_tickers(
    event_tickers: List[Tuple[str, str]],
    assets: List[str],
    spot_price_by_asset: Optional[Dict[str, float]],
    spot_window_by_asset: Optional[Dict[str, int]],
) -> List[Dict[str, Any]]:
    """Fetch markets for each event ticker, filter active, apply spot window."""
    result: List[Dict[str, Any]] = []
    for event_ticker, asset in event_tickers:
        markets, err = fetch_markets_for_event(event_ticker)
        if err or not markets:
            continue
        for m in markets:
            status = str(m.get("status", "")).lower()
            if status not in ("active", "open"):
                continue
            ticker = m.get("ticker")
            if not ticker:
                continue
            strike = extract_strike_from_market(m, ticker)
            if strike > 0 and spot_price_by_asset and spot_window_by_asset:
                spot = spot_price_by_asset.get(asset)
                window = spot_window_by_asset.get(asset) or 3000
                if spot is not None and abs(strike - spot) > window:
                    continue
            mins = _minutes_to_close_from_market(m)
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
            result.append({
                "ticker": ticker,
                "strike": strike,
                "yes_ask": yes_ask,
                "no_ask": no_ask,
                "yes_bid": m.get("yes_bid"),
                "no_bid": m.get("no_bid"),
                "subtitle": m.get("subtitle", ""),
                "minutes_to_close": mins,
                "asset": asset,
                "close_time": m.get("close_time"),
                "event_ticker": event_ticker,
                "window_id": event_ticker,
            })
    result.sort(key=lambda x: (x["minutes_to_close"], x["ticker"]))
    return result


def discover_daily_markets(
    assets: List[str],
    lookahead_hours: float = 48,
    series_ticker_prefix_by_asset: Optional[Dict[str, str]] = None,
    spot_price_by_asset: Optional[Dict[str, float]] = None,
    spot_window_by_asset: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    """
    Discover daily BTC/ETH markets closing at 5 PM ET within now..now+lookahead_hours.
    Uses event-ticker-based discovery (KXBTCD-YYMMMDD17, KXETHD-YYMMMDD17).
    Returns list of {ticker, strike, yes_ask, no_ask, subtitle, minutes_to_close, asset, close_time, ...}.
    """
    event_tickers = _generate_daily_event_tickers(assets, lookahead_hours)
    return _discover_markets_via_event_tickers(
        event_tickers, assets, spot_price_by_asset, spot_window_by_asset
    )


def get_daily_schedule_state(
    assets: List[str],
    late_window_minutes: float = 240,
    late_interval_minutes: float = 5,
    normal_interval_minutes: float = 30,
    lookahead_hours: float = 24,
    fallback_interval_minutes: float = 30,
) -> Tuple[bool, float]:
    """
    Two-speed schedule for daily: run every 30 min normally, every 5 min in last 4 hours.
    Returns (should_run, sleep_seconds).
    """
    markets = discover_daily_markets(assets, lookahead_hours=lookahead_hours)
    if not markets:
        return (False, fallback_interval_minutes * 60.0)
    min_mins = min(m.get("minutes_to_close", 999.0) for m in markets)
    if min_mins <= 0:
        return (False, fallback_interval_minutes * 60.0)
    if min_mins <= late_window_minutes:
        return (True, late_interval_minutes * 60.0)
    return (True, normal_interval_minutes * 60.0)


def discover_weekly_markets(
    assets: List[str],
    lookahead_days: float = 14,
    series_ticker_prefix_by_asset: Optional[Dict[str, str]] = None,
    spot_price_by_asset: Optional[Dict[str, float]] = None,
    spot_window_by_asset: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    """
    Discover weekly BTC/ETH markets (Friday 5 PM ET) within now..now+lookahead_days.
    Uses event-ticker-based discovery. Returns list of market dicts.
    """
    event_tickers = _generate_weekly_event_tickers(assets, lookahead_days)
    return _discover_markets_via_event_tickers(
        event_tickers, assets, spot_price_by_asset, spot_window_by_asset
    )
