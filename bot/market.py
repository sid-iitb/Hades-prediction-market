"""
Market discovery and data fetching for the bot.
Determines current hourly market, fetches tickers and orderbook.
"""
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests

from src.client.kalshi_client import KalshiClient
from src.offline_processing.generate_all_kalshi_urls import (
    ASSET_PREFIXES,
    ASSET_PREFIXES_15M,
    ASSET_PREFIXES_HOURLY_RANGE,
    generate_15min_slug,
    generate_kalshi_slug,
    generate_kalshi_slug_range,
)

KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
LA_TZ = pytz.timezone("America/Los_Angeles")
ET_TZ = pytz.timezone("US/Eastern")

VALID_PREFIXES = tuple(ASSET_PREFIXES.values())
# All hourly prefixes (above/below + range) for get_minutes_to_close validation
_valid_hourly_range = tuple(ASSET_PREFIXES_HOURLY_RANGE.values())
VALID_PREFIXES_HOURLY = tuple(set(VALID_PREFIXES) | set(_valid_hourly_range))
VALID_PREFIXES_15M = tuple(ASSET_PREFIXES_15M.values())


@dataclass
class MarketContext:
    """Current market context for a run."""
    hour_market_id: str
    event_ticker: str
    market_hour_la: str
    minutes_to_close: float
    is_late_window: bool


@dataclass
class TickerQuote:
    """Quote for a ticker (orderbook or fallback)."""
    ticker: str
    strike: float
    yes_ask: Optional[int]
    no_ask: Optional[int]
    yes_bid: Optional[int]
    no_bid: Optional[int]
    subtitle: str = ""
    # Range (B) markets: bounds for "spot in range" -> YES, else NO
    range_low: Optional[float] = None
    range_high: Optional[float] = None


def get_market_hour_la() -> datetime:
    """Current time in America/Los_Angeles."""
    return datetime.now(LA_TZ)


def get_current_hour_market_id(asset: str = "btc") -> str:
    """
    Get the event ticker for the current hourly market (above/below).
    asset: btc | eth | sol | xrp | doge. Format: kxbtcd-YYmmDDHH or kxethd-YYmmDDHH.
    """
    now_utc = datetime.now(pytz.utc)
    target = now_utc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    slug = generate_kalshi_slug(target, asset=asset)
    return slug.upper()


def get_previous_hour_market_id(asset: str = "btc") -> str:
    """Event ticker for the hourly market that just closed (previous hour, above/below)."""
    now_utc = datetime.now(pytz.utc)
    target = now_utc.replace(minute=0, second=0, microsecond=0)  # top of current hour = close of previous
    slug = generate_kalshi_slug(target, asset=asset)
    return slug.upper()


def get_current_hour_market_ids(asset: str) -> List[str]:
    """
    All current-hour market IDs for this asset (9 total across assets: 2 each btc/eth/sol/xrp, 1 doge).
    Returns [above_below_slug, range_slug] for btc/eth/sol/xrp, [range_slug] for doge.
    """
    now_utc = datetime.now(pytz.utc)
    target = now_utc.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    asset_l = str(asset).lower()
    ids: List[str] = []
    if asset_l in ("btc", "eth", "sol", "xrp"):
        ids.append(generate_kalshi_slug(target, asset=asset_l).upper())
        range_slug = generate_kalshi_slug_range(target, asset_l)
        if range_slug:
            ids.append(range_slug.upper())
    elif asset_l == "doge":
        range_slug = generate_kalshi_slug_range(target, asset_l)
        if range_slug:
            ids.append(range_slug.upper())
    return ids


def get_previous_hour_market_ids(asset: str) -> List[str]:
    """
    All previous-hour market IDs for this asset (for resolve). Same count as get_current_hour_market_ids.
    """
    now_utc = datetime.now(pytz.utc)
    target = now_utc.replace(minute=0, second=0, microsecond=0)  # previous hour close
    asset_l = str(asset).lower()
    ids: List[str] = []
    if asset_l in ("btc", "eth", "sol", "xrp"):
        ids.append(generate_kalshi_slug(target, asset=asset_l).upper())
        range_slug = generate_kalshi_slug_range(target, asset_l)
        if range_slug:
            ids.append(range_slug.upper())
    elif asset_l == "doge":
        range_slug = generate_kalshi_slug_range(target, asset_l)
        if range_slug:
            ids.append(range_slug.upper())
    return ids


def get_current_15min_market_id(asset: str = "btc") -> str:
    """
    Get the event ticker for the current 15-min market.
    The current market is the one that closes at the next :00, :15, :30, :45 boundary.
    asset: btc | eth. Format: KXBTC15M-YYmmmDDHHMM or KXETH15M-YYmmmDDHHMM.
    """
    now_utc = datetime.now(pytz.utc)
    et_tz = pytz.timezone("US/Eastern")
    now_et = now_utc.astimezone(et_tz)
    close_time = now_et.replace(second=0, microsecond=0)
    close_min = (close_time.minute // 15 + 1) * 15
    if close_min >= 60:
        close_time = (close_time + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    else:
        close_time = close_time.replace(minute=close_min, second=0, microsecond=0)
    slug = generate_15min_slug(close_time, asset=asset)
    return slug.upper()


def get_previous_15min_market_id(asset: str = "btc") -> str:
    """
    Get the event ticker for the 15-min market that just closed.
    (The previous slot: e.g. at 21:05, current is 21:15, previous is 21:00.)
    """
    now_utc = datetime.now(pytz.utc)
    et_tz = pytz.timezone("US/Eastern")
    now_et = now_utc.astimezone(et_tz)
    close_time = now_et.replace(second=0, microsecond=0)
    close_min = (close_time.minute // 15) * 15
    close_time = close_time.replace(minute=close_min, second=0, microsecond=0)
    slug = generate_15min_slug(close_time, asset=asset)
    return slug.upper()


def fetch_ticker_outcome(ticker: str) -> Optional[str]:
    """
    Fetch market resolution for a specific ticker from Kalshi API.
    GET /markets/{ticker} returns market with 'result' field.
    Returns 'yes', 'no', 'scalar', or None if not yet determined / unavailable.
    """
    if not ticker or not str(ticker).strip():
        return None
    try:
        url = f"{KALSHI_API_URL}/{ticker.strip()}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        m = data.get("market") if isinstance(data, dict) else data
        if not m or not isinstance(m, dict):
            return None
        result = m.get("result")
        if result in ("yes", "no", "scalar"):
            return result
        if isinstance(result, str) and result.strip():
            r = result.lower().strip()
            return r if r in ("yes", "no", "scalar") else None
        return None
    except Exception:
        return None


def fetch_15min_market_result(market_id: str) -> Optional[str]:
    """
    Fetch market resolution from Kalshi API.
    Returns 'yes', 'no', 'scalar', or None if not yet determined.
    """
    m = fetch_15min_market(market_id)
    if not m:
        return None
    result = m.get("result")
    if result in ("yes", "no", "scalar"):
        return result
    if isinstance(result, str) and result.strip():
        r = result.lower()
        return r if r in ("yes", "no", "scalar") else None
    return None


def get_hourly_schedule_state(
    assets: List[str],
    late_window_minutes: float = 10,
    late_interval_minutes: float = 1,
    late_interval_seconds: Optional[float] = None,
    fallback_interval_minutes: float = 3,
) -> Tuple[bool, float]:
    """
    Returns (should_run_now, sleep_seconds) for hourly bot.
    - In late window (0 < minutes_to_close <= late_window_minutes): should_run=True, sleep from late_interval_seconds
      (if set) or late_interval_minutes * 60.
    - Outside late window: should_run=False, sleep until late window.
    - Market closed: should_run=False, sleep fallback.
    """
    min_mins = 999.0
    for asset in assets:
        if str(asset).lower() not in ("btc", "eth", "sol", "xrp"):
            continue
        try:
            mkt_id = get_current_hour_market_id(asset=asset)
            mins = get_minutes_to_close(mkt_id)
            min_mins = min(min_mins, mins)
        except Exception:
            pass
    if min_mins <= 0:
        return (False, float(fallback_interval_minutes * 60))
    if min_mins > late_window_minutes:
        # Outside late window: sleep until we hit it
        sleep_seconds = (min_mins - late_window_minutes) * 60.0
        return (False, max(1.0, sleep_seconds))
    # In late window: prefer late_interval_seconds, else late_interval_minutes * 60
    if late_interval_seconds is not None and late_interval_seconds > 0:
        interval_sec = float(late_interval_seconds)
    else:
        interval_sec = float(late_interval_minutes * 60)
    return (True, interval_sec)


def get_15min_schedule_state(
    assets: List[str],
    late_window_seconds: int = 140,
    late_window_interval_seconds: int = 15,
    fallback_interval_minutes: int = 3,
) -> Tuple[bool, float]:
    """
    Returns (should_run_now, sleep_seconds) for 15-min bot.
    - In late window (0 < seconds_to_close <= late_window_seconds): should_run=True, sleep=interval.
    - Outside: should_run=False, sleep=seconds until late window.
    - Market closed (seconds_to_close <= 0): should_run=False, sleep=fallback.
    """
    min_mins = 999.0
    for asset in assets:
        if str(asset).lower() not in ("btc", "eth", "sol", "xrp"):
            continue
        try:
            mkt_id = get_current_15min_market_id(asset=asset)
            mins = get_minutes_to_close_15min(mkt_id)
            min_mins = min(min_mins, mins)
        except Exception:
            pass
    min_seconds = min_mins * 60.0
    if min_seconds <= 0:
        return (False, float(fallback_interval_minutes * 60))
    if 0 < min_seconds <= late_window_seconds:
        return (True, float(late_window_interval_seconds))
    # Outside late window: don't run, sleep until we hit it
    sleep_seconds = min_seconds - late_window_seconds
    return (False, max(1.0, sleep_seconds))


def get_sleep_seconds_15min(
    assets: List[str],
    late_window_seconds: int = 140,
    late_window_interval_seconds: int = 15,
    fallback_interval_minutes: int = 3,
) -> float:
    """Convenience: returns sleep seconds from get_15min_schedule_state."""
    _, sleep_secs = get_15min_schedule_state(
        assets, late_window_seconds, late_window_interval_seconds, fallback_interval_minutes
    )
    return sleep_secs


def get_minutes_to_close_15min(market_id: str) -> float:
    """
    Minutes until 15-min market close.
    Slug format: KXBTC15M-YYmmmDDHHMM (close time in ET).
    """
    slug = market_id.lower()
    if not any(slug.startswith(p + "-") for p in VALID_PREFIXES_15M):
        return 999.0
    dash = slug.find("-")
    rest = slug[dash + 1 :] if dash >= 0 else ""
    if len(rest) < 11:  # yy + mmm + dd + hh + mm
        return 999.0
    try:
        yy = int(rest[:2])
        month_abbr = rest[2:5]
        dd = int(rest[5:7])
        hh = int(rest[7:9])
        mm = int(rest[9:11])
        year = 2000 + yy
        months = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        }
        month = months.get(month_abbr, 1)
        close_time = ET_TZ.localize(datetime(year, month, dd, hh, mm, 0))
        now_et = datetime.now(ET_TZ)
        if now_et.tzinfo is None:
            now_et = ET_TZ.localize(now_et)
        delta = close_time - now_et
        seconds = delta.total_seconds()
        mins = max(0, min(15, seconds / 60.0))
        return mins
    except Exception:
        return 999.0


def get_15min_window_ids_for_hour(hour_market_id: str) -> List[str]:
    """
    Return the four 15-min market IDs that fall inside the given hourly market.
    E.g. KXBTCD-26FEB1616 -> [KXBTC15M-26FEB161600, KXBTC15M-26FEB161615, KXBTC15M-26FEB161630, KXBTC15M-26FEB161645].
    """
    slug = (hour_market_id or "").strip().upper()
    dash = slug.find("-")
    if dash < 0:
        return []
    prefix_hourly = slug[:dash]
    rest = slug[dash + 1:]
    if len(rest) < 9:
        return []
    # Map hourly prefix to 15m prefix (above/below only; range markets have no 15m)
    prefix_15m_map = {
        "KXBTCD": "KXBTC15M",
        "KXETHD": "KXETH15M",
        "KXSOLD": "KXSOL15M",
        "KXXRPD": "KXXRP15M",
    }
    prefix_15m = prefix_15m_map.get(prefix_hourly.upper())
    if not prefix_15m:
        return []
    base = rest[:9]
    return [
        f"{prefix_15m}-{base}00",
        f"{prefix_15m}-{base}15",
        f"{prefix_15m}-{base}30",
        f"{prefix_15m}-{base}45",
    ]


def get_minutes_to_close(hour_market_id: str) -> float:
    """
    Minutes until market close (top of the hour).
    Slug uses Eastern Time (per Kalshi/generate_kalshi_slug).
    Supports above/below (kxbtcd, kxethd, ...) and range (kxbtc, kxeth, kxdoge, ...) prefixes.
    """
    slug = hour_market_id.lower()
    if not any(slug.startswith(p + "-") for p in VALID_PREFIXES_HOURLY):
        return 999.0
    dash = slug.find("-")
    rest = slug[dash + 1:] if dash >= 0 else ""
    if len(rest) < 9:
        return 999.0
    try:
        yy = int(rest[:2])
        month_abbr = rest[2:5]
        dd = int(rest[5:7])
        hh = int(rest[7:9])
        year = 2000 + yy
        months = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
                  "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
        mm = months.get(month_abbr, 1)
        # Slug is in ET (matches generate_kalshi_slug)
        close_time = ET_TZ.localize(datetime(year, mm, dd, hh, 0, 0))
        now_et = datetime.now(ET_TZ)
        if now_et.tzinfo is None:
            now_et = ET_TZ.localize(now_et)
        delta = close_time - now_et
        seconds = delta.total_seconds()
        # Cap at 60 for hourly markets (if we're past close, next run will get new market)
        mins = max(0, min(60, seconds / 60.0))
        return mins
    except Exception:
        return 999.0


def get_market_context(
    hour_market_id: str,
    late_window_minutes: int = 10,
) -> MarketContext:
    """Build market context including late window flag."""
    minutes_to_close = get_minutes_to_close(hour_market_id)
    is_late = 0 < minutes_to_close <= late_window_minutes
    return MarketContext(
        hour_market_id=hour_market_id,
        event_ticker=hour_market_id,
        market_hour_la=hour_market_id,
        minutes_to_close=minutes_to_close,
        is_late_window=is_late,
    )


def fetch_markets_for_event(event_ticker: str) -> Tuple[List[Dict], Optional[str]]:
    """Fetch markets for an event ticker from Kalshi API (public)."""
    try:
        params = {"limit": 100, "event_ticker": event_ticker}
        resp = requests.get(KALSHI_API_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("markets", []), None
    except Exception as e:
        return [], str(e)


def fetch_markets_by_close_window(
    min_close_ts: int,
    max_close_ts: int,
    series_ticker: Optional[str] = None,
    event_ticker: Optional[str] = None,
) -> Tuple[List[Dict], Optional[str]]:
    """
    Fetch markets from Kalshi API (public) that close within [min_close_ts, max_close_ts].
    Use series_ticker or event_ticker to filter. Per Kalshi docs, min_close_ts/max_close_ts
    are compatible with status=closed or empty; we fetch without status then filter for active.
    """
    try:
        params: Dict[str, Any] = {
            "limit": 200,
            "min_close_ts": min_close_ts,
            "max_close_ts": max_close_ts,
        }
        if series_ticker:
            params["series_ticker"] = series_ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        resp = requests.get(KALSHI_API_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        markets = data.get("markets", [])
        cursor = data.get("cursor")
        while cursor and len(markets) < 500:
            params2 = dict(params)
            params2["cursor"] = cursor
            resp2 = requests.get(KALSHI_API_URL, params=params2, timeout=15)
            resp2.raise_for_status()
            data2 = resp2.json()
            more = data2.get("markets", [])
            markets.extend(more)
            cursor = data2.get("cursor")
            if not more or not cursor:
                break
        return markets, None
    except Exception as e:
        return [], str(e)


def _is_year_like_strike(v: float) -> bool:
    """True if v looks like a year (e.g. 2026); must not be used as strike for crypto."""
    try:
        f = float(v)
        return 2018 <= f <= 2035
    except (TypeError, ValueError):
        return False


def parse_strike_from_text(text: str) -> float:
    """
    Parse strike from text. Supports:
    - "$1,950" (existing)
    - "1950" or "1,950" without "$"
    Returns 0.0 if no valid strike found.
    """
    if not text or not isinstance(text, str):
        return 0.0
    # Format: $1,950 or $1950 or $1950.50
    match = re.search(r"\$([\d,]+(?:\.[\d]+)?)", text)
    if match:
        try:
            return float(match.group(1).replace(",", ""))
        except (ValueError, TypeError):
            pass
    # Format: 1,950 or 1950 or 1950.50 (no $) - require 3+ digits to avoid dates
    match = re.search(r"([\d,]{3,}(?:\.[\d]+)?)", text)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if v > 0 and v < 1e9:  # Sanity: plausible strike
                return v
        except (ValueError, TypeError):
            pass
    return 0.0


def parse_strike(subtitle: str) -> float:
    """Parse strike from subtitle. Delegates to parse_strike_from_text for compatibility."""
    return parse_strike_from_text(subtitle or "")


def _parse_strike_from_subtitle_strict(subtitle: str) -> float:
    """
    Parse strike only from subtitle text that clearly describes a price.
    Supports: "BTC > 68500", "> 68500", "above 68500", "$68,500".
    Rejects year-like values (2018-2035) to avoid parsing "2026" from dates.
    Returns 0.0 if no valid strike or value looks like a year.
    """
    if not subtitle or not isinstance(subtitle, str):
        return 0.0
    # Explicit price patterns (order matters: prefer > N, then above N, then $N)
    match = re.search(r">\s*\$?([\d,]+(?:\.[\d]+)?)", subtitle)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if v > 0 and not _is_year_like_strike(v):
                return v
        except (ValueError, TypeError):
            pass
    match = re.search(r"[Aa]bove\s+\$?([\d,]+(?:\.[\d]+)?)", subtitle)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if v > 0 and not _is_year_like_strike(v):
                return v
        except (ValueError, TypeError):
            pass
    match = re.search(r"\$([\d,]+(?:\.[\d]+)?)", subtitle)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if v > 0 and not _is_year_like_strike(v):
                return v
        except (ValueError, TypeError):
            pass
    return 0.0


def _parse_strike_from_ticker(ticker: str) -> float:
    """
    Try to extract strike from ticker ONLY when strike is explicitly encoded (legacy formats).
    - KXBTCD-26FEB1615-T67749.99 (hourly: -T<strike>)
    - KXDOGE-26FEB2818-B0.087 (bucket: -B<strike>)
    Do NOT use for 15-min tickers like KXBTC15M-26MAR061215-15 where -15 is a sequence ID, not strike.
    Returns 0.0 if no strike-like suffix found.
    """
    if not ticker or not isinstance(ticker, str):
        return 0.0
    # Pattern: -T1234.56 or -T1234 (hourly style, strike in cents or dollars)
    match = re.search(r"-T([\d.]+)$", ticker, re.IGNORECASE)
    if match:
        try:
            v = float(match.group(1))
            if v > 0 and v < 1e9 and not _is_year_like_strike(v):
                return v
        except (ValueError, TypeError):
            pass
    # Pattern: -B0.087 (range/bucket markets, strike in dollars e.g. DOGE)
    match = re.search(r"-B([\d.]+)$", ticker, re.IGNORECASE)
    if match:
        try:
            v = float(match.group(1))
            if v > 0 and v < 1e9:
                return v
        except (ValueError, TypeError):
            pass
    # Do NOT parse -26MAR061215 or -15 as strike (new 15-min format has no strike in ticker).
    return 0.0


def extract_strike_from_market(m: Dict[str, Any], ticker: str) -> float:
    """
    Extract strike from Kalshi API market object only. Do not infer strike from ticker string
    for 15-min markets (e.g. KXBTC15M-26MAR061215-15); the -15 is a sequence ID, not strike.
    Prefer API fields (floor_strike, strike, ceiling_strike, subtitle); reject year-like values (2026).
    """
    if not m or not isinstance(m, dict):
        return 0.0
    # a) Range markets: midpoint from API
    floor_val = m.get("floor_strike")
    ceiling_val = m.get("ceiling_strike")
    if floor_val is not None and ceiling_val is not None:
        try:
            f, c = float(floor_val), float(ceiling_val)
            if f > 0 and c > 0 and not _is_year_like_strike(f) and not _is_year_like_strike(c):
                return (f + c) / 2.0
        except (ValueError, TypeError):
            pass
    # b) Single API strike fields (primary source for 15-min)
    for key in ("floor_strike", "strike", "ceiling_strike"):
        strike_val = m.get(key)
        if strike_val is not None:
            try:
                v = float(strike_val)
                if v > 0 and not _is_year_like_strike(v):
                    return v
            except (ValueError, TypeError):
                pass
    # c) Subtitle with strict parsing (e.g. "BTC > 68500") — avoid parsing "2026" from dates
    v = _parse_strike_from_subtitle_strict(m.get("subtitle", ""))
    if v > 0:
        return v
    # d) Legacy ticker-encoded strike only (-T or -B suffix); never -26MAR061215 or -15
    v = _parse_strike_from_ticker(ticker or m.get("ticker", ""))
    if v > 0:
        return v
    return 0.0


def fetch_eligible_tickers(
    event_ticker: str,
    spot_price: Optional[float] = None,
    window: float = 1500,
) -> List[Dict]:
    """
    Fetch tickers for the event, filter by strike window if spot_price given.
    window: max distance (strike - spot) in same units as spot_price (e.g. dollars for DOGE 0.005, cents for BTC 1500).
    Returns list of {ticker, strike, yes_bid, yes_ask, no_bid, no_ask, subtitle}.
    """
    markets, err = fetch_markets_for_event(event_ticker)
    if err:
        return []
    result = []
    for m in markets:
        strike = extract_strike_from_market(m, m.get("ticker", ""))
        if strike <= 0:
            continue
        if spot_price is not None and abs(strike - spot_price) > float(window):
            continue
        row = {
            "ticker": m.get("ticker"),
            "strike": strike,
            "yes_bid": m.get("yes_bid"),
            "yes_ask": m.get("yes_ask"),
            "no_bid": m.get("no_bid"),
            "no_ask": m.get("no_ask"),
            "subtitle": m.get("subtitle", ""),
        }
        floor_val = m.get("floor_strike")
        ceiling_val = m.get("ceiling_strike")
        if floor_val is not None and ceiling_val is not None:
            try:
                row["floor_strike"] = float(floor_val)
                row["ceiling_strike"] = float(ceiling_val)
            except (ValueError, TypeError):
                pass
        result.append(row)
    result.sort(key=lambda x: x["strike"])
    return result


# In-memory cache for 15-min market metadata (strike/ticker unchanged per window). One REST call per market_id.
# Entries expire after one window (15 min) so data is refreshed when a new market window starts.
_FETCH_15MIN_MARKET_CACHE: Dict[str, Tuple[Optional[Dict], float]] = {}  # key -> (result, cached_at_ts)
_FETCH_15MIN_MARKET_CACHE_LOCK = threading.Lock()
_FETCH_15MIN_MARKET_CACHE_MAX = 128
_FETCH_15MIN_MARKET_CACHE_TTL_SECONDS = 900  # 15 min: refresh once new window starts


def fetch_15min_market(event_ticker: str) -> Optional[Dict]:
    """
    Fetch the single binary market for a 15-min event.
    15-min events have one market per event (ticker typically event_ticker-00).
    Returns first market dict or None. Results are cached per event_ticker; cache is refreshed
    when the entry is older than one window (15 min) so new market windows get fresh data.
    """
    key = (event_ticker or "").strip()
    if not key:
        return None
    now = time.time()
    with _FETCH_15MIN_MARKET_CACHE_LOCK:
        if key in _FETCH_15MIN_MARKET_CACHE:
            result, cached_at = _FETCH_15MIN_MARKET_CACHE[key]
            if (now - cached_at) < _FETCH_15MIN_MARKET_CACHE_TTL_SECONDS:
                return result
            # TTL expired (new window started); fall through to refetch
    markets, err = fetch_markets_for_event(event_ticker)
    if err or not markets:
        result = None
    else:
        result = markets[0]
    # Only cache valid results (poison-cache fix: do not cache None or missing ticker).
    # Only call .get() when result is a dict to avoid AttributeError on custom objects.
    ticker_val = result.get("ticker") if (result is not None and isinstance(result, dict)) else None
    if result is not None and isinstance(result, dict) and ticker_val:
        with _FETCH_15MIN_MARKET_CACHE_LOCK:
            if len(_FETCH_15MIN_MARKET_CACHE) >= _FETCH_15MIN_MARKET_CACHE_MAX:
                _FETCH_15MIN_MARKET_CACHE.pop(next(iter(_FETCH_15MIN_MARKET_CACHE)))
            _FETCH_15MIN_MARKET_CACHE[key] = (result, time.time())
    return result


def fetch_15min_quote(
    event_ticker: str,
    client: Optional[KalshiClient] = None,
) -> Optional[TickerQuote]:
    """
    Fetch market data for 15-min event and return TickerQuote.
    Uses orderbook if client provided, else market data.
    """
    m = fetch_15min_market(event_ticker)
    if not m:
        return None
    ticker = m.get("ticker")
    if not ticker:
        return None
    subtitle = m.get("subtitle", "")
    strike = extract_strike_from_market(m, ticker)
    tickers = [{
        "ticker": ticker,
        "strike": strike,
        "yes_bid": m.get("yes_bid"),
        "yes_ask": m.get("yes_ask"),
        "no_bid": m.get("no_bid"),
        "no_ask": m.get("no_ask"),
        "subtitle": subtitle,
    }]
    if client:
        quotes = enrich_with_orderbook(client, tickers)
    else:
        def _int_or_none(v):
            if v is None:
                return None
            try:
                return int(round(float(v)))
            except (TypeError, ValueError):
                return None
        t = tickers[0]
        rlo, rhi = t.get("floor_strike"), t.get("ceiling_strike")
        try:
            rl = float(rlo) if rlo is not None else None
            rh = float(rhi) if rhi is not None else None
        except (ValueError, TypeError):
            rl = rh = None
        quotes = [
            TickerQuote(
                ticker=ticker,
                strike=t.get("strike", 0) or 0,
                yes_ask=_int_or_none(t.get("yes_ask")),
                no_ask=_int_or_none(t.get("no_ask")),
                yes_bid=_int_or_none(t.get("yes_bid")),
                no_bid=_int_or_none(t.get("no_bid")),
                subtitle=t.get("subtitle", ""),
                range_low=rl,
                range_high=rh,
            )
        ]
    return quotes[0] if quotes else None


def enrich_with_orderbook(
    client: KalshiClient,
    tickers: List[Dict],
) -> List[TickerQuote]:
    """
    Prefer orderbook ask for each ticker; fallback to market data.
    """
    quotes = []
    for t in tickers:
        ticker = t.get("ticker")
        if not ticker:
            continue
        try:
            top = client.get_top_of_book(ticker)
            yes_ask = int(top["yes_ask"]) if top.get("yes_ask") is not None else t.get("yes_ask")
            no_ask = int(top["no_ask"]) if top.get("no_ask") is not None else t.get("no_ask")
            yes_bid = int(top["yes_bid"]) if top.get("yes_bid") is not None else t.get("yes_bid")
            no_bid = int(top["no_bid"]) if top.get("no_bid") is not None else t.get("no_bid")
        except Exception:
            yes_ask = t.get("yes_ask")
            no_ask = t.get("no_ask")
            yes_bid = t.get("yes_bid")
            no_bid = t.get("no_bid")
        # Ensure we have ints for price
        def _int_or_none(v):
            if v is None:
                return None
            try:
                return int(round(float(v)))
            except (TypeError, ValueError):
                return None
        rlo = t.get("floor_strike")
        rhi = t.get("ceiling_strike")
        try:
            range_low = float(rlo) if rlo is not None else None
            range_high = float(rhi) if rhi is not None else None
        except (ValueError, TypeError):
            range_low = range_high = None
        quotes.append(TickerQuote(
            ticker=ticker,
            strike=t.get("strike", 0),
            yes_ask=_int_or_none(yes_ask),
            no_ask=_int_or_none(no_ask),
            yes_bid=_int_or_none(yes_bid),
            no_bid=_int_or_none(no_bid),
            subtitle=t.get("subtitle", ""),
            range_low=range_low,
            range_high=range_high,
        ))
    return quotes
