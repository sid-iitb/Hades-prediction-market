"""
Central data layer for Bot V2: fetches oracles, caches strikes, builds WindowContext.
Strategies never fetch data themselves; they receive a WindowContext per tick.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from bot.pipeline.context import WindowContext

logger = logging.getLogger(__name__)

# Divergence sanity zones: if |spot_kraken - spot_coinbase| <= zone, use average distance; else use MIN (safety).
SANITY_ZONES: Dict[str, float] = {
    "btc": 30.0,
    "eth": 2.0,
    "sol": 0.10,
    "xrp": 0.002,
}
# Assets that always use MIN(oracle distances) for entry — no average. Used by last_90s distance gate.
DISTANCE_USE_MIN_FOR_ASSETS: frozenset = frozenset({"btc"})


def _is_year_like(value: float) -> bool:
    """Reject values that look like years (e.g. 2026 from dates)."""
    return 2020 <= value <= 2035


def _parse_strike_from_text_strict(text: str) -> Optional[float]:
    """
    Parse strike from text using clear price patterns. Rejects year-like values.
    Supports: "BTC > 68500", "> 68500", "above 68500", "$68,500". Returns None if no valid strike.
    """
    if not text or not isinstance(text, str):
        return None
    # Explicit price patterns (prefer > N, then above N, then $N)
    match = re.search(r">\s*\$?([\d,]+(?:\.[\d]+)?)", text)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if v > 0 and not _is_year_like(v):
                return v
        except (ValueError, TypeError):
            pass
    match = re.search(r"[Aa]bove\s+\$?([\d,]+(?:\.[\d]+)?)", text)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if v > 0 and not _is_year_like(v):
                return v
        except (ValueError, TypeError):
            pass
    match = re.search(r"\$([\d,]+(?:\.[\d]+)?)", text)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if v > 0 and not _is_year_like(v):
                return v
        except (ValueError, TypeError):
            pass
    # Fallback: number with 3+ digits (avoid single/double digit or years)
    match = re.search(r"([\d,]{3,}(?:\.[\d]+)?)", text)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if v > 0 and v < 1e9 and not _is_year_like(v):
                return v
        except (ValueError, TypeError):
            pass
    return None


def _parse_strike_from_ticker(ticker: str) -> Optional[float]:
    """
    Extract strike from ticker only when explicitly encoded: -T<strike> or -B<strike>.
    Returns None for 15-min tickers like KXBTC15M-26MAR061215-15 where -15 is sequence ID.
    """
    if not ticker or not isinstance(ticker, str):
        return None
    match = re.search(r"-T([\d.]+)$", ticker, re.IGNORECASE)
    if match:
        try:
            v = float(match.group(1))
            if v > 0 and v < 1e9 and not _is_year_like(v):
                return v
        except (ValueError, TypeError):
            pass
    match = re.search(r"-B([\d.]+)$", ticker, re.IGNORECASE)
    if match:
        try:
            v = float(match.group(1))
            if v > 0 and v < 1e9:
                return v
        except (ValueError, TypeError):
            pass
    return None


def _reject_strike_sanity(val: float, source: str, asset_lower: Optional[str]) -> bool:
    """
    Sanity guard: reject timeframe-looking numbers (15, 30, 60) from title/subtitle,
    and asset-specific unrealistic bounds. Returns True if strike should be rejected.
    """
    # 1. Reject timeframe-looking numbers from title/subtitle
    if source in ("title", "subtitle") and val in (15.0, 30.0, 60.0):
        return True
    # 2. Asset-specific bounds (realism check)
    if asset_lower == "btc" and val < 1000:
        return True
    if asset_lower == "eth" and val < 100:
        return True
    if asset_lower == "sol" and val < 5:
        return True
    if asset_lower == "xrp" and val > 10:
        return True
    return False


def _asset_from_ticker(ticker: str) -> Optional[str]:
    """Infer asset from market ticker (e.g. KXBTC15M-... -> btc)."""
    if not ticker or not isinstance(ticker, str):
        return None
    t = ticker.upper()
    if "BTC" in t:
        return "btc"
    if "ETH" in t:
        return "eth"
    if "SOL" in t:
        return "sol"
    if "XRP" in t:
        return "xrp"
    return None


def _extract_strike_from_market(market_data: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
    """
    High-resilience strike extraction.
    - API fields (floor_strike, cap_strike, etc.): trusted as-is; no year-like check (ETH can be ~2028).
    - Title/subtitle text: always apply _is_year_like (reject 2020–2035) to avoid using dates as strike.
    Sanity guard: rejects 15/30/60 from title/subtitle and asset-unrealistic bounds.
    """
    if not market_data or not isinstance(market_data, dict):
        return (None, None)
    # Debug only: if ticker is prefix-only (e.g. KXETH15M with no "-"), list API may return incomplete market
    logger.debug(
        "[DEBUG STRIKE] Ticker: %s | Subtitle: %s",
        market_data.get("ticker"),
        market_data.get("subtitle"),
    )
    asset_lower = _asset_from_ticker(market_data.get("ticker") or "")

    # 1. Try API Fields (Standard V2). Trust API — do not apply year-like reject (ETH strike can be ~2028).
    api_strike = market_data.get("strike_price") or market_data.get("cap_strike")
    if api_strike is not None:
        try:
            v = float(api_strike)
            if v > 0 and not _reject_strike_sanity(v, "api_fields", asset_lower):
                return (v, "api_fields")
        except (ValueError, TypeError):
            pass

    # Also try floor_strike, strike, ceiling_strike (API-authoritative; no year-like check)
    for key in ("floor_strike", "strike", "ceiling_strike"):
        val = market_data.get(key)
        if val is not None:
            try:
                v = float(val)
                if v > 0 and not _reject_strike_sanity(v, "api_fields", asset_lower):
                    return (v, "api_fields")
            except (ValueError, TypeError):
                pass

    # 2. Subtitle (text): strict patterns + year-like check inside _parse_strike_from_text_strict
    subtitle = (market_data.get("subtitle") or "") if isinstance(market_data.get("subtitle"), str) else ""
    v = _parse_strike_from_text_strict(subtitle)
    if v is not None and not _reject_strike_sanity(v, "subtitle", asset_lower):
        return (v, "subtitle")

    # 3. Title (text): strict patterns + year-like check inside _parse_strike_from_text_strict
    title = (market_data.get("title") or "") if isinstance(market_data.get("title"), str) else ""
    v = _parse_strike_from_text_strict(title)
    if v is not None and not _reject_strike_sanity(v, "title", asset_lower):
        return (v, "title")

    # 4. Fallback: numbers from subtitle — must apply year-like check (dates like 2026 in text)
    subtitle_clean = subtitle.replace(",", "")
    nums = re.findall(r"(\d+\.?\d*)", subtitle_clean)
    for s in reversed(nums):
        try:
            v = float(s)
            if v > 0 and not _is_year_like(v) and v < 1e9 and not _reject_strike_sanity(v, "subtitle", asset_lower):
                return (v, "subtitle")
        except (ValueError, TypeError):
            pass

    # 5. Fallback: numbers from title — must apply year-like check (dates like 2026 in text)
    title_clean = title.replace(",", "")
    nums = re.findall(r"(\d+\.?\d*)", title_clean)
    for s in reversed(nums):
        try:
            v = float(s)
            if v > 0 and not _is_year_like(v) and v < 1e9 and not _reject_strike_sanity(v, "title", asset_lower):
                return (v, "title")
        except (ValueError, TypeError):
            pass

    # Diagnostic when we fail (helps debug strike=None)
    if asset_lower in ("btc", "eth", "sol", "xrp"):
        logger.warning(
            "[STRIKE] Could not extract strike for %s: subtitle=%r title=%r floor_strike=%s cap_strike=%s strike=%s",
            (market_data.get("ticker") or "?"),
            (subtitle or "")[:80],
            (title or "")[:80],
            market_data.get("floor_strike"),
            market_data.get("cap_strike"),
            market_data.get("strike"),
        )
    return (None, None)


class DataLayer:
    """
    Builds WindowContext per (interval, asset) per tick.
    Caches strike per window_id; fetches spot (Kraken/Coinbase) via placeholder until Data Bus/REST hooked.
    """

    _strike_cache: Dict[str, float] = {}  # window_id -> strike (class-level, shared across instances)
    _strike_source_cache: Dict[str, str] = {}  # window_id -> source (only when strike is cached)
    _last_spot_source: str = "?"  # "WS" or "REST" after last _get_spot_prices call
    _last_window_id: Optional[str] = None  # for transition detection in run_unified

    def __init__(self, kalshi_client: Any = None) -> None:
        self._kalshi_client = kalshi_client

    def clear_caches(self) -> None:
        """Wipe strike and strike_source caches (e.g. after window transition)."""
        self._strike_cache.clear()
        self._strike_source_cache.clear()

    def _get_or_fetch_strike(self, window_id: str, market_data: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
        """
        Return (cached strike, cached source) for window_id, or extract from fresh or passed-in market data.
        Fast path: if window_id is in cache, return cached value.
        When not cached: force a fresh fetch from Kalshi for that market (by ticker) so we don't rely on
        stale/empty metadata; only cache when source is api_fields.
        """
        if window_id in self._strike_cache:
            return (self._strike_cache[window_id], self._strike_source_cache.get(window_id))

        ticker = (market_data or {}).get("ticker")
        # If ticker looks like a prefix only (e.g. KXETH15M with no "-"), get_market(ticker) may 404.
        # Use market_id from window_id for 15m (e.g. fifteen_min_KXETH15M-26MAR101815 -> KXETH15M-26MAR101815).
        fetch_ticker: Optional[str] = str(ticker).strip() if ticker else None
        if not fetch_ticker or "-" not in fetch_ticker:
            if window_id.startswith("fifteen_min_"):
                fetch_ticker = window_id.replace("fifteen_min_", "", 1)
            else:
                fetch_ticker = ticker

        fresh_market_data: Optional[Dict[str, Any]] = None
        if self._kalshi_client and fetch_ticker:
            try:
                fresh_market_data = self._kalshi_client.get_market(fetch_ticker)
            except Exception as e:
                logger.debug("Fresh fetch for strike failed for %s: %s", fetch_ticker, e)
        if not fresh_market_data or not isinstance(fresh_market_data, dict):
            fresh_market_data = market_data

        extracted_strike, extracted_source = _extract_strike_from_market(fresh_market_data)
        if extracted_strike is None:
            # Evict cache for this window so we retry next tick (e.g. spot was missing this tick).
            self._strike_cache.pop(window_id, None)
            self._strike_source_cache.pop(window_id, None)
            return (None, None)
        source = extracted_source or "api_fields"
        if source == "api_fields":
            self._strike_cache[window_id] = extracted_strike
            self._strike_source_cache[window_id] = source
            logger.debug("Cached strike for %s: %s (source=%s)", window_id, extracted_strike, source)
        else:
            ticker_display = (fresh_market_data or {}).get("ticker") or window_id
            logger.info(
                "Found potential strike %s from %s for %s, waiting for official api_fields...",
                extracted_strike,
                source,
                ticker_display,
            )
        return (extracted_strike, source)

    def _get_spot_prices(self, asset: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Return (kraken_price, coinbase_price) for the given asset.
        Uses Oracle WS (get_safe_spot_prices_sync) when available, else Kraken/Coinbase REST.
        """
        a = (asset or "").strip().upper() or "BTC"
        kraken_price: Optional[float] = None
        coinbase_price: Optional[float] = None
        self._last_spot_source = "REST"
        try:
            from bot.oracle_ws_manager import get_safe_spot_prices_sync, is_ws_running
            if is_ws_running():
                data = get_safe_spot_prices_sync(asset, max_age_seconds=5.0, require_both=False)
                if data:
                    kraken_price = data.get("kraken")
                    coinbase_price = data.get("cb")
                    if kraken_price is not None or coinbase_price is not None:
                        self._last_spot_source = "WS"
        except Exception as e:
            logger.debug("Oracle WS spot read failed for %s: %s", asset, e)
        if kraken_price is None or coinbase_price is None:
            try:
                from src.client.kraken_client import KrakenClient
                client = KrakenClient()
                if a == "ETH":
                    p = client.latest_eth_price()
                elif a == "SOL":
                    p = client.latest_sol_price()
                elif a == "XRP":
                    p = client.latest_xrp_price()
                else:
                    p = client.latest_btc_price()
                if p is not None and getattr(p, "price", None) is not None:
                    kraken_price = float(p.price) if kraken_price is None else kraken_price
            except Exception as e:
                logger.debug("Kraken REST spot failed for %s: %s", asset, e)
            if coinbase_price is None:
                symbol = f"{a}-USD" if a in ("BTC", "ETH", "SOL", "XRP") else "BTC-USD"
                try:
                    import urllib.request
                    with urllib.request.urlopen(
                        f"https://api.coinbase.com/v2/prices/{symbol}/spot",
                        timeout=2,
                    ) as resp:
                        if resp.status == 200:
                            import json
                            data = json.loads(resp.read().decode())
                            amount = (data or {}).get("data", {}).get("amount")
                            if amount is not None:
                                coinbase_price = float(amount)
                except Exception as e:
                    logger.debug("Coinbase REST spot failed for %s: %s", asset, e)
        return (kraken_price, coinbase_price)

    def build_context(
        self,
        interval: str,
        market_id: str,
        ticker: str,
        asset: str,
        seconds_to_close: float,
        quote: Dict[str, Any],
        positions: List[Dict[str, Any]],
        open_orders: List[Any],
        config: Dict[str, Any],
        market_data: Dict[str, Any],
    ) -> WindowContext:
        """
        Build a WindowContext for the current tick: resolve spot, strike (cached), and distances.
        """
        kraken_price, coinbase_price = self._get_spot_prices(asset)
        window_id = f"{interval}_{market_id}"
        strike, strike_source = self._get_or_fetch_strike(window_id, market_data)

        # Normalize quote to ints (cents)
        quote_normalized: Dict[str, int] = {
            "yes_bid": int(quote.get("yes_bid", 0) or 0),
            "yes_ask": int(quote.get("yes_ask", 0) or 0),
            "no_bid": int(quote.get("no_bid", 0) or 0),
            "no_ask": int(quote.get("no_ask", 0) or 0),
        }

        # Distance from strike (absolute). Compute both MIN and AVG; use:
        # - Entry distance: AVG for all assets except BTC (which stays on MIN, as tuned).
        # - Exit / stop-loss distance: strategies recompute MIN from distance_kraken/coinbase.
        distance_kraken: Optional[float] = None
        distance_coinbase: Optional[float] = None
        if strike is not None:
            if kraken_price is not None:
                distance_kraken = abs(kraken_price - strike)
            if coinbase_price is not None:
                distance_coinbase = abs(coinbase_price - strike)
        if distance_kraken is not None and distance_coinbase is not None:
            asset_lower = (asset or "").strip().lower()
            distance_min = min(distance_kraken, distance_coinbase)
            distance_avg = (distance_kraken + distance_coinbase) / 2.0
            if asset_lower in DISTANCE_USE_MIN_FOR_ASSETS:
                # BTC (and any other assets we explicitly configure) keep using MIN for entry distance.
                distance = distance_min
            else:
                # All other assets use AVG distance for entry logic.
                distance = distance_avg
        else:
            distance = distance_kraken if distance_coinbase is None else distance_coinbase

        return WindowContext(
            interval=interval,
            market_id=market_id,
            ticker=ticker,
            asset=asset,
            seconds_to_close=seconds_to_close,
            quote=quote_normalized,
            spot_kraken=kraken_price,
            spot_coinbase=coinbase_price,
            strike=strike,
            strike_source=strike_source,
            distance_kraken=distance_kraken,
            distance_coinbase=distance_coinbase,
            distance=distance,
            positions=positions,
            open_orders=open_orders,
            config=config,
        )
