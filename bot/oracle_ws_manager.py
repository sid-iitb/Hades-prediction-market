"""
WebSocket Spot Oracles for HFT: Kraken + Coinbase.

Maintains global_spot_prices updated in real time via WebSockets (no blocking REST).
Provides get_safe_spot_prices_sync() for the last_90s strategy with stale-data guard.

Alignment with common sample code:
- Kraken: Aligned. Same URL (wss://ws.kraken.com), same subscribe format
  (event/subscribe, subscription.name=ticker, pair=[XBT/USD, ETH/USD, SOL/USD, XRP/USD]).
  Ticker message: [channelID, ticker_obj, pair, channel_name]; we use data[2]=pair, data[1]["c"][0]=last price.
- Coinbase: Two feeds exist. We default to Exchange feed (ws-feed.exchange.coinbase.com,
  type/subscribe, channels=["ticker"], message type=ticker with price/product_id). Set
  COINBASE_WS_FEED=advanced_trade to use Advanced Trade (wss://advanced-trade-ws.coinbase.com,
  channel=ticker, message format events[0].tickers[0].price / product_id) to match sample.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _ws_ssl_context() -> ssl.SSLContext:
    """SSL context for WebSocket connections. Bypasses certificate verification
    (cert_reqs = CERT_NONE) for LibreSSL/Mac compatibility; use so wss:// works."""
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except AttributeError:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


# --- Global state (thread-safe) ---
# Structure: global_spot_prices['BTC'] = {'kraken': 71000.5, 'kraken_ts': 170000000.1, 'cb': 71002.0, 'cb_ts': 170000000.2}
_global_spot_prices: Dict[str, Dict[str, Any]] = {}
# RLock: _set_kraken_price / _set_cb_price hold the lock and call _ensure_asset_entry which also acquires it.
_lock = threading.RLock()
# One-time log per (asset, source) when first price received (so we can verify WS is feeding).
_first_kraken_logged: set = set()
_first_cb_logged: set = set()
_loop: Optional[asyncio.AbstractEventLoop] = None
_ws_task: Optional[threading.Thread] = None
_ws_running = False
# Hold ws refs so stop() can close them and unblock async for
_kraken_ws: Any = None
_cb_ws: Any = None


class StaleOracleDataException(Exception):
    """Raised when spot data is older than max_age_seconds; bot must refuse to trade."""

    pass


# Subscription targets (used by WS loops; exposed for tests)
KRAKEN_TICKER_PAIRS = ["XBT/USD", "ETH/USD", "SOL/USD", "XRP/USD"]
COINBASE_TICKER_PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"]
WS_ASSET_KEYS = ["BTC", "ETH", "SOL", "XRP"]

KRAKEN_PAIR_TO_ASSET = {"XBT/USD": "BTC", "ETH/USD": "ETH", "SOL/USD": "SOL", "XRP/USD": "XRP"}
COINBASE_PRODUCT_TO_ASSET = {"BTC-USD": "BTC", "ETH-USD": "ETH", "SOL-USD": "SOL", "XRP-USD": "XRP"}


def _asset_to_kraken_pair(asset: str) -> str:
    a = (asset or "").strip().upper()
    if a == "BTC":
        return "XBT/USD"
    if a in ("ETH", "SOL", "XRP"):
        return f"{a}/USD"
    return "XBT/USD"


def _asset_to_coinbase_product(asset: str) -> str:
    a = (asset or "").strip().upper()
    if a == "BTC":
        return "BTC-USD"
    if a in ("ETH", "SOL", "XRP"):
        return f"{a}-USD"
    return "BTC-USD"


def _ensure_asset_entry(asset_key: str) -> None:
    with _lock:
        if asset_key not in _global_spot_prices:
            _global_spot_prices[asset_key] = {
                "kraken": None,
                "kraken_ts": None,
                "cb": None,
                "cb_ts": None,
            }


def _set_kraken_price(asset_key: str, price: float) -> None:
    with _lock:
        _ensure_asset_entry(asset_key)
        _global_spot_prices[asset_key]["kraken"] = price
        _global_spot_prices[asset_key]["kraken_ts"] = time.time()
        if asset_key not in _first_kraken_logged:
            _first_kraken_logged.add(asset_key)
            logger.info("[oracle_ws] First Kraken price received for %s: %s", asset_key, price)
    try:
        import bot.data_bus as data_bus
        data_bus.write_spot(asset_key, price)
    except Exception:
        pass


def _set_cb_price(asset_key: str, price: float) -> None:
    with _lock:
        _ensure_asset_entry(asset_key)
        _global_spot_prices[asset_key]["cb"] = price
        _global_spot_prices[asset_key]["cb_ts"] = time.time()
        if asset_key not in _first_cb_logged:
            _first_cb_logged.add(asset_key)
            logger.info("[oracle_ws] First Coinbase price received for %s: %s", asset_key, price)
    try:
        import bot.data_bus as data_bus
        data_bus.write_spot(asset_key, price)
    except Exception:
        pass


def get_safe_spot_prices_sync(
    asset: str, max_age_seconds: float = 3.0, require_both: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Synchronous read for the strategy thread. Returns current spot data for the asset.

    - If require_both=True (default False for last_90s): returns data only when BOTH
      Kraken and Coinbase have data newer than max_age_seconds; otherwise None.
    - If require_both=False: returns data when AT LEAST ONE oracle has fresh data;
      the other key may be None (caller uses min(distance_kraken, distance_coinbase)
      or the single available distance). Reduces no-oracle-data skips when one feed
      is slow or disconnected.
    """
    asset_key = (asset or "").strip().upper() or "BTC"
    with _lock:
        entry = _global_spot_prices.get(asset_key)
    if not entry:
        return None
    now = time.time()
    k_ts = entry.get("kraken_ts")
    cb_ts = entry.get("cb_ts")
    k = entry.get("kraken")
    cb = entry.get("cb")

    k_fresh = k is not None and k_ts is not None and (now - k_ts) <= max_age_seconds
    cb_fresh = cb is not None and cb_ts is not None and (now - cb_ts) <= max_age_seconds

    if require_both:
        if not (k_fresh and cb_fresh):
            return None
        return {"kraken": k, "cb": cb, "kraken_ts": k_ts, "cb_ts": cb_ts}

    # At least one fresh
    if k_fresh and cb_fresh:
        return {"kraken": k, "cb": cb, "kraken_ts": k_ts, "cb_ts": cb_ts}
    if k_fresh:
        return {"kraken": k, "cb": None, "kraken_ts": k_ts, "cb_ts": None}
    if cb_fresh:
        return {"kraken": None, "cb": cb, "kraken_ts": None, "cb_ts": cb_ts}
    return None


def is_ws_running() -> bool:
    return _ws_running


def get_ws_status() -> Dict[str, Any]:
    """
    Read-only snapshot of current WS state for debugging/health checks.
    Returns e.g. {"running": True, "assets": {"BTC": {"kraken": 71000.5, "kraken_age_s": 0.1, "cb": 71002.0, "cb_age_s": 0.2}, ...}}
    """
    now = time.time()
    with _lock:
        assets = {}
        for key, entry in list(_global_spot_prices.items()):
            k = entry.get("kraken")
            k_ts = entry.get("kraken_ts")
            cb = entry.get("cb")
            cb_ts = entry.get("cb_ts")
            assets[key] = {
                "kraken": k,
                "kraken_age_s": round(now - k_ts, 2) if k_ts is not None else None,
                "cb": cb,
                "cb_age_s": round(now - cb_ts, 2) if cb_ts is not None else None,
            }
    return {"running": _ws_running, "assets": assets}


async def _kraken_ws_loop() -> None:
    try:
        import websockets
    except ImportError:
        logger.warning("[oracle_ws] websockets package not installed; run pip install websockets. WS oracles disabled.")
        return
    pairs = KRAKEN_TICKER_PAIRS
    asset_map = KRAKEN_PAIR_TO_ASSET
    url = "wss://ws.kraken.com"
    backoff = 1.0
    global _kraken_ws
    while True:
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=10, close_timeout=5, ssl=_ws_ssl_context()
            ) as ws:
                backoff = 1.0
                _kraken_ws = ws
                try:
                    sub = {"event": "subscribe", "pair": pairs, "subscription": {"name": "ticker"}}
                    await ws.send(json.dumps(sub))
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        if isinstance(data, dict):
                            if data.get("event") == "heartbeat":
                                continue
                            if data.get("status") == "subscribed":
                                continue
                        if isinstance(data, list) and len(data) >= 4:
                            # Kraken ticker: [channelID, tickerObj, channelName, pair] e.g. [324, {...}, "ticker", "XBT/USD"]
                            try:
                                pair = data[3]
                                ticker = data[1] if isinstance(data[1], dict) else None
                                if pair and ticker and isinstance(ticker, dict):
                                    c = ticker.get("c")
                                    if c and isinstance(c, (list, tuple)) and len(c) >= 1:
                                        try:
                                            price = float(c[0])
                                        except (TypeError, ValueError):
                                            continue
                                        asset_key = asset_map.get(pair, "BTC")
                                        _set_kraken_price(asset_key, price)
                            except Exception as inner_e:
                                logger.error("[oracle_ws] Kraken message processing error: %s; continuing", inner_e)
                                continue
                finally:
                    _kraken_ws = None
        except Exception as e:
            logger.error(f"[Oracle FATAL] Kraken connection failed: {e}", exc_info=True)
            await asyncio.sleep(max(backoff, 5))
            backoff = min(backoff * 1.5, 60.0)
            continue
        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.5, 60.0)


def _coinbase_ws_url_and_channel_key() -> tuple[str, str]:
    """Return (url, channel_key). channel_key is 'type' for Exchange, 'channel' for Advanced Trade."""
    if os.environ.get("COINBASE_WS_FEED") == "advanced_trade":
        return "wss://advanced-trade-ws.coinbase.com", "channel"
    return "wss://ws-feed.exchange.coinbase.com", "type"


async def _coinbase_ws_loop() -> None:
    try:
        import websockets
    except ImportError:
        return
    products = COINBASE_TICKER_PRODUCTS
    asset_map = COINBASE_PRODUCT_TO_ASSET
    url, channel_key = _coinbase_ws_url_and_channel_key()
    use_advanced_trade = "advanced-trade-ws" in url
    global _cb_ws
    backoff = 1.0
    while True:
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=10, close_timeout=5, ssl=_ws_ssl_context()
            ) as ws:
                backoff = 1.0
                _cb_ws = ws
                try:
                    if use_advanced_trade:
                        sub = {"type": "subscribe", "product_ids": products, "channel": "ticker"}
                    else:
                        sub = {"type": "subscribe", "product_ids": products, "channels": ["ticker"]}
                    await ws.send(json.dumps(sub))
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(data, dict):
                            continue
                        try:
                            if use_advanced_trade:
                                if data.get(channel_key) != "ticker":
                                    continue
                                events = data.get("events") or []
                                if not events:
                                    continue
                                tickers = (events[0].get("tickers") or []) if isinstance(events[0], dict) else []
                                if not tickers:
                                    continue
                                t = tickers[0] if isinstance(tickers[0], dict) else {}
                                price_s = t.get("price")
                                product = t.get("product_id")
                            else:
                                if data.get(channel_key) != "ticker":
                                    continue
                                price_s = data.get("price")
                                product = data.get("product_id")
                            if price_s and product:
                                try:
                                    price = float(price_s)
                                except (TypeError, ValueError):
                                    continue
                                asset_key = asset_map.get(product, "BTC")
                                _set_cb_price(asset_key, price)
                        except Exception as inner_e:
                            logger.error("[oracle_ws] Coinbase message processing error: %s; continuing", inner_e)
                            continue
                finally:
                    _cb_ws = None
        except Exception as e:
            logger.error(f"[Oracle FATAL] Coinbase connection failed: {e}", exc_info=True)
            await asyncio.sleep(max(backoff, 5))
            backoff = min(backoff * 1.5, 60.0)
            continue
        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.5, 60.0)


async def _close_oracle_connections() -> None:
    """Close both Kraken and Coinbase ws so run_until_complete can exit. Call from loop thread."""
    global _kraken_ws, _cb_ws
    for w in [_kraken_ws, _cb_ws]:
        if w is not None:
            try:
                await w.close()
            except Exception:
                pass
    _kraken_ws = None
    _cb_ws = None


async def _run_both() -> None:
    await asyncio.gather(_kraken_ws_loop(), _coinbase_ws_loop())


def _run_loop_in_thread(loop: asyncio.AbstractEventLoop) -> None:
    global _ws_running
    _ws_running = True
    try:
        loop.run_until_complete(_run_both())
    except Exception as e:
        logger.exception("[oracle_ws] WS thread crashed: %s", e)
    finally:
        _ws_running = False


def start_ws_oracles() -> None:
    """Start Kraken + Coinbase WebSocket oracles in a background thread. Idempotent."""
    global _loop, _ws_task
    logger.info("[oracle_ws] start_ws_oracles() entered.")
    if _loop is not None and _ws_task is not None:
        logger.info("[oracle_ws] WebSocket oracles already running; skipping start.")
        return
    try:
        import websockets
    except ImportError:
        logger.warning("[oracle_ws] websockets not installed; pip install websockets. WS oracles disabled.")
        return
    try:
        _loop = asyncio.new_event_loop()
        _ws_task = threading.Thread(target=_run_loop_in_thread, args=(_loop,), daemon=True)
        _ws_task.start()
        logger.info("[oracle_ws] WebSocket spot oracles (Kraken + Coinbase) started.")
    except Exception as e:
        logger.exception("[oracle_ws] Failed to start WS oracles: %s", e)
        _loop, _ws_task = None, None


def stop_ws_oracles() -> None:
    """Stop the WebSocket oracles: close connections and join the thread (prevents zombie processes)."""
    global _loop, _ws_task
    if _loop is None:
        return
    try:
        def _schedule_close():
            asyncio.ensure_future(_close_oracle_connections(), loop=_loop)
        _loop.call_soon_threadsafe(_schedule_close)
    except Exception:
        pass
    task = _ws_task
    _ws_task = None
    _loop = None
    if task is not None and task.is_alive():
        task.join(timeout=10)
