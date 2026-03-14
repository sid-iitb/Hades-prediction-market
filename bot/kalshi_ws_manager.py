"""
Kalshi WebSocket orderbook manager for HFT.

Maintains a thread-safe global dictionary of the latest order book (top of book)
per ticker from Kalshi's orderbook_delta channel. No REST orderbook calls.
Authentication required (same as REST: KALSHI_ACCESS_KEY, signature, timestamp).
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import ssl
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# --- Global state (thread-safe) ---
# ticker -> {"yes_bid", "no_bid", "yes_ask", "no_ask", "bid", "ask"}
_global_kalshi_books: Dict[str, Dict[str, Any]] = {}
# Per-ticker orderbook levels for delta application: ticker -> {"yes": {price_cents: contracts}, "no": {...}}
_global_kalshi_levels: Dict[str, Dict[str, Dict[int, int]]] = {}
_lock = threading.Lock()
_subscribed_tickers: List[str] = []
_subscribe_lock = threading.Lock()
_loop: Optional[asyncio.AbstractEventLoop] = None
_ws_task: Optional[threading.Thread] = None
_ws_running = False
_cmd_id = 0
_current_ws: Any = None  # set in loop so stop() can close and unblock

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_PATH = "/trade-api/ws/v2"


def _kalshi_ws_ssl_context() -> ssl.SSLContext:
    """SSL context that bypasses cert verification (LibreSSL/Mac compatibility for wss://)."""
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except AttributeError:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _kalshi_ws_headers() -> Optional[Dict[str, str]]:
    """Build auth headers for Kalshi WebSocket (GET /trade-api/ws/v2). Returns None if env not set."""
    api_key = os.getenv("KALSHI_API_KEY")
    key_path = os.getenv("KALSHI_PRIVATE_KEY")
    if not api_key or not key_path:
        return None
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
    except ImportError:
        logger.warning("[kalshi_ws] cryptography not installed; Kalshi WS disabled.")
        return None
    try:
        key_content = key_path.strip()
        if key_content.startswith("-----BEGIN"):
            pem_bytes = key_content.encode("utf-8")
        else:
            with open(key_path, "rb") as f:
                pem_bytes = f.read()
        private_key = load_pem_private_key(pem_bytes, password=None, backend=default_backend())
    except Exception as e:
        logger.warning("[kalshi_ws] Failed to load private key: %s", e)
        return None
    timestamp_ms = str(int(time.time() * 1000))
    message = f"{timestamp_ms}GET{WS_PATH}".encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    sig_b64 = base64.b64encode(signature).decode("utf-8")
    return {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-SIGNATURE": sig_b64,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
    }


def _best_bid_from_levels(levels: Dict[int, int]) -> Optional[int]:
    """Return best (highest) bid price with contracts > 0, or None."""
    candidates = [p for p, c in levels.items() if c and c > 0]
    return max(candidates) if candidates else None


def _apply_snapshot(ticker: str, msg: Dict[str, Any]) -> None:
    """Apply orderbook_snapshot to global state. yes/no are [[price_cents, contracts], ...] ascending."""
    yes_arr = msg.get("yes") or []
    no_arr = msg.get("no") or []
    yes_levels = {}
    for lev in yes_arr:
        if isinstance(lev, (list, tuple)) and len(lev) >= 2:
            try:
                p, c = int(lev[0]), int(lev[1])
                if c > 0:
                    yes_levels[p] = c
            except (TypeError, ValueError):
                pass
    no_levels = {}
    for lev in no_arr:
        if isinstance(lev, (list, tuple)) and len(lev) >= 2:
            try:
                p, c = int(lev[0]), int(lev[1])
                if c > 0:
                    no_levels[p] = c
            except (TypeError, ValueError):
                pass
    best_yes_bid = _best_bid_from_levels(yes_levels)
    best_no_bid = _best_bid_from_levels(no_levels)
    yes_ask = (100 - best_no_bid) if best_no_bid is not None else None
    no_ask = (100 - best_yes_bid) if best_yes_bid is not None else None
    with _lock:
        _global_kalshi_levels[ticker] = {"yes": yes_levels, "no": no_levels}
        bid = max((best_yes_bid or 0), (best_no_bid or 0)) or None
        ask = (yes_ask if (best_yes_bid or 0) >= (best_no_bid or 0) else no_ask) if (yes_ask is not None or no_ask is not None) else None
        _global_kalshi_books[ticker] = {
            "yes_bid": best_yes_bid,
            "no_bid": best_no_bid,
            "yes_ask": yes_ask,
            "no_ask": no_ask,
            "bid": bid,
            "ask": ask,
        }
    try:
        import bot.data_bus as data_bus
        data_bus.write_book(ticker, best_yes_bid, yes_ask, best_no_bid, no_ask)
    except Exception:
        pass


def _apply_delta(ticker: str, msg: Dict[str, Any]) -> None:
    """Apply orderbook_delta to stored levels and recompute top of book."""
    price = msg.get("price")
    delta = msg.get("delta")
    side = (msg.get("side") or "").lower()
    if price is None or delta is None or side not in ("yes", "no"):
        return
    try:
        price = int(price)
        delta = int(delta)
    except (TypeError, ValueError):
        return
    with _lock:
        if ticker not in _global_kalshi_levels:
            _global_kalshi_levels[ticker] = {"yes": {}, "no": {}}
        levels = _global_kalshi_levels[ticker].get(side, {})
        levels[price] = levels.get(price, 0) + delta
        if levels[price] <= 0:
            del levels[price]
        _global_kalshi_levels[ticker][side] = levels
        yes_levels = _global_kalshi_levels[ticker].get("yes", {})
        no_levels = _global_kalshi_levels[ticker].get("no", {})
    best_yes_bid = _best_bid_from_levels(yes_levels)
    best_no_bid = _best_bid_from_levels(no_levels)
    yes_ask = (100 - best_no_bid) if best_no_bid is not None else None
    no_ask = (100 - best_yes_bid) if best_yes_bid is not None else None
    with _lock:
        if ticker not in _global_kalshi_books:
            _global_kalshi_books[ticker] = {}
        bid = max((best_yes_bid or 0), (best_no_bid or 0)) or None
        ask = (yes_ask if (best_yes_bid or 0) >= (best_no_bid or 0) else no_ask) if (yes_ask is not None or no_ask is not None) else None
        _global_kalshi_books[ticker].update({
            "yes_bid": best_yes_bid,
            "no_bid": best_no_bid,
            "yes_ask": yes_ask,
            "no_ask": no_ask,
            "bid": bid,
            "ask": ask,
        })
    try:
        import bot.data_bus as data_bus
        data_bus.write_book(ticker, best_yes_bid, yes_ask, best_no_bid, no_ask)
    except Exception:
        pass


def subscribe_to_tickers(tickers: List[str]) -> None:
    """Replace the list of tickers to subscribe to. On change, force reconnect so WS picks up new list."""
    if not tickers:
        return
    tickers_list = [t for t in tickers if t and str(t).strip()]
    if not tickers_list:
        return
    with _subscribe_lock:
        global _subscribed_tickers
        old_set = set(_subscribed_tickers)
        new_set = set(tickers_list)
        if old_set == new_set:
            return
        _subscribed_tickers = list(tickers_list)
        # Force reconnect so WS subscribes to the new ticker list
        if _current_ws is not None and _loop is not None:
            try:
                def _close_and_reconnect():
                    if _current_ws is not None:
                        try:
                            asyncio.ensure_future(_current_ws.close(), loop=_loop)
                        except Exception:
                            pass
                _loop.call_soon_threadsafe(_close_and_reconnect)
            except Exception as e:
                logger.warning("[kalshi_ws] Failed to trigger reconnect on ticker change: %s", e)


def _get_subscribed_tickers_snapshot() -> List[str]:
    with _subscribe_lock:
        return list(_subscribed_tickers)


async def _kalshi_ws_loop() -> None:
    global _current_ws
    try:
        import websockets
    except ImportError:
        logger.warning("[kalshi_ws] websockets not installed; pip install websockets. Kalshi WS disabled.")
        return
    backoff = 1.0
    while True:
        try:
            headers = _kalshi_ws_headers()
            if not headers:
                logger.warning("[kalshi_ws] No Kalshi auth (KALSHI_API_KEY/KALSHI_PRIVATE_KEY); reconnecting in %.0fs", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 60.0)
                continue
            async with websockets.connect(
                WS_URL,
                additional_headers=headers,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
                ssl=_kalshi_ws_ssl_context(),
            ) as ws:
                backoff = 1.0
                _current_ws = ws
                try:
                    tickers = _get_subscribed_tickers_snapshot()
                    if tickers:
                        global _cmd_id
                        _cmd_id += 1
                        sub = {"id": _cmd_id, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_tickers": tickers}}
                        try:
                            await ws.send(json.dumps(sub))
                        except Exception as e:
                            logger.error("[kalshi_ws] Subscribe send failed: %s", e)
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        try:
                            if not isinstance(data, dict):
                                continue
                            msg_type = data.get("type")
                            msg = data.get("msg") or {}
                            ticker = msg.get("market_ticker") or msg.get("market_id")
                            if msg_type == "orderbook_snapshot" and ticker:
                                _apply_snapshot(ticker, msg)
                            elif msg_type == "orderbook_delta" and ticker:
                                _apply_delta(ticker, msg)
                            elif msg_type == "subscribed":
                                pass
                            elif msg_type == "error":
                                logger.error("[kalshi_ws] Server error: %s", data)
                        except Exception as inner_e:
                            logger.error("[kalshi_ws] Message processing error: %s; continuing", inner_e)
                            continue
                finally:
                    _current_ws = None
        except Exception as e:
            logger.exception("[kalshi_ws] Fatal WS error: %s; reconnecting...", e)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.5, 60.0)


def _run_loop_in_thread(loop: asyncio.AbstractEventLoop) -> None:
    global _ws_running
    _ws_running = True
    try:
        loop.run_until_complete(_kalshi_ws_loop())
    finally:
        _ws_running = False


def stop_kalshi_ws() -> None:
    """Stop Kalshi WebSocket: close connection and join thread (prevents zombie processes)."""
    global _loop, _ws_task, _current_ws
    if _loop is None:
        return
    try:
        def _close_ws():
            global _current_ws
            if _current_ws is not None:
                try:
                    asyncio.ensure_future(_current_ws.close(), loop=_loop)
                except Exception:
                    pass
        _loop.call_soon_threadsafe(_close_ws)
    except Exception:
        pass
    task = _ws_task
    _ws_task = None
    _loop = None
    _current_ws = None
    if task is not None and task.is_alive():
        task.join(timeout=10)
    logger.info("[kalshi_ws] Kalshi WebSocket stopped.")


def start_kalshi_ws() -> None:
    """Start Kalshi orderbook WebSocket in a background thread. Idempotent."""
    global _loop, _ws_task
    if _loop is not None and _ws_task is not None:
        return
    try:
        import websockets
    except ImportError:
        logger.warning("[kalshi_ws] websockets not installed; pip install websockets. Kalshi WS disabled.")
        return
    _loop = asyncio.new_event_loop()
    _ws_task = threading.Thread(target=_run_loop_in_thread, args=(_loop,), daemon=True)
    _ws_task.start()
    logger.info("[kalshi_ws] Kalshi orderbook WebSocket started.")


def is_kalshi_ws_running() -> bool:
    return _ws_running


def get_safe_orderbook(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Thread-safe getter for latest top-of-book for the given ticker.
    Returns dict with keys: yes_bid, no_bid, yes_ask, no_ask, bid, ask (ints or None).
    Returns None if WS not running or no data for ticker (caller should skip tick, no REST fallback).
    """
    if not ticker or not str(ticker).strip():
        return None
    key = str(ticker).strip()
    with _lock:
        book = _global_kalshi_books.get(key)
    if not book:
        return None
    return dict(book)
