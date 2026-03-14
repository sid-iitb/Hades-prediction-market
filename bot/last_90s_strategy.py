"""
Last-90s Limit-99c Strategy (15-min markets only).

Runs as an independent loop. For select crypto 15-minute markets (BTC, ETH, SOL, XRP),
places a single LIMIT buy at 99¢ during the final 90 seconds before market close,
to capture late settlement behavior (contracts often finish at $1.00 or $0.01).

Side selection: by default places on the side where the bet (bid) is higher:
compares yes_bid vs no_bid from the orderbook/market; if yes_bid >= no_bid places
YES, else places NO. Override with config side: "yes" | "no" | "auto".

Does not modify or depend on the existing 15-min or hourly strategies.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from bot.kalshi_ws_manager import get_safe_orderbook, start_kalshi_ws, subscribe_to_tickers
from bot.market import (
    extract_strike_from_market,
    fetch_15min_market,
    fetch_15min_market_result,
    get_current_15min_market_id,
    get_minutes_to_close_15min,
    get_previous_15min_market_id,
)
from bot.no_trade_window import in_no_trade_window
from bot.state import (
    clear_last_90s_skip_aggregator,
    get_last_90s_placed,
    get_last_90s_skip_aggregator,
    get_last_90s_placements_for_stoploss,
    get_last_90s_stop_loss_triggered,
    get_last_90s_unresolved_for_market,
    persist_last_90s_skip_aggregator,
    set_last_90s_placed,
    set_last_90s_stop_loss_triggered,
    update_last_90s_resolved,
)
from bot.strategy_report_db import (
    ensure_report_db,
    update_post_placement_telemetry,
    update_resolution_last_90s,
    write_row_last_90s,
)

try:
    import pytz
    from src.offline_processing.generate_all_kalshi_urls import generate_15min_slug
except Exception:
    pytz = None
    generate_15min_slug = None

# Oracle divergence guard (disabled for REST stability). When enabled: max allowed |kraken - coinbase| in USD.
# Entry and Defense gates both use conservative min(dist_kraken, dist_cb) when both oracles available.
MAX_DIVERGENCE = {"btc": 5.0, "eth": 0.50, "sol": 0.05, "xrp": 0.0005}

# Strike sanity: if |spot - strike| exceeds this (USD), strike is likely wrong (e.g. year 2026 parsed as strike). Skip market.
MAX_ABSURD_DISTANCE_USD = {"btc": 5000.0, "eth": 500.0, "sol": 50.0, "xrp": 5.0}
# Stop-loss redundancy: if Kalshi bid drops below this (cents) while we hold a position, panic-sell regardless of Oracle distance.
PANIC_SELL_BID_CENTS = 50


def _get_cfg(config: dict) -> Optional[dict]:
    """Get last_90s_limit_99 config from fifteen_min (optional top-level)."""
    fm = config.get("fifteen_min") or {}
    out = fm.get("last_90s_limit_99") or config.get("last_90s_limit_99")
    return out if isinstance(out, dict) else None


@contextmanager
def _db_section(db_lock: Optional[threading.Lock]):
    """Acquire db_lock for DB access if provided; no-op otherwise (single-threaded caller)."""
    if db_lock is not None:
        with db_lock:
            yield
    else:
        yield


# --- In-memory time-series skip aggregator (one-row guarantee). Thread-safe. ---
# Exactly one row per (window_id, asset) in strategy_report_last_90s:
#   Case A: All skipped till end of window -> at resolve we write 1 row/asset with aggregated_skips + last-seen context.
#   Case B: Skip for some time then place -> on placement we write 1 row/asset with full placement + pre_placement skip_details.
#   Case C: Place first time (no prior skips) -> on placement we write 1 row/asset with full placement, skip_details empty.
# All LAST_90S_COLUMNS are set in A (or null where N/A); B/C set placement fields, resolution fields filled later by update_resolution_last_90s.
_last_90s_skip_aggregator: Dict[tuple, Dict[str, Any]] = {}
_last_90s_skip_aggregator_lock = threading.Lock()

# --- In-memory per-window placement state (dual-strike: at most one limit + one market per window/asset). ---
_last_90s_placed_windows: Dict[tuple, Dict[str, bool]] = {}
_last_90s_placed_windows_lock = threading.Lock()

# --- Throttle for "skipping placement" log (avoid flooding when no_trade_window or no client). ---
_last_90s_skip_placement_log_ts: float = 0.0
_LAST_90S_SKIP_PLACEMENT_LOG_INTERVAL: float = 60.0

# --- Post-placement telemetry (MAE & full trajectory). Key = (market_id, asset). ---
# Value: {"order_id", "trajectory": [{sec_to_close, dist, bid}, ...], "lowest_dist", "lowest_bid"}
# Flush on window close (market_id changed) or on stop-loss; single UPDATE by order_id.
_post_placement_trajectory: Dict[Tuple[str, str], Dict[str, Any]] = {}
_post_placement_trajectory_lock = threading.Lock()

# --- Pre-placement telemetry (context accumulation). Key = (market_id, asset). ---
# List of {sec, dist_krak, dist_cb, bid, ask} per tick; written to row on place or on resolve (aggregated skip).
_pre_placement_trajectory: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
_pre_placement_trajectory_lock = threading.Lock()


def _skip_aggregate_record(
    market_id: str,
    asset: str,
    bid: Optional[int],
    distance: Optional[float],
    skip_reason: str,
    *,
    ticker: Optional[str] = None,
    side: Optional[str] = None,
    spot: Optional[float] = None,
    strike: Optional[float] = None,
    seconds_to_close: Optional[float] = None,
    min_distance_threshold: Optional[float] = None,
    db_path: Optional[str] = None,
) -> None:
    """On skip: update in-memory tracker and persist to DB so resolve can build skip_details when window closes (or after restart).
    If in-memory is empty, hydrate from DB first so we merge with data from a previous process run (e.g. restart mid-window)."""
    key = (market_id, asset.lower())
    with _last_90s_skip_aggregator_lock:
        if key not in _last_90s_skip_aggregator:
            # Hydrate from DB so we don't lose history after process restart mid-window
            seed = None
            if db_path:
                seed = get_last_90s_skip_aggregator(db_path, market_id, asset)
            if seed:
                rec = {
                    "check_count": int(seed.get("check_count", 0)),
                    "bid_history": list(seed.get("bid_history") or []),
                    "distance_history": list(seed.get("distance_history") or []),
                    "skip_reasons": dict(seed.get("skip_reasons") or {}),
                    "last_ticker": seed.get("last_ticker"),
                    "last_side": seed.get("last_side"),
                    "last_spot": seed.get("last_spot"),
                    "last_strike": seed.get("last_strike"),
                    "last_seconds_to_close": seed.get("last_seconds_to_close"),
                    "last_min_distance_threshold": seed.get("last_min_distance_threshold"),
                }
                _last_90s_skip_aggregator[key] = rec
            else:
                _last_90s_skip_aggregator[key] = {
                    "check_count": 0,
                    "bid_history": [],
                    "distance_history": [],
                    "skip_reasons": {},
                    "last_ticker": None,
                    "last_side": None,
                    "last_spot": None,
                    "last_strike": None,
                    "last_seconds_to_close": None,
                    "last_min_distance_threshold": None,
                }
        rec = _last_90s_skip_aggregator[key]
        rec["check_count"] += 1
        rec["bid_history"].append(bid if bid is not None else 0)
        rec["distance_history"].append(distance if distance is not None else 0.0)
        rec["skip_reasons"][skip_reason] = rec["skip_reasons"].get(skip_reason, 0) + 1
        if ticker is not None:
            rec["last_ticker"] = ticker
        if side is not None:
            rec["last_side"] = side
        if spot is not None:
            rec["last_spot"] = spot
        if strike is not None:
            rec["last_strike"] = strike
        if seconds_to_close is not None:
            rec["last_seconds_to_close"] = seconds_to_close
        if min_distance_threshold is not None:
            rec["last_min_distance_threshold"] = min_distance_threshold
        if db_path:
            persist_last_90s_skip_aggregator(db_path, market_id, asset, _copy_aggregator_rec(rec))
    # Debug hook: show what key we just wrote and what keys exist in memory.
    logging.getLogger(__name__).info(
        "[DEBUG_SKIP_WRITE] Recorded skip for market_id=%s, asset=%s. In-memory keys: %s",
        market_id,
        asset,
        list(_last_90s_skip_aggregator.keys()),
    )


def _copy_aggregator_rec(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of aggregator record for persistence (no shared mutable refs)."""
    return {
        "check_count": rec.get("check_count", 0),
        "bid_history": list(rec.get("bid_history") or []),
        "distance_history": list(rec.get("distance_history") or []),
        "skip_reasons": dict(rec.get("skip_reasons") or {}),
        "last_ticker": rec.get("last_ticker"),
        "last_side": rec.get("last_side"),
        "last_spot": rec.get("last_spot"),
        "last_strike": rec.get("last_strike"),
        "last_seconds_to_close": rec.get("last_seconds_to_close"),
        "last_min_distance_threshold": rec.get("last_min_distance_threshold"),
    }


def _skip_aggregate_peek(market_id: str, asset: str) -> Optional[Dict[str, Any]]:
    """Return a copy of the aggregated record without clearing. Use for formatting when we may not place (e.g. OBSERVE)."""
    key = (market_id, asset.lower())
    with _last_90s_skip_aggregator_lock:
        rec = _last_90s_skip_aggregator.get(key)
    if not rec:
        return None
    return {
        "check_count": rec.get("check_count", 0),
        "bid_history": list(rec.get("bid_history") or []),
        "distance_history": list(rec.get("distance_history") or []),
        "skip_reasons": dict(rec.get("skip_reasons") or {}),
        "last_ticker": rec.get("last_ticker"),
        "last_side": rec.get("last_side"),
        "last_spot": rec.get("last_spot"),
        "last_strike": rec.get("last_strike"),
        "last_seconds_to_close": rec.get("last_seconds_to_close"),
        "last_min_distance_threshold": rec.get("last_min_distance_threshold"),
    }


def _skip_aggregate_get_without_clear(market_id: str, asset: str, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Return aggregated payload for skip_details WITHOUT clearing. Used in resolution so we can write the row first.
    If in-memory is empty (e.g. after restart), load from last_90s_skip_aggregator DB table by prev_market_id."""
    key = (market_id, asset.lower())
    with _last_90s_skip_aggregator_lock:
        rec = _last_90s_skip_aggregator.get(key)
    if rec is not None:
        return _copy_aggregator_rec(rec)
    if db_path:
        return get_last_90s_skip_aggregator(db_path, market_id, asset)
    return None


def _skip_aggregate_clear_only(market_id: str, asset: str, db_path: Optional[str] = None) -> None:
    """Remove aggregator data for (market_id, asset) from memory and DB. Call only AFTER write_row_last_90s has succeeded (resolution), or after placement to clear current window."""
    key = (market_id, asset.lower())
    with _last_90s_skip_aggregator_lock:
        _last_90s_skip_aggregator.pop(key, None)
    if db_path:
        clear_last_90s_skip_aggregator(db_path, market_id, asset)


def _skip_aggregate_get_and_clear(market_id: str, asset: str, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Return aggregated payload and clear. ONLY call during resolution phase, and only AFTER write_row_last_90s has successfully finished (use get_without_clear then clear_only instead)."""
    rec = _skip_aggregate_get_without_clear(market_id, asset, db_path)
    _skip_aggregate_clear_only(market_id, asset, db_path)
    return rec


def _skip_aggregate_normalize_rec(rec: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Ensure aggregator record has expected keys (e.g. when loaded from DB)."""
    if not rec:
        return None
    return {
        "check_count": rec.get("check_count", 0),
        "bid_history": list(rec.get("bid_history") or []),
        "distance_history": list(rec.get("distance_history") or []),
        "skip_reasons": dict(rec.get("skip_reasons") or {}),
        "last_ticker": rec.get("last_ticker"),
        "last_side": rec.get("last_side"),
        "last_spot": rec.get("last_spot"),
        "last_strike": rec.get("last_strike"),
        "last_seconds_to_close": rec.get("last_seconds_to_close"),
        "last_min_distance_threshold": rec.get("last_min_distance_threshold"),
    }


def _skip_aggregate_format_details(rec: Optional[Dict[str, Any]], prefix: str = "Pre-placement history") -> str:
    """Format aggregator record into skip_details string. Uses normalized rec so DB-loaded payloads work."""
    if not rec:
        return ""
    rec = _skip_aggregate_normalize_rec(rec) or rec
    check_count = rec.get("check_count", 0)
    bid_history = rec.get("bid_history", [])
    distance_history = rec.get("distance_history", [])
    reasons = rec.get("skip_reasons", {})
    most_frequent = max(reasons.items(), key=lambda x: x[1])[0] if reasons else "unknown"
    return (
        f"{prefix}: Checks: {check_count} | Bids: {bid_history} | Distances: {distance_history} | Primary blocker: {most_frequent}"
    )


_price_shadow_logger: Optional[logging.Logger] = None
_price_shadow_logger_lock = threading.Lock()


def _price_shadow_enabled() -> bool:
    """Return whether Coinbase shadow monitoring is enabled (env-toggle)."""
    val = os.getenv("LAST90S_PRICE_SHADOW_ENABLED", "1").strip().lower()
    return val in ("1", "true", "yes", "on")


def _price_shadow_threshold_pct() -> float:
    """Percentage difference threshold (Kraken vs Coinbase) for warnings."""
    raw = os.getenv("LAST90S_PRICE_SHADOW_THRESHOLD_PCT", "0.15")
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return 0.15


def _price_shadow_log_path() -> str:
    """Path for the price disagreement log file."""
    return os.getenv("LAST90S_PRICE_SHADOW_LOG_FILE", "price_disagreement_log.txt")


def _get_price_shadow_logger() -> logging.Logger:
    """Lazy-init logger that writes to the dedicated disagreement log file."""
    global _price_shadow_logger
    if _price_shadow_logger is not None:
        return _price_shadow_logger
    with _price_shadow_logger_lock:
        if _price_shadow_logger is not None:
            return _price_shadow_logger
        logger = logging.getLogger("price_shadow_monitor")
        logger.setLevel(logging.WARNING)
        log_path = _price_shadow_log_path()
        try:
            os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
        except Exception:
            # If directory creation fails, fall back to current directory.
            log_path = "price_disagreement_log.txt"
        try:
            handler = logging.FileHandler(log_path)
            handler.setLevel(logging.WARNING)
            formatter = logging.Formatter("%(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        except Exception:
            # If we cannot create the file handler, fall back to the root logger.
            logger = logging.getLogger(__name__)
        _price_shadow_logger = logger
        return logger


def _coinbase_symbol(asset: str) -> Optional[str]:
    """Map internal asset name to Coinbase symbol (e.g. BTC-USD)."""
    a = (asset or "").strip().upper()
    if a in ("BTC", "ETH", "SOL", "XRP"):
        return f"{a}-USD"
    return None


def _get_coinbase_spot_price(asset: str) -> Optional[float]:
    """Fetch spot price for asset from Coinbase public API. Returns None on failure."""
    symbol = _coinbase_symbol(asset)
    if not symbol:
        return None
    url = f"https://api.coinbase.com/v2/prices/{symbol}/spot"
    try:
        resp = requests.get(url, timeout=0.75)
        if resp.status_code != 200:
            return None
        data = resp.json()
        amount = (((data or {}).get("data") or {}).get("amount"))
        if amount is None:
            return None
        return float(amount)
    except Exception:
        return None


def _shadow_check_spot_disagreement(asset: str, kraken_price: Optional[float]) -> None:
    """Background checker: compare Kraken spot vs Coinbase; log large disagreements.

    This is designed to be fire-and-forget: all errors are swallowed and never
    interfere with the main trading loop.
    """
    if not _price_shadow_enabled():
        return
    try:
        if kraken_price is None:
            return
        cb_price = _get_coinbase_spot_price(asset)
        if cb_price is None or cb_price <= 0:
            return
        if kraken_price <= 0:
            return
        diff_pct = abs(kraken_price - cb_price) / kraken_price * 100.0
        threshold = _price_shadow_threshold_pct()
        if diff_pct <= threshold:
            return
        ts = datetime.now(timezone.utc).isoformat()
        logger = _get_price_shadow_logger()
        logger.warning(
            "[%s] [%s] WARNING: Kraken Spot = $%.4f | Coinbase Spot = $%.4f | Diff = %.4f%%",
            ts,
            (asset or "").upper(),
            kraken_price,
            cb_price,
            diff_pct,
        )
    except Exception:
        # Never propagate errors from the shadow monitor.
        return


def _get_spot_price(asset: str) -> Optional[float]:
    """Fetch spot price for asset (Kraken) and asynchronously shadow-check via Coinbase.

    Returns the Kraken price (or None on failure). Coinbase is used only for
    monitoring and never feeds into trading decisions.
    """
    price: Optional[float] = None
    try:
        from src.client.kraken_client import KrakenClient

        client = KrakenClient()
        a = str(asset).lower()
        if a == "eth":
            price = client.latest_eth_price().price
        elif a == "sol":
            price = client.latest_sol_price().price
        elif a == "xrp":
            price = client.latest_xrp_price().price
        else:
            price = client.latest_btc_price().price
    except Exception:
        price = None

    # Fire-and-forget Coinbase shadow monitor in a background thread so we never
    # block or slow down the main Kalshi trading loop.
    try:
        t = threading.Thread(
            target=_shadow_check_spot_disagreement,
            args=(asset, price),
            daemon=True,
        )
        t.start()
    except Exception:
        # If thread creation fails, ignore; trading should continue unaffected.
        pass

    return price


def _assets(cfg: dict, config: dict) -> List[str]:
    """Assets for last-90s (default BTC, ETH, SOL, XRP)."""
    a = cfg.get("assets")
    if a is not None and isinstance(a, (list, tuple)):
        return [str(x).lower() for x in a if str(x).lower() in ("btc", "eth", "sol", "xrp")]
    return [str(x).lower() for x in (config.get("assets") or ["btc", "eth", "sol", "xrp"]) if str(x).lower() in ("btc", "eth", "sol", "xrp")]


def _max_cost_cents(cfg: dict, config: dict, asset: str) -> int:
    """Resolve max_cost_cents for this asset."""
    by_asset = (cfg.get("max_cost_cents_by_asset") or {}) or (config.get("order", {}).get("fifteen_min") or {}).get("max_cost_cents_by_asset") or {}
    base = cfg.get("max_cost_cents") or (config.get("order", {}).get("fifteen_min") or {}).get("max_cost_cents") or 1000
    return int(by_asset.get(str(asset).lower(), base))


def _fmt_price(val: Optional[float], default: str = "n/a") -> str:
    """Format price/distance for logging: 4 decimals when value is small (e.g. XRP/SOL), else 2. Helps debug min_distance_at_placement for low-price assets."""
    if val is None:
        return default
    try:
        v = float(val)
        if v < 20:  # XRP ~1.x, SOL ~80; show 4 decimals so 0.0075 is visible
            return "%.4f" % v
        return "%.2f" % v
    except (TypeError, ValueError):
        return default


def _min_distance_at_placement(cfg: dict, asset: str) -> float:
    """Min distance_from_strike (USD) required to place; 0 = no filter. From min_distance_at_placement by asset.
    For XRP: strike/spot are typically 2–4 decimals (e.g. 1.30); Kraken spot is full float. distance = |spot - strike|
    can be sub-cent. A value like 0.0075 (0.75¢) requires spot to be at least that far from strike, avoiding at-the-money risk."""
    val = _get_asset_config(cfg.get("min_distance_at_placement"), asset, 0.0)
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _get_asset_config(config_val: Any, asset: str, default_val: Any) -> Any:
    """Resolve config value that may be a scalar or a dict keyed by asset (e.g. stop_loss_pct: 10 or { btc: 10, eth: 15 })."""
    if config_val is None:
        return default_val
    if isinstance(config_val, dict):
        a = (asset or "").strip().lower()
        if a in config_val:
            return config_val[a]
        if asset in config_val:
            return config_val[asset]
        return default_val
    return config_val


def _stop_loss_distance_factor(cfg: dict, asset: str) -> float:
    """Fraction of min_distance_at_placement below which we allow stop loss (e.g. 0.8 = 80%). Per-asset if config is dict. Default 0.8."""
    v = _get_asset_config(cfg.get("stop_loss_distance_factor"), asset, 0.8)
    if v is None:
        return 0.8
    try:
        f = float(v)
        if f <= 0 or f > 1.0:
            return 0.8
        return f
    except (TypeError, ValueError):
        return 0.8


def _get_time_decay_multiplier(seconds_to_close: float) -> float:
    """Piecewise time-decay multiplier for distance thresholds based on seconds_to_close."""
    try:
        s = float(seconds_to_close)
    except (TypeError, ValueError):
        return 1.0
    if s >= 55:
        return 1.0
    if s >= 45:
        return 0.8
    if s >= 35:
        return 0.6
    if s >= 25:
        return 0.4
    return 0.2


def _get_bids(quote: Optional[Any]) -> tuple:
    """Return (yes_bid, no_bid) as ints; 0 if missing."""
    if not quote:
        return 0, 0
    yes_bid = getattr(quote, "yes_bid", None) or (quote.get("yes_bid") if isinstance(quote, dict) else None)
    no_bid = getattr(quote, "no_bid", None) or (quote.get("no_bid") if isinstance(quote, dict) else None)
    return int(yes_bid) if yes_bid is not None else 0, int(no_bid) if no_bid is not None else 0


def _get_asks(quote: Optional[Any]) -> tuple:
    """Return (yes_ask, no_ask) as ints; 0 if missing."""
    if not quote:
        return 0, 0
    yes_ask = getattr(quote, "yes_ask", None) or (quote.get("yes_ask") if isinstance(quote, dict) else None)
    no_ask = getattr(quote, "no_ask", None) or (quote.get("no_ask") if isinstance(quote, dict) else None)
    return int(yes_ask) if yes_ask is not None else 0, int(no_ask) if no_ask is not None else 0


def _choose_side(
    cfg: dict,
    quote: Optional[Any],
    logger: logging.Logger,
    asset: str,
) -> str:
    """
    Choose limit order side: place on the side where the bet (bid) is higher.
    Config: side = "auto" | "yes" | "no". Default "auto": yes if yes_bid >= no_bid, else no.
    """
    side_cfg = (cfg.get("side") or "auto").strip().lower()
    if side_cfg == "yes":
        return "yes"
    if side_cfg == "no":
        return "no"
    if not quote:
        return "yes"
    yes_bid, no_bid = _get_bids(quote)
    chosen = "yes" if yes_bid >= no_bid else "no"
    logger.debug(
        "[last_90s] [%s] side=auto yes_bid=%s no_bid=%s -> %s",
        asset.upper(), yes_bid, no_bid, chosen,
    )
    return chosen


def _maybe_stop_loss(
    config: dict,
    logger: logging.Logger,
    kalshi_client: Optional[Any],
    db_path: str,
    asset: str,
    market_id: str,
    stop_loss_pct: float,
    cfg: dict,
    db_lock: Optional[threading.Lock] = None,
    ticker: Optional[str] = None,
) -> bool:
    """
    If we have filled last_90s orders for this (market_id, asset) and any position is
    down >= stop_loss_pct (e.g. 10%) while the underlying has moved back inside the
    danger zone, sell to close and record stop_loss_triggered for all open orders.
    Returns True if any stop-loss was triggered (caller should flush post-placement telemetry).
    Uses Kalshi WebSocket orderbook via ticker; if get_safe_orderbook(ticker) is None, skips (no REST).
    """
    if not kalshi_client or stop_loss_pct <= 0:
        return False

    with _db_section(db_lock):
        if get_last_90s_stop_loss_triggered(db_path, market_id, asset):
            return False
        active_orders = get_last_90s_placements_for_stoploss(db_path, market_id, asset)

    if not active_orders:
        return False

    # Current market snapshot from Kalshi WebSocket (no REST).
    quote = get_safe_orderbook(ticker) if ticker else None
    if quote is None:
        return False
    yes_bid, no_bid = _get_bids(quote)

    # Time-decayed distance threshold for danger zone.
    base_min_dist = _min_distance_at_placement(cfg, asset)
    stop_loss_dist_factor = _stop_loss_distance_factor(cfg, asset)
    distance_now = None
    secs_to_close = None
    if base_min_dist > 0:
        try:
            m_sl = fetch_15min_market(market_id)
            # All last_90s orders for an asset/window use the same underlying ticker.
            first_ticker = active_orders[0][1]
            strike_sl = extract_strike_from_market(m_sl, first_ticker) if m_sl else 0.0
            # DEFENSE GATE: Strict Dual Oracle = min(Kraken, Coinbase). Prefer WebSocket when enabled; fallback to REST.
            spot_k = None
            spot_cb = None
            use_ws_oracles = bool(cfg.get("use_ws_oracles", False))
            if use_ws_oracles and strike_sl and strike_sl > 0:
                try:
                    from bot.oracle_ws_manager import get_safe_spot_prices_sync, is_ws_running
                    if is_ws_running():
                        data = get_safe_spot_prices_sync(asset, max_age_seconds=3.0, require_both=False)
                        if data is not None:
                            spot_k = data.get("kraken")
                            spot_cb = data.get("cb")
                except Exception as e:
                    logger.debug("[last_90s] [%s] Stop loss WS oracle read failed, falling back to REST: %s", asset.upper(), e)
            if not use_ws_oracles:
                if spot_k is None:
                    spot_k = _get_spot_price(asset)
                if spot_cb is None:
                    try:
                        spot_cb = _get_coinbase_spot_price(asset)
                    except Exception:
                        spot_cb = None
            dist_k = abs(float(spot_k) - float(strike_sl)) if strike_sl and strike_sl > 0 and spot_k is not None else None
            dist_cb = abs(float(spot_cb) - float(strike_sl)) if strike_sl and strike_sl > 0 and spot_cb is not None else None
            # Avoid min(None, x) / min(x, None) TypeError when WS has no data yet
            if dist_k is not None and dist_cb is not None:
                distance_now = min(dist_k, dist_cb)
            elif dist_k is not None:
                distance_now = dist_k
            else:
                distance_now = dist_cb
        except Exception as e:
            logger.warning("[last_90s] [%s] Distance fetch for stop loss failed (ignoring distance filter): %s", asset.upper(), e)
    try:
        secs_to_close = get_minutes_to_close_15min(market_id) * 60.0
    except Exception:
        secs_to_close = None

    panic_seconds = int(cfg.get("panic_seconds", 30))
    in_panic_zone = secs_to_close is not None and secs_to_close < panic_seconds
    panic_loss_threshold = max(stop_loss_pct, 0.50)

    # Outside the panic zone we still respect the distance-based danger threshold.
    if (not in_panic_zone) and base_min_dist > 0 and distance_now is not None and secs_to_close is not None:
        decay_mult = _get_time_decay_multiplier(secs_to_close)
        decayed_min_dist = base_min_dist * decay_mult
        danger_threshold = decayed_min_dist * stop_loss_dist_factor
        if distance_now > danger_threshold:
            logger.info(
                "[last_90s] [%s] HOLD: distance_from_strike=%.2f > danger_threshold=%.2f (base_min=%.2f decay=%.2f factor=%.2f) — treat as temporary reversal (outside panic zone)",
                asset.upper(),
                distance_now,
                danger_threshold,
                base_min_dist,
                decay_mult,
                stop_loss_dist_factor,
            )
            return False

    mode = (config.get("mode", "OBSERVE")).upper()
    any_triggered = False

    for order_id, ticker, count, limit_price_cents, side in active_orders:
        if not order_id or not ticker or count <= 0:
            continue
        try:
            order = kalshi_client.get_order(order_id)
            fill_count = int(order.get("fill_count") or order.get("filled_count") or 0)
        except Exception as e:
            logger.warning("[last_90s] [%s] get_order %s for stop loss: %s", asset.upper(), order_id, e)
            continue
        if fill_count < 1:
            continue

        side_l = (side or "yes").lower()
        current_bid = yes_bid if side_l == "yes" else no_bid
        current_bid = current_bid or 0
        # Redundancy: if Kalshi bid crashed below 50¢, panic-sell immediately regardless of Oracle distance.
        panic_sell_bid = current_bid < PANIC_SELL_BID_CENTS
        entry = max(1, limit_price_cents or 99)
        loss_pct = (entry - current_bid) / entry if entry else 0.0
        loss_threshold = panic_loss_threshold if in_panic_zone else stop_loss_pct
        if not panic_sell_bid and loss_pct < loss_threshold:
            continue

        sell_price = max(1, min(current_bid, 99)) if current_bid else 1
        if panic_sell_bid:
            logger.warning(
                "[last_90s] [%s] PANIC_SELL: Kalshi bid=%dc < %dc; selling %s x %d immediately for %s",
                asset.upper(), current_bid, PANIC_SELL_BID_CENTS, side_l, fill_count, ticker,
            )
        if mode != "TRADE":
            logger.info(
                "[last_90s] [%s] OBSERVE: would stop loss sell %s x %d (market) (entry=%dc loss=%.1f%%) for %s",
                asset.upper(),
                side_l,
                fill_count,
                entry,
                loss_pct * 100,
                ticker,
            )
            continue

        # Use market order so we exit at best available bid when market is crashing (limit at current_bid may not fill).
        try:
            resp = kalshi_client.place_market_order(
                ticker=ticker,
                action="sell",
                side=side_l,
                count=fill_count,
                client_order_id=f"last90s:{uuid.uuid4().hex[:12]}",
            )
        except Exception as e:
            logger.exception("[last_90s] [%s] Stop loss error for %s: %s", asset.upper(), order_id, e)
            continue

        ok = resp and (isinstance(resp, dict) or getattr(resp, "status_code", 0) in (200, 201))
        if not ok:
            err = (resp.get("message") or resp.get("error") or str(resp)) if isinstance(resp, dict) else (getattr(resp, "text", None) or getattr(resp, "reason", str(resp)))
            logger.warning("[last_90s] [%s] Stop loss sell failed for %s: %s", asset.upper(), order_id, err)
            continue

        any_triggered = True
        # Market fill price unknown until order fills; use current_bid as estimate for reporting, pnl may be updated on resolve.
        resolution_price_estimate = sell_price
        pnl_stop = (resolution_price_estimate - entry) * fill_count if (resolution_price_estimate and entry and fill_count) else None
        with _db_section(db_lock):
            try:
                update_resolution_last_90s(
                    db_path,
                    order_id=order_id,
                    window_id=market_id,
                    asset=asset,
                    filled=fill_count,
                    fill_price=entry,
                    resolution_price=resolution_price_estimate,
                    final_outcome=None,
                    pnl_cents=pnl_stop,
                    is_stop_loss=1,
                    stop_loss_sell_price=sell_price,
                    is_filled=1,
                )
            except Exception as e:
                logger.warning(
                    "[last_90s] [%s] Report update_resolution_last_90s (stop loss) failed for %s: %s",
                    asset.upper(),
                    order_id,
                    e,
                )

        logger.info(
            "[last_90s] [%s] STOP_LOSS market sell %s x %d for %s (entry=%dc bid_was=%dc loss=%.1f%%)",
            asset.upper(),
            side_l,
            fill_count,
            ticker,
            entry,
            sell_price,
            loss_pct * 100,
        )
        try:
            m_sl = fetch_15min_market(market_id)
            strike_sl = extract_strike_from_market(m_sl, ticker) if m_sl else 0.0
            use_ws_sl_report = bool(cfg.get("use_ws_oracles", False))
            if use_ws_sl_report:
                from bot.oracle_ws_manager import get_safe_spot_prices_sync, is_ws_running
                data_slr = get_safe_spot_prices_sync(asset, max_age_seconds=3.0, require_both=False) if is_ws_running() else None
                spot_sl = data_slr.get("kraken") if data_slr else None
            else:
                spot_sl = _get_spot_price(asset)
            win_sec = int(_get_asset_config(cfg.get("window_seconds"), asset, 90))
            min_bid = int(cfg.get("min_bid_cents", 90))
            _stop_loss_report(
                logger,
                asset,
                market_id,
                ticker,
                side_l,
                entry,
                sell_price,
                fill_count,
                loss_pct,
                stop_loss_pct,
                secs_to_close,
                strike_sl,
                spot_sl,
                win_sec,
                min_bid,
                order_id,
            )
        except Exception as e_sl:
            logger.warning("[last_90s] [%s] Stop loss report failed for %s: %s", asset.upper(), order_id, e_sl)

    if any_triggered:
        with _db_section(db_lock):
            set_last_90s_stop_loss_triggered(db_path, market_id, asset)
    return any_triggered


def _flush_post_placement_telemetry(
    db_path: str,
    market_id: str,
    asset: str,
    db_lock: Optional[threading.Lock] = None,
) -> None:
    """Flush in-memory trajectory for (market_id, asset) to DB (single UPDATE by order_id)."""
    with _post_placement_trajectory_lock:
        entry = _post_placement_trajectory.pop((market_id, asset.lower()), None)
    if not entry:
        return
    order_id = entry.get("order_id")
    if not order_id:
        return
    trajectory = entry.get("trajectory") or []
    lowest_dist = entry.get("lowest_dist", float("inf"))
    lowest_bid = entry.get("lowest_bid", 100)
    min_dist_val = None if lowest_dist == float("inf") else lowest_dist
    with _db_section(db_lock):
        try:
            update_post_placement_telemetry(
                db_path,
                order_id,
                min_dist_after_placement=min_dist_val,
                min_bid_after_placement=lowest_bid,
                post_placement_history=json.dumps(trajectory) if trajectory else None,
            )
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.warning(
                "[last_90s] [%s] Post-placement telemetry update failed for %s: %s",
                asset.upper(),
                order_id,
                e,
            )


def _limit_price_from_quote(quote: Optional[Any], side: str, default_cents: int = 99) -> int:
    """
    Use the bid for the chosen side as limit price; if that bid is 100 or missing, use default_cents (e.g. 99).
    Returns value in [1, 99].
    """
    yes_bid, no_bid = _get_bids(quote)
    bid = yes_bid if (side or "yes").lower() == "yes" else no_bid
    if bid is None or bid <= 0:
        return max(1, min(default_cents, 99))
    if bid >= 100:
        return max(1, min(default_cents, 99))
    return max(1, min(bid, 99))


def _resolve_and_log_outcomes(
    config: dict,
    logger: logging.Logger,
    kalshi_client: Optional[Any],
    db_path: str,
    db_lock: Optional[threading.Lock] = None,
) -> None:
    """
    For the previous 15-min market (just closed) per asset, resolve order status and
    market result; log LAST_90S_OUTCOME per order and LAST_90S_MARKET_SUMMARY per market.
    """
    cfg = _get_cfg(config)
    if not cfg or not cfg.get("enabled", False):
        return
    assets = _assets(cfg, config)
    if not assets:
        return
    markets_resolved = set()
    # Log once per run what we consider "previous" window (for debugging missing DB rows)
    try:
        _first_asset = (assets or ["btc"])[0]
        _cur = get_current_15min_market_id(asset=_first_asset)
        _prev = get_previous_15min_market_id(asset=_first_asset)
        logger.info(
            "[last_90s] Resolve pass: current_15min_market=%s previous_15min_market=%s (rows written for previous)",
            _cur,
            _prev,
        )
    except Exception:
        pass
    for asset in assets:
        try:
            prev_market_id = get_previous_15min_market_id(asset=asset)
            if not prev_market_id:
                continue
            with _db_section(db_lock):
                placed_prev = get_last_90s_placed(db_path, prev_market_id, asset)
            if not placed_prev:
                # Expiration: never placed for this (prev_market_id, asset). Before writing an aggregated row,
                # check whether one already exists in the report table so we don't overwrite good skip_details
                # with the fallback on subsequent resolution passes.
                try:
                    import sqlite3

                    conn_chk = sqlite3.connect(db_path)
                    try:
                        cur_chk = conn_chk.cursor()
                        cur_chk.execute(
                            "SELECT 1 FROM strategy_report_last_90s WHERE window_id = ? AND asset = ? AND placed = 0 LIMIT 1",
                            (prev_market_id, str(asset).lower()),
                        )
                        if cur_chk.fetchone():
                            logger.info(
                                "[DEBUG_SKIP_READ] Aggregated skip row already exists for %s / %s; skipping rewrite.",
                                prev_market_id,
                                asset,
                            )
                            continue
                    finally:
                        conn_chk.close()
                except Exception as e_chk:
                    logger.warning(
                        "[last_90s] DB_CHECK_FAIL: existing aggregated row window_id=%s asset=%s error=%s",
                        prev_market_id,
                        asset,
                        e_chk,
                    )

                # Expiration and no existing aggregated row: write one aggregated row. Do NOT clear skip
                # aggregator until after write succeeds.
                logger.info(
                    "[DEBUG_SKIP_READ] Resolving placed=0. Attempting to fetch skips for prev_market_id=%s, asset=%s",
                    prev_market_id,
                    asset,
                )
                # Snapshot in-memory keys at the exact moment of resolution.
                with _last_90s_skip_aggregator_lock:
                    current_memory_keys = list(_last_90s_skip_aggregator.keys())
                logger.info(
                    "[DEBUG_SKIP_READ] Current in-memory skip-aggregator keys: %s",
                    current_memory_keys,
                )

                rec = _skip_aggregate_get_without_clear(prev_market_id, asset, db_path)
                if rec is None:
                    logger.info(
                        "[last_90s] No skip-aggregator data for %s / %s (window may not have been entered or all ticks returned early e.g. stale WS); writing row with fallback details.",
                        prev_market_id,
                        asset,
                    )
                else:
                    logger.info(
                        "[DEBUG_SKIP_SUCCESS] Successfully fetched skip details for %s / %s.",
                        prev_market_id,
                        asset,
                    )
                    rec = _skip_aggregate_normalize_rec(rec) or rec
                details = _skip_aggregate_format_details(rec, prefix="Aggregated skips") if rec else "No skip history (window not entered or all ticks returned early e.g. stale oracle)"
                last_bid = None
                last_distance = None
                if rec:
                    bid_hist = rec.get("bid_history") or []
                    dist_hist = rec.get("distance_history") or []
                    last_bid = bid_hist[-1] if bid_hist else None
                    last_distance = dist_hist[-1] if dist_hist else None
                ticker_out = rec.get("last_ticker") if rec else None
                side_out = rec.get("last_side") if rec else "yes"
                spot_out = rec.get("last_spot") if rec else None
                strike_out = rec.get("last_strike") if rec else None
                seconds_out = rec.get("last_seconds_to_close") if rec else None
                min_dist_out = rec.get("last_min_distance_threshold") if rec else None
                need_fallback = not rec or ticker_out is None or (spot_out is None and strike_out is None)
                if need_fallback:
                    try:
                        m_prev = fetch_15min_market(prev_market_id)
                        if m_prev:
                            if not ticker_out:
                                ticker_out = m_prev.get("ticker") or (prev_market_id + "-00")
                            if strike_out is None and ticker_out:
                                strike_out = extract_strike_from_market(m_prev, ticker_out)
                        if spot_out is None:
                            use_ws_resolve = bool(cfg.get("use_ws_oracles", False))
                            if use_ws_resolve:
                                from bot.oracle_ws_manager import get_safe_spot_prices_sync, is_ws_running
                                data_r = get_safe_spot_prices_sync(asset, max_age_seconds=5.0, require_both=False) if is_ws_running() else None
                                spot_out = data_r.get("kraken") if data_r else None
                            else:
                                spot_out = _get_spot_price(asset)
                    except Exception:
                        pass
                with _db_section(db_lock):
                    try:
                        logger.info(
                            "[last_90s] Writing aggregated skip row for window_id=%s asset=%s (placed=0)",
                            prev_market_id,
                            asset,
                        )
                        pre_placement_json = None
                        with _pre_placement_trajectory_lock:
                            pre_placement_list = _pre_placement_trajectory.pop((prev_market_id, asset.lower()), [])
                        if pre_placement_list:
                            pre_placement_json = json.dumps(pre_placement_list)
                        write_row_last_90s(
                            db_path,
                            prev_market_id,
                            asset,
                            placed=0,
                            skip_reason="aggregated_skips",
                            skip_details=details,
                            ticker=ticker_out,
                            side=side_out,
                            spot=spot_out,
                            strike=strike_out,
                            seconds_to_close=seconds_out,
                            distance=last_distance,
                            min_distance_threshold=min_dist_out,
                            bid=last_bid,
                            spot_kraken=spot_out,
                            spot_coinbase=None,
                            distance_kraken=last_distance,
                            distance_coinbase=None,
                            pre_placement_history=pre_placement_json,
                        )
                        _skip_aggregate_clear_only(prev_market_id, asset, db_path)
                    except Exception as e:
                        logger.warning(
                            "[last_90s] DB_WRITE_FAIL: aggregated skip row window_id=%s asset=%s error=%s",
                            prev_market_id,
                            asset.upper(),
                            e,
                        )
                continue
            with _db_section(db_lock):
                rows = get_last_90s_unresolved_for_market(db_path, prev_market_id)
            for row in rows:
                if len(row) >= 7:
                    _market_id, _asset, order_id, ticker, count, limit_price_cents, side = row[0], row[1], row[2], row[3], row[4], row[5], (row[6] or "yes")
                else:
                    _market_id, _asset, order_id, ticker, count, limit_price_cents = row[0], row[1], row[2], row[3], row[4], row[5]
                    side = "yes"
                if not order_id or not kalshi_client:
                    continue
                executed_count = 0
                order_status = "unknown"
                try:
                    order = kalshi_client.get_order(order_id)
                    order_status = str(order.get("status", "")).lower()
                    executed_count = int(order.get("fill_count") or order.get("filled_count") or 0)
                except Exception as e:
                    logger.warning("[last_90s] [%s] get_order %s failed: %s", _asset.upper(), order_id, e)
                market_result = fetch_15min_market_result(prev_market_id)
                side_l = (side or "yes").lower()
                if executed_count > 0 and market_result in ("yes", "no"):
                    outcome = "positive" if (side_l == "yes" and market_result == "yes") or (side_l == "no" and market_result == "no") else "negative"
                elif executed_count > 0:
                    outcome = "executed_unknown_result"
                else:
                    outcome = "unfilled"
                is_filled = 1 if executed_count > 0 else 0
                pnl = None
                if order_status in ("canceled", "expired") and executed_count == 0:
                    pnl = 0
                    is_filled = 0
                elif executed_count > 0 and limit_price_cents is not None:
                    if outcome == "positive":
                        pnl = (100 - limit_price_cents) * executed_count
                    elif outcome == "negative":
                        pnl = -limit_price_cents * executed_count
                with _db_section(db_lock):
                    update_last_90s_resolved(
                        db_path,
                        prev_market_id,
                        _asset,
                        executed_count=executed_count,
                        market_result=market_result,
                        outcome=outcome,
                        order_id=order_id,
                    )
                    try:
                        res_price = 100 if outcome == "positive" else (0 if outcome == "negative" else None)
                        final = "WIN" if outcome == "positive" else ("LOSS" if outcome == "negative" else None)
                        update_resolution_last_90s(
                            db_path,
                            order_id=order_id,
                            filled=executed_count or 0,
                            fill_price=limit_price_cents,
                            resolution_price=res_price,
                            final_outcome=final,
                            pnl_cents=pnl,
                            is_filled=is_filled,
                            respect_stop_loss=True,
                        )
                    except Exception as e:
                        logger.warning(
                            "[last_90s] DB_UPDATE_FAIL: update_resolution_last_90s window_id=%s asset=%s order_id=%s error=%s",
                            prev_market_id,
                            _asset.upper(),
                            order_id,
                            e,
                        )
                logger.info(
                    "[last_90s] OUTCOME [%s] market=%s order_id=%s side=%s placed=%d executed=%d status=%s result=%s outcome=%s",
                    _asset.upper(), prev_market_id, order_id, side_l, count or 0, executed_count, order_status, market_result or "?", outcome,
                )
                logger.info(
                    "%s",
                    _json_outcome("LAST_90S_OUTCOME", prev_market_id, _asset, order_id, count or 0, executed_count, order_status, market_result, outcome, side=side_l),
                )
                markets_resolved.add(prev_market_id)
        except Exception as e:
            logger.exception("[last_90s] RESOLVE_ERROR: asset=%s error=%s", asset, e)
    for market_id in markets_resolved:
        try:
            with _db_section(db_lock):
                _log_market_summary(logger, db_path, market_id)
        except Exception as e:
            logger.warning("[last_90s] Summary log error for %s: %s", market_id, e)


def _log_market_summary(logger: logging.Logger, db_path: str, market_id: str) -> None:
    """Log LAST_90S_MARKET_SUMMARY: orders_placed, orders_executed, orders_positive for this market."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*), SUM(CASE WHEN order_id IS NOT NULL AND order_id != '' THEN 1 ELSE 0 END),
                   COALESCE(SUM(CASE WHEN executed_count > 0 THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN outcome = 'positive' THEN 1 ELSE 0 END), 0)
            FROM last_90s_placed WHERE market_id = ?
            """,
            (market_id,),
        )
        row = cur.fetchone()
        if not row:
            return
        total, with_order_id, executed, positive = row
        with_order_id = with_order_id or 0
        executed = executed or 0
        positive = positive or 0
        logger.info(
            "[last_90s] SUMMARY market=%s | limit_orders_placed=%d | limit_orders_executed=%d | positive_outcomes=%d",
            market_id, with_order_id, executed, positive,
        )
        logger.info(
            "%s",
            _json_summary("LAST_90S_MARKET_SUMMARY", market_id, with_order_id, executed, positive),
        )
    finally:
        conn.close()


def _json_outcome(
    event: str,
    market_id: str,
    asset: str,
    order_id: str,
    placed_count: int,
    executed_count: int,
    order_status: str,
    market_result: Optional[str],
    outcome: str,
    side: str = "yes",
) -> str:
    import json
    return json.dumps({
        "event": event,
        "market_id": market_id,
        "asset": asset.upper(),
        "order_id": order_id,
        "side": side,
        "placed_count": placed_count,
        "executed_count": executed_count,
        "order_status": order_status,
        "market_result": market_result,
        "outcome": outcome,
    }, default=str)


def _json_summary(
    event: str,
    market_id: str,
    orders_placed: int,
    orders_executed: int,
    orders_positive: int,
) -> str:
    import json
    return json.dumps({
        "event": event,
        "market_id": market_id,
        "limit_orders_placed": orders_placed,
        "limit_orders_executed": orders_executed,
        "positive_outcomes": orders_positive,
    }, default=str)


def _run_last_90s_for_asset(
    asset: str,
    config: dict,
    logger: logging.Logger,
    kalshi_client: Optional[Any],
    db_path: str,
    db_lock: Optional[threading.Lock],
    cfg: dict,
    config_limit_cents: int,
    mode: str,
) -> None:
    """Process one asset: in-window check, stop-loss or fetch/place; all DB access under db_lock. Kalshi orderbook from WebSocket only (no REST)."""
    window_seconds = int(_get_asset_config(cfg.get("window_seconds"), asset, 90))
    monitor_window_seconds = int(_get_asset_config(cfg.get("monitor_window_seconds"), asset, 120))
    stop_loss_pct = float(_get_asset_config(cfg.get("stop_loss_pct"), asset, 10)) / 100.0  # e.g. 10 -> 0.10
    for _ in [0]:  # single-iteration loop so "continue" skips this tick without exiting the strategy
        try:
            market_id = get_current_15min_market_id(asset=asset)
        except Exception:
            continue
        m_tick = fetch_15min_market(market_id)
        ticker = m_tick.get("ticker") if m_tick else None
        if ticker:
            start_kalshi_ws()
            subscribe_to_tickers([ticker])
            quote_tick = get_safe_orderbook(ticker)
            # REST fallback when Kalshi WebSocket has no orderbook (e.g. WS not ready, reconnect, or no snapshot yet)
            if quote_tick is None and kalshi_client is not None:
                try:
                    quote_tick = kalshi_client.get_top_of_book(ticker)
                    if quote_tick is not None:
                        logger.debug("[last_90s] [%s] Kalshi orderbook from REST fallback (WS had no data)", asset.upper())
                except Exception as e:
                    logger.debug("[last_90s] [%s] REST orderbook fallback failed: %s", asset.upper(), e)
            if quote_tick is None:
                continue
        else:
            quote_tick = None
        # Flush post-placement telemetry for previous windows (same asset, different market_id).
        with _post_placement_trajectory_lock:
            to_flush = [(mid, a) for (mid, a) in _post_placement_trajectory if a == asset.lower() and mid != market_id]
        for (mid, a) in to_flush:
            _flush_post_placement_telemetry(db_path, mid, a, db_lock)
        try:
            minutes_to_close = get_minutes_to_close_15min(market_id)
            seconds_to_close = minutes_to_close * 60.0
            if seconds_to_close <= 0 or seconds_to_close > monitor_window_seconds:
                continue
            # Pre-placement telemetry: append one tick (sec, dist_krak, dist_cb, bid, ask) before any skip/place logic.
            # When use_ws_oracles: strict WS only (zero REST). Else REST.
            use_ws_oracles = bool(cfg.get("use_ws_oracles", False))
            if use_ws_oracles:
                spot_tick = None
                spot_cb_tick = None
                try:
                    from bot.oracle_ws_manager import get_safe_spot_prices_sync, is_ws_running
                    if is_ws_running():
                        data = get_safe_spot_prices_sync(asset, max_age_seconds=3.0, require_both=False)
                        if data is not None:
                            spot_tick = data.get("kraken")
                            spot_cb_tick = data.get("cb")
                except Exception:
                    pass
            else:
                spot_tick = _get_spot_price(asset)
                spot_cb_tick = None
                try:
                    spot_cb_tick = _get_coinbase_spot_price(asset)
                except Exception:
                    pass
            ticker_tick = m_tick.get("ticker") if m_tick else None
            strike_tick = extract_strike_from_market(m_tick, ticker_tick) if m_tick and ticker_tick else 0.0
            dist_krak = round(abs(spot_tick - strike_tick), 4) if spot_tick is not None and strike_tick and strike_tick > 0 else None
            dist_cb = round(abs(spot_cb_tick - strike_tick), 4) if spot_cb_tick is not None and strike_tick and strike_tick > 0 else None
            yes_bid_tick, no_bid_tick = _get_bids(quote_tick)
            yes_ask_tick, no_ask_tick = _get_asks(quote_tick)
            current_bid_tick = (yes_bid_tick if (yes_bid_tick or 0) >= (no_bid_tick or 0) else no_bid_tick) or 0
            current_ask_tick = (yes_ask_tick if (yes_bid_tick or 0) >= (no_bid_tick or 0) else no_ask_tick) or 0
            tick_data = {
                "sec": round(seconds_to_close, 2),
                "dist_krak": round(dist_krak, 4) if dist_krak is not None else None,
                "dist_cb": round(dist_cb, 4) if dist_cb is not None else None,
                "bid": current_bid_tick,
                "ask": current_ask_tick,
            }
            key_pp = (market_id, asset.lower())
            with _pre_placement_trajectory_lock:
                if key_pp not in _pre_placement_trajectory:
                    _pre_placement_trajectory[key_pp] = []
                _pre_placement_trajectory[key_pp].append(tick_data)
            # Action gate: only evaluate skips / place when inside the trading window.
            # Record that we're in monitor window but not yet in trading window so resolve has a reason if we never enter.
            if seconds_to_close > window_seconds:
                _skip_aggregate_record(
                    market_id,
                    asset,
                    current_bid_tick,
                    dist_krak if dist_krak is not None else dist_cb,
                    "outside_trading_window",
                    seconds_to_close=seconds_to_close,
                    db_path=db_path,
                )
                continue
            with _db_section(db_lock):
                placed = get_last_90s_placed(db_path, market_id, asset)
            # If we already have a placement for this (market_id, asset), run stop-loss checks
            # and record post-placement trajectory tick; flush telemetry on stop-loss.
            if placed:
                with _db_section(db_lock):
                    active_orders = get_last_90s_placements_for_stoploss(db_path, market_id, asset)
                if active_orders:
                    order_id, _ticker, _count, _limit_cents, side = active_orders[0]
                    side_l = (side or "yes").lower()
                    quote = quote_tick
                    yes_bid, no_bid = _get_bids(quote)
                    current_bid = yes_bid if side_l == "yes" else no_bid
                    current_bid = current_bid if current_bid is not None else 0
                    current_distance = None
                    try:
                        m_sl = fetch_15min_market(market_id)
                        strike_sl = extract_strike_from_market(m_sl, ticker) if m_sl else 0.0
                        if use_ws_oracles:
                            from bot.oracle_ws_manager import get_safe_spot_prices_sync, is_ws_running
                            data_sl = get_safe_spot_prices_sync(asset, max_age_seconds=3.0, require_both=False) if is_ws_running() else None
                            spot_sl = data_sl.get("kraken") if data_sl else None
                        else:
                            spot_sl = _get_spot_price(asset)
                        if strike_sl and strike_sl > 0 and spot_sl is not None:
                            current_distance = abs(float(spot_sl) - float(strike_sl))
                    except Exception:
                        pass
                    try:
                        secs_to_close = get_minutes_to_close_15min(market_id) * 60.0
                    except Exception:
                        secs_to_close = None
                    key = (market_id, asset.lower())
                    with _post_placement_trajectory_lock:
                        if key not in _post_placement_trajectory:
                            _post_placement_trajectory[key] = {
                                "order_id": order_id,
                                "trajectory": [],
                                "lowest_dist": float("inf"),
                                "lowest_bid": 100,
                            }
                        ent = _post_placement_trajectory[key]
                        ent["trajectory"].append({
                            "sec_to_close": round(secs_to_close, 2) if secs_to_close is not None else None,
                            "dist": round(current_distance, 4) if current_distance is not None else None,
                            "bid": current_bid,
                        })
                        if current_distance is not None:
                            ent["lowest_dist"] = min(ent["lowest_dist"], current_distance)
                        ent["lowest_bid"] = min(ent["lowest_bid"], current_bid)
                any_triggered = _maybe_stop_loss(
                    config,
                    logger,
                    kalshi_client,
                    db_path,
                    asset,
                    market_id,
                    stop_loss_pct,
                    cfg,
                    db_lock=db_lock,
                    ticker=ticker_tick,
                )
                if any_triggered:
                    _flush_post_placement_telemetry(db_path, market_id, asset, db_lock)
                continue
            m = m_tick if m_tick is not None else fetch_15min_market(market_id)
            if not m:
                logger.warning("[last_90s] [%s] No market for %s", asset.upper(), market_id)
                _skip_aggregate_record(
                    market_id, asset, None, None, "no_market",
                    seconds_to_close=seconds_to_close, db_path=db_path,
                )
                continue
            ticker = m.get("ticker")
            if not ticker:
                # Record skip reason so resolve has a specific blocker (not "No skip history").
                yes_b, no_b = _get_bids(quote_tick)
                bid_t = (yes_b or 0) if (yes_b or 0) >= (no_b or 0) else (no_b or 0) if quote_tick else None
                _skip_aggregate_record(
                    market_id, asset, bid_t, None, "no_ticker",
                    seconds_to_close=seconds_to_close, db_path=db_path,
                )
                continue
            quote = quote_tick
            yes_bid, no_bid = _get_bids(quote)
            side = _choose_side(cfg, quote, logger, asset)
            current_bid = yes_bid if (side or "yes").lower() == "yes" else no_bid
            min_bid_cents = int(cfg.get("min_bid_cents", 90))
            strike = extract_strike_from_market(m, ticker) if m else 0.0
            # Spot: use_ws_oracles enables Kraken/Coinbase WebSocket; when false we use REST only for oracles.
            # Orderbook: always from Kalshi WebSocket (get_safe_orderbook); no REST orderbook calls.
            use_ws_oracles = bool(cfg.get("use_ws_oracles", False))
            spot = None
            spot_cb = None
            used_ws_this_tick = False
            try:
                if use_ws_oracles:
                    # require_both=False: use at least one oracle (Kraken or Coinbase) to avoid no-oracle-data when one feed is slow.
                    from bot.oracle_ws_manager import get_safe_spot_prices_sync, is_ws_running
                    if is_ws_running():
                        data = get_safe_spot_prices_sync(asset, max_age_seconds=3.0, require_both=False)
                        if data is not None:
                            spot = data.get("kraken")
                            spot_cb = data.get("cb")
                            used_ws_this_tick = True
                        else:
                            logger.info(
                                "[last_90s] [%s] Stale or missing WS oracle data (max_age=3s); skipping REST (strict WS mode)",
                                asset.upper(),
                            )
                            # spot/spot_cb stay None; no REST fallback when use_ws_oracles
            except Exception as e:
                logger.warning("[last_90s] [%s] WS oracle read failed: %s (strict WS mode, no REST)", asset.upper(), e)
            # DEBUG_WS_1: Are we getting asset price via WS? (when use_ws_oracles=true)
            if use_ws_oracles:
                try:
                    from bot.oracle_ws_manager import is_ws_running
                    ws_up = is_ws_running()
                except Exception:
                    ws_up = False
                logger.info(
                    "[last_90s] [DEBUG_WS_1] [%s] use_ws_oracles=true | is_ws_running=%s | spot(kraken)=%s | spot_cb=%s | used_ws_this_tick=%s",
                    asset.upper(), ws_up, spot, spot_cb, used_ws_this_tick,
                )
            if not use_ws_oracles:
                if spot is None:
                    spot = _get_spot_price(asset)
                if spot_cb is None:
                    try:
                        spot_cb = _get_coinbase_spot_price(asset)
                    except Exception as e:
                        logger.warning("[last_90s] [%s] Coinbase spot failed for entry distance, using Kraken only: %s", asset.upper(), e)
            if used_ws_this_tick and spot is not None:
                logger.debug("[last_90s] [%s] Entry gate using WS spot kraken=%s cb=%s", asset.upper(), spot, spot_cb)
            dist_kraken = (abs(spot - strike) if spot is not None and strike and strike > 0 else None)
            dist_cb = (abs(spot_cb - strike) if spot_cb is not None and strike and strike > 0 else None)
            # Divergence guard disabled for REST oracles stability; re-enable when oracles are stable.
            # max_div = MAX_DIVERGENCE.get(asset.lower(), 5.0)
            # if (
            #     dist_kraken is not None
            #     and dist_cb is not None
            #     and abs(dist_kraken - dist_cb) > max_div
            # ):
            #     logger.warning(
            #         "[last_90s] [%s] [ORACLE DESYNC] |dist_kraken - dist_cb|=%.4f > MAX_DIVERGENCE=%.4f; skipping entry",
            #         asset.upper(), abs(dist_kraken - dist_cb), max_div,
            #     )
            #     _skip_aggregate_record(
            #         market_id, asset, current_bid, None, "oracle_desync",
            #         ticker=ticker, side=side, spot=spot, strike=strike,
            #         seconds_to_close=seconds_to_close, min_distance_threshold=None, db_path=db_path,
            #     )
            #     return
            # ENTRY GATE: Conservative min(Kraken, Coinbase). If both available, use the smaller distance; if only one, use that one.
            # Guard against None when WS has not received first tick yet (avoid TypeError on min/compare).
            if dist_kraken is not None and dist_cb is not None:
                if dist_kraken <= dist_cb:
                    distance = dist_kraken
                    official_spot = spot
                else:
                    distance = dist_cb
                    official_spot = spot_cb
            elif dist_kraken is not None:
                distance = dist_kraken
                official_spot = spot
                logger.warning("[last_90s] [%s] Coinbase spot unavailable for entry distance, using Kraken only", asset.upper())
            elif dist_cb is not None:
                distance = dist_cb
                official_spot = spot_cb
                logger.warning("[last_90s] [%s] Kraken spot unavailable for entry distance, using Coinbase only", asset.upper())
            else:
                distance = None
                official_spot = None
                logger.debug("[last_90s] [%s] No oracle data yet (WS first tick); skipping entry this tick", asset.upper())
            # Strike sanity: if distance is absurdly large, strike was likely hallucinated (e.g. year 2026 parsed as strike). Skip market.
            asset_l = asset.lower()
            if distance is not None and strike is not None and strike > 0:
                max_absurd = MAX_ABSURD_DISTANCE_USD.get(asset_l, 5000.0)
                if distance > max_absurd:
                    logger.error(
                        "[last_90s] [%s] STRIKE_SANITY_CHECK_FAILED: distance=%.2f > max_absurd=%.0f (strike=%.2f spot~%s); skipping market %s — likely hallucinated strike.",
                        asset.upper(), distance, max_absurd, strike, official_spot, market_id,
                    )
                    _skip_aggregate_record(
                        market_id, asset, current_bid, None, "strike_sanity_check_failed",
                        ticker=ticker, side=side, spot=official_spot, strike=strike,
                        seconds_to_close=seconds_to_close, min_distance_threshold=None, db_path=db_path,
                    )
                    continue
            # DEBUG_WS_2: Can we compute distance? (when use_ws_oracles=true)
            if use_ws_oracles:
                logger.info(
                    "[last_90s] [DEBUG_WS_2] [%s] strike=%s | dist_kraken=%s | dist_cb=%s | distance=%s | official_spot=%s",
                    asset.upper(), strike, dist_kraken, dist_cb, distance, official_spot,
                )
            if distance is None and official_spot is None:
                if use_ws_oracles:
                    logger.info("[last_90s] [DEBUG_WS_2] [%s] No distance (no oracle data); skipping this tick, recording no_oracle_data skip", asset.upper())
                _skip_aggregate_record(
                    market_id, asset, current_bid, None, "no_oracle_data",
                    seconds_to_close=seconds_to_close, db_path=db_path,
                )
                continue
            base_min_dist_required = _min_distance_at_placement(cfg, asset)
            decay_mult = _get_time_decay_multiplier(seconds_to_close)
            decayed_min_dist_required = base_min_dist_required * decay_mult if base_min_dist_required > 0 else 0.0
            # Hard floors per asset so decayed threshold never becomes too small.
            asset_l = asset.lower()
            if decayed_min_dist_required > 0:
                if asset_l == "sol":
                    decayed_min_dist_required = max(decayed_min_dist_required, 0.08)
                elif asset_l == "xrp":
                    decayed_min_dist_required = max(decayed_min_dist_required, 0.0020)
                elif asset_l == "eth":
                    decayed_min_dist_required = max(decayed_min_dist_required, 1.5)
                elif asset_l == "btc":
                    decayed_min_dist_required = max(decayed_min_dist_required, 8.0)

            # Allow up to one market-style and one limit per window (e.g. market early, then limit at 99¢ later).
            window_key = (market_id, asset.lower())
            with _last_90s_placed_windows_lock:
                placed_state = _last_90s_placed_windows.get(window_key, {"has_limit": False, "has_market": False})
            has_limit = bool(placed_state.get("has_limit"))
            has_market = bool(placed_state.get("has_market"))
            # Empty-book override: when both yes_bid and no_bid are 0 but distance gate passes,
            # choose side from spot vs strike and allow a resting 99c limit even though bid < min_bid.
            empty_book_override = False
            if (
                (yes_bid or 0) == 0
                and (no_bid or 0) == 0
                and distance is not None
                and strike is not None
                and strike > 0
                and official_spot is not None
            ):
                if official_spot == strike:
                    # With a positive distance buffer this should not happen in practice, but be explicit: skip.
                    _skip_aggregate_record(
                        market_id,
                        asset,
                        current_bid,
                        distance,
                        "spot_equals_strike",
                        ticker=ticker,
                        side=side,
                        spot=official_spot,
                        strike=strike,
                        seconds_to_close=seconds_to_close,
                        min_distance_threshold=decayed_min_dist_required if decayed_min_dist_required > 0 else None,
                        db_path=db_path,
                    )
                    continue
                # If spot > strike, YES is in-the-money; if spot < strike, NO is in-the-money.
                side = "yes" if official_spot > strike else "no"
                current_bid = 0
                empty_book_override = True
            # Time-weighted dynamic floor: sec > 65 requires bid >= 98c; after 65s use min_bid_cents (e.g. 90). Entry allowed from start of window (e.g. 85s).
            dynamic_floor = 98 if seconds_to_close > 65 else min_bid_cents
            # DEBUG_WS_3: Can we evaluate and place? (when use_ws_oracles=true)
            if use_ws_oracles:
                logger.info(
                    "[last_90s] [DEBUG_WS_3] [%s] sec_to_close=%.0f | current_bid=%s | dynamic_floor=%s | distance=%s | decayed_min_dist_required=%s | passed_distance_buffer=%s",
                    asset.upper(), seconds_to_close, current_bid, dynamic_floor, distance,
                    decayed_min_dist_required if decayed_min_dist_required else 0,
                    (distance is not None and (decayed_min_dist_required <= 0 or distance > decayed_min_dist_required)),
                )
            if current_bid < dynamic_floor and not empty_book_override:
                if use_ws_oracles:
                    logger.info("[last_90s] [DEBUG_WS_3] [%s] SKIP reason=min_bid (bid=%s < floor=%s)", asset.upper(), current_bid, dynamic_floor)
                logger.info(
                    "[last_90s] [%s] SKIP: %s bid=%dc < dynamic_floor=%d (sec_to_close=%.0f) for %s (too risky), wait for next run",
                    asset.upper(), side, current_bid, dynamic_floor, seconds_to_close, ticker or "n/a",
                )
                _skip_aggregate_record(
                    market_id,
                    asset,
                    current_bid,
                    distance,
                    "min_bid",
                    ticker=ticker,
                    side=side,
                    spot=spot,
                    strike=strike,
                    seconds_to_close=seconds_to_close,
                    min_distance_threshold=decayed_min_dist_required if decayed_min_dist_required > 0 else None,
                    db_path=db_path,
                )
                continue
            if base_min_dist_required > 0 and (distance is None or distance <= decayed_min_dist_required):
                if use_ws_oracles:
                    logger.info("[last_90s] [DEBUG_WS_3] [%s] SKIP reason=distance_buffer (distance=%s <= decayed_min=%s)", asset.upper(), distance, decayed_min_dist_required)
                detail = f"distance={distance or 0:.4f} <= decayed_min_distance_at_placement={decayed_min_dist_required:.4f}"
                logger.info(
                    "[last_90s] [%s] SKIP: distance_from_strike=%s <= decayed_min_distance_at_placement=%s (base=%s decay=%.2f) for %s (strike=%s spot=%s), wait for next run",
                    asset.upper(),
                    _fmt_price(distance),
                    _fmt_price(decayed_min_dist_required),
                    _fmt_price(base_min_dist_required),
                    decay_mult,
                    ticker or "n/a",
                    _fmt_price(strike) if strike else "n/a",
                    _fmt_price(spot),
                )
                _skip_aggregate_record(
                    market_id,
                    asset,
                    current_bid,
                    distance,
                    "distance_buffer",
                    ticker=ticker,
                    side=side,
                    spot=spot,
                    strike=strike,
                    seconds_to_close=seconds_to_close,
                    min_distance_threshold=decayed_min_dist_required if decayed_min_dist_required > 0 else None,
                    db_path=db_path,
                )
                continue
            use_market_style = (current_bid < 99 and current_bid > 0)
            yes_ask, no_ask = _get_asks(quote)
            ask_cents = yes_ask if (side or "yes").lower() == "yes" else no_ask
            if use_market_style:
                # If we've already placed a market-style order for this window, skip; allow a limit strike instead.
                if has_market:
                    logger.info(
                        "[last_90s] [%s] SKIP: market-style order already placed for %s in this window",
                        asset.upper(),
                        ticker or "n/a",
                    )
                    _skip_aggregate_record(
                        market_id,
                        asset,
                        current_bid,
                        distance,
                        "market_already_placed",
                        ticker=ticker,
                        side=side,
                        spot=spot,
                        strike=strike,
                        seconds_to_close=seconds_to_close,
                        min_distance_threshold=decayed_min_dist_required if decayed_min_dist_required > 0 else None,
                        db_path=db_path,
                    )
                    continue
                if ask_cents <= 0 or ask_cents > 99:
                    detail = f"bid={current_bid}c but ask={ask_cents} invalid for market-style"
                    logger.warning("[last_90s] [%s] SKIP: bid=%dc < 99 but ask=%s invalid for market-style for %s", asset.upper(), current_bid, ask_cents, ticker or "n/a")
                    _skip_aggregate_record(
                        market_id,
                        asset,
                        current_bid,
                        distance,
                        "invalid_ask",
                        ticker=ticker,
                        side=side,
                        spot=spot,
                        strike=strike,
                        seconds_to_close=seconds_to_close,
                        min_distance_threshold=decayed_min_dist_required if decayed_min_dist_required > 0 else None,
                        db_path=db_path,
                    )
                    continue
                price_to_use = max(1, min(99, ask_cents))
                order_type_label = "market"
            else:
                if has_limit:
                    logger.info(
                        "[last_90s] [%s] SKIP: limit order already placed for %s in this window",
                        asset.upper(),
                        ticker or "n/a",
                    )
                    _skip_aggregate_record(
                        market_id,
                        asset,
                        current_bid,
                        distance,
                        "limit_already_placed",
                        ticker=ticker,
                        side=side,
                        spot=spot,
                        strike=strike,
                        seconds_to_close=seconds_to_close,
                        min_distance_threshold=decayed_min_dist_required if decayed_min_dist_required > 0 else None,
                        db_path=db_path,
                    )
                    continue
                price_to_use = config_limit_cents
                order_type_label = "limit"
            max_cost = _max_cost_cents(cfg, config, asset)
            count = max(1, max_cost // price_to_use)
            # Peek only: do not clear here. Clearing when we don't place (e.g. OBSERVE) would lose history for the aggregated row at resolve.
            pre_placement_rec = _skip_aggregate_peek(market_id, asset)
            pre_placement_skip_details = _skip_aggregate_format_details(pre_placement_rec, prefix="Pre-placement history")
            if mode != "TRADE":
                if use_ws_oracles:
                    logger.info("[last_90s] [DEBUG_WS_3] [%s] SKIP reason=OBSERVE (would place but mode != TRADE)", asset.upper())
                logger.info(
                    "[last_90s] [%s] OBSERVE: would place %s %s @ %dc x %d for %s (%.0fs to close) yes_bid=%s no_bid=%s",
                    asset.upper(), order_type_label, side, price_to_use, count, ticker, seconds_to_close, yes_bid, no_bid,
                )
                continue
            # Universal Market-to-Limit fallback:
            # 1) Try a true MARKET order (no price, no time_in_force) to sweep the book.
            # 2) If it fails, is unfilled, or partially fills, place the remainder as a resting GTC LIMIT at target price.
            if use_ws_oracles:
                logger.info(
                    "[last_90s] [DEBUG_WS_3] [%s] All gates passed, attempting placement ticker=%s side=%s price=%dc count=%d",
                    asset.upper(), ticker, side, price_to_use, count,
                )
            order_id = None
            final_count = count
            resp_mkt = kalshi_client.place_market_order(
                ticker=ticker,
                action="buy",
                side=side,
                count=count,
                client_order_id=f"last90s:{uuid.uuid4().hex[:12]}",
            )
            if resp_mkt and (isinstance(resp_mkt, dict) or getattr(resp_mkt, "status_code", 0) in (200, 201)):
                body_mkt = resp_mkt if isinstance(resp_mkt, dict) else (resp_mkt.json() if getattr(resp_mkt, "json", None) else {})
                order_id_mkt = (body_mkt.get("order") or {}).get("order_id") or body_mkt.get("order_id")
                fill_count_mkt = 0
                remaining_mkt = count
                status_mkt = ""
                try:
                    order_info = kalshi_client.get_order(order_id_mkt)
                    fill_count_mkt = int(order_info.get("fill_count") or 0)
                    remaining_mkt = int(order_info.get("remaining_count") or count)
                    status_mkt = (order_info.get("status") or "").strip().lower()
                except Exception as e:
                    logger.debug("[last_90s] [%s] get_order after MARKET failed: %s; treating as unfilled", asset.upper(), e)
                if status_mkt == "executed" and remaining_mkt == 0:
                    order_id = order_id_mkt
                    order_type_label = "market"
                    final_count = fill_count_mkt
                    logger.info(
                        "[last_90s] [%s] Market filled %d contract(s) for %s (%.0fs to close) order_id=%s",
                        asset.upper(), final_count, ticker, seconds_to_close, order_id,
                    )
                else:
                    fallback_count = max(1, count - fill_count_mkt)
                    # Resting limit only: explicit good_till_canceled so order shows in app queue.
                    resp_lim = kalshi_client.place_limit_order(
                        ticker=ticker,
                        action="buy",
                        side=side,
                        price_cents=price_to_use,
                        count=fallback_count,
                        client_order_id=f"last90s:{uuid.uuid4().hex[:12]}",
                        time_in_force="good_till_canceled",
                    )
                    if resp_lim and getattr(resp_lim, "status_code", 0) in (200, 201):
                        body_lim = resp_lim.json() if getattr(resp_lim, "json", None) and getattr(resp_lim, "text", None) else {}
                        order_id = (body_lim.get("order") or {}).get("order_id") or body_lim.get("order_id")
                        order_type_label = "limit_fallback"
                        final_count = fallback_count
                        logger.info(
                            "[last_90s] [%s] Market (IOC) not filled; placed limit fallback %d @ %dc for %s (%.0fs to close) order_id=%s",
                            asset.upper(), final_count, price_to_use, ticker, seconds_to_close, order_id,
                        )
                    else:
                        order_id = None
                        status_lim = getattr(resp_lim, "status_code", None) if resp_lim else None
                        err_lim = getattr(resp_lim, "text", None) if resp_lim else "no response"
                        logger.error(
                            "[KALSHI API ERROR] Limit fallback (after market partial fill) failed for %s ticker=%s status=%s response=%s",
                            asset.upper(), ticker, status_lim, err_lim,
                        )
            if order_id is None:
                # Market failed or rejected; try limit at full count as fallback (resting order only).
                resp_lim = kalshi_client.place_limit_order(
                    ticker=ticker,
                    action="buy",
                    side=side,
                    price_cents=price_to_use,
                    count=count,
                    client_order_id=f"last90s:{uuid.uuid4().hex[:12]}",
                    time_in_force="good_till_canceled",
                )
                if resp_lim and getattr(resp_lim, "status_code", 0) in (200, 201):
                    body_lim = resp_lim.json() if getattr(resp_lim, "json", None) and getattr(resp_lim, "text", None) else {}
                    order_id = (body_lim.get("order") or {}).get("order_id") or body_lim.get("order_id")
                    order_type_label = "limit_fallback"
                    final_count = count
                    logger.info(
                        "[last_90s] [%s] IOC failed/rejected; placed limit fallback %d @ %dc for %s (%.0fs to close) order_id=%s",
                        asset.upper(), final_count, price_to_use, ticker, seconds_to_close, order_id,
                    )
                else:
                    status_lim = getattr(resp_lim, "status_code", None) if resp_lim else None
                    err_lim = getattr(resp_lim, "text", None) if resp_lim else "no response"
                    err_mkt = getattr(resp_mkt, "text", None) if resp_mkt else "no response"
                    logger.error(
                        "[KALSHI API ERROR] Limit fallback (after market reject) failed for %s ticker=%s limit_status=%s limit_response=%s market_response=%s",
                        asset.upper(), ticker, status_lim, err_lim, err_mkt,
                    )
            if order_id:
                with _db_section(db_lock):
                    set_last_90s_placed(
                        db_path, market_id, asset,
                        order_id=order_id, ticker=ticker, count=final_count, limit_price_cents=price_to_use, side=side,
                    )
                try:
                    ensure_report_db(db_path)
                    pre_placement_json = None
                    with _pre_placement_trajectory_lock:
                        pre_placement_list = _pre_placement_trajectory.pop((market_id, asset.lower()), [])
                    if pre_placement_list:
                        pre_placement_json = json.dumps(pre_placement_list)
                    if use_ws_oracles:
                        logger.info(
                            "[last_90s] [DEBUG_WS_4] [%s] Writing placement row to DB window_id=%s asset=%s placed=1 order_id=%s",
                            asset.upper(), market_id, asset, order_id or "",
                        )
                    write_row_last_90s(
                        db_path,
                        market_id,
                        asset,
                        ticker=ticker,
                        side=side,
                        spot=official_spot,
                        strike=strike,
                        seconds_to_close=seconds_to_close,
                        distance=distance,
                        min_distance_threshold=decayed_min_dist_required if decayed_min_dist_required > 0 else None,
                        bid=current_bid,
                        limit_or_market=order_type_label,
                        price_cents=price_to_use,
                        placed=1,
                        order_id=order_id or "",
                        skip_details=pre_placement_skip_details or None,
                        spot_kraken=spot,
                        spot_coinbase=spot_cb,
                        distance_kraken=dist_kraken,
                        distance_coinbase=dist_cb,
                        pre_placement_history=pre_placement_json,
                    )
                except Exception as e:
                    logger.warning(
                        "[last_90s] DB_WRITE_FAIL: placement row window_id=%s asset=%s error=%s",
                        market_id,
                        asset.upper(),
                        e,
                    )
                _skip_aggregate_clear_only(market_id, asset, db_path)
                # Update in-memory dual-strike placement state.
                # "limit" and "limit_fallback" use the limit slot; "market" uses the market slot (so we can still place limit later if bid goes to 99).
                with _last_90s_placed_windows_lock:
                    state = _last_90s_placed_windows.get(window_key, {"has_limit": False, "has_market": False})
                    if order_type_label in ("limit", "limit_fallback"):
                        state["has_limit"] = True
                    else:
                        state["has_market"] = True
                    _last_90s_placed_windows[window_key] = state
                strike = extract_strike_from_market(m, ticker) if m else 0.0
                if use_ws_oracles:
                    from bot.oracle_ws_manager import get_safe_spot_prices_sync, is_ws_running
                    data_rep = get_safe_spot_prices_sync(asset, max_age_seconds=3.0, require_both=False) if is_ws_running() else None
                    spot = data_rep.get("kraken") if data_rep is not None else spot
                else:
                    spot = _get_spot_price(asset)
                logger.info(
                    "[last_90s] [%s] PLACED %s %s @ %dc x %d for %s (%.0fs to close) yes_bid=%s no_bid=%s order_id=%s",
                    asset.upper(), order_type_label, side, price_to_use, final_count, ticker, seconds_to_close, yes_bid, no_bid, order_id,
                )
                logger.info(
                    "%s",
                    __json_evt("LAST_90S_LIMIT_99", asset, market_id, ticker, price_to_use, final_count, seconds_to_close, order_id, side=side, yes_bid=yes_bid, no_bid=no_bid, order_type=order_type_label),
                )
                _placement_report(
                    logger, asset, market_id, ticker, side, price_to_use, final_count, seconds_to_close,
                    order_id, order_type_label, yes_bid, no_bid, yes_ask, no_ask,
                    strike, spot, window_seconds, min_bid_cents, config_limit_cents,
                )
            else:
                err = getattr(resp_ioc, "text", None) or getattr(resp_ioc, "reason", str(resp_ioc))
                logger.error("[KALSHI API ERROR] Place failed for %s %s: %s", asset.upper(), ticker, err)
        except Exception as e:
            logger.exception("[last_90s] [%s] Error: %s", asset.upper(), e)


def run_last_90s_once(
    config: dict,
    logger: logging.Logger,
    kalshi_client: Optional[Any],
    db_path: str,
    db_lock: Optional[threading.Lock] = None,
) -> None:
    """
    One iteration of the last-90s strategy.

    Flow per run:
    1. Resolve previous market: for each asset, get the previous 15-min market (just closed),
       fetch unresolved last_90s orders, get order status + market result, compute outcome
       (positive/negative/unfilled), update DB, log LAST_90S_OUTCOME and LAST_90S_MARKET_SUMMARY.
    2. For each asset in the current 15-min market (parallel via ThreadPoolExecutor), if
       seconds_to_close is in (0, window_seconds]: stop-loss check or fetch/place; DB access
       is serialized via db_lock when provided.
    Respects no_trade_window, mode (OBSERVE/TRADE), and config stop_loss_pct (0 = disabled).
    """
    cfg = _get_cfg(config)
    if not cfg or not cfg.get("enabled", False):
        return
    config_limit_cents = int(cfg.get("limit_price_cents", 99))
    if config_limit_cents < 1 or config_limit_cents > 99:
        logger.warning(
            "[last_90s] Skipping run: limit_price_cents=%s invalid (must be 1-99).",
            config_limit_cents,
        )
        return
    config_limit_cents = max(1, min(99, config_limit_cents))
    mode = (config.get("mode", "OBSERVE")).upper()
    if mode not in ("OBSERVE", "TRADE"):
        mode = "OBSERVE"
    assets = _assets(cfg, config)
    if not assets:
        logger.warning("[last_90s] Skipping run: no assets configured for last_90s_limit_99.")
        return
    # Always resolve previous market first (write aggregated skip or outcome rows) so every window
    # gets a DB row even when we skip placement due to no_trade_window, bot restart, or missing Kalshi client.
    # Resolve runs even when kalshi_client is None (writes aggregated skip rows; skips order-status fetch for placed orders).
    _resolve_and_log_outcomes(config, logger, kalshi_client, db_path, db_lock)
    logger.debug("[last_90s] Resolve completed for this tick")
    no_trade = in_no_trade_window(config)
    now_ts = time.time()
    global _last_90s_skip_placement_log_ts
    if no_trade:
        if now_ts - _last_90s_skip_placement_log_ts >= _LAST_90S_SKIP_PLACEMENT_LOG_INTERVAL:
            logger.info(
                "[last_90s] Skipping placement this tick: no_trade_window=true (resolve still ran)."
            )
            _last_90s_skip_placement_log_ts = now_ts
        return
    if not kalshi_client:
        if now_ts - _last_90s_skip_placement_log_ts >= _LAST_90S_SKIP_PLACEMENT_LOG_INTERVAL:
            logger.info(
                "[last_90s] Skipping placement this tick: kalshi_client=None (resolve still ran; fix Kalshi init to place)."
            )
            _last_90s_skip_placement_log_ts = now_ts
        return

    start_kalshi_ws()

    def run_asset(a: str) -> None:
        _run_last_90s_for_asset(
            a, config, logger, kalshi_client, db_path, db_lock,
            cfg, config_limit_cents, mode,
        )

    # Timeout so one stuck asset (e.g. WS/API block with use_ws_oracles=true) cannot block resolve for
    # the next window. Without this, the loop never reaches the next tick and never runs resolve for 26MAR060015 etc.
    _ASSET_WAIT_TIMEOUT = 25.0

    if db_lock is not None:
        max_workers = min(len(assets), 8)
        executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="last_90s_asset")
        try:
            futures = [executor.submit(run_asset, asset) for asset in assets]
            done, not_done = wait(futures, timeout=_ASSET_WAIT_TIMEOUT)
            if not_done:
                logger.warning(
                    "[last_90s] %d asset task(s) did not complete within %.0fs (possible WS/API block); continuing so resolve can run next tick.",
                    len(not_done),
                    _ASSET_WAIT_TIMEOUT,
                )
                for f in not_done:
                    f.cancel()
            for fut in done:
                try:
                    fut.result()
                except Exception as e:
                    logger.exception("[last_90s] Asset task error: %s", e)
        finally:
            executor.shutdown(wait=False)
    else:
        for asset in assets:
            run_asset(asset)


def _placement_report(
    logger: logging.Logger,
    asset: str,
    market_id: str,
    ticker: str,
    side: str,
    price_cents: int,
    count: int,
    seconds_to_close: float,
    order_id: Optional[str],
    order_type: str,
    yes_bid: int,
    no_bid: int,
    yes_ask: int,
    no_ask: int,
    strike: float,
    spot: Optional[float],
    window_seconds: int,
    min_bid_cents: int,
    limit_bid_cents: int,
) -> None:
    """Log one JSON line with all placement data for tuning (window_seconds, min_bid_cents, etc.)."""
    distance_from_strike = (abs(spot - strike) if spot is not None and strike and strike > 0 else None)
    payload = {
        "event": "LAST_90S_PLACEMENT_REPORT",
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "asset": asset.upper(),
        "market_id": market_id,
        "ticker": ticker,
        "side": side.lower(),
        "order_type": order_type,
        "price_cents": price_cents,
        "count": count,
        "seconds_to_close": round(seconds_to_close, 1),
        "order_id": order_id,
        "yes_bid": yes_bid,
        "no_bid": no_bid,
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "strike": strike if strike else None,
        "spot": spot,
        "distance_from_strike": round(distance_from_strike, 4) if distance_from_strike is not None else None,
        "window_seconds": window_seconds,
        "min_bid_cents": min_bid_cents,
        "limit_bid_cents": limit_bid_cents,
    }
    logger.info("%s", json.dumps(payload, default=str))


def _stop_loss_report(
    logger: logging.Logger,
    asset: str,
    market_id: str,
    ticker: str,
    side: str,
    entry_cents: int,
    sell_price_cents: int,
    fill_count: int,
    loss_pct: float,
    stop_loss_pct: float,
    seconds_to_close: Optional[float],
    strike: float,
    spot: Optional[float],
    window_seconds: int,
    min_bid_cents: int,
    order_id: Optional[str],
) -> None:
    """Log one JSON line when stop loss triggers for tuning stop_loss_pct, window_seconds, min_bid_cents."""
    distance_from_strike = (abs(spot - strike) if spot is not None and strike and strike > 0 else None)
    payload = {
        "event": "LAST_90S_STOP_LOSS_REPORT",
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "asset": asset.upper(),
        "market_id": market_id,
        "ticker": ticker,
        "side": side.lower(),
        "entry_cents": entry_cents,
        "sell_price_cents": sell_price_cents,
        "fill_count": fill_count,
        "loss_pct": round(loss_pct * 100, 2),
        "stop_loss_pct": round(stop_loss_pct * 100, 2),
        "seconds_to_close": round(seconds_to_close, 1) if seconds_to_close is not None else None,
        "strike": strike if strike else None,
        "spot": spot,
        "distance_from_strike": round(distance_from_strike, 4) if distance_from_strike is not None else None,
        "window_seconds": window_seconds,
        "min_bid_cents": min_bid_cents,
        "order_id": order_id,
    }
    logger.info("%s", json.dumps(payload, default=str))


def __json_evt(
    event: str,
    asset: str,
    market_id: str,
    ticker: str,
    price_cents: int,
    count: int,
    seconds_to_close: float,
    order_id: Optional[str],
    side: str = "yes",
    yes_bid: Optional[int] = None,
    no_bid: Optional[int] = None,
    order_type: Optional[str] = None,
) -> str:
    payload = {
        "event": event,
        "asset": asset.upper(),
        "market_id": market_id,
        "ticker": ticker,
        "side": side.lower(),
        "limit_price_cents": price_cents,
        "count": count,
        "seconds_to_close": round(seconds_to_close, 1),
        "order_id": order_id,
    }
    if yes_bid is not None:
        payload["yes_bid"] = yes_bid
    if no_bid is not None:
        payload["no_bid"] = no_bid
    if order_type is not None:
        payload["order_type"] = order_type
    return json.dumps(payload, default=str)


# Max number of 15-min slots to backfill at startup (e.g. 24 = 6 hours of missed windows).
_CATCH_UP_MAX_SLOTS = 24


def _catch_up_previous_window_at_startup(
    config: dict,
    logger: logging.Logger,
    db_path: str,
    db_lock: Optional[threading.Lock],
) -> None:
    """
    At startup, backfill any missing strategy_report_last_90s rows for windows that
    closed in the last _CATCH_UP_MAX_SLOTS slots (e.g. 6h). Handles bot being down
    across midnight or multiple boundaries (e.g. 26MAR060000 never resolved).
    """
    import sqlite3
    cfg = _get_cfg(config)
    if not cfg or not db_path or not pytz or not generate_15min_slug:
        return
    assets = _assets(cfg, config)
    if not assets:
        return
    try:
        et_tz = pytz.timezone("US/Eastern")
        now_utc = datetime.now(timezone.utc)
        now_et = now_utc.astimezone(et_tz)
        close_min = (now_et.minute // 15) * 15
        close_time = now_et.replace(minute=close_min, second=0, microsecond=0)
    except Exception:
        return
    ensure_report_db(db_path)
    total_written = 0
    for slot_offset in range(1, _CATCH_UP_MAX_SLOTS + 1):
        try:
            slot_close = close_time - timedelta(minutes=15 * slot_offset)
            window_id_for_btc = generate_15min_slug(slot_close, asset="btc").upper()
        except Exception:
            continue
        for asset in assets:
            try:
                window_id = generate_15min_slug(slot_close, asset=asset).upper()
            except Exception:
                continue
            with _db_section(db_lock):
                try:
                    conn = sqlite3.connect(db_path)
                    try:
                        cur = conn.cursor()
                        cur.execute(
                            "SELECT 1 FROM strategy_report_last_90s WHERE window_id = ? AND asset = ? LIMIT 1",
                            (window_id, str(asset).lower()),
                        )
                        if cur.fetchone():
                            continue
                    finally:
                        conn.close()
                    write_row_last_90s(
                        db_path,
                        window_id,
                        asset,
                        placed=0,
                        skip_reason="aggregated_skips",
                        skip_details="Catch-up: no row at startup (window missed).",
                    )
                    total_written += 1
                    logger.info(
                        "[last_90s] Catch-up: wrote fallback row for window_id=%s asset=%s (window missed).",
                        window_id,
                        asset,
                    )
                except Exception as e:
                    logger.warning(
                        "[last_90s] Catch-up write failed window_id=%s asset=%s: %s",
                        window_id,
                        asset,
                        e,
                    )
    if total_written:
        logger.info(
            "[last_90s] Catch-up: wrote %d fallback row(s) for missed window(s).",
            total_written,
        )


def run_last_90s_loop(
    config: dict,
    logger: logging.Logger,
    db_path: str,
    stop_event: Optional[Any] = None,
) -> None:
    """
    Independent scheduler loop for the last-90s strategy. Runs until stop_event is set.
    Creates its own Kalshi client; uses config logging and state.
    Uses a single DB lock so concurrent asset workers do not race on SQLite.
    """
    cfg = _get_cfg(config)
    if not cfg or not cfg.get("enabled", False):
        logger.warning(
            "[last_90s] Aborting thread early: config missing or enabled=false. Check config/fifteen_min.last_90s_limit_99."
        )
        return
    # use_ws_oracles: this strategy uses asset price from WS oracles when true, REST when false.
    # WS oracles are started at bot startup (global); not started here.
    interval_seconds = float(cfg.get("run_interval_seconds", 15))
    interval_seconds = max(0.5, min(interval_seconds, 60.0))  # allow 500 ms to match hourly_last_90s
    kalshi_client = None
    try:
        from src.client.kalshi_client import KalshiClient
        kalshi_client = KalshiClient()
    except Exception as e:
        logger.warning(
            "[last_90s] Kalshi client init failed; resolve (skip rows) still runs, placement disabled. Debug: %s", e
        )

    db_lock = threading.Lock()
    logger.info(
        "[last_90s] Loop started (interval=%.0fs) kalshi_client=%s use_ws_oracles=%s db_path=%s",
        interval_seconds,
        kalshi_client is not None,
        bool(cfg.get("use_ws_oracles", False)),
        db_path or "none",
    )
    _catch_up_previous_window_at_startup(config, logger, db_path or "", db_lock)
    logger.info("[last_90s] Entering main while loop")
    while stop_event is None or not getattr(stop_event, "is_set", lambda: False)():
        try:
            run_last_90s_once(config, logger, kalshi_client, db_path, db_lock=db_lock)
        except Exception as e:
            logger.exception("[last_90s] RUN_ERROR: %s", e)
        time.sleep(interval_seconds)
    logger.info("[last_90s] Exited main while loop")
