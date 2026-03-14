"""
Hourly last-90s style strategy (limit 99): use same signal logic as regular hourly.

Runs in the last window_seconds of each hour. For each of the 9 markets we call
generate_signals_farthest(..., pick_all_in_range=True) so all tickers with YES or NO
ask in threshold qualify; then when placing we apply min_bid_cents: if bid < min_bid_cents
skip, else place limit at 99. Aligns side selection with regular hourly (ask-based qual,
bid check for the same side). Log prefix [hourly_last_90s] for report parsing.
"""
from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@contextmanager
def _db_section(db_lock: Optional[threading.Lock]):
    """Acquire db_lock for DB access if provided; no-op otherwise."""
    if db_lock is not None:
        with db_lock:
            yield
    else:
        yield

from bot.market import (
    _parse_strike_from_ticker,
    enrich_with_orderbook,
    fetch_eligible_tickers,
    fetch_ticker_outcome,
    get_current_hour_market_ids,
    get_minutes_to_close,
    get_previous_hour_market_ids,
    TickerQuote,
)
from bot.state import (
    clear_hourly_last_90s_skip_aggregator,
    clear_hourly_last_90s_skip_aggregator_for_windows,
    get_all_hourly_last_90s_skip_aggregator_for_windows,
    get_hourly_last_90s_skip_aggregator,
    get_hourly_limit_99_placed,
    get_hourly_limit_99_placements_for_stoploss,
    get_hourly_limit_99_unresolved_for_market,
    persist_hourly_last_90s_skip_aggregator,
    set_hourly_limit_99_placed,
    set_hourly_limit_99_stop_loss_triggered,
    update_hourly_limit_99_resolved,
)
from bot.strategy import Signal, generate_signals_farthest
from bot.strategy_report_db import (
    ensure_report_db,
    update_resolution_hourly_last_90s,
    write_row_hourly_last_90s,
)


def _get_cfg(config: dict) -> Optional[dict]:
    """Get hourly_last_90s_limit_99 config (top-level or under schedule)."""
    out = config.get("hourly_last_90s_limit_99") or (config.get("schedule") or {}).get("hourly_last_90s_limit_99")
    return out if isinstance(out, dict) else None


# --- In-memory skip aggregator (one-row-per (window_id, asset, ticker, side) guarantee). ---
# Exactly one row per (window_id, asset, ticker, side) in strategy_report_hourly_last_90s:
#   Case A: All skipped till end of window -> at resolve we write 1 row per key with aggregated_skips + last-seen context.
#   Case B: Skip for some time then place -> on placement we write 1 row with full placement + pre_placement skip_details.
#   Case C: Place first time (no prior skips) -> on placement we write 1 row with full placement, skip_details empty.
# All HOURLY_LAST_90S_COLUMNS are set in A (or null where N/A); B/C set placement fields; resolution fields filled later by update_resolution_hourly_last_90s.
_hourly_last_90s_skip_aggregator: Dict[tuple, Dict[str, Any]] = {}
_hourly_last_90s_skip_aggregator_lock = threading.Lock()


def _copy_hourly_aggregator_rec(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of aggregator record for persistence (no shared mutable refs)."""
    return {
        "check_count": rec.get("check_count", 0),
        "bid_history": list(rec.get("bid_history") or []),
        "distance_history": list(rec.get("distance_history") or []),
        "skip_reasons": dict(rec.get("skip_reasons") or {}),
        "last_spot": rec.get("last_spot"),
        "last_strike": rec.get("last_strike"),
        "last_seconds_to_close": rec.get("last_seconds_to_close"),
        "last_min_distance_threshold": rec.get("last_min_distance_threshold"),
    }


def _skip_aggregate_record(
    hour_market_id: str,
    asset: str,
    ticker: str,
    side: str,
    bid: int,
    distance: Optional[float],
    skip_reason: str,
    *,
    spot: Optional[float] = None,
    strike: Optional[float] = None,
    seconds_to_close: Optional[float] = None,
    min_distance_threshold: Optional[float] = None,
    db_path: Optional[str] = None,
) -> None:
    """On skip: update in-memory stats and persist to DB so resolve has data after process restart."""
    key = (hour_market_id, asset.lower(), ticker, (side or "yes").lower())
    with _hourly_last_90s_skip_aggregator_lock:
        if key not in _hourly_last_90s_skip_aggregator:
            _hourly_last_90s_skip_aggregator[key] = {
                "check_count": 0,
                "bid_history": [],
                "distance_history": [],
                "skip_reasons": {},
                "last_spot": None,
                "last_strike": None,
                "last_seconds_to_close": None,
                "last_min_distance_threshold": None,
            }
        rec = _hourly_last_90s_skip_aggregator[key]
        rec["check_count"] += 1
        rec["bid_history"].append(bid)
        rec["distance_history"].append(distance if distance is not None else 0.0)
        rec["skip_reasons"][skip_reason] = rec["skip_reasons"].get(skip_reason, 0) + 1
        if spot is not None:
            rec["last_spot"] = spot
        if strike is not None:
            rec["last_strike"] = strike
        if seconds_to_close is not None:
            rec["last_seconds_to_close"] = seconds_to_close
        if min_distance_threshold is not None:
            rec["last_min_distance_threshold"] = min_distance_threshold
        if db_path:
            persist_hourly_last_90s_skip_aggregator(
                db_path, hour_market_id, asset, ticker, side or "yes", _copy_hourly_aggregator_rec(rec)
            )


def _skip_aggregate_peek(hour_market_id: str, asset: str, ticker: str, side: str) -> Optional[Dict[str, Any]]:
    """Return a copy of the aggregated record without clearing. Use for formatting when we may not place (e.g. OBSERVE or place fails)."""
    key = (hour_market_id, asset.lower(), ticker, (side or "yes").lower())
    with _hourly_last_90s_skip_aggregator_lock:
        rec = _hourly_last_90s_skip_aggregator.get(key)
    if not rec:
        return None
    return {
        "check_count": rec.get("check_count", 0),
        "bid_history": list(rec.get("bid_history") or []),
        "distance_history": list(rec.get("distance_history") or []),
        "skip_reasons": dict(rec.get("skip_reasons") or {}),
        "last_spot": rec.get("last_spot"),
        "last_strike": rec.get("last_strike"),
        "last_seconds_to_close": rec.get("last_seconds_to_close"),
        "last_min_distance_threshold": rec.get("last_min_distance_threshold"),
    }


def _skip_aggregate_get_and_clear(
    hour_market_id: str, asset: str, ticker: str, side: str, db_path: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Return aggregated payload for skip_details; clear memory and persisted store. If in-memory empty, load from DB."""
    key = (hour_market_id, asset.lower(), ticker, (side or "yes").lower())
    with _hourly_last_90s_skip_aggregator_lock:
        rec = _hourly_last_90s_skip_aggregator.pop(key, None)
    if rec is not None:
        if db_path:
            clear_hourly_last_90s_skip_aggregator(db_path, hour_market_id, asset, ticker, side or "yes")
        return rec
    if db_path:
        rec = get_hourly_last_90s_skip_aggregator(db_path, hour_market_id, asset, ticker, side)
        if rec is not None:
            clear_hourly_last_90s_skip_aggregator(db_path, hour_market_id, asset, ticker, side or "yes")
            return rec
    return None


def _skip_aggregate_get_and_clear_all_for_windows(
    prev_hour_market_ids: List[str], db_path: Optional[str] = None
) -> List[tuple]:
    """Return and remove all aggregator entries whose window_id is in prev_hour_market_ids (memory + DB). Returns [(window_id, asset, ticker, side, rec), ...]."""
    prev_set = set(prev_hour_market_ids or [])
    out: List[tuple] = []
    with _hourly_last_90s_skip_aggregator_lock:
        keys_to_remove = [k for k in _hourly_last_90s_skip_aggregator if k[0] in prev_set]
        for k in keys_to_remove:
            rec = _hourly_last_90s_skip_aggregator.pop(k, None)
            if rec is not None:
                out.append((k[0], k[1], k[2], k[3], rec))
    if db_path:
        db_rows = get_all_hourly_last_90s_skip_aggregator_for_windows(db_path, prev_hour_market_ids)
        seen = {(r[0], r[1], r[2], r[3]) for r in out}
        for row in db_rows:
            key_tuple = (row[0], row[1], row[2], row[3])
            if key_tuple not in seen:
                out.append(row)
                seen.add(key_tuple)
        clear_hourly_last_90s_skip_aggregator_for_windows(db_path, prev_hour_market_ids)
    return out


def _skip_aggregate_format_details(rec: Optional[Dict[str, Any]], prefix: str = "Pre-placement history") -> str:
    """Format aggregator record into skip_details string."""
    if not rec:
        return ""
    check_count = rec.get("check_count", 0)
    bid_history = rec.get("bid_history", [])
    distance_history = rec.get("distance_history", [])
    reasons = rec.get("skip_reasons", {})
    most_frequent = max(reasons.items(), key=lambda x: x[1])[0] if reasons else "unknown"
    return (
        f"{prefix}: Checks: {check_count} | Bids: {bid_history} | Distances: {distance_history} | Primary blocker: {most_frequent}"
    )


def _get_spot_price(asset: str) -> Optional[float]:
    """Fetch spot price for asset (Kraken)."""
    try:
        from src.client.kraken_client import KrakenClient
        client = KrakenClient()
        a = str(asset).lower()
        if a == "eth":
            return client.latest_eth_price().price
        if a == "sol":
            return client.latest_sol_price().price
        if a == "xrp":
            return client.latest_xrp_price().price
        if a == "doge":
            return client.latest_doge_price().price
        return client.latest_btc_price().price
    except Exception:
        return None


def _assets(cfg: dict, config: dict) -> List[str]:
    """Assets for hourly last-90s; all 5 supported (btc, eth, sol, xrp, doge) -> 9 market IDs total."""
    allowed = ("btc", "eth", "sol", "xrp", "doge")
    a = cfg.get("assets")
    if a is not None and isinstance(a, (list, tuple)):
        return [str(x).lower() for x in a if str(x).lower() in allowed]
    return [str(x).lower() for x in (config.get("assets") or ["btc", "eth", "sol", "xrp", "doge"]) if str(x).lower() in allowed]


def _spot_window(config: dict, asset: str) -> float:
    """Spot window for hourly (from config). In same units as spot price (e.g. dollars for DOGE 0.005, cents for BTC 1500)."""
    by_asset = (config.get("spot_window_by_asset") or {}) or {}
    val = by_asset.get(str(asset).lower(), config.get("spot_window", 1500))
    try:
        return float(val)
    except (TypeError, ValueError):
        return 1500.0


def _max_cost_cents(cfg: dict, config: dict, asset: str) -> int:
    by_asset = cfg.get("max_cost_cents_by_asset") or {}
    order_hourly = (config.get("order") or {}).get("hourly") or {}
    base = cfg.get("max_cost_cents") or order_hourly.get("max_cost_cents", 1000)
    if isinstance(base, dict):
        base = base.get("max_cost_cents", 1000)
    return int(by_asset.get(str(asset).lower(), base))


def _min_distance_at_placement(cfg: dict, asset: str) -> float:
    """Min distance_from_strike (USD) required to place; 0 = no filter. doge default 0."""
    by_asset = cfg.get("min_distance_at_placement") or {}
    if not isinstance(by_asset, dict):
        return 0.0
    v = by_asset.get(str(asset).lower())
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _get_per_asset(cfg: dict, key: str, asset: str, default: Any) -> Any:
    """Resolve config value that may be a scalar or a dict keyed by asset (e.g. stop_loss_pct: 10 or stop_loss_pct: { btc: 10, eth: 15 })."""
    v = cfg.get(key)
    if v is None:
        return default
    if isinstance(v, dict):
        a = (asset or "").strip().lower()
        out = v.get(a)
        if out is None:
            out = v.get(asset)
        return default if out is None else out
    return v


def _stop_loss_distance_factor(cfg: dict, asset: str) -> float:
    """Fraction of min_distance_at_placement below which we allow stop loss (e.g. 0.8 = 80%). Per-asset if config is dict. Default 0.8."""
    v = _get_per_asset(cfg, "stop_loss_distance_factor", asset, 0.8)
    if v is None:
        return 0.8
    try:
        f = float(v)
        if f <= 0 or f > 1.0:
            return 0.8
        return f
    except (TypeError, ValueError):
        return 0.8


def _maybe_stop_loss_hourly(
    config: dict,
    logger: logging.Logger,
    kalshi_client: Any,
    db_path: str,
    hour_market_id: str,
    asset: str,
    ticker: str,
    side: str,
    order_id: str,
    count: int,
    limit_price_cents: int,
    stop_loss_pct: float,
    cfg: dict,
    db_lock: Optional[threading.Lock] = None,
) -> None:
    """
    If the filled position for (ticker, side) is down >= stop_loss_pct, sell to close
    and record stop_loss_triggered (same idea as last_90s_limit_99).
    """
    if not kalshi_client or not order_id or stop_loss_pct <= 0:
        return
    try:
        order = kalshi_client.get_order(order_id)
        fill_count = int(order.get("fill_count") or order.get("filled_count") or 0)
    except Exception as e:
        logger.warning(
            "[hourly_last_90s] [%s] get_order %s for stop loss: %s",
            asset.upper(), order_id, e,
        )
        return
    if fill_count < 1:
        return
    try:
        book = kalshi_client.get_top_of_book(ticker)
    except Exception as e:
        logger.warning("[hourly_last_90s] [%s] orderbook for %s: %s", asset.upper(), ticker, e)
        return
    yes_bid = book.get("yes_bid")
    no_bid = book.get("no_bid")
    current_bid = (yes_bid if (side or "yes").lower() == "yes" else no_bid) or 0
    entry = max(1, limit_price_cents)
    loss_pct = (entry - current_bid) / entry if entry else 0
    if loss_pct < stop_loss_pct:
        return
    # Exit criteria (same as last_90s): only trigger stop loss if distance has fallen to <= stop_loss_distance_factor of min_distance_at_placement (temporary reversal filter).
    min_dist = _min_distance_at_placement(cfg, asset)
    stop_loss_dist_factor = _stop_loss_distance_factor(cfg, asset)
    if min_dist > 0:
        try:
            spot_sl = _get_spot_price(asset)
            if spot_sl is not None:
                distance_now: Optional[float] = None
                # Range (B) markets: compute distance to nearest range boundary using floor/ceiling strikes.
                try:
                    mkt = kalshi_client.get_market(ticker) if kalshi_client else None
                    if isinstance(mkt, dict):
                        floor_v = mkt.get("floor_strike")
                        ceil_v = mkt.get("ceiling_strike")
                        if floor_v is not None and ceil_v is not None:
                            floor_f = float(floor_v)
                            ceil_f = float(ceil_v)
                            if floor_f > 0 and ceil_f > 0:
                                spot_f = float(spot_sl)
                                if spot_f < floor_f:
                                    distance_now = floor_f - spot_f
                                elif spot_f > ceil_f:
                                    distance_now = spot_f - ceil_f
                                else:
                                    distance_now = min(spot_f - floor_f, ceil_f - spot_f)
                except Exception:
                    # If market fetch/parsing fails, fall back to strike-based distance below.
                    distance_now = None

                # Above/below (T) markets or fallback: distance to strike.
                if distance_now is None:
                    strike_sl = _parse_strike_from_ticker(ticker)
                    if strike_sl > 0:
                        distance_now = abs(float(spot_sl) - float(strike_sl))

                if distance_now is not None:
                    threshold = stop_loss_dist_factor * min_dist
                    if distance_now > threshold:
                        logger.info(
                            "[hourly_last_90s] [%s] HOLD: loss=%.1f%% but distance=%.2f > %.2f*min_dist=%.2f (%.2f) for %s — treat as temporary reversal",
                            asset.upper(), loss_pct * 100, distance_now, stop_loss_dist_factor, min_dist, threshold, ticker,
                        )
                        return
        except Exception as e:
            logger.warning("[hourly_last_90s] [%s] Distance check for stop loss failed (proceeding with stop): %s", asset.upper(), e)
    mode = (config.get("mode", "OBSERVE")).upper()
    sell_price = max(1, min(current_bid, 99)) if current_bid else 1
    if mode != "TRADE":
        logger.info(
            "[hourly_last_90s] [%s] OBSERVE: would stop loss sell %s x %d @ %dc for %s (entry=%dc loss=%.1f%%)",
            asset.upper(), side, fill_count, sell_price, ticker, entry, loss_pct * 100,
        )
        return
    logger.info(
        "[hourly_last_90s] [%s] STOP-LOSS: selling %s x %d @ %dc for %s (entry=%dc loss=%.1f%%)",
        asset.upper(), side, fill_count, sell_price, ticker, entry, loss_pct * 100,
    )
    try:
        resp = kalshi_client.place_limit_order(
            ticker=ticker,
            action="sell",
            side=side,
            price_cents=sell_price,
            count=fill_count,
            client_order_id=str(uuid.uuid4()),
        )
        if resp and getattr(resp, "status_code", 0) in (200, 201):
            with _db_section(db_lock):
                set_hourly_limit_99_stop_loss_triggered(db_path, hour_market_id, asset, ticker, side)
            try:
                pnl_stop = (sell_price - entry) * fill_count if (sell_price and entry and fill_count) else None
                with _db_section(db_lock):
                    update_resolution_hourly_last_90s(
                        db_path,
                        order_id=order_id,
                        window_id=hour_market_id,
                        asset=asset,
                        ticker=ticker,
                        side=side,
                        filled=1,
                        fill_price=entry,
                        resolution_price=sell_price,
                        final_outcome=None,
                        pnl_cents=pnl_stop,
                        is_stop_loss=1,
                        stop_loss_sell_price=sell_price,
                    )
            except Exception as e_res:
                logger.warning("[hourly_last_90s] [%s] Report update_resolution_hourly_last_90s (stop loss) failed: %s", asset.upper(), e_res)
            logger.info(
                "[hourly_last_90s] [%s] STOP_LOSS sell %s x %d @ %dc for %s (entry=%dc loss=%.1f%%)",
                asset.upper(), side, fill_count, sell_price, ticker, entry, loss_pct * 100,
            )
            logger.info(
                "%s",
                json.dumps({
                    "event": "HOURLY_LAST_90S_STOP_LOSS_REPORT",
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "asset": asset.upper(),
                    "hour_market_id": hour_market_id,
                    "ticker": ticker,
                    "side": side.lower(),
                    "entry_cents": entry,
                    "sell_price_cents": sell_price,
                    "fill_count": fill_count,
                    "loss_pct": round(loss_pct * 100, 2),
                    "stop_loss_pct": round(stop_loss_pct * 100, 2),
                    "order_id": order_id,
                }, default=str),
            )
        else:
            err = getattr(resp, "text", None) or getattr(resp, "reason", str(resp))
            logger.warning("[hourly_last_90s] [%s] Stop loss sell failed for %s: %s", asset.upper(), ticker, err)
    except Exception as e:
        logger.exception("[hourly_last_90s] [%s] Stop loss error for %s: %s", asset.upper(), ticker, e)


def _resolve_previous_hour(
    config: dict,
    logger: logging.Logger,
    db_path: str,
    db_lock: Optional[threading.Lock] = None,
) -> None:
    """Resolve unresolved hourly_limit_99 orders for the previous hour (all 9 markets: 2 per btc/eth/sol/xrp, 1 doge).
    First: write one aggregated row per (window_id, asset, ticker, side) that had skips but never placed."""
    cfg = _get_cfg(config)
    if not cfg:
        return
    assets = _assets(cfg, config)
    prev_ids: List[str] = []
    for asset in assets:
        prev_ids.extend(get_previous_hour_market_ids(asset))
    cleared = _skip_aggregate_get_and_clear_all_for_windows(prev_ids, db_path)
    with _db_section(db_lock):
        ensure_report_db(db_path)
        for (window_id, asset, ticker, side, rec) in cleared:
            last_bid = None
            last_distance = None
            if rec:
                bid_hist = rec.get("bid_history") or []
                dist_hist = rec.get("distance_history") or []
                last_bid = bid_hist[-1] if bid_hist else None
                last_distance = dist_hist[-1] if dist_hist else None
            details = _skip_aggregate_format_details(rec, "Aggregated skips") if rec else "No skip history recorded"
            try:
                write_row_hourly_last_90s(
                    db_path,
                    window_id,
                    asset,
                    ticker,
                    side,
                    placed=0,
                    skip_reason="aggregated_skips",
                    skip_details=details,
                    spot=rec.get("last_spot") if rec else None,
                    strike=rec.get("last_strike") if rec else None,
                    seconds_to_close=rec.get("last_seconds_to_close") if rec else None,
                    distance=last_distance,
                    min_distance_threshold=rec.get("last_min_distance_threshold") if rec else None,
                    bid=last_bid,
                )
            except Exception as e:
                logger.warning("[hourly_last_90s] [%s] Write aggregated skip row failed: %s", (asset or "").upper(), e)
    for asset in assets:
        for prev_id in get_previous_hour_market_ids(asset):
            try:
                with _db_section(db_lock):
                    rows = get_hourly_limit_99_unresolved_for_market(db_path, prev_id)
                if not rows:
                    continue
                for row in rows:
                    hour_market_id, a, ticker, side, order_id, count, limit_cents = row
                    if not order_id:
                        continue
                    try:
                        from src.client.kalshi_client import KalshiClient
                        client = KalshiClient()
                        order = client.get_order(order_id)
                    except Exception as e:
                        logger.warning("[hourly_last_90s] [%s] get_order %s failed: %s", a.upper(), order_id, e)
                        continue
                    status = (order.get("status") or "").lower()
                    executed = int(order.get("fill_count") or order.get("filled_count") or 0)
                    result = fetch_ticker_outcome(ticker) if ticker else None
                    outcome = "unfilled"
                    if executed and result:
                        outcome = "positive" if (side == "yes" and result == "yes") or (side == "no" and result == "no") else "negative"
                    elif executed:
                        outcome = "executed_unknown_result"
                    elif status == "canceled":
                        outcome = "unfilled"
                    with _db_section(db_lock):
                        update_hourly_limit_99_resolved(
                            db_path, hour_market_id, a, ticker, side,
                            executed_count=executed,
                            market_result=result or "?",
                            outcome=outcome,
                        )
                    # final_outcome = WIN | LOSS from Kalshi resolution (is_stop_loss/stop_loss_sell_price already set on stop loss).
                    try:
                        res_price = 100 if outcome == "positive" else (0 if outcome == "negative" else None)
                        final = "WIN" if outcome == "positive" else ("LOSS" if outcome == "negative" else None)
                        pnl = None
                        if executed and limit_cents is not None:
                            if outcome == "positive":
                                pnl = (100 - limit_cents) * executed
                            elif outcome == "negative":
                                pnl = -limit_cents * executed
                        with _db_section(db_lock):
                            update_resolution_hourly_last_90s(
                                db_path,
                                order_id=order_id,
                                window_id=hour_market_id,
                                asset=a,
                                ticker=ticker,
                                side=side,
                                filled=executed or 0,
                                fill_price=limit_cents,
                                resolution_price=res_price,
                                final_outcome=final,
                                pnl_cents=pnl,
                            )
                    except Exception as e_res:
                        logger.warning("[hourly_last_90s] [%s] Report update_resolution_hourly_last_90s failed: %s", a.upper(), e_res)
                    logger.info(
                        "[hourly_last_90s] OUTCOME [%s] market=%s ticker=%s side=%s order_id=%s executed=%d status=%s outcome=%s",
                        a.upper(), hour_market_id, ticker, side, order_id, executed, status, outcome,
                    )
            except Exception as e:
                logger.exception("[hourly_last_90s] Resolve error for %s %s: %s", asset, prev_id, e)


def run_hourly_last_90s_once(
    config: dict,
    logger: logging.Logger,
    kalshi_client: Optional[Any],
    db_path: str,
    db_lock: Optional[threading.Lock] = None,
) -> None:
    """
    One iteration: resolve previous hour; then for each asset in the last window_seconds
    use generate_signals_farthest(..., pick_all_in_range=True) for all 9 markets.
    Place: market-style or limit from live bid (same as last_90s); min_bid_cents skip.
    When db_lock is provided, assets are run in parallel via ThreadPoolExecutor; DB access is serialized.
    """
    cfg = _get_cfg(config)
    if not cfg or not cfg.get("enabled", False):
        return
    config_limit_cents = int(cfg.get("limit_price_cents", 99))
    if config_limit_cents < 1 or config_limit_cents > 99:
        config_limit_cents = 99
    config_limit_cents = max(1, min(99, config_limit_cents))
    min_bid_cents = int(cfg.get("min_bid_cents", 95))
    mode = (config.get("mode", "OBSERVE")).upper()
    if mode not in ("OBSERVE", "TRADE"):
        mode = "OBSERVE"
    assets = _assets(cfg, config)
    if not assets or not kalshi_client:
        return

    thresholds = (config.get("thresholds") or {}).copy()
    if not thresholds:
        thresholds = {"yes_min": 92, "yes_max": 99, "no_min": 92, "no_max": 99}
    # Last-90s is always "late" window; use late band if present
    if isinstance(thresholds.get("late"), dict):
        pass  # generate_signals_farthest will use thresholds["late"]
    else:
        thresholds.setdefault("yes_min", 92)
        thresholds.setdefault("yes_max", 99)
        thresholds.setdefault("no_min", 92)
        thresholds.setdefault("no_max", 99)

    _resolve_previous_hour(config, logger, db_path, db_lock=db_lock)

    def run_for_asset(asset: str) -> None:
        window_seconds = int(_get_per_asset(cfg, "window_seconds", asset, 75))
        stop_loss_pct = float(_get_per_asset(cfg, "stop_loss_pct", asset, 0)) / 100.0  # e.g. 15 -> 0.15; 0 = disabled
        spot = _get_spot_price(asset)
        if spot is None:
            logger.warning("[hourly_last_90s] [%s] No spot price", asset.upper())
            return
        for hour_market_id in get_current_hour_market_ids(asset):
            try:
                minutes_to_close = get_minutes_to_close(hour_market_id)
                seconds_to_close = minutes_to_close * 60.0
                if seconds_to_close <= 0 or seconds_to_close > window_seconds:
                    continue

                # Stop loss: for each unresolved placement (not yet stop-lossed), check fill and PnL; sell if loss >= stop_loss_pct
                if stop_loss_pct > 0:
                    with _db_section(db_lock):
                        stop_rows = get_hourly_limit_99_placements_for_stoploss(db_path, hour_market_id)
                    for row in stop_rows:
                        (_hm, a, t, s, oid, cnt, lpc) = row
                        _maybe_stop_loss_hourly(
                            config, logger, kalshi_client, db_path,
                            hour_market_id, a, t, s, oid or "", cnt or 0, lpc or 99,
                            stop_loss_pct, cfg,
                            db_lock=db_lock,
                        )

                window = _spot_window(config, asset)
                tickers_raw = fetch_eligible_tickers(hour_market_id, spot_price=spot, window=window)
                if not tickers_raw:
                    logger.info("[hourly_last_90s] [%s] No eligible tickers for %s", asset.upper(), hour_market_id)
                    continue
                quotes = enrich_with_orderbook(kalshi_client, tickers_raw)
                if not quotes:
                    continue

                # Same signal logic as regular hourly + bid qual. For above/below markets (KXBTCD, KXETHD, KXSOLD, KXXRPD)
                # only emit the direction-consistent side: spot < strike -> NO, spot > strike -> YES.
                above_below_prefixes = ("KXBTCD", "KXETHD", "KXSOLD", "KXXRPD")
                is_above_below = any(hour_market_id.upper().startswith(p) for p in above_below_prefixes)
                signals: List[Signal] = generate_signals_farthest(
                    quotes, spot, ctx_late_window=True, thresholds=thresholds, pick_all_in_range=True,
                    min_bid_cents=min_bid_cents,
                    filter_side_by_spot_strike=is_above_below,
                )
                # Cap to max 2 YES + 2 NO per market (9 markets × 4 = 36/hour).
                # Pick the 2 closest YES and 2 closest NO (by distance to spot/strike) for better liquidity and fill rate;
                # then min_distance_at_placement and min_bid_cents are applied in the placement loop.
                # Signals from generate_signals_farthest are farthest-first, so reverse to get closest-first before slicing.
                max_yes = max(0, int(cfg.get("max_yes_per_market", 2)))
                max_no = max(0, int(cfg.get("max_no_per_market", 2)))
                if max_yes > 0 or max_no > 0:
                    yes_sigs = [s for s in signals if str(s.side or "").lower() == "yes"]
                    no_sigs = [s for s in signals if str(s.side or "").lower() == "no"]
                    yes_sigs = list(reversed(yes_sigs))[:max_yes] if max_yes > 0 else yes_sigs
                    no_sigs = list(reversed(no_sigs))[:max_no] if max_no > 0 else no_sigs
                    signals = yes_sigs + no_sigs

                quote_by_ticker: Dict[str, TickerQuote] = {q.ticker: q for q in quotes}

                max_cost = _max_cost_cents(cfg, config, asset)
                min_dist_required = _min_distance_at_placement(cfg, asset)

                for sig in signals:
                    ticker = sig.ticker
                    side = str(sig.side or "").lower()
                    if side not in ("yes", "no"):
                        continue
                    with _db_section(db_lock):
                        already_placed = get_hourly_limit_99_placed(db_path, hour_market_id, asset, ticker, side)
                    if already_placed:
                        continue
                    quote = quote_by_ticker.get(ticker)
                    if not quote:
                        continue
                    strike = float(quote.strike or 0)
                    # B (range) markets: use distance to nearest range boundary (start/end), not midpoint.
                    # Skip if spot is within min_dist of either edge — small move could flip into another bucket.
                    range_low = getattr(quote, "range_low", None)
                    range_high = getattr(quote, "range_high", None)
                    if range_low is not None and range_high is not None:
                        if spot < range_low:
                            distance = range_low - spot
                        elif spot > range_high:
                            distance = spot - range_high
                        else:
                            distance = min(spot - range_low, range_high - spot)
                    else:
                        distance = abs(spot - strike) if strike > 0 else None
                    bid = (quote.yes_bid if side == "yes" else quote.no_bid) or 0
                    if min_dist_required > 0 and distance is not None and distance <= min_dist_required:
                        logger.info(
                            "[hourly_last_90s] [%s] SKIP: distance_to_boundary=%s <= min_distance_at_placement=%s for %s (range=%s..%s spot=%s)",
                            asset.upper(), distance, min_dist_required, ticker,
                            range_low if range_low is not None else strike, range_high if range_high is not None else strike, spot,
                        )
                        _skip_aggregate_record(
                            hour_market_id, asset, ticker, side, bid, distance, "distance_buffer",
                            spot=spot, strike=strike, seconds_to_close=seconds_to_close,
                            min_distance_threshold=min_dist_required if min_dist_required > 0 else None,
                            db_path=db_path,
                        )
                        continue
                    # B (range) sanity: YES only when spot inside bucket; NO only when spot outside bucket.
                    if range_low is not None and range_high is not None:
                        spot_in_bucket = range_low <= spot <= range_high
                        if side == "yes" and not spot_in_bucket:
                            logger.info(
                                "[hourly_last_90s] [%s] SKIP: YES but spot outside bucket (range=%s..%s spot=%s)",
                                asset.upper(), range_low, range_high, spot,
                            )
                            _skip_aggregate_record(
                                hour_market_id, asset, ticker, side, bid, distance, "spot_outside_bucket",
                                spot=spot, strike=strike, seconds_to_close=seconds_to_close,
                                min_distance_threshold=min_dist_required if min_dist_required > 0 else None,
                                db_path=db_path,
                            )
                            continue
                        if side == "no" and spot_in_bucket:
                            logger.info(
                                "[hourly_last_90s] [%s] SKIP: NO but spot inside bucket (range=%s..%s spot=%s)",
                                asset.upper(), range_low, range_high, spot,
                            )
                            _skip_aggregate_record(
                                hour_market_id, asset, ticker, side, bid, distance, "spot_inside_bucket_no",
                                spot=spot, strike=strike, seconds_to_close=seconds_to_close,
                                min_distance_threshold=min_dist_required if min_dist_required > 0 else None,
                                db_path=db_path,
                            )
                            continue
                    # Bid for the side we are placing (same side as signal; for range markets YES/NO are per range)
                    current_bid = bid
                    if current_bid < min_bid_cents:
                        logger.info(
                            "[hourly_last_90s] [%s] SKIP: %s bid=%dc < min_bid_cents=%d for %s (too risky)",
                            asset.upper(), side, current_bid, min_bid_cents, ticker,
                        )
                        _skip_aggregate_record(
                            hour_market_id, asset, ticker, side, current_bid, distance, "min_bid",
                            spot=spot, strike=strike, seconds_to_close=seconds_to_close,
                            min_distance_threshold=min_dist_required if min_dist_required > 0 else None,
                            db_path=db_path,
                        )
                        continue
                    # Market-style vs limit from live bid (same as last_90s): aggressive when bid < 99 and > 0, else passive limit at config.
                    use_market_style = (current_bid < 99 and current_bid > 0)
                    ask_cents = (quote.yes_ask if side == "yes" else quote.no_ask) or 0
                    if use_market_style:
                        if ask_cents <= 0 or ask_cents > 99:
                            logger.warning(
                                "[hourly_last_90s] [%s] SKIP: bid=%dc < 99 but ask=%s invalid for market-style for %s",
                                asset.upper(), current_bid, ask_cents, ticker,
                            )
                            _skip_aggregate_record(
                                hour_market_id, asset, ticker, side, current_bid, distance, "invalid_ask",
                                spot=spot, strike=strike, seconds_to_close=seconds_to_close,
                                min_distance_threshold=min_dist_required if min_dist_required > 0 else None,
                                db_path=db_path,
                            )
                            continue
                        price_to_use = max(1, min(99, ask_cents))
                        order_type_label = "market"
                    else:
                        price_to_use = config_limit_cents
                        order_type_label = "limit"
                    count = max(1, max_cost // price_to_use)
                    # Peek only: do not clear here. Clearing when we don't place (OBSERVE or place fails) would lose history for the aggregated row at resolve.
                    pre_placement_rec = _skip_aggregate_peek(hour_market_id, asset, ticker, side)
                    pre_placement_skip_details = _skip_aggregate_format_details(pre_placement_rec, prefix="Pre-placement history")
                    if mode == "OBSERVE":
                        logger.info(
                            "[hourly_last_90s] [%s] OBSERVE: would place %s %s @ %dc x %d for %s (%.0fs to close)",
                            asset.upper(), order_type_label, ticker, price_to_use, count, ticker, seconds_to_close,
                        )
                        continue
                    try:
                        resp = kalshi_client.place_limit_order(
                            ticker=ticker,
                            action="buy",
                            side=side,
                            price_cents=price_to_use,
                            count=count,
                            client_order_id=str(uuid.uuid4()),
                        )
                    except Exception as e:
                        logger.warning("[hourly_last_90s] [%s] Place failed for %s %s: %s", asset.upper(), ticker, side, e)
                        continue
                    if resp and getattr(resp, "status_code", 0) in (200, 201):
                        body = resp.json() if getattr(resp, "json", None) and getattr(resp, "text", None) else {}
                        order_id = (body.get("order") or {}).get("order_id") or body.get("order_id")
                        with _db_section(db_lock):
                            set_hourly_limit_99_placed(
                                db_path, hour_market_id, asset, ticker, side,
                                order_id=order_id, count=count, limit_price_cents=price_to_use,
                            )
                            ensure_report_db(db_path)
                            try:
                                write_row_hourly_last_90s(
                                    db_path,
                                    hour_market_id,
                                    asset,
                                    ticker,
                                    side,
                                    ts_utc=datetime.now(timezone.utc).isoformat(),
                                    spot=spot,
                                    strike=strike,
                                    distance=distance,
                                    min_distance_threshold=min_dist_required if min_dist_required > 0 else None,
                                    bid=current_bid,
                                    limit_or_market=order_type_label,
                                    price_cents=price_to_use,
                                    seconds_to_close=seconds_to_close,
                                    placed=1,
                                    order_id=order_id or "",
                                    skip_details=pre_placement_skip_details or None,
                                )
                            except Exception as e:
                                logger.warning("[hourly_last_90s] [%s] Report write_row_hourly_last_90s failed: %s", asset.upper(), e)
                        _skip_aggregate_get_and_clear(hour_market_id, asset, ticker, side, db_path)  # clear now that we wrote the placement row
                        logger.info(
                            "[hourly_last_90s] [%s] PLACED %s %s %s @ %dc x %d for %s (%.0fs to close) order_id=%s",
                            asset.upper(), order_type_label, side, ticker, price_to_use, count, ticker, seconds_to_close, order_id,
                        )
                        logger.info(
                            "%s",
                            json.dumps({
                                "event": "HOURLY_LAST_90S_PLACEMENT",
                                "ts_utc": datetime.now(timezone.utc).isoformat(),
                                "asset": asset.upper(),
                                "hour_market_id": hour_market_id,
                                "ticker": ticker,
                                "side": side,
                                "order_type": order_type_label,
                                "price_cents": price_to_use,
                                "count": count,
                                "seconds_to_close": seconds_to_close,
                                "order_id": order_id,
                            }, default=str),
                        )
            except Exception as e:
                logger.exception("[hourly_last_90s] [%s] Error for %s: %s", asset.upper(), hour_market_id, e)

    if db_lock is not None:
        max_workers = min(len(assets), 8)
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="hourly_last_90s_asset") as executor:
            futures = [executor.submit(run_for_asset, asset) for asset in assets]
            wait(futures)
            for fut in futures:
                try:
                    fut.result()
                except Exception as e:
                    logger.exception("[hourly_last_90s] Asset task error: %s", e)
    else:
        for asset in assets:
            run_for_asset(asset)


def run_hourly_last_90s_loop(
    config: dict,
    logger: logging.Logger,
    db_path: str,
    interval_seconds: float = 0.5,
) -> None:
    """Loop: run once every interval_seconds until interrupted. Uses a single DB lock so concurrent asset workers do not race on SQLite."""
    cfg = _get_cfg(config)
    interval_seconds = float(cfg.get("run_interval_seconds", 0.5) if cfg else interval_seconds)
    db_lock = threading.Lock()
    logger.info("[hourly_last_90s] Loop started (interval=%.1fs)", interval_seconds)
    try:
        from src.client.kalshi_client import KalshiClient
        client = KalshiClient()
    except Exception as e:
        logger.warning("[hourly_last_90s] Kalshi client init failed: %s", e)
        client = None
    while True:
        try:
            run_hourly_last_90s_once(config, logger, client, db_path, db_lock=db_lock)
        except Exception as e:
            logger.exception("[hourly_last_90s] Run error: %s", e)
        time.sleep(interval_seconds)
