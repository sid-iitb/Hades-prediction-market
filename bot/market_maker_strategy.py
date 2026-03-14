"""
Coin Toss Market Maker Strategy for 15-min Kalshi crypto markets.

Spread-capturing market maker: places Limit Buys on both YES and NO when the
market is a 50/50 coin toss (proximity, parity, spread, velocity gates).
Uses a high-speed Scratch Loop to protect against adverse selection (TTL,
adverse price, spot velocity ripcords).

Runs as an independent loop; does not interfere with last_90s or other strategies.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from bot.market import (
    extract_strike_from_market,
    fetch_15min_market,
    fetch_15min_quote,
    get_current_15min_market_id,
    get_minutes_to_close_15min,
)
from bot.state import ensure_state_db

MM_TABLE = "strategy_report_market_maker"

# In-memory: one active MM round per (market_id, asset). Value: dict with order_id_yes, order_id_no, ticker, spot_at_entry, leg1_side, etc.
_active_mm: Dict[Tuple[str, str], Dict[str, Any]] = {}

# State lock: strict mutual exclusion. When True we are in 'Managing' (fill/scratch monitor only). When False we are in 'Hunting'.
# Only set False when trade explicitly concludes: both legs fill, scratch fires, or zero-fill timeout (both canceled).
active_trade: Dict[Tuple[str, str], bool] = {}


def _get_cfg(config: dict) -> Optional[dict]:
    """Get market_maker_strategy config from fifteen_min."""
    fm = config.get("fifteen_min") or {}
    out = fm.get("market_maker_strategy") or config.get("market_maker_strategy")
    return out if isinstance(out, dict) else None


def _get_asset_config(value: Any, asset: str, default: Any) -> Any:
    """Resolve scalar or per-asset dict config."""
    if value is None:
        return default
    if isinstance(value, dict):
        return value.get(asset.lower(), value.get(asset.upper(), default))
    return value


def _get_spot_kraken(asset: str) -> Optional[float]:
    """Kraken spot price for asset."""
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
        return client.latest_btc_price().price
    except Exception:
        return None


def _get_spot_coinbase(asset: str) -> Optional[float]:
    """Coinbase spot price for asset."""
    a = (asset or "").strip().upper()
    if a not in ("BTC", "ETH", "SOL", "XRP"):
        return None
    url = f"https://api.coinbase.com/v2/prices/{a}-USD/spot"
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


def _quote_to_bids_asks(quote: Any) -> Tuple[int, int, int, int]:
    """Return (yes_bid, yes_ask, no_bid, no_ask) as ints, 0 if missing."""
    if not quote:
        return 0, 0, 0, 0
    yb = getattr(quote, "yes_bid", None) or (quote.get("yes_bid") if isinstance(quote, dict) else None)
    ya = getattr(quote, "yes_ask", None) or (quote.get("yes_ask") if isinstance(quote, dict) else None)
    nb = getattr(quote, "no_bid", None) or (quote.get("no_bid") if isinstance(quote, dict) else None)
    na = getattr(quote, "no_ask", None) or (quote.get("no_ask") if isinstance(quote, dict) else None)
    return (
        int(yb) if yb is not None else 0,
        int(ya) if ya is not None else 0,
        int(nb) if nb is not None else 0,
        int(na) if na is not None else 0,
    )


def _cancel_all_active_orders_for_market(
    client: Optional[Any],
    market_id: str,
    logger: logging.Logger,
    asset: str,
) -> None:
    """
    Orphaned order cleanup: cancel all resting limit orders for this 15-min market
    so no orders are left exposed in the final 2 minutes. Call when transitioning to Closed.
    """
    if not client:
        return
    m = fetch_15min_market(market_id)
    if not m:
        logger.info(
            "[mm] [%s] No market for %s during orphan cleanup — waiting for next 15-min market",
            asset.upper(),
            market_id,
        )
        return
    ticker = m.get("ticker")
    if not ticker:
        return
    try:
        # Kalshi uses 'resting' for open limit orders
        data = client.get_orders(ticker=ticker, status="resting", limit=100)
        orders = data.get("orders", data if isinstance(data, list) else [])
        if not orders:
            return
        for o in orders:
            oid = o.get("order_id") or o.get("id")
            if not oid:
                continue
            # Only cancel MM-owned orders. Other strategies (e.g. last_90s) may intentionally leave
            # resting orders during this window.
            coid = (o.get("client_order_id") or "").strip()
            if not coid.startswith("mm:"):
                continue
            try:
                client.cancel_order(oid)
                logger.info("[mm] [%s] Orphan cleanup: canceled order %s (ticker=%s)", asset.upper(), oid, ticker)
            except Exception as e:
                logger.warning("[mm] [%s] Orphan cleanup: cancel %s failed: %s", asset.upper(), oid, e)
    except Exception as e:
        logger.warning("[mm] [%s] Orphan cleanup: get_orders for %s failed: %s", asset.upper(), ticker, e)


def _write_mm_row(
    db_path: str,
    order_id: str,
    market_id: str,
    asset: str,
    *,
    spot_kraken: Optional[float] = None,
    spot_coinbase: Optional[float] = None,
    distance_kraken: Optional[float] = None,
    distance_coinbase: Optional[float] = None,
    leg1_side: Optional[str] = None,
    leg1_fill_price: Optional[int] = None,
    leg2_fill_price: Optional[int] = None,
    time_to_leg_seconds: Optional[float] = None,
    scratch_triggered: int = 0,
    scratch_reason: str = "None",
    pre_placement_history: Optional[str] = None,
    legging_telemetry_history: Optional[str] = None,
) -> None:
    """Insert one row into strategy_report_market_maker."""
    ensure_state_db(db_path)
    ts = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            f"""
            INSERT INTO {MM_TABLE} (
                order_id, market_id, asset, ts_utc,
                spot_kraken, spot_coinbase, distance_kraken, distance_coinbase,
                leg1_side, leg1_fill_price, leg2_fill_price, time_to_leg_seconds,
                scratch_triggered, scratch_reason,
                pre_placement_history, legging_telemetry_history
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                market_id,
                asset.lower(),
                ts,
                spot_kraken,
                spot_coinbase,
                distance_kraken,
                distance_coinbase,
                leg1_side,
                leg1_fill_price,
                leg2_fill_price,
                time_to_leg_seconds,
                scratch_triggered,
                scratch_reason,
                pre_placement_history,
                legging_telemetry_history,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _execute_scratch_and_fill_monitor(
    key: Tuple[str, str],
    client: Any,
    cfg: dict,
    db_path: str,
    logger: logging.Logger,
    pre_placement_history: Dict[Tuple[str, str], List[dict]],
) -> bool:
    """
    One tick of fill/scratch monitor. Returns True if trade concluded (caller should not re-enter),
    False if still in trade (stay in Managing state).
    On conclusion: write DB row, clear _active_mm[key], clear pre_placement_history[key], set active_trade[key]=False.
    """
    market_id, asset = key[0], key[1]
    ent = _active_mm.get(key)
    if not ent:
        active_trade[key] = False
        return True
    order_id_yes = ent.get("order_id_yes")
    order_id_no = ent.get("order_id_no")
    ticker = ent.get("ticker")
    price_yes = ent.get("price_yes")
    price_no = ent.get("price_no")
    placed_at = ent.get("placed_at") or 0
    zero_fill_timeout = float(cfg.get("zero_fill_timeout_seconds", 60))
    scratch_timeout = float(cfg.get("scratch_timeout_seconds", 4.0))
    max_loss_cents = int(cfg.get("max_acceptable_loss_cents", 10))
    spot_threshold = _get_asset_config(cfg.get("scratch_spot_movement_threshold"), asset, 75.0)

    try:
        order_yes = client.get_order(order_id_yes) if order_id_yes else {}
        order_no = client.get_order(order_id_no) if order_id_no else {}
    except Exception as e:
        logger.warning("[mm] [%s] get_order failed in monitor: %s", asset.upper(), e)
        return False

    fill_yes = int(order_yes.get("fill_count") or order_yes.get("filled_count") or 0)
    fill_no = int(order_no.get("fill_count") or order_no.get("filled_count") or 0)

    # Both filled natively (spread captured)
    if fill_yes >= 1 and fill_no >= 1:
        if ent.get("leg1_side") is None:
            ent["leg1_side"] = "yes"
            ent["leg1_fill_price"] = price_yes
            ent["leg2_fill_price"] = price_no
            ent["time_to_leg_seconds"] = 0.0
        _finalize_trade(key, ent, 0, "None", pre_placement_history, db_path, logger)
        return True

    # Detect leg1 if not yet set
    if ent.get("leg1_side") is None:
        if fill_yes >= 1:
            ent["leg1_side"] = "yes"
            ent["leg1_fill_price"] = price_yes
            ent["leg1_fill_time"] = time.time()
            logger.info("[mm] [%s] LEG1_FILL side=yes price=%dc order_id=%s", asset.upper(), price_yes, order_id_yes)
        elif fill_no >= 1:
            ent["leg1_side"] = "no"
            ent["leg1_fill_price"] = price_no
            ent["leg1_fill_time"] = time.time()
            logger.info("[mm] [%s] LEG1_FILL side=no price=%dc order_id=%s", asset.upper(), price_no, order_id_no)
        else:
            # Neither filled: check zero-fill timeout
            if (time.time() - placed_at) >= zero_fill_timeout:
                try:
                    client.cancel_order(order_id_yes)
                    client.cancel_order(order_id_no)
                except Exception as e:
                    logger.warning("[mm] [%s] Zero-fill cancel failed: %s", asset.upper(), e)
                ent["scratch_triggered"] = 1
                ent["scratch_reason"] = "Zero_Fill_Timeout"
                ent["time_to_leg_seconds"] = time.time() - placed_at
                logger.info("[mm] [%s] Zero-Fill Timeout: canceled both orders after %.0fs", asset.upper(), zero_fill_timeout)
                _finalize_trade(key, ent, 1, "Zero_Fill_Timeout", pre_placement_history, db_path, logger)
                return True
            return False

    # Leg1 filled; check leg2 and ripcords
    unfilled_side = "no" if ent["leg1_side"] == "yes" else "yes"
    unfilled_fill = fill_no if unfilled_side == "no" else fill_yes
    if unfilled_fill >= 1:
        ent["leg2_fill_price"] = price_no if unfilled_side == "no" else price_yes
        ent["time_to_leg_seconds"] = time.time() - ent["leg1_fill_time"]
        logger.info("[mm] [%s] EXIT happy leg2 filled side=%s price=%dc time_to_leg=%.2fs",
                    asset.upper(), unfilled_side, ent["leg2_fill_price"], ent["time_to_leg_seconds"])
        _finalize_trade(key, ent, 0, "None", pre_placement_history, db_path, logger)
        return True

    stopwatch = time.time() - ent["leg1_fill_time"]
    q = fetch_15min_quote(market_id, client)
    yb, ya, nb, na = _quote_to_bids_asks(q)
    sp = _get_spot_kraken(asset) or _get_spot_coinbase(asset)
    ent["legging_telemetry"].append({
        "elapsed": round(stopwatch, 2),
        "yes_bid": yb, "yes_ask": ya, "no_bid": nb, "no_ask": na,
        "spot": round(sp, 4) if sp is not None else None,
    })

    if stopwatch >= scratch_timeout:
        _do_scratch(key, ent, unfilled_side, order_id_yes, order_id_no, ticker, client, logger)
        ent["scratch_triggered"] = 1
        ent["scratch_reason"] = "Timeout"
        ent["time_to_leg_seconds"] = stopwatch
        logger.info("[mm] [%s] RIPCORD Timeout stopwatch=%.2fs >= %.2fs", asset.upper(), stopwatch, scratch_timeout)
        _finalize_trade(key, ent, 1, "Timeout", pre_placement_history, db_path, logger)
        return True

    current_ask = na if unfilled_side == "no" else ya
    cap = (100 - ent["leg1_fill_price"]) + max_loss_cents
    if current_ask is not None and current_ask >= cap:
        _do_scratch(key, ent, unfilled_side, order_id_yes, order_id_no, ticker, client, logger)
        ent["scratch_triggered"] = 1
        ent["scratch_reason"] = "Adverse_Price"
        ent["time_to_leg_seconds"] = stopwatch
        logger.info("[mm] [%s] RIPCORD Adverse_Price unfilled_ask=%s >= cap=%d", asset.upper(), current_ask, cap)
        _finalize_trade(key, ent, 1, "Adverse_Price", pre_placement_history, db_path, logger)
        return True

    if sp is not None and ent.get("spot_at_entry") is not None and abs(sp - ent["spot_at_entry"]) > spot_threshold:
        _do_scratch(key, ent, unfilled_side, order_id_yes, order_id_no, ticker, client, logger)
        ent["scratch_triggered"] = 1
        ent["scratch_reason"] = "Spot_Velocity"
        ent["time_to_leg_seconds"] = stopwatch
        logger.info("[mm] [%s] RIPCORD Spot_Velocity |spot_delta|=%.4f > %.4f", asset.upper(), abs(sp - ent["spot_at_entry"]), spot_threshold)
        _finalize_trade(key, ent, 1, "Spot_Velocity", pre_placement_history, db_path, logger)
        return True

    return False


def _do_scratch(
    key: Tuple[str, str],
    ent: Dict[str, Any],
    unfilled_side: str,
    order_id_yes: str,
    order_id_no: str,
    ticker: str,
    client: Any,
    logger: logging.Logger,
) -> None:
    unfilled_order_id = order_id_no if unfilled_side == "no" else order_id_yes
    logger.info("[mm] [%s] EXIT scratch reason=%s cancel order_id=%s then market_buy side=%s",
                key[1].upper(), ent.get("scratch_reason"), unfilled_order_id, unfilled_side)
    try:
        client.cancel_order(unfilled_order_id)
    except Exception as e:
        logger.warning("[mm] [%s] Cancel unfilled %s failed: %s", key[1].upper(), unfilled_order_id, e)
    try:
        client.place_limit_at_best_ask(ticker, unfilled_side, max_cost_cents=100)
    except Exception as e:
        logger.warning("[mm] [%s] Scratch market buy %s failed: %s", key[1].upper(), unfilled_side, e)


def _finalize_trade(
    key: Tuple[str, str],
    ent: Dict[str, Any],
    scratch_triggered: int,
    scratch_reason: str,
    pre_placement_history: Dict[Tuple[str, str], List[dict]],
    db_path: str,
    logger: logging.Logger,
) -> None:
    """Write DB row, clear state, release active_trade lock."""
    market_id, asset = key[0], key[1]
    report_order_id = f"mm:{market_id}:{asset}:{uuid.uuid4().hex[:12]}"
    hist = pre_placement_history.get(key, [])
    pre_placement_json = json.dumps(hist)
    legging_json = json.dumps(ent.get("legging_telemetry") or [])
    time_to_leg = ent.get("time_to_leg_seconds")
    _write_mm_row(
        db_path,
        report_order_id,
        market_id,
        asset,
        spot_kraken=ent.get("spot_kraken"),
        spot_coinbase=ent.get("spot_coinbase"),
        distance_kraken=ent.get("distance_kraken"),
        distance_coinbase=ent.get("distance_coinbase"),
        leg1_side=ent.get("leg1_side"),
        leg1_fill_price=ent.get("leg1_fill_price"),
        leg2_fill_price=ent.get("leg2_fill_price"),
        time_to_leg_seconds=time_to_leg,
        scratch_triggered=scratch_triggered,
        scratch_reason=scratch_reason,
        pre_placement_history=pre_placement_json,
        legging_telemetry_history=legging_json,
    )
    if key in pre_placement_history:
        pre_placement_history[key].clear()
    _active_mm.pop(key, None)
    active_trade[key] = False
    logger.info("[mm] [%s] Done report_id=%s scratch_triggered=%s reason=%s leg1=%s leg1_price=%s leg2_price=%s time_to_leg=%.2fs",
               asset.upper(), report_order_id, scratch_triggered, scratch_reason,
               ent.get("leg1_side"), ent.get("leg1_fill_price"), ent.get("leg2_fill_price"), time_to_leg or 0)


def _run_mm_for_asset(
    asset: str,
    config: dict,
    logger: logging.Logger,
    client: Any,
    db_path: str,
    cfg: dict,
    pre_placement_history: Dict[Tuple[str, str], List[dict]],
    spot_history: Dict[Tuple[str, str], deque],
) -> None:
    """One asset: strict state machine. Managing (fill/scratch) OR Hunting (gates + place). Never both."""
    # Prefer strategy-specific mode (market_maker_strategy.mode) over global config.mode
    mode = (cfg.get("mode") or config.get("mode") or "OBSERVE").upper()
    if mode not in ("OBSERVE", "TRADE"):
        mode = "OBSERVE"

    try:
        market_id = get_current_15min_market_id(asset=asset)
    except Exception:
        return
    try:
        minutes_to_close = get_minutes_to_close_15min(market_id)
        seconds_to_close = minutes_to_close * 60.0
    except Exception:
        return

    key = (market_id, asset.lower())
    min_sec = int(cfg.get("min_seconds_to_close", 120))
    max_sec = int(cfg.get("max_seconds_to_close", 840))

    # ----- Primary loop branching: strict mutual exclusion -----
    if active_trade.get(key):
        # STRICTLY in 'Managing' state. Do NOT evaluate gates. Do NOT place.
        _execute_scratch_and_fill_monitor(key, client, cfg, db_path, logger, pre_placement_history)
        return

    # ----- STRICTLY in 'Hunting' state below -----

    # End of MM window (seconds_to_close < 120): orphan cleanup then no-trade write if needed
    if seconds_to_close < min_sec:
        # Orphaned order cleanup: cancel all resting orders for this market so none are exposed in final 2 minutes
        _cancel_all_active_orders_for_market(client, market_id, logger, asset)
        if key in pre_placement_history and len(pre_placement_history[key]) > 0 and key not in _active_mm:
            report_order_id = f"mm:{market_id}:{asset}:{uuid.uuid4().hex[:12]}"
            pre_placement_json = json.dumps(pre_placement_history[key])
            spot_k_nt = _get_spot_kraken(asset)
            spot_cb_nt = _get_spot_coinbase(asset)
            m_nt = fetch_15min_market(market_id)
            ticker_nt = m_nt.get("ticker") if m_nt else None
            strike_nt = extract_strike_from_market(m_nt, ticker_nt) if m_nt and ticker_nt else 0.0
            dist_k_nt = abs(spot_k_nt - strike_nt) if spot_k_nt is not None and strike_nt and strike_nt > 0 else None
            dist_cb_nt = abs(spot_cb_nt - strike_nt) if spot_cb_nt is not None and strike_nt and strike_nt > 0 else None
            _write_mm_row(
                db_path,
                report_order_id,
                market_id,
                asset,
                spot_kraken=spot_k_nt,
                spot_coinbase=spot_cb_nt,
                distance_kraken=dist_k_nt,
                distance_coinbase=dist_cb_nt,
                leg1_side="None",
                scratch_triggered=0,
                scratch_reason="Window_End",
                pre_placement_history=pre_placement_json,
                legging_telemetry_history=json.dumps([]),
            )
            logger.info(
                "[mm] [%s] No-trade write at window end (sec_to_close=%.1f) report_id=%s pre_placement_ticks=%d",
                asset.upper(), seconds_to_close, report_order_id, len(pre_placement_history[key]),
            )
            pre_placement_history[key].clear()
        if key in spot_history:
            spot_history[key] = deque(maxlen=12)
        return

    # Too early (seconds_to_close >= 840): clear and return
    if seconds_to_close >= max_sec:
        if key in pre_placement_history:
            pre_placement_history[key] = []
        if key in spot_history:
            spot_history[key] = deque(maxlen=12)
        return

    # Observer: collect pre_placement_history (dist_kraken, dist_cb, yes_bid, yes_ask, no_bid, no_ask)
    m = fetch_15min_market(market_id)
    if not m:
        logger.info(
            "[mm] [%s] No market for %s — waiting for next 15-min market before running MM logic",
            asset.upper(),
            market_id,
        )
        return
    ticker = m.get("ticker")
    if not ticker:
        return

    # Failsafe: in Hunting we must have zero resting orders. If any exist, cancel and skip this tick.
    try:
        data = client.get_orders(ticker=ticker, status="resting", limit=100) if client else {}
        open_orders = data.get("orders", data if isinstance(data, list) else [])
    except Exception as e:
        logger.warning("[mm] [%s] Failsafe get_orders failed: %s", asset.upper(), e)
        return
    if open_orders:
        logger.warning(
            "[mm] [%s] Failsafe: Found %d orphaned order(s) while in Hunting state! Canceling and skipping tick.",
            asset.upper(), len(open_orders),
        )
        for o in open_orders:
            oid = o.get("order_id") or o.get("id")
            if oid:
                try:
                    coid = (o.get("client_order_id") or "").strip()
                    if not coid.startswith("mm:"):
                        # Do not cancel other strategies' orders (e.g. last_90s_limit_99c).
                        continue
                    client.cancel_order(oid)
                except Exception as e2:
                    logger.warning("[mm] [%s] Failsafe cancel %s failed: %s", asset.upper(), oid, e2)
        return

    quote = fetch_15min_quote(market_id, client)
    yes_bid, yes_ask, no_bid, no_ask = _quote_to_bids_asks(quote)
    spot_k = _get_spot_kraken(asset)
    spot_cb = _get_spot_coinbase(asset)
    strike = extract_strike_from_market(m, ticker) or 0.0
    dist_kraken = abs(spot_k - strike) if spot_k is not None and strike and strike > 0 else None
    dist_cb = abs(spot_cb - strike) if spot_cb is not None and strike and strike > 0 else None

    if key not in pre_placement_history:
        pre_placement_history[key] = []
    pre_placement_history[key].append({
        "dist_kraken": round(dist_kraken, 4) if dist_kraken is not None else None,
        "dist_cb": round(dist_cb, 4) if dist_cb is not None else None,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
    })

    # Rolling window for spot (5s ago)
    if key not in spot_history:
        spot_history[key] = deque(maxlen=12)
    current_spot = spot_k if spot_k is not None else spot_cb
    if current_spot is not None:
        spot_history[key].append((time.time(), current_spot))
    spot_5s_ago = None
    if len(spot_history[key]) >= 6:
        # Use entry from ~5 seconds ago (index -6)
        old = spot_history[key][-6]
        spot_5s_ago = old[1]

    # No mutual exclusion: when a trade concludes we pop from _active_mm so we can hunt again immediately.
    # If currently in fill monitor for this (market_id, asset), do not place another pair.
    if key in _active_mm:
        return

    # Gates 2–5
    max_dist = _get_asset_config(cfg.get("max_mm_distance"), asset, 15.0)
    min_parity = int(cfg.get("min_price_parity", 45))
    max_parity = int(cfg.get("max_price_parity", 55))
    min_spread = int(cfg.get("min_spread_cents", 8))
    velocity_thresh = _get_asset_config(cfg.get("safe_velocity_threshold_5s"), asset, 50.0)

    dist_min = None
    if dist_kraken is not None and dist_cb is not None:
        dist_min = min(dist_kraken, dist_cb)
    elif dist_kraken is not None:
        dist_min = dist_kraken
    elif dist_cb is not None:
        dist_min = dist_cb
    gate2 = dist_min is not None and dist_min <= max_dist

    gate3 = (min_parity <= yes_ask <= max_parity) and (min_parity <= no_ask <= max_parity)

    gate4 = (yes_ask - yes_bid) >= min_spread and (no_ask - no_bid) >= min_spread

    gate5 = True
    if spot_5s_ago is not None and current_spot is not None:
        gate5 = abs(current_spot - spot_5s_ago) <= velocity_thresh

    if not (gate2 and gate3 and gate4 and gate5):
        failed = []
        if not gate2:
            failed.append("proximity(dist_min=%.4f>%.4f)" % (dist_min or 0, max_dist))
        if not gate3:
            failed.append("parity(yes_ask=%s no_ask=%s not in [%d,%d])" % (yes_ask, no_ask, min_parity, max_parity))
        if not gate4:
            failed.append("spread(yes=%d no=%d need>=%d)" % (yes_ask - yes_bid, no_ask - no_bid, min_spread))
        if not gate5:
            move = abs((current_spot or 0) - (spot_5s_ago or 0))
            failed.append("velocity(|spot_delta|=%.4f>%.4f)" % (move, velocity_thresh))
        logger.debug(
            "[mm] [%s] Skip gates: %s | sec_to_close=%.1f dist_min=%s yes_ask=%s no_ask=%s",
            asset.upper(), ", ".join(failed), seconds_to_close, dist_min, yes_ask, no_ask,
        )
        return

    # Place Limit Buy YES at yes_bid+1, Limit Buy NO at no_bid+1 (clamp 1–99)
    # Strict contract sizing: always 1 contract per side for clean 1-to-1 Auto-Netting (ignore max_cost_cents).
    price_yes = max(1, min(99, yes_bid + 1))
    price_no = max(1, min(99, no_bid + 1))
    count = 1

    if mode != "TRADE":
        logger.info(
            "[mm] [%s] OBSERVE: would place YES @ %dc NO @ %dc x %d (gates pass) ticker=%s",
            asset.upper(), price_yes, price_no, count, ticker,
        )
        return

    if not client:
        return

    try:
        mm_leg_id = uuid.uuid4().hex[:12]
        resp_yes = client.place_limit_order(
            ticker=ticker,
            action="buy",
            side="yes",
            price_cents=price_yes,
            count=count,
            client_order_id=f"mm:{market_id}:{asset.lower()}:{mm_leg_id}:yes",
        )
        resp_no = client.place_limit_order(
            ticker=ticker,
            action="buy",
            side="no",
            price_cents=price_no,
            count=count,
            client_order_id=f"mm:{market_id}:{asset.lower()}:{mm_leg_id}:no",
        )
    except Exception as e:
        logger.warning("[mm] [%s] Place failed: %s", asset.upper(), e)
        return

    order_id_yes = None
    order_id_no = None
    if resp_yes and getattr(resp_yes, "status_code", 0) in (200, 201):
        try:
            body = resp_yes.json() if getattr(resp_yes, "json", None) else {}
            order_id_yes = (body.get("order") or {}).get("order_id") or body.get("order_id")
        except Exception:
            pass
    if resp_no and getattr(resp_no, "status_code", 0) in (200, 201):
        try:
            body = resp_no.json() if getattr(resp_no, "json", None) else {}
            order_id_no = (body.get("order") or {}).get("order_id") or body.get("order_id")
        except Exception:
            pass

    if not order_id_yes or not order_id_no:
        logger.warning("[mm] [%s] One or both orders failed to return order_id", asset.upper())
        return

    spot_at_entry = current_spot
    logger.info(
        "[mm] [%s] ENTRY placed ticker=%s YES@%dc NO@%dc x%d order_id_yes=%s order_id_no=%s sec_to_close=%.1f dist_min=%s spot=%.4f",
        asset.upper(), ticker, price_yes, price_no, count, order_id_yes, order_id_no,
        seconds_to_close, dist_min, spot_at_entry or 0,
    )
    _active_mm[key] = {
        "order_id_yes": order_id_yes,
        "order_id_no": order_id_no,
        "ticker": ticker,
        "spot_at_entry": spot_at_entry,
        "leg1_side": None,
        "leg1_fill_price": None,
        "leg2_fill_price": None,
        "legging_telemetry": [],
        "leg1_fill_time": None,
        "placed_at": time.time(),
        "price_yes": price_yes,
        "price_no": price_no,
        "spot_kraken": spot_k,
        "spot_coinbase": spot_cb,
        "distance_kraken": dist_kraken,
        "distance_coinbase": dist_cb,
    }
    active_trade[key] = True  # Transition to Managing; next tick runs fill/scratch monitor only.
    return


def run_market_maker_once(
    config: dict,
    logger: logging.Logger,
    client: Optional[Any],
    db_path: str,
    pre_placement_history: Dict[Tuple[str, str], List[dict]],
    spot_history: Dict[Tuple[str, str], deque],
) -> None:
    """One iteration: run MM logic for each configured asset."""
    cfg = _get_cfg(config)
    if not cfg or not cfg.get("enabled", False):
        return
    assets = cfg.get("assets") or ["btc", "eth", "sol", "xrp"]
    assets = [a for a in assets if str(a).lower() in ("btc", "eth", "sol", "xrp")]
    if not assets:
        return
    for asset in assets:
        try:
            _run_mm_for_asset(asset, config, logger, client, db_path, cfg, pre_placement_history, spot_history)
        except Exception as e:
            logger.exception("[mm] [%s] Error: %s", asset.upper(), e)


def run_market_maker_loop(
    config: dict,
    logger: logging.Logger,
    db_path: str,
    stop_event: Optional[Any] = None,
) -> None:
    """Main loop: run every run_interval_seconds until stop_event is set."""
    cfg = _get_cfg(config)
    if not cfg or not cfg.get("enabled", False):
        return
    interval = float(cfg.get("run_interval_seconds", 1))
    interval = max(0.5, min(interval, 60.0))
    client = None
    try:
        from src.client.kalshi_client import KalshiClient
        client = KalshiClient()
    except Exception as e:
        logger.warning("[mm] Kalshi client init failed: %s", e)

    pre_placement_history: Dict[Tuple[str, str], List[dict]] = {}
    spot_history: Dict[Tuple[str, str], deque] = {}

    logger.info("[mm] Coin Toss Market Maker loop started (interval=%.1fs)", interval)
    while stop_event is None or not getattr(stop_event, "is_set", lambda: False)():
        try:
            run_market_maker_once(config, logger, client, db_path, pre_placement_history, spot_history)
        except Exception as e:
            logger.exception("[mm] Run error: %s", e)
        time.sleep(interval)
