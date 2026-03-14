"""
ATM Breakout Sniper strategy for 15-min Kalshi crypto markets.

Directional breakout: when the market is coiled (48–52 parity) and spot moves
enough in 3 seconds, enter YES (spot up) or NO (spot down). Exit on
take-profit (bid >= 75c), stop-loss (bid <= 35c), or window end.

Strict state machine: HUNTING (evaluate gates, collect telemetry) vs IN_TRADE
(track position, exit loop). OBSERVE mode simulates entry/exit with live ask/bid.
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

ATM_BREAKOUT_TABLE = "strategy_report_atm_breakout"

# In-trade state per (market_id, asset). None = HUNTING. Dict = IN_TRADE with direction, entry_price, ticker, etc.
_in_trade: Dict[Tuple[str, str], Optional[Dict[str, Any]]] = {}


def _get_cfg(config: dict) -> Optional[dict]:
    """Get atm_breakout_strategy config from fifteen_min."""
    fm = config.get("fifteen_min") or {}
    out = fm.get("atm_breakout_strategy") or config.get("atm_breakout_strategy")
    return out if isinstance(out, dict) else None


def _get_asset_config(value: Any, asset: str, default: Any) -> Any:
    """Resolve scalar or per-asset dict config."""
    if value is None:
        return default
    if isinstance(value, dict):
        return value.get(asset.lower(), value.get(asset.upper(), default))
    return value


def _get_spot_kraken(asset: str) -> Optional[float]:
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


def _poll_order_fill(
    client: Any,
    order_id: str,
    logger: logging.Logger,
    asset: str,
    max_wait_sec: float = 15.0,
    poll_interval_sec: float = 0.5,
) -> Tuple[int, str]:
    """Poll get_order until executed/canceled or timeout. Returns (fill_count, status)."""
    if not client or not order_id:
        return 0, "no_client"
    deadline = time.time() + max_wait_sec
    while time.time() < deadline:
        try:
            order = client.get_order(order_id)
            status = (order.get("status") or "").strip().lower()
            fill_count = int(order.get("fill_count") or order.get("filled_count") or 0)
            if fill_count >= 1 or status in ("executed", "canceled", "expired"):
                return fill_count, status
        except Exception as e:
            logger.debug("[atm] [%s] get_order %s: %s", asset.upper(), order_id, e)
        time.sleep(poll_interval_sec)
    return 0, "timeout"


def _write_atm_breakout_row(
    db_path: str,
    order_id: str,
    market_id: str,
    asset: str,
    *,
    direction: Optional[str] = None,
    entry_price: Optional[int] = None,
    exit_price: Optional[int] = None,
    exit_reason: str = "None",
    time_in_trade_seconds: Optional[float] = None,
    pre_placement_history: Optional[str] = None,
    trade_telemetry_history: Optional[str] = None,
) -> None:
    """Insert one row into strategy_report_atm_breakout."""
    ensure_state_db(db_path)
    ts = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            f"""
            INSERT INTO {ATM_BREAKOUT_TABLE} (
                order_id, market_id, asset, ts_utc,
                direction, entry_price, exit_price, exit_reason,
                time_in_trade_seconds, pre_placement_history, trade_telemetry_history
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                market_id,
                asset.lower(),
                ts,
                direction,
                entry_price,
                exit_price,
                exit_reason,
                time_in_trade_seconds,
                pre_placement_history,
                trade_telemetry_history,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _run_atm_for_asset(
    asset: str,
    config: dict,
    logger: logging.Logger,
    client: Any,
    db_path: str,
    cfg: dict,
    pre_placement_history: Dict[Tuple[str, str], List[dict]],
    spot_history: Dict[Tuple[str, str], deque],
) -> None:
    """One asset: strict HUNTING vs IN_TRADE; gates, entry at ask, exit loop, DB logging."""
    # Prefer strategy-specific mode (atm_breakout_strategy.mode) over global config.mode
    mode = (cfg.get("mode") or config.get("mode") or "OBSERVE").strip().upper()
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

    # ----- IN_TRADE: exit loop only -----
    ent = _in_trade.get(key)
    if ent is not None and isinstance(ent, dict):
        _execute_exit_loop(key, ent, client, cfg, db_path, logger, pre_placement_history, seconds_to_close, min_sec)
        return

    # ----- HUNTING below -----

    # Window end: no-trade write if we have pre_placement history, then return
    if seconds_to_close < min_sec:
        if key in pre_placement_history and len(pre_placement_history[key]) > 0:
            report_order_id = f"atm:{market_id}:{asset}:{uuid.uuid4().hex[:12]}"
            pre_placement_json = json.dumps(pre_placement_history[key])
            _write_atm_breakout_row(
                db_path,
                report_order_id,
                market_id,
                asset,
                direction="None",
                entry_price=None,
                exit_price=None,
                exit_reason="NoTrade",
                time_in_trade_seconds=None,
                pre_placement_history=pre_placement_json,
                trade_telemetry_history=json.dumps([]),
            )
            logger.info(
                "[atm] [%s] No-trade write at window end (sec_to_close=%.1f) report_id=%s pre_placement_ticks=%d",
                asset.upper(), seconds_to_close, report_order_id, len(pre_placement_history[key]),
            )
            pre_placement_history[key].clear()
        if key in spot_history:
            spot_history[key] = deque(maxlen=8)
        _in_trade.pop(key, None)
        return

    # Too early
    if seconds_to_close >= max_sec:
        if key in pre_placement_history:
            pre_placement_history[key] = []
        if key in spot_history:
            spot_history[key] = deque(maxlen=8)
        _in_trade.pop(key, None)
        return

    # Fetch market and quote; if market is not yet listed for this 15-min slot, skip until next run.
    m = fetch_15min_market(market_id)
    if not m:
        logger.info(
            "[atm] [%s] No market for %s — waiting for next 15-min market before running breakout logic",
            asset.upper(),
            market_id,
        )
        return
    ticker = m.get("ticker")
    if not ticker:
        return
    quote = fetch_15min_quote(market_id, client)
    yes_bid, yes_ask, no_bid, no_ask = _quote_to_bids_asks(quote)
    spot_k = _get_spot_kraken(asset)
    spot_cb = _get_spot_coinbase(asset)
    strike = extract_strike_from_market(m, ticker) or 0.0
    dist_kraken = abs(spot_k - strike) if spot_k is not None and strike and strike > 0 else None
    dist_cb = abs(spot_cb - strike) if spot_cb is not None and strike and strike > 0 else None
    current_spot = spot_k if spot_k is not None else spot_cb

    # Pre-placement history (hunting telemetry)
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

    # Spot history for 3s-ago (keep last ~5 samples at 1s interval)
    if key not in spot_history:
        spot_history[key] = deque(maxlen=8)
    if current_spot is not None:
        spot_history[key].append((time.time(), current_spot))
    now = time.time()
    spot_3s_ago = None
    for (t, s) in reversed(list(spot_history[key])):
        if now - t >= 2.5:
            spot_3s_ago = s
            break

    # Gate 1: time window
    if not (min_sec <= seconds_to_close <= max_sec):
        return

    # Gate 2: coil (parity 48–52)
    min_parity = int(cfg.get("min_price_parity", 48))
    max_parity = int(cfg.get("max_price_parity", 52))
    gate2 = (min_parity <= yes_ask <= max_parity) and (min_parity <= no_ask <= max_parity)
    if not gate2:
        return

    # Gate 3: momentum trigger (need spot_3s_ago)
    if current_spot is None or spot_3s_ago is None:
        return
    momentum_thresh = _get_asset_config(cfg.get("momentum_trigger_3s"), asset, 15.0)
    delta = current_spot - spot_3s_ago
    trigger_yes = delta >= momentum_thresh
    trigger_no = (spot_3s_ago - current_spot) >= momentum_thresh
    if not trigger_yes and not trigger_no:
        return

    direction = "YES" if trigger_yes else "NO"
    entry_price = yes_ask if direction == "YES" else no_ask
    if entry_price <= 0:
        return

    report_order_id = f"atm:{market_id}:{asset}:{uuid.uuid4().hex[:12]}"

    # Entry size: config contracts, capped by max_cost_cents if set (contracts <= max_cost_cents // entry_price).
    contracts_config = max(1, int(cfg.get("contracts", 1)))
    max_cost = cfg.get("max_cost_cents")
    if max_cost is not None and int(max_cost) > 0 and entry_price > 0:
        cap = max(1, int(max_cost) // entry_price)
        entry_contracts = min(contracts_config, cap)
    else:
        entry_contracts = contracts_config

    if mode == "TRADE" and client:
        # Place entry order and only transition to IN_TRADE after verified fill (avoid phantom trades).
        try:
            resp = client.place_limit_order(
                ticker=ticker,
                action="buy",
                side=direction.lower(),
                price_cents=entry_price,
                count=entry_contracts,
                client_order_id=f"atm:{market_id}:{asset}:{report_order_id[-12:]}",
            )
        except Exception as e:
            logger.warning("[atm] [%s] ATM Entry order place failed: %s", asset.upper(), e)
            logger.warning("[atm] [%s] ATM Entry Failed or 0 Fills. Aborting trade tracking.", asset.upper())
            return
        if not resp or getattr(resp, "status_code", 0) not in (200, 201):
            err = getattr(resp, "text", None) if resp else "no response"
            logger.warning("[atm] [%s] ATM Entry order rejected: %s", asset.upper(), err)
            logger.warning("[atm] [%s] ATM Entry Failed or 0 Fills. Aborting trade tracking.", asset.upper())
            return
        body = resp.json() if getattr(resp, "json", None) and getattr(resp, "text", None) else {}
        kalshi_order_id = (body.get("order") or {}).get("order_id") or body.get("order_id")
        if not kalshi_order_id:
            logger.warning("[atm] [%s] ATM Entry response had no order_id: %s", asset.upper(), body)
            logger.warning("[atm] [%s] ATM Entry Failed or 0 Fills. Aborting trade tracking.", asset.upper())
            return
        logger.info("[atm] [%s] ATM Entry Order Placed. Awaiting fill confirmation...", asset.upper())
        fill_count, order_status = _poll_order_fill(client, kalshi_order_id, logger, asset, max_wait_sec=15.0, poll_interval_sec=0.5)
        if fill_count < 1:
            logger.warning(
                "[atm] [%s] ATM Entry Failed or 0 Fills. Aborting trade tracking. (status=%s fill_count=%d)",
                asset.upper(), order_status, fill_count,
            )
            # Cancel the unfilled resting order so it does not block other strategies (e.g. last_90s) on the same ticker.
            try:
                client.cancel_order(kalshi_order_id)
                logger.info("[atm] [%s] Canceled unfilled entry order %s so book is clear for other strategies.", asset.upper(), kalshi_order_id)
            except Exception as e:
                logger.warning("[atm] [%s] Failed to cancel unfilled entry order %s: %s", asset.upper(), kalshi_order_id, e)
            return
        logger.info("[atm] [%s] ATM Entry Filled! Starting TakeProfit/StopLoss monitoring.", asset.upper())
        _in_trade[key] = {
            "direction": direction,
            "entry_price": entry_price,
            "ticker": ticker,
            "market_id": market_id,
            "entered_at": time.time(),
            "trade_telemetry": [],
            "report_order_id": report_order_id,
            "kalshi_order_id": kalshi_order_id,
            "contracts_filled": fill_count,
            "highest_seen_bid": 0,
        }
        return
    else:
        # OBSERVE: simulate fill at ask (no Kalshi order; do not write real trade to CSV as completed).
        logger.info(
            "[atm] [%s] ENTRY direction=%s entry_price=%dc x %d (simulated at ask) ticker=%s sec_to_close=%.1f",
            asset.upper(), direction, entry_price, entry_contracts, ticker, seconds_to_close,
        )
        _in_trade[key] = {
            "direction": direction,
            "entry_price": entry_price,
            "ticker": ticker,
            "market_id": market_id,
            "entered_at": time.time(),
            "trade_telemetry": [],
            "report_order_id": report_order_id,
            "kalshi_order_id": None,
            "contracts_filled": entry_contracts,
            "highest_seen_bid": 0,
        }
        return


def _place_and_verify_exit_order(
    client: Any,
    ticker: str,
    direction: str,
    exit_price: int,
    contracts: int,
    logger: logging.Logger,
    asset: str,
    exit_reason: str,
    use_market: bool = False,
) -> Tuple[bool, int]:
    """Place exit (sell) order and poll until filled. Returns (success, fill_count).
    Market orders must NOT include price_cents/price (Kalshi rejects them)."""
    if not client or contracts < 1:
        return False, 0
    try:
        if use_market:
            # Do NOT pass price_cents or price to market order — Kalshi will reject it.
            resp = client.place_market_order(ticker=ticker, action="sell", side=direction.lower(), count=contracts)
        else:
            resp = client.place_limit_order(
                ticker=ticker, action="sell", side=direction.lower(), price_cents=exit_price, count=contracts,
            )
    except Exception as e:
        logger.error(
            "[EXECUTION FATAL] Kalshi API rejected the exit order: %s",
            str(e),
            exc_info=True,
        )
        return False, 0
    ok = resp and (isinstance(resp, dict) or getattr(resp, "status_code", 0) in (200, 201))
    if not ok:
        err_text = (resp.get("message") or resp.get("error") or str(resp)) if isinstance(resp, dict) else (getattr(resp, "text", None) if resp else "no response")
        logger.error(
            "[EXECUTION FATAL] Kalshi API rejected the exit order (%s): status=%s body=%s",
            exit_reason,
            getattr(resp, "status_code", None) if not isinstance(resp, dict) else "N/A",
            err_text,
        )
        return False, 0
    body = resp if isinstance(resp, dict) else (resp.json() if getattr(resp, "json", None) else {})
    order_id = (body.get("order") or {}).get("order_id") or body.get("order_id")
    if not order_id:
        logger.error("[EXECUTION FATAL] Exit order response had no order_id: %s", body)
        return False, 0
    fill_count, status = _poll_order_fill(client, order_id, logger, asset, max_wait_sec=20.0, poll_interval_sec=0.5)
    if fill_count >= 1:
        logger.info("[atm] [%s] Exit order filled (%s) fill_count=%d", asset.upper(), exit_reason, fill_count)
        return True, fill_count
    if use_market and status == "canceled":
        logger.error(
            "[EXECUTION FATAL] Kalshi instantly canceled the Market Order (Likely Spread/Price Protection). "
            "exit_reason=%s ticker=%s order_id=%s",
            exit_reason, ticker, order_id,
        )
    else:
        logger.warning(
            "[atm] [%s] Exit order not filled (%s) status=%s fill_count=%d",
            asset.upper(), exit_reason, status, fill_count,
        )
    return False, fill_count


# Exit loop config fallbacks: take_profit_cents=75, stop_loss_cents=35, trailing_stop_cents=10


def _execute_exit_loop(
    key: Tuple[str, str],
    ent: Dict[str, Any],
    client: Any,
    cfg: dict,
    db_path: str,
    logger: logging.Logger,
    pre_placement_history: Dict[Tuple[str, str], List[dict]],
    seconds_to_close: float,
    min_sec: int,
) -> None:
    """One tick of exit loop: collect telemetry, check window end / ratchet take-profit / stop-loss, place exit and verify fill, then write row."""
    market_id, asset = key[0], key[1]
    direction = (ent.get("direction") or "YES").upper()
    take_profit = int(cfg.get("take_profit_cents", 75))
    stop_loss = int(cfg.get("stop_loss_cents", 35))
    trailing_stop = int(cfg.get("trailing_stop_cents", 10))
    ticker = ent.get("ticker")
    report_order_id = ent.get("report_order_id") or f"atm:{market_id}:{asset}:{uuid.uuid4().hex[:12]}"
    kalshi_order_id = ent.get("kalshi_order_id")
    contracts = max(1, int(ent.get("contracts_filled") or 1))

    quote = fetch_15min_quote(market_id, client)
    yb, ya, nb, na = _quote_to_bids_asks(quote)
    # Exit bid: when holding YES we sell at best_yes_bid; when holding NO we sell at best_no_bid.
    our_bid = yb if direction == "YES" else nb
    current_bid = our_bid if our_bid is not None else 0
    elapsed = time.time() - ent["entered_at"]
    ent.setdefault("trade_telemetry", []).append({
        "elapsed": round(elapsed, 2),
        "yes_bid": yb, "yes_ask": ya, "no_bid": nb, "no_ask": na,
        "our_bid": our_bid,
    })

    # Update high-water mark for ratchet trailing stop
    highest_seen_bid = max(ent.get("highest_seen_bid", 0), current_bid)
    ent["highest_seen_bid"] = highest_seen_bid

    # Window end: close at current bid
    if seconds_to_close < min_sec:
        exit_price = our_bid if our_bid and our_bid > 0 else ent["entry_price"]
        if kalshi_order_id and client:
            ok, _ = _place_and_verify_exit_order(
                client, ticker, direction, exit_price, contracts, logger, asset, "WindowEnd", use_market=True,
            )
            if not ok:
                logger.warning("[atm] [%s] WindowEnd exit order did not fill; writing row anyway to clear state.", asset.upper())
        _close_trade(key, ent, exit_price, "WindowEnd", pre_placement_history, db_path, logger)
        return

    # Ratchet trailing stop: trigger when bid falls back from high
    if highest_seen_bid >= take_profit:
        dynamic_floor = max(take_profit, highest_seen_bid - trailing_stop)
        if current_bid <= dynamic_floor:
            if kalshi_order_id and client:
                logger.info(
                    "[atm] Trailing TP triggered! High: %dc, Floor: %dc, Current: %dc. Firing 1¢ Limit Sweep.",
                    highest_seen_bid, dynamic_floor, current_bid,
                )
                exit_price_est = our_bid if our_bid and our_bid > 0 else dynamic_floor
                ok, _ = _place_and_verify_exit_order(
                    client, ticker, direction, 1, contracts, logger, asset, "TakeProfit", use_market=False,
                )
                if not ok:
                    logger.warning("[atm] [%s] Trailing TP market sell did not fill; will retry next tick.", asset.upper())
                    return
                _close_trade(key, ent, exit_price_est, "TakeProfit", pre_placement_history, db_path, logger)
            else:
                _close_trade(key, ent, dynamic_floor, "TakeProfit", pre_placement_history, db_path, logger)
            return

    # Stop-loss: trigger on <= (not ==) to catch gaps and zero bid when market crashes.
    # Use current_bid (our side: YES -> yes_bid, NO -> no_bid). Execute via market order (sweep), not limit.
    if current_bid <= stop_loss:
        if kalshi_order_id and client:
            logger.warning(
                "[atm] STOP-LOSS TRIGGERED! Bid %s <= %s. Firing 1¢ Limit Sweep.",
                current_bid, stop_loss,
            )
            ok, _ = _place_and_verify_exit_order(
                client, ticker, direction, 1, contracts, logger, asset, "StopLoss", use_market=False,
            )
            if not ok:
                logger.warning(
                    "[atm] [%s] StopLoss market sell did not fill (e.g. rate limit); keeping trade active, will retry next tick.",
                    asset.upper(),
                )
                return
        _close_trade(key, ent, current_bid, "StopLoss", pre_placement_history, db_path, logger)
        return

    # Still in trade
    return


def _close_trade(
    key: Tuple[str, str],
    ent: Dict[str, Any],
    exit_price: int,
    exit_reason: str,
    pre_placement_history: Dict[Tuple[str, str], List[dict]],
    db_path: str,
    logger: logging.Logger,
) -> None:
    """Write DB row and clear IN_TRADE state."""
    market_id, asset = key[0], key[1]
    report_order_id = ent.get("report_order_id") or f"atm:{market_id}:{asset}:{uuid.uuid4().hex[:12]}"
    time_in_trade = time.time() - ent["entered_at"]
    hist = pre_placement_history.get(key, [])
    pre_placement_json = json.dumps(hist)
    trade_telemetry_json = json.dumps(ent.get("trade_telemetry") or [])

    _write_atm_breakout_row(
        db_path,
        report_order_id,
        market_id,
        asset,
        direction=ent.get("direction"),
        entry_price=ent.get("entry_price"),
        exit_price=exit_price,
        exit_reason=exit_reason,
        time_in_trade_seconds=time_in_trade,
        pre_placement_history=pre_placement_json,
        trade_telemetry_history=trade_telemetry_json,
    )
    if key in pre_placement_history:
        pre_placement_history[key].clear()
    _in_trade.pop(key, None)
    logger.info(
        "[atm] [%s] EXIT report_id=%s direction=%s entry=%dc exit=%dc reason=%s time_in_trade=%.2fs",
        asset.upper(), report_order_id, ent.get("direction"), ent.get("entry_price"), exit_price, exit_reason, time_in_trade,
    )


def run_atm_breakout_once(
    config: dict,
    logger: logging.Logger,
    client: Optional[Any],
    db_path: str,
    pre_placement_history: Dict[Tuple[str, str], List[dict]],
    spot_history: Dict[Tuple[str, str], deque],
) -> None:
    """One iteration: run ATM breakout for each configured asset."""
    cfg = _get_cfg(config)
    if not cfg or not cfg.get("enabled", False):
        return
    assets = cfg.get("assets") or ["btc", "eth", "sol", "xrp"]
    assets = [a for a in assets if str(a).lower() in ("btc", "eth", "sol", "xrp")]
    if not assets:
        return
    for asset in assets:
        try:
            _run_atm_for_asset(asset, config, logger, client, db_path, cfg, pre_placement_history, spot_history)
        except Exception as e:
            logger.exception("[atm] [%s] Error: %s", asset.upper(), e)


def run_atm_breakout_loop(
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
        logger.warning("[atm] Kalshi client init failed: %s", e)

    pre_placement_history: Dict[Tuple[str, str], List[dict]] = {}
    spot_history: Dict[Tuple[str, str], deque] = {}

    logger.info("[atm] ATM Breakout Sniper loop started (interval=%.1fs)", interval)
    while stop_event is None or not getattr(stop_event, "is_set", lambda: False)():
        try:
            run_atm_breakout_once(config, logger, client, db_path, pre_placement_history, spot_history)
        except Exception as e:
            logger.exception("[atm] Run error: %s", e)
        time.sleep(interval)
