"""
Single pipeline cycle for Bot V2: one tick per interval.
Builds context per asset, evaluates strategies, aggregates intents, executes exits then entries.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from bot.market import (
    fetch_15min_market,
    fetch_markets_for_event,
    get_current_15min_market_id,
    get_current_hour_market_id,
    get_minutes_to_close,
    get_minutes_to_close_15min,
)

if TYPE_CHECKING:
    from bot.pipeline.aggregator import OrderAggregator
    from bot.pipeline.data_layer import DataLayer
    from bot.pipeline.executor import PipelineExecutor
    from bot.pipeline.registry import OrderRegistry
    from bot.pipeline.strategies.base import BaseV2Strategy

logger = logging.getLogger(__name__)


def _close_ts_from_market(market: Dict[str, Any]) -> Optional[int]:
    """Extract close timestamp (seconds since epoch) from market dict."""
    for key in ("close_time", "expected_expiration_time", "expiration_time"):
        val = market.get(key)
        if val is None:
            continue
        try:
            if isinstance(val, (int, float)):
                return int(val)
            dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except (ValueError, TypeError):
            pass
    return None


def _normalize_position(p: dict) -> Optional[Dict[str, Any]]:
    """Normalize Kalshi position to {ticker, side, count, entry_price_cents}. Returns None if invalid."""
    ticker = p.get("ticker") or p.get("market_ticker") or p.get("event_ticker")
    if not ticker:
        return None
    raw_side = p.get("side") if p.get("side") is not None else p.get("position")
    side = str(raw_side).lower() if raw_side is not None else ""
    count = p.get("contracts") or p.get("quantity") or p.get("count") or 0
    if not count and isinstance(raw_side, (int, float)):
        count = int(abs(raw_side))
    if isinstance(raw_side, (int, float)) and raw_side != 0:
        side = "yes" if raw_side > 0 else "no"
    if side not in {"yes", "no"} or not count or int(count) < 1:
        return None
    avg_price = p.get("avg_price") or p.get("average_price")
    price_cents = None
    if avg_price is not None:
        try:
            if isinstance(avg_price, (int, float)):
                price_cents = int(round(float(avg_price) * 100)) if float(avg_price) < 100 else int(avg_price)
            else:
                price_cents = int(round(float(str(avg_price).replace("$", "").replace(",", "")) * 100))
        except (TypeError, ValueError):
            pass
    if price_cents is None:
        cost = p.get("total_cost_dollars") or p.get("total_cost") or p.get("market_exposure_dollars")
        if cost is not None and count:
            try:
                c = float(str(cost).replace("$", "").replace(",", ""))
                price_cents = int(round(c * 100 / int(count)))
            except (TypeError, ValueError):
                pass
    if price_cents is None or price_cents < 1:
        return None
    return {"ticker": ticker, "side": side, "count": int(count), "entry_price_cents": price_cents}


def run_pipeline_cycle(
    interval: str,
    config: dict,
    data_layer: "DataLayer",
    strategies: List["BaseV2Strategy"],
    aggregator: "OrderAggregator",
    executor: "PipelineExecutor",
    registry: "OrderRegistry",
    kalshi_client: Any = None,
) -> None:
    """
    Run one cycle of the unified pipeline for the given interval.
    For each asset: fetch real market/quote/positions/orders, build context, evaluate strategies,
    aggregate intents, execute exits then entries.
    """
    intervals_block = config.get("intervals") or {}
    interval_config = intervals_block.get(interval)
    if not isinstance(interval_config, dict):
        logger.warning("No config for interval %s, skipping cycle", interval)
        return
    assets = interval_config.get("assets")
    if not isinstance(assets, (list, tuple)):
        assets = []

    # Detect window transition: current_window_id changed from last cycle
    first_asset = str(assets[0]).strip().lower() if assets else None
    if first_asset:
        if interval == "fifteen_min":
            current_window_id = get_current_15min_market_id(asset=first_asset)
        else:
            current_window_id = get_current_hour_market_id(asset=first_asset)
        last_window_id = getattr(data_layer, "_last_window_id", None)
        if last_window_id is not None and current_window_id != last_window_id:
            logger.info("[TRANSITION] Window expired. Initiating 20s cooldown and cache purge...")
            data_layer.clear_caches()
            time.sleep(20)
        data_layer._last_window_id = current_window_id

    for asset in assets:
        asset = str(asset).strip().lower()
        if not kalshi_client:
            logger.warning("[%s] [%s] No Kalshi client, skipping asset", interval, asset.upper())
            continue
        try:
            if interval == "fifteen_min":
                market_id = get_current_15min_market_id(asset=asset)
                market = fetch_15min_market(market_id)
            else:
                market_id = get_current_hour_market_id(asset=asset)
                markets, _ = fetch_markets_for_event(market_id)
                market = markets[0] if markets else None
            if not market or not isinstance(market, dict):
                logger.warning("[%s] [%s] No active market, skipping asset", interval, asset.upper())
                continue
            ticker = market.get("ticker")
            if not ticker:
                logger.warning("[%s] [%s] Market missing ticker, skipping asset", interval, asset.upper())
                continue
            close_ts = _close_ts_from_market(market)
            if close_ts is not None:
                now_ts = int(datetime.now(timezone.utc).timestamp())
                seconds_to_close = max(0.0, float(close_ts - now_ts))
            else:
                if interval == "fifteen_min":
                    mins = get_minutes_to_close_15min(market_id)
                else:
                    mins = get_minutes_to_close(market_id)
                seconds_to_close = max(0.0, mins * 60.0) if mins is not None else None
            if seconds_to_close is None or seconds_to_close < 0:
                logger.warning("[%s] [%s] Invalid seconds_to_close=%s, skipping asset", interval, asset.upper(), seconds_to_close)
                continue
            quote_source = "REST"
            try:
                from bot.kalshi_ws_manager import get_safe_orderbook
                top = get_safe_orderbook(ticker)
                if top and isinstance(top, dict) and (top.get("yes_bid") is not None or top.get("no_bid") is not None):
                    quote_source = "WS"
                else:
                    top = kalshi_client.get_top_of_book(ticker)
            except Exception:
                top = kalshi_client.get_top_of_book(ticker)
            if not top or not isinstance(top, dict):
                logger.warning("[%s] [%s] Orderbook fetch failed, skipping asset", interval, asset.upper())
                continue
            quote: Dict[str, int] = {
                "yes_bid": int(top.get("yes_bid") or 0),
                "yes_ask": int(top.get("yes_ask") or 0),
                "no_bid": int(top.get("no_bid") or 0),
                "no_ask": int(top.get("no_ask") or 0),
            }
            positions_raw = kalshi_client.get_positions(limit=200)
            positions_list = positions_raw.get("positions", []) if isinstance(positions_raw, dict) else []
            positions = []
            for p in positions_list:
                if not isinstance(p, dict):
                    continue
                pt = p.get("ticker") or p.get("market_ticker")
                if pt != ticker:
                    continue
                norm = _normalize_position(p)
                if norm:
                    positions.append(norm)
            orders_resp = kalshi_client.get_orders(status="resting", ticker=ticker, limit=100)
            open_orders = orders_resp.get("orders", []) if isinstance(orders_resp, dict) else []
            market_data = market
        except Exception as e:
            logger.warning("[%s] [%s] Fetch error, skipping asset: %s", interval, asset.upper(), e)
            continue

        ctx = data_layer.build_context(
            interval=interval,
            market_id=market_id,
            ticker=ticker,
            asset=asset,
            seconds_to_close=seconds_to_close,
            quote=quote,
            positions=positions,
            open_orders=open_orders,
            config=config,
            market_data=market_data,
        )
        if ctx.distance is None:
            logger.warning(
                "[%s] [%s] Distance is None (no strike or spot), skipping asset — strike=%s spot_kraken=%s spot_coinbase=%s",
                interval, asset.upper(), ctx.strike, ctx.spot_kraken, ctx.spot_coinbase,
            )
            continue
        # Dynamic float precision by asset: BTC/ETH 2, SOL 3, XRP 5
        _ndp = 2 if asset in ("btc", "eth") else 3 if asset == "sol" else 5 if asset == "xrp" else 2
        def _fmt(v: Any) -> str:
            if v is None:
                return "None"
            if isinstance(v, (int, float)):
                return f"{float(v):.{_ndp}f}"
            return str(v)
        strike_src = ctx.strike_source or "?"
        # Spot vs strike for UP/DOWN: use average of K and CB when both present, else whichever is present.
        _k, _cb, _strike = ctx.spot_kraken, ctx.spot_coinbase, ctx.strike
        if _strike is not None and (_k is not None or _cb is not None):
            spot_for_dir = (_k + _cb) / 2.0 if (_k is not None and _cb is not None) else (_k if _k is not None else _cb)
            dist_dir = "UP" if spot_for_dir > _strike else "DOWN"
        else:
            dist_dir = ""
        dist_str = f"{_fmt(ctx.distance)} ({dist_dir})" if dist_dir else _fmt(ctx.distance)
        logger.info(
            "[V2 DATA] %s | Strike: %s (%s) | K: %s | CB: %s | Dist: %s",
            asset.upper(),
            _fmt(ctx.strike),
            strike_src,
            _fmt(ctx.spot_kraken),
            _fmt(ctx.spot_coinbase),
            dist_str,
        )

        # --- Order status sync (critical for ATM exits) ---
        # Registry only records placements; we must infer fills so evaluate_exit can fire stop_loss/take_profit.
        open_order_ids = set()
        try:
            for o in (open_orders or []):
                if isinstance(o, dict) and o.get("order_id"):
                    open_order_ids.add(str(o.get("order_id")))
        except Exception:
            open_order_ids = set()

        asset_intents: List[tuple] = []
        asset_exits: List[Any] = []
        for strat in strategies:
            # If an order was previously registered as 'resting' but no longer appears in open_orders,
            # treat it as filled (best-effort) so exits can manage it.
            resting = registry.get_orders_by_strategy(
                strat.strategy_id, interval, market_id=market_id, asset=asset, active_only=True
            )
            for o in resting:
                if o.order_id and o.order_id not in open_order_ids:
                    try:
                        registry.update_order_status(o.order_id, "filled", int(o.count or 0))
                    except Exception:
                        pass

            # For exits, include filled orders too (not just resting).
            my_orders = registry.get_orders_by_strategy(
                strat.strategy_id, interval, market_id=market_id, asset=asset, active_only=False
            )
            exits = strat.evaluate_exit(ctx, my_orders)
            asset_exits.extend(exits)
            intent = strat.evaluate_entry(ctx, my_orders=my_orders)
            if intent is not None:
                asset_intents.append((intent, strat.strategy_id))

        interval_slice = config.get(interval) or {}
        active_orders = registry.get_all_active_orders_for_cap_check(interval, market_id=market_id)
        current_cost_by_strategy: Dict[str, int] = {}
        for _, strategy_id in asset_intents:
            if strategy_id not in current_cost_by_strategy:
                orders = registry.get_orders_by_strategy(
                    strategy_id, interval, market_id=market_id, asset=asset, active_only=False
                )
                current_cost_by_strategy[strategy_id] = sum(
                    o.count * (o.limit_price_cents or 99) for o in orders
                )
        final_intents = aggregator.resolve_intents(
            asset_intents, interval_slice, active_orders, current_cost_by_strategy
        )
        executor.execute_cycle(
            final_intents,
            asset_exits,
            interval,
            market_id,
            asset,
            ticker=ticker,
        )
