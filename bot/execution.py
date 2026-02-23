"""
Order execution - only runs when MODE=TRADE.
Enforces caps before placing orders.
Ladder: try bid at x-2, x-1, x (x=ask) back-to-back; fall back to ask if no fill.
"""
import time
import uuid
from dataclasses import dataclass
from typing import List, Optional

from bot.state import (
    get_ticker_order_count,
    get_total_order_count,
    increment_order_count,
)
from bot.strategy import Signal


def _parse_cents(v) -> Optional[int]:
    """Parse value to int cents, return None if invalid."""
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


@dataclass
class ExecutionResult:
    signal: Signal
    status: str  # PLACED, SKIPPED_CAP_REACHED, FAILED
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    price_cents: Optional[int] = None
    contracts: Optional[int] = None
    error: Optional[str] = None
    # Top-of-book at order time (for analysis)
    yes_bid_cents: Optional[int] = None
    yes_ask_cents: Optional[int] = None
    no_bid_cents: Optional[int] = None
    no_ask_cents: Optional[int] = None


def should_skip_cap(
    db_path: str,
    hour_market_id: str,
    ticker: str,
    cap_scope: str,
    max_per_ticker: int,
    max_total: int,
    side: Optional[str] = None,
) -> bool:
    """True if we should skip due to cap. For per_side caps, pass side."""
    ticker_count = get_ticker_order_count(
        db_path, hour_market_id, ticker, cap_scope, side=side
    )
    total_count = get_total_order_count(db_path, hour_market_id)
    return ticker_count >= max_per_ticker or total_count >= max_total


def execute_signals(
    signals: List[Signal],
    hour_market_id: str,
    db_path: str,
    kalshi_client,  # Optional; required only when mode=TRADE
    config: dict,
    mode: str,
    interval: str = "hourly",
    max_cost_cents_override: Optional[int] = None,
    asset: Optional[str] = None,
) -> List[ExecutionResult]:
    """
    Execute signals. In OBSERVE mode, returns results with status=SKIPPED_CAP_REACHED (simulated).
    In TRADE mode, places orders and enforces caps.
    interval: "hourly" | "15min" - uses different cap limits for 15-min markets.
    max_cost_cents_override: if set (e.g. for stop-loss roll re-entry), use this instead of config order size.
    asset: optional; when set, uses max_cost_cents_by_asset[asset] for per-asset order sizing.
    """
    results = []
    caps = config.get("caps", {})
    cap_scope = caps.get("cap_scope", "combined")

    if interval == "15min":
        fm_caps = caps.get("fifteen_min", {}) or {}
        max_per_ticker = fm_caps.get("max_orders_per_ticker", caps.get("max_orders_per_ticker", 1))
        max_total = fm_caps.get("max_total_orders_per_15min", 10)
    elif interval == "daily":
        dm_caps = caps.get("daily", {}) or {}
        max_per_ticker = dm_caps.get("max_orders_per_ticker", 1)
        max_total = dm_caps.get("max_total_orders_per_window", 2)
    else:
        hm_caps = caps.get("hourly", {}) or {}
        max_per_ticker = hm_caps.get("max_orders_per_ticker", caps.get("max_orders_per_ticker", 5))
        max_total = hm_caps.get("max_total_orders_per_hour", caps.get("max_total_orders_per_hour", 50))

    order_config = config.get("order", {})
    if interval == "15min":
        order_cfg = (order_config.get("fifteen_min") or order_config)
    elif interval == "daily":
        order_cfg = (order_config.get("daily") or order_config)
    else:
        order_cfg = (order_config.get("hourly") or order_config)
    contracts = order_cfg.get("contracts", order_config.get("contracts", 1))
    base_max = order_cfg.get("max_cost_cents", order_config.get("max_cost_cents", 100))
    max_limit_price_cents = order_cfg.get("max_limit_price_cents") or order_config.get("max_limit_price_cents")
    if max_limit_price_cents is not None:
        try:
            max_limit_price_cents = int(max_limit_price_cents)
        except (TypeError, ValueError):
            max_limit_price_cents = None
    if max_cost_cents_override is not None:
        max_cost_cents = max_cost_cents_override
    elif asset:
        by_asset = order_cfg.get("max_cost_cents_by_asset") or {}
        max_cost_cents = by_asset.get(str(asset).lower(), base_max)
    else:
        max_cost_cents = base_max

    for sig in signals:
        # Generate distinct client_order_id per attempt (idempotency for same ticker+side retries)
        client_order_id = str(uuid.uuid4())

        if mode != "TRADE":
            if should_skip_cap(
                db_path, hour_market_id, sig.ticker, cap_scope, max_per_ticker,
                max_total, side=sig.side,
            ):
                results.append(ExecutionResult(
                    signal=sig, status="SKIPPED_CAP_REACHED",
                    client_order_id=client_order_id,
                ))
            else:
                results.append(ExecutionResult(
                    signal=sig, status="WOULD_TRADE",
                    client_order_id=client_order_id,
                ))
            continue

        if should_skip_cap(
            db_path, hour_market_id, sig.ticker, cap_scope, max_per_ticker,
            max_total, side=sig.side,
        ):
            results.append(ExecutionResult(
                signal=sig,
                status="SKIPPED_CAP_REACHED",
                client_order_id=client_order_id,
            ))
            continue

        if kalshi_client is None:
            results.append(ExecutionResult(
                signal=sig, status="FAILED", error="Kalshi client unavailable",
                client_order_id=client_order_id,
            ))
            continue

        # Get price and contracts for logging (before API call)
        yes_bid_cents = None
        yes_ask_cents = None
        no_bid_cents = None
        no_ask_cents = None
        try:
            top = kalshi_client.get_top_of_book(sig.ticker)
            ask_key = f"{sig.side}_ask"
            ask_cents = top.get(ask_key)
            price_cents = int(ask_cents) if ask_cents is not None else None
            contracts = int(max_cost_cents // int(ask_cents)) if ask_cents and int(ask_cents) > 0 else None
            # Capture full top-of-book at order time for analysis
            yes_bid_cents = _parse_cents(top.get("yes_bid"))
            yes_ask_cents = _parse_cents(top.get("yes_ask"))
            no_bid_cents = _parse_cents(top.get("no_bid"))
            no_ask_cents = _parse_cents(top.get("no_ask"))
        except Exception:
            price_cents = None
            contracts = None

        try:
            # Ladder: try bid at x-2, x-1, x back-to-back; fall back to ask if no fill
            ladder_cfg = order_cfg.get("ladder") or order_config.get("ladder") or {}
            ladder_enabled = ladder_cfg.get("enabled", False)
            use_ladder = (
                ladder_enabled
                and max_limit_price_cents is not None
                and price_cents is not None
                and 1 <= price_cents <= max_limit_price_cents
            )

            if use_ladder:
                offsets = ladder_cfg.get("offsets_from_ask", [2, 1, 0])
                wait_ms = int(ladder_cfg.get("wait_ms", 50))
                ask_cents = price_cents
                remaining = max(1, contracts or (max_cost_cents // ask_cents) if ask_cents else 1)
                total_filled = 0
                last_order_id = None
                last_price = ask_cents
                last_count = remaining
                last_coid = client_order_id

                for offset in offsets:
                    if remaining <= 0:
                        break
                    limit_price = max(1, min(ask_cents - offset, max_limit_price_cents))
                    last_coid = str(uuid.uuid4())
                    resp = kalshi_client.place_limit_order(
                        ticker=sig.ticker,
                        action="buy",
                        side=sig.side,
                        price_cents=limit_price,
                        count=remaining,
                        client_order_id=last_coid,
                    )
                    if resp.status_code not in (200, 201):
                        break
                    body = resp.json() if resp.text else {}
                    order_id = body.get("order", {}).get("order_id") or body.get("order_id")
                    last_order_id = order_id
                    last_price = limit_price
                    last_count = remaining

                    time.sleep(wait_ms / 1000.0)

                    try:
                        order = kalshi_client.get_order(order_id)
                    except Exception:
                        order = {}
                    filled = int(order.get("fill_count", 0))
                    status = str(order.get("status", "")).lower()
                    total_filled += filled
                    remaining -= filled

                    if remaining <= 0:
                        break
                    # Cancel resting order only if not at ask (offset=0); at ask we take liquidity, leave it
                    if status == "resting" and offset != 0:
                        try:
                            kalshi_client.cancel_order(order_id)
                        except Exception:
                            pass

                contracts = total_filled or last_count
                price_cents = last_price
                if last_order_id is not None:
                    increment_order_count(db_path, hour_market_id, sig.ticker, sig.side, last_order_id)
                    results.append(ExecutionResult(
                        signal=sig,
                        status="PLACED",
                        order_id=last_order_id,
                        client_order_id=last_coid,
                        price_cents=price_cents,
                        contracts=contracts,
                        yes_bid_cents=yes_bid_cents,
                        yes_ask_cents=yes_ask_cents,
                        no_bid_cents=no_bid_cents,
                        no_ask_cents=no_ask_cents,
                    ))
                    continue

            # Non-ladder: place at max of range or at best ask
            if max_limit_price_cents is not None and price_cents is not None and 1 <= price_cents <= max_limit_price_cents:
                limit_price = max_limit_price_cents
                count_at_limit = max(1, max_cost_cents // limit_price)
                resp = kalshi_client.place_limit_order(
                    ticker=sig.ticker,
                    action="buy",
                    side=sig.side,
                    price_cents=limit_price,
                    count=count_at_limit,
                    client_order_id=client_order_id,
                )
                contracts = count_at_limit
                price_cents = limit_price
            else:
                resp = kalshi_client.place_limit_at_best_ask(
                    ticker=sig.ticker,
                    side=sig.side,
                    max_cost_cents=max_cost_cents,
                    client_order_id=client_order_id,
                )
            if resp.status_code in (200, 201):
                body = resp.json() if resp.text else {}
                order_id = body.get("order", {}).get("order_id") or body.get("order_id")
                increment_order_count(db_path, hour_market_id, sig.ticker, sig.side, order_id)
                results.append(ExecutionResult(
                    signal=sig,
                    status="PLACED",
                    order_id=order_id,
                    client_order_id=client_order_id,
                    price_cents=price_cents,
                    contracts=contracts,
                    yes_bid_cents=yes_bid_cents,
                    yes_ask_cents=yes_ask_cents,
                    no_bid_cents=no_bid_cents,
                    no_ask_cents=no_ask_cents,
                ))
            else:
                results.append(ExecutionResult(
                    signal=sig,
                    status="FAILED",
                    client_order_id=client_order_id,
                    price_cents=price_cents,
                    contracts=contracts,
                    error=f"HTTP {resp.status_code}: {resp.text[:200]}",
                    yes_bid_cents=yes_bid_cents,
                    yes_ask_cents=yes_ask_cents,
                    no_bid_cents=no_bid_cents,
                    no_ask_cents=no_ask_cents,
                ))
        except Exception as e:
            results.append(ExecutionResult(
                signal=sig,
                status="FAILED",
                client_order_id=client_order_id,
                price_cents=price_cents,
                contracts=contracts,
                error=str(e),
                yes_bid_cents=yes_bid_cents,
                yes_ask_cents=yes_ask_cents,
                no_bid_cents=no_bid_cents,
                no_ask_cents=no_ask_cents,
            ))
    return results
