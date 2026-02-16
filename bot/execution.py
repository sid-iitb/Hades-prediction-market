"""
Order execution - only runs when MODE=TRADE.
Enforces caps before placing orders.
"""
from dataclasses import dataclass
from typing import List, Optional

from bot.state import (
    get_ticker_order_count,
    get_total_order_count,
    increment_order_count,
)
from bot.strategy import Signal


@dataclass
class ExecutionResult:
    signal: Signal
    status: str  # PLACED, SKIPPED_CAP_REACHED, FAILED
    order_id: Optional[str] = None
    error: Optional[str] = None


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
) -> List[ExecutionResult]:
    """
    Execute signals. In OBSERVE mode, returns results with status=SKIPPED_CAP_REACHED (simulated).
    In TRADE mode, places orders and enforces caps.
    interval: "hourly" | "15min" - uses different cap limits for 15-min markets.
    """
    results = []
    caps = config.get("caps", {})
    cap_scope = caps.get("cap_scope", "combined")

    if interval == "15min":
        fm_caps = caps.get("fifteen_min", {}) or {}
        max_per_ticker = fm_caps.get("max_orders_per_ticker", caps.get("max_orders_per_ticker", 1))
        max_total = fm_caps.get("max_total_orders_per_15min", 10)
    else:
        max_per_ticker = caps.get("max_orders_per_ticker", 5)
        max_total = caps.get("max_total_orders_per_hour", 50)

    order_config = config.get("order", {})
    if interval == "15min":
        order_cfg = (order_config.get("fifteen_min") or order_config)
    else:
        order_cfg = (order_config.get("hourly") or order_config)
    contracts = order_cfg.get("contracts", order_config.get("contracts", 1))
    max_cost_cents = order_cfg.get("max_cost_cents", order_config.get("max_cost_cents", 100))

    for sig in signals:
        if mode != "TRADE":
            if should_skip_cap(
                db_path, hour_market_id, sig.ticker, cap_scope, max_per_ticker,
                max_total, side=sig.side,
            ):
                results.append(ExecutionResult(signal=sig, status="SKIPPED_CAP_REACHED"))
            else:
                results.append(ExecutionResult(signal=sig, status="WOULD_TRADE"))
            continue

        if should_skip_cap(
            db_path, hour_market_id, sig.ticker, cap_scope, max_per_ticker,
            max_total, side=sig.side,
        ):
            results.append(ExecutionResult(
                signal=sig,
                status="SKIPPED_CAP_REACHED",
            ))
            continue

        if kalshi_client is None:
            results.append(ExecutionResult(signal=sig, status="FAILED", error="Kalshi client unavailable"))
            continue

        try:
            resp = kalshi_client.place_limit_at_best_ask(
                ticker=sig.ticker,
                side=sig.side,
                max_cost_cents=max_cost_cents,
            )
            if resp.status_code in (200, 201):
                body = resp.json() if resp.text else {}
                order_id = body.get("order", {}).get("order_id") or body.get("order_id")
                increment_order_count(db_path, hour_market_id, sig.ticker, sig.side, order_id)
                results.append(ExecutionResult(
                    signal=sig,
                    status="PLACED",
                    order_id=order_id,
                ))
            else:
                results.append(ExecutionResult(
                    signal=sig,
                    status="FAILED",
                    error=f"HTTP {resp.status_code}: {resp.text[:200]}",
                ))
        except Exception as e:
            results.append(ExecutionResult(
                signal=sig,
                status="FAILED",
                error=str(e),
            ))
    return results
