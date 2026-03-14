"""
Pipeline intent and record types for Bot V2.
Used by strategy layer (evaluate_entry → OrderIntent, evaluate_exit → ExitAction)
and executor/registry (OrderRecord from v2_order_registry).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrderIntent:
    """Intent to place a single order; produced by strategy.evaluate_entry(ctx)."""

    side: str  # "yes" | "no"
    price_cents: int
    count: int
    order_type: str  # e.g. "limit" | "market"
    client_order_id: str

    def __post_init__(self) -> None:
        if self.side not in ("yes", "no"):
            logger.warning("OrderIntent.side=%r is not 'yes' or 'no'", self.side)
        if self.price_cents < 0 or self.price_cents > 100:
            logger.debug("OrderIntent.price_cents=%s outside typical 0-100", self.price_cents)
        if self.count < 1:
            raise ValueError("OrderIntent.count must be >= 1")


# Allowed values for ExitAction.action
EXIT_ACTION_STOP_LOSS = "stop_loss"
EXIT_ACTION_TAKE_PROFIT = "take_profit"
EXIT_ACTION_MARKET_SELL = "market_sell"
EXIT_ACTION_CANCEL_ONLY = "cancel_only"

EXIT_ACTIONS = (EXIT_ACTION_STOP_LOSS, EXIT_ACTION_TAKE_PROFIT, EXIT_ACTION_MARKET_SELL, EXIT_ACTION_CANCEL_ONLY)


@dataclass(frozen=True)
class ExitAction:
    """Request to exit an order; produced by strategy.evaluate_exit(ctx, my_orders)."""

    order_id: str
    action: str  # "stop_loss" | "take_profit" | "market_sell" | "cancel_only"
    # For take_profit: price at which to place a resting limit sell (provide liquidity).
    # Executor uses this to avoid market-selling and to dedupe existing limit sells.
    limit_price_cents: Optional[int] = None

    def __post_init__(self) -> None:
        if self.action not in EXIT_ACTIONS:
            raise ValueError(
                f"ExitAction.action must be one of {EXIT_ACTIONS}, got {self.action!r}"
            )


@dataclass
class OrderRecord:
    """
    In-memory representation of a row in v2_order_registry.
    Used when passing 'my_orders' to strategy.evaluate_exit.
    """

    order_id: str
    strategy_id: str
    interval: str
    market_id: str
    asset: str
    ticker: str
    side: str
    status: str  # "resting" | "filled" | "canceled" | "exited"
    filled_count: int
    count: int
    limit_price_cents: Optional[int]
    placed_at: float

    def is_active(self) -> bool:
        """True if order is still resting (not filled/canceled)."""
        return self.status == "resting"
