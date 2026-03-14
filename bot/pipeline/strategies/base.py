"""
Base interface for all Bot V2 strategies.
Strategies are stateless: they receive WindowContext and return OrderIntent / ExitAction.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from bot.pipeline.context import WindowContext
from bot.pipeline.intents import ExitAction, OrderIntent, OrderRecord


class BaseV2Strategy(ABC):
    """
    Blueprint for V2 strategies. Config is passed at construction; per-tick config
    is read from ctx.config via _get_strategy_config(ctx).
    """

    def __init__(self, strategy_id: str, config: dict) -> None:
        self.strategy_id = strategy_id
        self.config = config

    @abstractmethod
    def evaluate_entry(
        self, ctx: WindowContext, my_orders: Optional[List[OrderRecord]] = None
    ) -> Optional[OrderIntent]:
        """Return one OrderIntent if this strategy wants to place an order this tick, else None.
        my_orders: optional list of this strategy's orders in this window (for position limits)."""
        ...

    @abstractmethod
    def evaluate_exit(self, ctx: WindowContext, my_orders: List[OrderRecord]) -> List[ExitAction]:
        """Return zero or more ExitActions for orders that should be closed (e.g. stop-loss)."""
        ...

    def _get_strategy_config(self, ctx: WindowContext) -> dict:
        """
        Safely extract this strategy's config block from the merged config.
        Path: ctx.config[ctx.interval]['strategies'][self.strategy_id].
        Returns {} if any key is missing.
        """
        if not ctx.config:
            return {}
        interval_block = ctx.config.get(ctx.interval)
        if not isinstance(interval_block, dict):
            return {}
        strategies = interval_block.get("strategies")
        if not isinstance(strategies, dict):
            return {}
        out = strategies.get(self.strategy_id)
        return out if isinstance(out, dict) else {}
