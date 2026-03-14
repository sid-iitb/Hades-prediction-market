"""
Order aggregator for Bot V2: conflict resolution and cap enforcement.
Takes (OrderIntent, strategy_id) pairs, sorts by config strategy_priority, enforces caps.
"""
from __future__ import annotations

import logging
from typing import List, Tuple

from bot.pipeline.intents import OrderIntent, OrderRecord

logger = logging.getLogger(__name__)


class OrderAggregator:
    """
    Resolves multiple strategy intents into at most one order per market (or per cap rules).
    Caller passes interval-specific config slice so config["pipeline"]["strategy_priority"] exists.
    """

    def resolve_intents(
        self,
        intents_with_ids: List[Tuple[OrderIntent, str]],
        config: dict,
        active_orders: List[OrderRecord],
        current_cost_by_strategy: dict | None = None,
    ) -> List[Tuple[OrderIntent, str]]:
        """
        Sort intents by strategy priority, enforce per-strategy cost cap.
        config: interval slice (e.g. config["fifteen_min"]) with config["pipeline"]["strategy_priority"].
        active_orders: orders already resting for this (interval, market_id, asset) for cap check.
        current_cost_by_strategy: {strategy_id: cents} total cost of orders (filled + resting) per strategy.
        Returns the list of (OrderIntent, strategy_id) that are allowed (at most one per cycle).
        """
        if not intents_with_ids:
            return []

        pipeline = config.get("pipeline") or {}
        strategy_priority = pipeline.get("strategy_priority")
        if not isinstance(strategy_priority, list):
            logger.warning("Missing or invalid pipeline.strategy_priority; using order of first appearance")
            priority_rank = {sid: i for i, (_, sid) in enumerate(intents_with_ids)}
        else:
            priority_rank = {sid: i for i, sid in enumerate(strategy_priority)}

        def sort_key(item: Tuple[OrderIntent, str]) -> int:
            _, strategy_id = item
            return priority_rank.get(strategy_id, 999)

        sorted_intents = sorted(intents_with_ids, key=sort_key)
        intent, strategy_id = sorted_intents[0]
        strategies_cfg = config.get("strategies") or {}
        strat_cfg = strategies_cfg.get(strategy_id) or {}
        max_cost_cents = int(strat_cfg.get("max_cost_cents", 600))
        current_cost = (current_cost_by_strategy or {}).get(strategy_id, 0)
        new_cost = intent.count * (intent.price_cents or 99)
        if current_cost + new_cost > max_cost_cents:
            logger.debug(
                "Cap: current_cost=%d + new_cost=%d > max_cost=%d, skipping intent for %s",
                current_cost,
                new_cost,
                max_cost_cents,
                strategy_id,
            )
            return []

        return [sorted_intents[0]]
