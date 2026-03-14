"""
WindowContext: normalized per-tick data for strategy evaluation (Bot V2).
Built by the data layer once per (asset, interval) per tick.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal

logger = logging.getLogger(__name__)

IntervalType = Literal["fifteen_min", "hourly"]


def _default_quote() -> Dict[str, int]:
    return {"yes_bid": 0, "yes_ask": 0, "no_bid": 0, "no_ask": 0}


@dataclass
class WindowContext:
    """
    All normalized data for the current tick (one asset, one interval).
    Strategies read from ctx for evaluate_entry(ctx) and evaluate_exit(ctx, my_orders).
    """

    interval: IntervalType
    market_id: str
    ticker: str
    asset: str
    seconds_to_close: float
    quote: Dict[str, int] = field(default_factory=_default_quote)  # yes_bid, yes_ask, no_bid, no_ask (cents)
    spot_kraken: float | None = None
    spot_coinbase: float | None = None
    strike: float | None = None
    strike_source: str | None = None  # "api_fields", "subtitle", "title", or "ticker"
    distance_kraken: float | None = None
    distance_coinbase: float | None = None
    distance: float | None = None  # conservative (e.g. min of kraken/coinbase when both present)
    positions: List[Dict[str, Any]] = field(default_factory=list)  # [{ticker, side, count, entry_price_cents}, ...]
    open_orders: List[Any] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for key in ("yes_bid", "yes_ask", "no_bid", "no_ask"):
            if key not in self.quote:
                self.quote[key] = 0
        for pos in self.positions:
            if not all(k in pos for k in ("ticker", "side", "count", "entry_price_cents")):
                logger.warning(
                    "WindowContext.positions entry missing required keys: %s",
                    list(pos.keys()),
                )

    @property
    def yes_bid(self) -> int:
        return self.quote.get("yes_bid", 0)

    @property
    def yes_ask(self) -> int:
        return self.quote.get("yes_ask", 0)

    @property
    def no_bid(self) -> int:
        return self.quote.get("no_bid", 0)

    @property
    def no_ask(self) -> int:
        return self.quote.get("no_ask", 0)
