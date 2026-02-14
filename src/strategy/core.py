from __future__ import annotations

import datetime
from dataclasses import asdict, dataclass
from typing import Optional

from src.client.kalshi_client import KalshiClient
from src.utils.fetch_current_predictions_kalshi import fetch_kalshi_data_struct


@dataclass
class StrategyConfig:
    mode: str = "paper"  # "paper" or "live"
    base_offset_usd: float = 750.0
    reanchor_step_usd: float = 750.0
    profit_target_pct: float = 0.015
    stop_loss_pct: float = 0.50
    max_cost_cents: int = 2000


@dataclass
class OpenPosition:
    ticker: str
    strike: float
    entry_yes_price_cents: int
    count: int
    side: str = "yes"
    opened_at: str = ""


@dataclass
class StrategyState:
    current_offset_usd: float = 750.0
    active: Optional[OpenPosition] = None
    last_action: str = "idle"
    last_reason: str = ""
    last_run_at: str = ""


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _select_market(markets: list[dict], spot: float, offset_usd: float) -> Optional[dict]:
    target = float(spot) - float(offset_usd)
    eligible = [m for m in markets if m.get("strike") is not None and float(m["strike"]) <= target]
    if not eligible:
        return None
    return max(eligible, key=lambda m: float(m["strike"]))


def _pnl_pct(entry: int, mark: Optional[int]) -> Optional[float]:
    if entry <= 0 or mark is None:
        return None
    return (float(mark) - float(entry)) / float(entry)


class OffsetReanchorStrategy:
    """
    Rule-based strategy:
    - Buy YES on strike closest to (spot - offset).
    - Take profit around 1-2%.
    - Stop out at 50% loss and re-anchor another $750 lower.
    """

    def __init__(self, client: KalshiClient, config: Optional[StrategyConfig] = None):
        self.client = client
        self.config = config or StrategyConfig()
        self.state = StrategyState(current_offset_usd=self.config.base_offset_usd)

    def set_config(self, **kwargs):
        data = asdict(self.config)
        data.update(kwargs)
        self.config = StrategyConfig(**data)
        if self.state.current_offset_usd <= 0:
            self.state.current_offset_usd = self.config.base_offset_usd

    def step(self, spot_override: Optional[float] = None, portfolio_snapshot: Optional[dict] = None) -> dict:
        self.state.last_run_at = _now_iso()
        data, err = fetch_kalshi_data_struct()
        if err:
            self.state.last_action = "error"
            self.state.last_reason = str(err)
            return self.snapshot({"error": str(err)})

        spot_source = spot_override if spot_override is not None else data.get("current_price")
        spot = float(spot_source or 0.0)
        markets = data.get("markets", [])
        if spot <= 0 or not markets:
            self.state.last_action = "hold"
            self.state.last_reason = "No spot/market data"
            return self.snapshot()

        active = self.state.active
        if active is None:
            result = self._enter_new_position(spot, markets)
        else:
            result = self._manage_open_position(spot, markets, active)
        if portfolio_snapshot is not None:
            result.setdefault("details", {})
            result["details"]["portfolio_snapshot"] = portfolio_snapshot
        return result

    def _enter_new_position(self, spot: float, markets: list[dict]) -> dict:
        candidate = _select_market(markets, spot=spot, offset_usd=self.state.current_offset_usd)
        if candidate is None:
            self.state.last_action = "hold"
            self.state.last_reason = "No strike below target offset"
            return self.snapshot()

        ticker = candidate.get("ticker")
        if not ticker:
            self.state.last_action = "hold"
            self.state.last_reason = "Missing ticker on candidate market"
            return self.snapshot()

        top = self.client.get_top_of_book(ticker)
        yes_ask = top.get("yes_ask")
        if yes_ask is None or yes_ask <= 0 or yes_ask >= 100:
            self.state.last_action = "hold"
            self.state.last_reason = "No tradable YES ask"
            return self.snapshot({"candidate_ticker": ticker, "top_of_book": top})

        expected_return = (100.0 - float(yes_ask)) / float(yes_ask)
        if expected_return < self.config.profit_target_pct:
            self.state.last_action = "hold"
            self.state.last_reason = "Expected return below target"
            return self.snapshot(
                {
                    "candidate_ticker": ticker,
                    "yes_ask": yes_ask,
                    "expected_return_pct": round(expected_return, 6),
                }
            )

        count = int(self.config.max_cost_cents // int(yes_ask))
        if count < 1:
            self.state.last_action = "hold"
            self.state.last_reason = "Budget too small"
            return self.snapshot({"candidate_ticker": ticker, "yes_ask": yes_ask})

        order_result = {"mode": self.config.mode, "status": "paper_filled"}
        if self.config.mode == "live":
            resp = self.client.place_limit_order(
                ticker=ticker,
                action="buy",
                side="yes",
                price_cents=int(yes_ask),
                count=count,
            )
            order_result = {
                "mode": "live",
                "status_code": resp.status_code,
                "response": _safe_json(resp),
            }

        self.state.active = OpenPosition(
            ticker=ticker,
            strike=float(candidate["strike"]),
            entry_yes_price_cents=int(yes_ask),
            count=count,
            side="yes",
            opened_at=_now_iso(),
        )
        self.state.last_action = "buy_yes"
        self.state.last_reason = "Opened position at target offset"
        return self.snapshot(
            {
                "spot": spot,
                "candidate_ticker": ticker,
                "entry_yes_price_cents": int(yes_ask),
                "count": count,
                "order": order_result,
            }
        )

    def _manage_open_position(self, spot: float, markets: list[dict], active: OpenPosition) -> dict:
        top = self.client.get_top_of_book(active.ticker)
        yes_bid = top.get("yes_bid")
        yes_ask = top.get("yes_ask")
        mark = yes_bid if yes_bid is not None else yes_ask
        pnl = _pnl_pct(active.entry_yes_price_cents, mark)

        if pnl is None:
            self.state.last_action = "hold"
            self.state.last_reason = "No mark to value position"
            return self.snapshot({"spot": spot, "top_of_book": top})

        if pnl >= self.config.profit_target_pct:
            return self._exit_position(active, yes_bid, reason="take_profit", next_offset=self.config.base_offset_usd)

        if pnl <= -self.config.stop_loss_pct:
            return self._exit_position(
                active,
                yes_bid,
                reason="stop_loss_reanchor",
                next_offset=self.state.current_offset_usd + self.config.reanchor_step_usd,
            )

        self.state.last_action = "hold"
        self.state.last_reason = "Position open"
        return self.snapshot(
            {
                "spot": spot,
                "ticker": active.ticker,
                "mark_yes_price_cents": mark,
                "unrealized_pnl_pct": round(pnl, 6),
                "top_of_book": top,
            }
        )

    def _exit_position(self, active: OpenPosition, yes_bid: Optional[int], reason: str, next_offset: float) -> dict:
        order_result = {"mode": self.config.mode, "status": "paper_closed"}
        if self.config.mode == "live":
            if yes_bid is None or yes_bid <= 0:
                self.state.last_action = "hold"
                self.state.last_reason = "No YES bid to exit live position"
                return self.snapshot({"ticker": active.ticker})
            resp = self.client.place_limit_order(
                ticker=active.ticker,
                action="sell",
                side="yes",
                price_cents=int(yes_bid),
                count=int(active.count),
            )
            order_result = {
                "mode": "live",
                "status_code": resp.status_code,
                "response": _safe_json(resp),
            }

        self.state.active = None
        self.state.current_offset_usd = max(float(next_offset), self.config.base_offset_usd)
        self.state.last_action = "sell_yes"
        self.state.last_reason = reason
        return self.snapshot({"closed_ticker": active.ticker, "order": order_result, "next_offset_usd": self.state.current_offset_usd})

    def snapshot(self, extras: Optional[dict] = None) -> dict:
        base = {
            "config": asdict(self.config),
            "state": {
                "current_offset_usd": self.state.current_offset_usd,
                "active": asdict(self.state.active) if self.state.active else None,
                "last_action": self.state.last_action,
                "last_reason": self.state.last_reason,
                "last_run_at": self.state.last_run_at,
            },
        }
        if extras:
            base["details"] = extras
        return base


def _safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return resp.text
