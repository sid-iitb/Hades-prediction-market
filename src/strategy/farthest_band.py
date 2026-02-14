from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Optional

from src.client.kalshi_client import KalshiClient


@dataclass
class FarthestBandConfig:
    direction: str = "lower"  # "lower" or "upper" relative to spot
    side: str = "yes"  # "yes" or "no"
    ask_min_cents: int = 95
    ask_max_cents: int = 99
    max_cost_cents: int = 500
    mode: str = "paper"  # "paper" or "live"
    interval_minutes: int = 15
    stop_loss_pct: float = 0.20
    rebalance_each_interval: bool = True


def _safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return resp.text


def _parse_number(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalize_direction(direction: str) -> str:
    v = str(direction or "").strip().lower()
    if v not in {"lower", "upper"}:
        raise ValueError("direction must be 'lower' or 'upper'")
    return v


def _normalize_side(side: str) -> str:
    v = str(side or "").strip().lower()
    if v not in {"yes", "no"}:
        raise ValueError("side must be 'yes' or 'no'")
    return v


def select_farthest_band_market(
    spot: float,
    markets: list[dict],
    config: FarthestBandConfig,
    min_distance_from_spot: float = 0.0,
    exclude_ticker: str | None = None,
) -> dict:
    direction = _normalize_direction(config.direction)
    side = _normalize_side(config.side)
    ask_key = f"{side}_ask"

    if config.ask_min_cents > config.ask_max_cents:
        raise ValueError("ask_min_cents cannot be greater than ask_max_cents")

    directional = []
    eligible = []
    for market in markets or []:
        strike = _parse_number(market.get("strike"))
        ask = _parse_number(market.get(ask_key))
        ticker = market.get("ticker")
        if strike is None or ask is None or not ticker:
            continue
        if exclude_ticker and str(ticker) == str(exclude_ticker):
            continue

        ask_cents = int(round(ask))
        if direction == "lower" and strike > float(spot):
            continue
        if direction == "upper" and strike < float(spot):
            continue

        distance = abs(float(spot) - strike)
        if distance <= float(min_distance_from_spot):
            continue
        expected_return_pct = (100.0 - float(ask_cents)) / float(ask_cents) if ask_cents > 0 else None
        row = {
            "ticker": ticker,
            "strike": float(strike),
            "subtitle": market.get("subtitle"),
            "ask_key": ask_key,
            "ask_cents": ask_cents,
            "distance_from_spot": distance,
            "expected_return_pct": expected_return_pct,
        }
        directional.append(row)

        if ask_cents < int(config.ask_min_cents) or ask_cents > int(config.ask_max_cents):
            continue
        eligible.append(row)

    def _ask_band_distance(ask_cents: int) -> int:
        if ask_cents < int(config.ask_min_cents):
            return int(config.ask_min_cents) - int(ask_cents)
        if ask_cents > int(config.ask_max_cents):
            return int(ask_cents) - int(config.ask_max_cents)
        return 0

    if not directional:
        return {
            "selected": None,
            "reason": "No market matched direction filter",
            "eligible_count": 0,
            "nearest_candidates": [],
        }

    if not eligible:
        nearest = sorted(
            directional,
            key=lambda m: (
                _ask_band_distance(int(m["ask_cents"])),
                float(m["distance_from_spot"]),
            ),
        )[:5]
        nearest_candidates = []
        for n in nearest:
            c = dict(n)
            c["ask_band_distance_cents"] = _ask_band_distance(int(n["ask_cents"]))
            nearest_candidates.append(c)

        return {
            "selected": None,
            "reason": "No market matched ask band filters",
            "eligible_count": 0,
            "nearest_candidates": nearest_candidates,
        }

    eligible.sort(
        key=lambda m: (
            float(m["distance_from_spot"]),
            float(m["ask_cents"]),
        ),
        reverse=True,
    )
    selected = eligible[0]
    count = int(config.max_cost_cents // int(selected["ask_cents"])) if selected["ask_cents"] > 0 else 0
    estimated_cost_cents = int(count * int(selected["ask_cents"])) if count > 0 else 0

    nearest = sorted(
        directional,
        key=lambda m: (
            _ask_band_distance(int(m["ask_cents"])),
            float(m["distance_from_spot"]),
        ),
    )[:5]
    nearest_candidates = []
    for n in nearest:
        c = dict(n)
        c["ask_band_distance_cents"] = _ask_band_distance(int(n["ask_cents"]))
        nearest_candidates.append(c)

    return {
        "selected": selected,
        "eligible_count": len(eligible),
        "count": count,
        "estimated_cost_cents": estimated_cost_cents,
        "nearest_candidates": nearest_candidates,
        "config": asdict(config),
    }


def execute_farthest_band_trade(client: KalshiClient, selection: dict, config: FarthestBandConfig) -> dict:
    selected = (selection or {}).get("selected")
    if not selected:
        return {"action": "hold", "reason": (selection or {}).get("reason", "No candidate")}

    if int(selection.get("count", 0)) < 1:
        return {
            "action": "hold",
            "reason": "Budget too small for 1 contract at selected ask",
            "selection": selection,
        }

    if str(config.mode).lower() != "live":
        return {
            "action": "paper_buy",
            "selection": selection,
        }

    ticker = selected["ticker"]
    if str(config.side).lower() == "yes":
        resp = client.place_yes_limit_at_best_ask(
            ticker=ticker,
            max_cost_cents=int(config.max_cost_cents),
        )
    else:
        resp = client.place_no_limit_at_best_ask(
            ticker=ticker,
            max_cost_cents=int(config.max_cost_cents),
        )

    return {
        "action": "live_buy",
        "status_code": resp.status_code,
        "response": _safe_json(resp),
        "selection": selection,
    }


def _mark_price_and_pnl(client: KalshiClient, active_position: dict) -> dict:
    side = str(active_position.get("side") or "").lower()
    if side not in {"yes", "no"}:
        return {"error": "active_position side must be yes/no"}

    top = client.get_top_of_book(active_position["ticker"])
    bid_key = f"{side}_bid"
    ask_key = f"{side}_ask"
    mark_cents = top.get(bid_key)
    if mark_cents is None:
        mark_cents = top.get(ask_key)

    entry = int(active_position.get("entry_price_cents") or 0)
    pnl_pct = None
    if entry > 0 and mark_cents is not None:
        pnl_pct = (float(mark_cents) - float(entry)) / float(entry)
    return {
        "top_of_book": top,
        "mark_cents": mark_cents,
        "pnl_pct": pnl_pct,
    }


def _enter_position(client: KalshiClient, selection: dict, config: FarthestBandConfig, spot: float, reason: str) -> dict:
    selected = (selection or {}).get("selected")
    count = int((selection or {}).get("count", 0))
    if not selected or count < 1:
        return {
            "action": "hold",
            "reason": (selection or {}).get("reason", "No candidate"),
            "selection": selection,
            "active_position": None,
        }

    side = str(config.side).lower()
    ask_cents = int(selected["ask_cents"])
    active = {
        "ticker": selected["ticker"],
        "strike": float(selected["strike"]),
        "side": side,
        "entry_price_cents": ask_cents,
        "count": count,
        "opened_spot": float(spot),
    }

    if str(config.mode).lower() != "live":
        return {
            "action": reason,
            "mode": "paper",
            "selection": selection,
            "active_position": active,
        }

    resp = client.place_limit_order(
        ticker=active["ticker"],
        action="buy",
        side=side,
        price_cents=ask_cents,
        count=count,
    )
    return {
        "action": reason,
        "mode": "live",
        "status_code": resp.status_code,
        "response": _safe_json(resp),
        "selection": selection,
        "active_position": active,
    }


def _exit_position(client: KalshiClient, active_position: dict, config: FarthestBandConfig, reason: str) -> dict:
    side = str(active_position.get("side") or "").lower()
    count = int(active_position.get("count") or 0)
    if side not in {"yes", "no"} or count < 1:
        return {"action": "hold", "reason": "Invalid active position for exit"}

    top = client.get_top_of_book(active_position["ticker"])
    bid_cents = top.get(f"{side}_bid")
    if bid_cents is None:
        bid_cents = top.get(f"{side}_ask")
    if bid_cents is None or int(bid_cents) < 1:
        return {"action": "hold", "reason": "No bid/mark available to exit", "top_of_book": top}

    if str(config.mode).lower() != "live":
        return {
            "action": reason,
            "mode": "paper",
            "exit_price_cents": int(bid_cents),
            "top_of_book": top,
        }

    resp = client.place_limit_order(
        ticker=active_position["ticker"],
        action="sell",
        side=side,
        price_cents=int(bid_cents),
        count=count,
    )
    return {
        "action": reason,
        "mode": "live",
        "status_code": resp.status_code,
        "response": _safe_json(resp),
        "exit_price_cents": int(bid_cents),
        "top_of_book": top,
    }


def run_farthest_band_cycle(
    client: KalshiClient,
    spot: float,
    markets: list[dict],
    config: FarthestBandConfig,
    active_position: dict | None = None,
    force_rebalance: bool = False,
) -> dict:
    active = dict(active_position) if active_position else None
    stop_loss_pct = abs(float(config.stop_loss_pct))
    current_tickers = {str(m.get("ticker")) for m in (markets or []) if m.get("ticker")}

    if active:
        if force_rebalance:
            exit_result = _exit_position(client, active, config, reason="scheduled_rebalance_exit")
            if str(config.mode).lower() == "live" and exit_result.get("action") == "hold":
                return {
                    "action": "hold_active",
                    "reason": "Scheduled rebalance due but exit could not be placed",
                    "exit": exit_result,
                    "active_position": active,
                }
            selection = select_farthest_band_market(spot=spot, markets=markets, config=config)
            entry = _enter_position(
                client=client,
                selection=selection,
                config=config,
                spot=spot,
                reason="scheduled_rebalance_reenter",
            )
            return {
                "action": "scheduled_rebalance",
                "reason": "Scheduled interval rebalance",
                "exited_position": active,
                "exit": exit_result,
                "entry": entry,
                "active_position": entry.get("active_position"),
            }

        # If the active ticker is not in the latest ingest market set, roll to current event.
        if str(active.get("ticker")) not in current_tickers:
            exit_result = _exit_position(client, active, config, reason="rollover_exit_stale_ticker")
            if str(config.mode).lower() == "live" and exit_result.get("action") == "hold":
                return {
                    "action": "hold_active",
                    "reason": "Active ticker stale but live exit could not be placed",
                    "exit": exit_result,
                    "active_position": active,
                }
            selection = select_farthest_band_market(spot=spot, markets=markets, config=config)
            entry = _enter_position(
                client=client,
                selection=selection,
                config=config,
                spot=spot,
                reason="rollover_reenter_latest_event",
            )
            return {
                "action": "rollover_reenter",
                "reason": "Active ticker not present in latest ingest markets",
                "exited_position": active,
                "exit": exit_result,
                "entry": entry,
                "active_position": entry.get("active_position"),
            }

        mark_state = _mark_price_and_pnl(client, active)
        pnl_pct = mark_state.get("pnl_pct")
        if pnl_pct is not None and pnl_pct <= -stop_loss_pct:
            exit_result = _exit_position(client, active, config, reason="stop_loss_exit")
            if exit_result.get("action") == "hold":
                return {
                    "action": "hold_active",
                    "reason": "Stop-loss triggered but exit could not be placed",
                    "pnl_pct": pnl_pct,
                    "exit": exit_result,
                    "active_position": active,
                }
            previous_distance = abs(float(spot) - float(active.get("strike") or 0.0))
            next_selection = select_farthest_band_market(
                spot=spot,
                markets=markets,
                config=config,
                min_distance_from_spot=previous_distance,
                exclude_ticker=active.get("ticker"),
            )
            reentry_result = _enter_position(
                client=client,
                selection=next_selection,
                config=config,
                spot=spot,
                reason="stop_loss_rebuy_further",
            )
            return {
                "action": "stop_loss_rotate",
                "stop_loss_trigger_pct": -stop_loss_pct,
                "pnl_pct": pnl_pct,
                "exited_position": active,
                "exit": exit_result,
                "reentry": reentry_result,
                "active_position": reentry_result.get("active_position"),
            }

        return {
            "action": "hold_active",
            "reason": "Stop-loss not triggered",
            "active_position": active,
            "mark": mark_state,
        }

    selection = select_farthest_band_market(spot=spot, markets=markets, config=config)
    entry = _enter_position(
        client=client,
        selection=selection,
        config=config,
        spot=spot,
        reason="enter_farthest_band",
    )
    return {
        "action": entry.get("action", "hold"),
        "entry": entry,
        "active_position": entry.get("active_position"),
    }
