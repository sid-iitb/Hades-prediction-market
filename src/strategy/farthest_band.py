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


def select_farthest_band_market(spot: float, markets: list[dict], config: FarthestBandConfig) -> dict:
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

        ask_cents = int(round(ask))
        if direction == "lower" and strike > float(spot):
            continue
        if direction == "upper" and strike < float(spot):
            continue

        distance = abs(float(spot) - strike)
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
