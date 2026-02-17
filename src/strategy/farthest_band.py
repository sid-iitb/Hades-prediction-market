from __future__ import annotations

from dataclasses import asdict, dataclass, replace
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
    stop_loss_pct: float = 0.25
    stop_loss_check_seconds: int = 300
    rebalance_each_interval: bool = True
    skip_nearest_levels: int = 2


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


def _normalize_contract_price_cents(value) -> Optional[int]:
    v = _parse_number(value)
    if v is None or v <= 0:
        return None
    if v <= 1.0:
        cents = v * 100.0
        return int(round(cents)) if 0 < cents <= 100 else None
    if v <= 100:
        return int(round(v))
    # Over-scaled defensive normalization (e.g., 705 -> 70.5).
    scaled = float(v)
    for _ in range(4):
        scaled /= 10.0
        if 0 < scaled <= 100:
            return int(round(scaled))
    return None


def _extract_order_count(order: dict) -> int:
    filled_count = (
        order.get("filled_count")
        or order.get("matched_count")
        or order.get("executed_count")
        or 0
    )
    c = _parse_number(filled_count)
    if c is not None and c > 0:
        return int(round(c))
    status_txt = str(order.get("status") or "").lower()
    if status_txt in {"filled", "executed", "closed"}:
        for k in ("count", "quantity"):
            v = _parse_number(order.get(k))
            if v is not None and v > 0:
                return int(round(v))
    return 0


def _extract_order_price_cents(order: dict) -> Optional[int]:
    for k in ("avg_price", "yes_price", "no_price", "price", "limit_price"):
        px = _normalize_contract_price_cents(order.get(k))
        if px is not None:
            return px
    return None


def _reconstruct_entry_cents_from_order_history(client: KalshiClient, ticker: str, side: str) -> Optional[int]:
    """
    Best-effort weighted-average buy price reconstruction from order history.
    Used only when seeded entry basis is invalid.
    """
    if not ticker or side not in {"yes", "no"}:
        return None
    try:
        orders = client.get_all_orders(ticker=ticker, limit=200, max_pages=50)
    except Exception:
        return None
    total_contracts = 0
    total_cost = 0.0
    for o in orders or []:
        if str(o.get("ticker") or "") != str(ticker):
            continue
        if str(o.get("side") or "").lower() != side:
            continue
        if str(o.get("action") or "").lower() != "buy":
            continue
        count = _extract_order_count(o)
        if count < 1:
            continue
        px = _extract_order_price_cents(o)
        if px is None:
            continue
        total_contracts += int(count)
        total_cost += float(px) * float(count)
    if total_contracts < 1:
        return None
    avg = total_cost / float(total_contracts)
    return _normalize_contract_price_cents(avg)


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

    all_directional = []
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
        all_directional.append(row)

    skip_n = max(0, int(getattr(config, "skip_nearest_levels", 0) or 0))
    skipped_tickers = set()
    if skip_n > 0 and all_directional:
        if direction == "lower":
            nearest_first = sorted(all_directional, key=lambda r: float(r["strike"]), reverse=True)
        else:
            nearest_first = sorted(all_directional, key=lambda r: float(r["strike"]))
        for row in nearest_first[:skip_n]:
            skipped_tickers.add(str(row["ticker"]))

    directional = [r for r in all_directional if str(r["ticker"]) not in skipped_tickers]
    eligible = []
    for row in directional:
        ask_cents = int(row["ask_cents"])

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
            "reason": "No market matched direction filter after nearest-level skip",
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
    mark_cents = top.get(bid_key)
    # Stop-loss should be based on executable downside (bid), not optimistic ask.
    # If bid is unavailable, treat mark as 0 so risk control remains conservative.
    if mark_cents is None:
        mark_cents = 0

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
    status_code = int(resp.status_code)
    body = _safe_json(resp)
    if status_code not in {200, 201}:
        return {
            "action": "hold",
            "reason": f"Live buy failed with status {status_code}",
            "mode": "live",
            "status_code": status_code,
            "response": body,
            "selection": selection,
            "active_position": None,
        }
    return {
        "action": reason,
        "mode": "live",
        "status_code": status_code,
        "response": body,
        "selection": selection,
        "active_position": active,
    }


def _with_nearest_fallback(selection: dict, config: FarthestBandConfig) -> dict:
    """
    If no exact ask-band match exists, pick the first nearest candidate as fallback.
    Used for stop-loss re-entry only.
    """
    sel = dict(selection or {})
    if isinstance(sel.get("selected"), dict):
        return sel

    nearest = sel.get("nearest_candidates") or []
    for candidate in nearest:
        ask_cents = int(_parse_number((candidate or {}).get("ask_cents")) or 0)
        if ask_cents < 1:
            continue
        count = int(config.max_cost_cents // ask_cents)
        if count < 1:
            continue
        picked = dict(candidate)
        sel["selected"] = picked
        sel["count"] = count
        sel["estimated_cost_cents"] = int(count * ask_cents)
        sel["fallback_used"] = True
        sel["fallback_reason"] = "No exact ask-band match; using nearest farther candidate"
        return sel
    return sel


def _exit_position(
    client: KalshiClient,
    active_position: dict,
    config: FarthestBandConfig,
    reason: str,
    market_like: bool = False,
) -> dict:
    side = str(active_position.get("side") or "").lower()
    count = int(active_position.get("count") or 0)
    if side not in {"yes", "no"} or count < 1:
        return {"action": "hold", "reason": "Invalid active position for exit"}

    top = client.get_top_of_book(active_position["ticker"])
    bid_cents = top.get(f"{side}_bid")
    # For stop-loss we want market-like behavior: sell at executable bid only.
    # For other exits we keep legacy ask fallback.
    if bid_cents is None and not market_like:
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
    status_code = int(resp.status_code)
    body = _safe_json(resp)
    if status_code not in {200, 201}:
        return {
            "action": "hold",
            "reason": f"Live sell failed with status {status_code}",
            "mode": "live",
            "status_code": status_code,
            "response": body,
            "exit_price_cents": int(bid_cents),
            "top_of_book": top,
        }
    return {
        "action": reason,
        "mode": "live",
        "status_code": status_code,
        "response": body,
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
        entry_cents = _normalize_contract_price_cents(active.get("entry_price_cents"))
        if entry_cents is None:
            reconstructed = _reconstruct_entry_cents_from_order_history(
                client=client,
                ticker=str(active.get("ticker") or ""),
                side=str(active.get("side") or "").lower(),
            )
            if reconstructed is not None:
                active["entry_price_cents"] = int(reconstructed)
            else:
                return {
                    "action": "hold_active",
                    "reason": (
                        f"Invalid entry basis for stop-loss ({active.get('entry_price_cents')}); "
                        "reconstruction from order history failed"
                    ),
                    "active_position": active,
                }
        else:
            active["entry_price_cents"] = int(entry_cents)

        # If the active ticker is not in the latest ingest market set, roll to current event.
        if str(active.get("ticker")) not in current_tickers:
            return {
                "action": "hold_active",
                "reason": "Active ticker stale; rollover auto-sell disabled",
                "active_position": active,
            }

        mark_state = _mark_price_and_pnl(client, active)
        pnl_pct = mark_state.get("pnl_pct")
        # Trigger only when loss is strictly greater than stop_loss_pct.
        if pnl_pct is not None and pnl_pct < -stop_loss_pct:
            exit_result = _exit_position(
                client,
                active,
                config,
                reason="stop_loss_exit",
                market_like=True,
            )
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
            next_selection = _with_nearest_fallback(next_selection, config)
            # If nearest-level skip removed all directional candidates, retry once with relaxed skip.
            if not (next_selection or {}).get("selected"):
                reason_txt = str((next_selection or {}).get("reason") or "").lower()
                if "after nearest-level skip" in reason_txt:
                    relaxed = replace(config, skip_nearest_levels=0)
                    next_selection = select_farthest_band_market(
                        spot=spot,
                        markets=markets,
                        config=relaxed,
                        min_distance_from_spot=previous_distance,
                        exclude_ticker=active.get("ticker"),
                    )
                    next_selection = _with_nearest_fallback(next_selection, relaxed)
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

        if force_rebalance:
            selection = select_farthest_band_market(spot=spot, markets=markets, config=config)
            entry = _enter_position(
                client=client,
                selection=selection,
                config=config,
                spot=spot,
                reason="scheduled_add_entry",
            )
            # Keep the current active position if no new order was selected/placed.
            next_active = entry.get("active_position") or active
            return {
                "action": "scheduled_add_entry",
                "reason": "Scheduled interval add entry (no sell)",
                "previous_active_position": active,
                "entry": entry,
                "active_position": next_active,
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
