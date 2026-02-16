"""
Exit criteria logic for the bot - same as Farthest Band Strategy.
Evaluates stop-loss and take-profit on every run for each active position.
"""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from src.client.kalshi_client import KalshiClient


def _parse_cents(v: Any) -> Optional[int]:
    """Parse price to cents (1-99). Handles dollars (0.95) or cents (95)."""
    if v is None:
        return None
    try:
        x = float(v)
        if 0 < x <= 1.0:
            return int(round(x * 100))
        if 1 < x <= 100:
            return int(round(x))
        return None
    except (TypeError, ValueError):
        return None


def _normalize_position(p: dict) -> Optional[dict]:
    """
    Normalize a Kalshi position to {ticker, side, count, entry_price_cents}.
    Returns None if invalid.
    """
    ticker = p.get("ticker") or p.get("market_ticker") or p.get("event_ticker")
    if not ticker:
        return None

    raw_side = p.get("side") if p.get("side") is not None else p.get("position")
    side = str(raw_side).lower() if raw_side is not None else ""
    count = p.get("contracts") or p.get("quantity") or p.get("count") or 0
    if not count and isinstance(raw_side, (int, float)):
        count = int(abs(raw_side))
    if isinstance(raw_side, (int, float)) and raw_side != 0:
        side = "yes" if raw_side > 0 else "no"

    if side not in {"yes", "no"} or not count or int(count) < 1:
        return None

    avg_price = p.get("avg_price") or p.get("average_price")
    price_cents = _parse_cents(avg_price)
    if price_cents is None:
        cost = p.get("total_cost_dollars") or p.get("total_cost") or p.get("market_exposure_dollars")
        if cost is not None:
            try:
                c = float(str(cost).replace("$", "").replace(",", ""))
                price_cents = int(round(c * 100 / int(count))) if c and int(count) else None
            except (TypeError, ValueError):
                pass
    if price_cents is None or price_cents < 1:
        return None

    return {
        "ticker": ticker,
        "side": side,
        "count": int(count),
        "entry_price_cents": price_cents,
    }


def _mark_price_and_pnl(client: KalshiClient, pos: dict) -> dict:
    """Get mark price and PnL% for a position."""
    side = str(pos.get("side") or "").lower()
    if side not in {"yes", "no"}:
        return {"error": "position side must be yes/no"}

    try:
        top = client.get_top_of_book(pos["ticker"])
    except Exception:
        return {"error": "Could not fetch orderbook", "mark_cents": None, "pnl_pct": None}

    bid_key = f"{side}_bid"
    ask_key = f"{side}_ask"
    mark_cents = top.get(bid_key)
    if mark_cents is None:
        mark_cents = top.get(ask_key)

    entry = int(pos.get("entry_price_cents") or 0)
    pnl_pct = None
    if entry > 0 and mark_cents is not None:
        pnl_pct = (float(mark_cents) - float(entry)) / float(entry)
    return {
        "top_of_book": top,
        "mark_cents": mark_cents,
        "pnl_pct": pnl_pct,
    }


def _build_exit_eval(
    pos: dict,
    pnl_pct: Optional[float],
    mark_state: dict,
    stop_loss_pct: float,
    profit_target_pct: Optional[float],
    stop_loss_triggered: bool,
    take_profit_triggered: bool,
    what_happened: str,
) -> dict:
    """Build exit criteria evaluation summary."""
    side = str(pos.get("side") or "").upper()
    return {
        "evaluated": True,
        "side": side,
        "ticker": pos.get("ticker"),
        "entry_price_cents": pos.get("entry_price_cents"),
        "mark_cents": mark_state.get("mark_cents"),
        "pnl_pct": round(pnl_pct, 6) if pnl_pct is not None else None,
        "stop_loss_threshold_pct": -stop_loss_pct,
        "stop_loss_triggered": stop_loss_triggered,
        "profit_target_threshold_pct": profit_target_pct,
        "take_profit_triggered": take_profit_triggered,
        "what_happened": what_happened,
    }


@dataclass
class ExitResult:
    """Result of exit criteria evaluation for one position."""
    position: dict
    action: str  # HOLD | STOP_LOSS | TAKE_PROFIT
    exit_criteria_evaluated: dict
    pnl_pct: Optional[float] = None
    sell_result: Optional[dict] = None


def evaluate_positions(
    positions: List[dict],
    client: KalshiClient,
    stop_loss_pct: float,
    profit_target_pct: Optional[float],
    mode: str,
    hour_market_id: str,
) -> List[ExitResult]:
    """
    Evaluate exit criteria for each position. Same logic as Farthest Band Strategy.
    - Take-profit checked first (optional)
    - Stop-loss checked second
    Returns list of ExitResult (one per position).
    """
    stop_loss_pct = abs(float(stop_loss_pct))
    if profit_target_pct is not None:
        profit_target_pct = abs(float(profit_target_pct))

    results = []
    event_prefix = (hour_market_id or "").upper()

    for p in positions:
        norm = _normalize_position(p)
        if not norm:
            continue
        ticker = norm.get("ticker", "")
        if event_prefix and not ticker.upper().startswith(event_prefix):
            continue

        mark_state = _mark_price_and_pnl(client, norm)
        if mark_state.get("error"):
            results.append(ExitResult(
                position=norm,
                action="HOLD",
                exit_criteria_evaluated={
                    "evaluated": True,
                    "ticker": norm.get("ticker"),
                    "what_happened": mark_state.get("error", "Could not evaluate"),
                    "stop_loss_triggered": False,
                    "take_profit_triggered": False,
                },
                pnl_pct=None,
            ))
            continue

        pnl_pct = mark_state.get("pnl_pct")
        take_profit_triggered = False
        stop_loss_triggered = False
        action = "HOLD"
        sell_result = None

        if profit_target_pct is not None and pnl_pct is not None and pnl_pct >= profit_target_pct:
            take_profit_triggered = True
            action = "TAKE_PROFIT"
            if mode == "TRADE":
                sell_result = _place_sell(client, norm, "take_profit_exit")
            exit_eval = _build_exit_eval(
                norm, pnl_pct, mark_state, stop_loss_pct, profit_target_pct,
                stop_loss_triggered=False, take_profit_triggered=True,
                what_happened=f"Take-profit triggered at {pnl_pct*100:.2f}% (target {profit_target_pct*100:.2f}%).",
            )
        elif pnl_pct is not None and pnl_pct <= -stop_loss_pct:
            stop_loss_triggered = True
            action = "STOP_LOSS"
            if mode == "TRADE":
                sell_result = _place_sell(client, norm, "stop_loss_exit")
            exit_eval = _build_exit_eval(
                norm, pnl_pct, mark_state, stop_loss_pct, profit_target_pct,
                stop_loss_triggered=True, take_profit_triggered=False,
                what_happened=f"Stop-loss triggered at {pnl_pct*100:.2f}% (threshold -{stop_loss_pct*100:.0f}%).",
            )
        else:
            exit_eval = _build_exit_eval(
                norm, pnl_pct, mark_state, stop_loss_pct, profit_target_pct,
                stop_loss_triggered=False, take_profit_triggered=False,
                what_happened=f"Holding. PnL {pnl_pct*100:.2f}%" if pnl_pct is not None else "Holding. No mark.",
            )

        results.append(ExitResult(
            position=norm,
            action=action,
            exit_criteria_evaluated=exit_eval,
            pnl_pct=pnl_pct,
            sell_result=sell_result,
        ))
    return results


def _place_sell(client: KalshiClient, pos: dict, reason: str) -> dict:
    """Place sell order for a position."""
    side = str(pos.get("side") or "").lower()
    count = int(pos.get("count") or 0)
    if side not in {"yes", "no"} or count < 1:
        return {"action": "hold", "reason": "Invalid position"}

    try:
        top = client.get_top_of_book(pos["ticker"])
        bid_cents = top.get(f"{side}_bid")
        if bid_cents is None:
            bid_cents = top.get(f"{side}_ask")
        if bid_cents is None or int(bid_cents) < 1:
            return {"action": "hold", "reason": "No bid to exit", "top_of_book": top}

        resp = client.place_limit_order(
            ticker=pos["ticker"],
            action="sell",
            side=side,
            price_cents=int(bid_cents),
            count=count,
        )
        return {
            "action": reason,
            "status_code": resp.status_code,
            "response": resp.json() if resp.text else {},
            "exit_price_cents": int(bid_cents),
        }
    except Exception as e:
        return {"action": "hold", "reason": str(e), "error": str(e)}


def run_exit_criteria(
    client: Optional[KalshiClient],
    hour_market_id: str,
    config: dict,
    mode: str,
    stop_loss_pct_override: Optional[float] = None,
    profit_target_pct_override: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Run exit criteria for all positions in the current market.
    Fetches positions from Kalshi, evaluates, and executes sells in TRADE mode.
    Use stop_loss_pct_override/profit_target_pct_override for market-specific
    values (e.g. fifteen_min.stop_loss_pct).
    Returns {exit_results: [...], evaluations: [...], positions_checked: int}.
    """
    exit_cfg = config.get("exit_criteria", {})
    stop_loss_pct = stop_loss_pct_override if stop_loss_pct_override is not None else exit_cfg.get("stop_loss_pct", 0.20)
    profit_target_pct = profit_target_pct_override if profit_target_pct_override is not None else exit_cfg.get("profit_target_pct")

    if client is None:
        return {
            "exit_results": [],
            "evaluations": [],
            "positions_checked": 0,
            "error": "Kalshi client unavailable",
        }

    try:
        positions_resp = client.get_positions(limit=200)
        positions_list = positions_resp.get("market_positions") or positions_resp.get("positions") or []
    except Exception as e:
        return {
            "exit_results": [],
            "evaluations": [],
            "positions_checked": 0,
            "error": str(e),
        }

    results = evaluate_positions(
        positions=positions_list,
        client=client,
        stop_loss_pct=stop_loss_pct,
        profit_target_pct=profit_target_pct,
        mode=mode,
        hour_market_id=hour_market_id,
    )

    evaluations = [r.exit_criteria_evaluated for r in results]
    return {
        "exit_results": [
            {
                "position": r.position,
                "action": r.action,
                "pnl_pct": r.pnl_pct,
                "sell_result": r.sell_result,
            }
            for r in results
        ],
        "evaluations": evaluations,
        "positions_checked": len(results),
    }
