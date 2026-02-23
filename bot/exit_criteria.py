"""
Exit criteria logic for the bot - same as Farthest Band Strategy.

Evaluation and exit are per-ticker (per position): each position is evaluated
independently and only that position is sold when stop-loss or take-profit
triggers. There is no portfolio-level or market-level aggregation.

Evaluates stop-loss and take-profit on each run for positions in the current
market. Hard-flip exit (spot vs strike) checked first when enabled.

Tracks MFE/MAE (max favorable/adverse excursion) per position for exit diagnostics.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.client.kalshi_client import KalshiClient

# Position excursion tracking (MFE/MAE): key -> {mfe_pct, mae_pct, mfe_ts, mae_ts}
_position_excursion: Dict[str, Dict[str, Any]] = {}

# Stop-Loss Persistence: key -> consecutive polls below stop_loss threshold (before firing)
_sl_warning_streak: Dict[str, int] = {}


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


def _excursion_key(hour_market_id: str, ticker: str, side: str) -> str:
    return f"{hour_market_id}|{ticker}|{str(side).lower()}"


def _update_excursion(
    hour_market_id: str,
    ticker: str,
    side: str,
    pnl_pct: Optional[float],
) -> Dict[str, Any]:
    """Update MFE/MAE for position; return current excursion state."""
    key = _excursion_key(hour_market_id, ticker, side)
    now_iso = datetime.now(timezone.utc).isoformat()
    if pnl_pct is None:
        return _position_excursion.get(key, {})
    if key not in _position_excursion:
        _position_excursion[key] = {
            "mfe_pct": pnl_pct,
            "mae_pct": pnl_pct,
            "mfe_ts": now_iso,
            "mae_ts": now_iso,
        }
    else:
        ex = _position_excursion[key]
        if pnl_pct > ex.get("mfe_pct", -999):
            ex["mfe_pct"] = pnl_pct
            ex["mfe_ts"] = now_iso
        if pnl_pct < ex.get("mae_pct", 999):
            ex["mae_pct"] = pnl_pct
            ex["mae_ts"] = now_iso
    return dict(_position_excursion[key])


def _pop_excursion(hour_market_id: str, ticker: str, side: str) -> Dict[str, Any]:
    """Get and remove excursion for exited position."""
    key = _excursion_key(hour_market_id, ticker, side)
    ex = _position_excursion.pop(key, {})
    return dict(ex)


def _build_exit_eval(
    pos: dict,
    pnl_pct: Optional[float],
    mark_state: dict,
    stop_loss_pct: float,
    profit_target_pct: Optional[float],
    stop_loss_triggered: bool,
    take_profit_triggered: bool,
    what_happened: str,
    hour_market_id: str = "",
    excursion: Optional[Dict[str, Any]] = None,
) -> dict:
    """Build exit criteria evaluation summary."""
    side = str(pos.get("side") or "").upper()
    top = mark_state.get("top_of_book") or {}
    # Raw bid/ask cents for spread and slippage diagnostics
    yes_bid = _parse_cents(top.get("yes_bid")) if top.get("yes_bid") is not None else None
    yes_ask = _parse_cents(top.get("yes_ask")) if top.get("yes_ask") is not None else None
    no_bid = _parse_cents(top.get("no_bid")) if top.get("no_bid") is not None else None
    no_ask = _parse_cents(top.get("no_ask")) if top.get("no_ask") is not None else None
    side_l = str(side).lower()
    # Target SL price = bid on the side we're selling
    target_sl_price_cents = yes_bid if side_l == "yes" else no_bid
    # TOB spread for our side (ask - bid)
    tob_spread_cents = (yes_ask - yes_bid) if (yes_ask is not None and yes_bid is not None) else None
    if tob_spread_cents is None and (no_ask is not None and no_bid is not None):
        tob_spread_cents = no_ask - no_bid
    out = {
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
        "top_of_book_yes_bid": yes_bid is not None,
        "top_of_book_yes_ask": yes_ask is not None,
        "top_of_book_no_bid": no_bid is not None,
        "top_of_book_no_ask": no_ask is not None,
        "yes_bid_cents": yes_bid,
        "yes_ask_cents": yes_ask,
        "no_bid_cents": no_bid,
        "no_ask_cents": no_ask,
        "target_sl_price_cents": target_sl_price_cents,
        "tob_spread_cents": tob_spread_cents,
    }
    if excursion:
        out["mfe_pct"] = excursion.get("mfe_pct")
        out["mae_pct"] = excursion.get("mae_pct")
        out["mfe_ts"] = excursion.get("mfe_ts")
        out["mae_ts"] = excursion.get("mae_ts")
    return out


@dataclass
class ExitResult:
    """Result of exit criteria evaluation for one position."""
    position: dict
    action: str  # HOLD | STOP_LOSS | TAKE_PROFIT
    exit_criteria_evaluated: dict
    pnl_pct: Optional[float] = None
    sell_result: Optional[dict] = None


def _check_hard_flip(
    side: str,
    spot: Optional[float],
    strike: Optional[float],
) -> bool:
    """True if hard flip: YES + spot < strike, or NO + spot > strike."""
    if spot is None or strike is None or strike <= 0:
        return False
    s = str(side).lower()
    if s == "yes":
        return spot < strike
    if s == "no":
        return spot > strike
    return False


def evaluate_positions(
    positions: List[dict],
    client: KalshiClient,
    stop_loss_pct: float,
    profit_target_pct: Optional[float],
    mode: str,
    hour_market_id: str,
    spot_price: Optional[float] = None,
    ticker_to_strike: Optional[Dict[str, float]] = None,
    seconds_to_close: Optional[float] = None,
    hard_flip_cfg: Optional[Dict[str, Any]] = None,
    sl_persistence_polls: int = 2,
    panic_stop_loss_pct: Optional[float] = None,
) -> List[ExitResult]:
    """
    Evaluate exit criteria for each position.
    - Hard-flip exit first (if enabled and conditions met)
    - Take-profit second (optional)
    - Stop-loss third
    """
    stop_loss_pct = abs(float(stop_loss_pct))
    if profit_target_pct is not None:
        profit_target_pct = abs(float(profit_target_pct))

    ticker_to_strike = ticker_to_strike or {}
    hard_flip_enabled = False
    hard_flip_seconds_lte = 999999
    if hard_flip_cfg and hard_flip_cfg.get("enabled", False):
        hard_flip_enabled = True
        hard_flip_seconds_lte = hard_flip_cfg.get("enable_only_if_seconds_to_close_lte", 999999)

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
        hard_flip_triggered = False
        action = "HOLD"
        sell_result = None

        # Update MFE/MAE excursion tracking
        excursion = _update_excursion(hour_market_id, ticker, norm.get("side", ""), pnl_pct)

        # Hard-flip exit (higher priority than % stop-loss)
        if hard_flip_enabled and seconds_to_close is not None and seconds_to_close <= hard_flip_seconds_lte:
            strike = ticker_to_strike.get(ticker)
            if _check_hard_flip(norm.get("side", ""), spot_price, strike):
                hard_flip_triggered = True
                action = "HARD_FLIP"
                if mode == "TRADE":
                    sell_result = _place_sell(client, norm, "hard_flip_exit")
                exc_final = _pop_excursion(hour_market_id, ticker, norm.get("side", ""))
                exit_eval = _build_exit_eval(
                    norm, pnl_pct, mark_state, stop_loss_pct, profit_target_pct,
                    stop_loss_triggered=False, take_profit_triggered=False,
                    what_happened="Hard flip: spot crossed strike.",
                    hour_market_id=hour_market_id,
                    excursion=exc_final,
                )
                exit_eval["hard_flip_triggered"] = True
                results.append(ExitResult(
                    position=norm,
                    action=action,
                    exit_criteria_evaluated=exit_eval,
                    pnl_pct=pnl_pct,
                    sell_result=sell_result,
                ))
                continue

        if profit_target_pct is not None and pnl_pct is not None and pnl_pct >= profit_target_pct:
            take_profit_triggered = True
            action = "TAKE_PROFIT"
            if mode == "TRADE":
                sell_result = _place_sell(client, norm, "take_profit_exit")
            exc_final = _pop_excursion(hour_market_id, ticker, norm.get("side", ""))
            exit_eval = _build_exit_eval(
                norm, pnl_pct, mark_state, stop_loss_pct, profit_target_pct,
                stop_loss_triggered=False, take_profit_triggered=True,
                what_happened=f"Take-profit triggered at {pnl_pct*100:.2f}% (target {profit_target_pct*100:.2f}%).",
                hour_market_id=hour_market_id,
                excursion=exc_final,
            )
        elif pnl_pct is not None and pnl_pct <= -stop_loss_pct:
            panic_threshold = abs(float(panic_stop_loss_pct)) if panic_stop_loss_pct is not None else None
            # Panic Stop: PnL <= panic_stop_loss_pct → immediate sell, bypass persistence
            if panic_threshold is not None and pnl_pct <= -panic_threshold:
                stop_loss_triggered = True
                action = "STOP_LOSS"
                sl_key = _excursion_key(hour_market_id, ticker, norm.get("side", ""))
                if sl_key in _sl_warning_streak:
                    del _sl_warning_streak[sl_key]
                if mode == "TRADE":
                    sell_result = _place_sell(client, norm, "panic_stop_exit")
                exc_final = _pop_excursion(hour_market_id, ticker, norm.get("side", ""))
                exit_eval = _build_exit_eval(
                    norm, pnl_pct, mark_state, stop_loss_pct, profit_target_pct,
                    stop_loss_triggered=True, take_profit_triggered=False,
                    what_happened=f"Panic stop triggered at {pnl_pct*100:.2f}% (≤ -{panic_threshold*100:.0f}%, immediate exit).",
                    hour_market_id=hour_market_id,
                    excursion=exc_final,
                )
                exit_eval["panic_stop_triggered"] = True
            else:
                # Normal Stop-Loss: PnL ≤ stop_loss_pct but > panic_stop_loss_pct
                # Apply persistence; log SL_WARNING until streak reached
                sl_key = _excursion_key(hour_market_id, ticker, norm.get("side", ""))
                streak = _sl_warning_streak.get(sl_key, 0) + 1
                _sl_warning_streak[sl_key] = streak
                if streak >= sl_persistence_polls:
                    stop_loss_triggered = True
                    action = "STOP_LOSS"
                    del _sl_warning_streak[sl_key]
                    if mode == "TRADE":
                        sell_result = _place_sell(client, norm, "stop_loss_exit")
                    exc_final = _pop_excursion(hour_market_id, ticker, norm.get("side", ""))
                    exit_eval = _build_exit_eval(
                        norm, pnl_pct, mark_state, stop_loss_pct, profit_target_pct,
                        stop_loss_triggered=True, take_profit_triggered=False,
                        what_happened=f"Stop-loss triggered at {pnl_pct*100:.2f}% (threshold -{stop_loss_pct*100:.0f}%, persisted {streak} polls).",
                        hour_market_id=hour_market_id,
                        excursion=exc_final,
                    )
                    exit_eval["sl_warning_streak"] = streak
                else:
                    # SL_WARNING: below threshold but not yet persistent
                    action = "SL_WARNING"
                    exit_eval = _build_exit_eval(
                        norm, pnl_pct, mark_state, stop_loss_pct, profit_target_pct,
                        stop_loss_triggered=False, take_profit_triggered=False,
                        what_happened=f"SL_WARNING: PnL {pnl_pct*100:.2f}% below -{stop_loss_pct*100:.0f}% (poll {streak}/{sl_persistence_polls}, awaiting persistence).",
                        hour_market_id=hour_market_id,
                        excursion=excursion,
                    )
                    exit_eval["sl_warning_streak"] = streak
                    exit_eval["sl_persistence_required"] = sl_persistence_polls
        else:
            # Above threshold: reset SL warning streak
            sl_key = _excursion_key(hour_market_id, ticker, norm.get("side", ""))
            if sl_key in _sl_warning_streak:
                del _sl_warning_streak[sl_key]
            exit_eval = _build_exit_eval(
                norm, pnl_pct, mark_state, stop_loss_pct, profit_target_pct,
                stop_loss_triggered=False, take_profit_triggered=False,
                what_happened=f"Holding. PnL {pnl_pct*100:.2f}%" if pnl_pct is not None else "Holding. No mark.",
                hour_market_id=hour_market_id,
                excursion=excursion,
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
    spot_price: Optional[float] = None,
    ticker_to_strike: Optional[Dict[str, float]] = None,
    seconds_to_close: Optional[float] = None,
    interval: str = "hourly",
    hard_flip_cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Run exit criteria for all positions in the current market.
    Fetches positions from Kalshi, evaluates, and executes sells in TRADE mode.
    Use stop_loss_pct_override/profit_target_pct_override for market-specific
    values (e.g. fifteen_min.stop_loss_pct).
    Returns {exit_results: [...], evaluations: [...], positions_checked: int}.
    """
    exit_cfg = config.get("exit_criteria", {})
    interval_key = "fifteen_min" if interval == "15min" else interval
    interval_cfg = exit_cfg.get(interval_key, {}) or {}
    stop_loss_pct = stop_loss_pct_override if stop_loss_pct_override is not None else interval_cfg.get("stop_loss_pct", exit_cfg.get("stop_loss_pct", 0.20))
    profit_target_pct = profit_target_pct_override if profit_target_pct_override is not None else interval_cfg.get("profit_target_pct") or exit_cfg.get("profit_target_pct")
    sl_persistence_polls = int(interval_cfg.get("stop_loss_persistence_polls") or exit_cfg.get("stop_loss_persistence_polls", 2))
    # Panic stop: immediate exit when PnL ≤ panic_stop_loss_pct (bypass persistence)
    panic_stop_loss_pct = interval_cfg.get("panic_stop_loss_pct")

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
        spot_price=spot_price,
        ticker_to_strike=ticker_to_strike,
        seconds_to_close=seconds_to_close,
        hard_flip_cfg=hard_flip_cfg,
        sl_persistence_polls=sl_persistence_polls,
        panic_stop_loss_pct=panic_stop_loss_pct,
    )

    evaluations = [r.exit_criteria_evaluated for r in results]
    return {
        "exit_results": [
            {
                "position": r.position,
                "action": r.action,
                "pnl_pct": r.pnl_pct,
                "sell_result": r.sell_result,
                "exit_criteria_evaluated": r.exit_criteria_evaluated,
            }
            for r in results
        ],
        "evaluations": evaluations,
        "positions_checked": len(results),
    }
