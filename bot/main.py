#!/usr/bin/env python3
"""
Bot entrypoint - Phase 1: Verification Mode.
Default: OBSERVE (no trading). Use MODE=TRADE to enable orders.
Supports BTC and ETH hourly markets.

Usage:
  python -m bot.main --config config.yaml
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml
from dotenv import load_dotenv

from bot.config_loader import load_config

from src.client.kalshi_client import KalshiClient
from src.client.kraken_client import KrakenClient

from bot.execution import execute_signals
from bot.exit_criteria import evaluate_positions, run_exit_criteria
from bot.hour_summary import check_and_emit_hour_summary
from bot.hourly_report import maybe_emit_hourly_report
from bot.risk_guards import (
    _min_distance_required as get_min_distance_required,
    apply_guards_filter,
    gate_allow_entry,
    consume_hourly_roll,
    emit_window_summary,
    get_roll_stopped_ticker,
    get_roll_used_side,
    is_hourly_roll_available,
    mark_hourly_roll_exhausted,
    record_entry,
    record_exit,
    record_hourly_stoploss_for_roll,
    record_stopout,
    reset_window_on_expiry,
)
from bot.logging import log_run, log_run_15min, print_console_summary, print_console_summary_15min, setup_logging
from bot.market import (
    enrich_with_orderbook,
    fetch_15min_market_result,
    fetch_15min_quote,
    fetch_eligible_tickers,
    get_15min_schedule_state,
    get_current_15min_market_id,
    get_current_hour_market_id,
    get_hourly_schedule_state,
    get_market_context,
    get_minutes_to_close_15min,
    get_previous_15min_market_id,
)
from bot.market_daily_weekly import (
    discover_daily_markets,
    discover_weekly_markets,
    get_daily_schedule_state,
)
from bot.outcomes_15min import get_stored_outcome, save_outcome
from bot.scheduler import run_loop, should_run, sleep_until_next_interval
from bot.state import (
    add_paper_position,
    ensure_state_db,
    get_basket_cooldown_until,
    get_exit_check_last_run,
    get_last_run_hour,
    get_last_run_15min,
    get_paper_positions,
    get_schedule_last_run,
    get_ticker_order_count,
    get_total_order_count,
    prune_stale_paper_positions,
    remove_paper_position,
    set_basket_cooldown,
    set_exit_check_last_run,
    set_last_run_hour,
    set_last_run_15min,
    set_schedule_last_run,
)
from bot.analysis_log import (
    EXIT_HARD_FLIP,
    EXIT_STOPLOSS,
    EXIT_TAKE_PROFIT,
    build_analysis_event,
    build_exit_analysis_event,
    build_order_event,
    build_stoploss_counterfactual_event,
    build_window_summary_event,
)
from bot.strategy import Signal, generate_signals_farthest
from bot.strategy_15min import generate_signals_15min, get_no_signal_reason
from bot.strategy_daily_basket import (
    BasketSignal,
    evaluate_basket_exit,
    gate_allow_basket_entry,
    generate_basket_signals,
)

def _exit_check_due(
    interval_key: str, market_id: str, config: dict, db_path: str
) -> Tuple[bool, Optional[float], Optional[float]]:
    """
    Returns (run_now, stop_loss_pct_override, profit_target_pct_override).
    If evaluation_interval is configured and not elapsed, run_now is False (skip exit criteria this run).
    Otherwise run_now is True and overrides are from exit_criteria.<interval_key>.
    Last-run times are persisted in DB so schedule survives restarts/crashes.
    """
    exit_cfg = config.get("exit_criteria", {}) or {}
    now_sec = time.time()

    if interval_key == "hourly":
        h = exit_cfg.get("hourly", {}) or {}
        stop_override = h.get("stop_loss_pct", exit_cfg.get("stop_loss_pct", 0.20))
        profit_override = h.get("profit_target_pct") or exit_cfg.get("profit_target_pct")
        interval_sec = h.get("evaluation_interval_seconds")
        if interval_sec is None:
            interval_min = h.get("evaluation_interval_minutes")
            interval_sec = interval_min * 60 if interval_min is not None else None
        if interval_sec is not None:
            last = get_exit_check_last_run(db_path, "hourly", market_id)
            if now_sec - last < interval_sec:
                return (False, None, None)
            set_exit_check_last_run(db_path, "hourly", market_id, now_sec)
        return (True, stop_override, profit_override)

    if interval_key == "15min":
        f = exit_cfg.get("fifteen_min", {}) or {}
        stop_override = f.get("stop_loss_pct", exit_cfg.get("stop_loss_pct", 0.30))
        profit_override = f.get("profit_target_pct") or exit_cfg.get("profit_target_pct")
        interval_sec = f.get("evaluation_interval_seconds")
        if interval_sec is not None:
            last = get_exit_check_last_run(db_path, "15min", market_id)
            if now_sec - last < interval_sec:
                return (False, None, None)
            set_exit_check_last_run(db_path, "15min", market_id, now_sec)
        return (True, stop_override, profit_override)

    if interval_key == "daily":
        d = exit_cfg.get("daily", {}) or {}
        stop_override = d.get("stop_loss_pct", exit_cfg.get("stop_loss_pct", 0.30))
        profit_override = d.get("profit_target_pct") or exit_cfg.get("profit_target_pct")
        interval_sec = (d.get("evaluation_interval_minutes") or 60) * 60
        last = get_exit_check_last_run(db_path, "daily", market_id)
        if now_sec - last < interval_sec:
            return (False, None, None)
        set_exit_check_last_run(db_path, "daily", market_id, now_sec)
        return (True, stop_override, profit_override)

    if interval_key == "weekly":
        w = exit_cfg.get("weekly", {}) or {}
        stop_override = w.get("stop_loss_pct", exit_cfg.get("stop_loss_pct", 0.30))
        profit_override = w.get("profit_target_pct") or exit_cfg.get("profit_target_pct")
        interval_sec = (w.get("evaluation_interval_minutes") or 360) * 60
        last = get_exit_check_last_run(db_path, "weekly", market_id)
        if now_sec - last < interval_sec:
            return (False, None, None)
        set_exit_check_last_run(db_path, "weekly", market_id, now_sec)
        return (True, stop_override, profit_override)

    return (True, exit_cfg.get("stop_loss_pct", 0.20), exit_cfg.get("profit_target_pct"))


def _select_hourly_stoploss_roll_ticker(
    quotes,
    stopped_ticker: str,
    side: str,
    stopped_strike: Optional[float],
    ask_min_cents: int,
    ask_max_cents: int,
):
    """
    Pick the "next available" ticker for stop-loss roll:
    - same side as stopped position
    - not the same ticker
    - ask price in [ask_min_cents, ask_max_cents]
    - prefer the next strike beyond stopped_strike in the same direction:
        YES -> lower strike (strike < stopped_strike), pick closest below
        NO  -> higher strike (strike > stopped_strike), pick closest above
    Falls back to any different ticker in band if stopped_strike unknown or no directional candidate.
    """
    s = str(side).lower()
    if s not in ("yes", "no"):
        return None
    lo = int(min(ask_min_cents, ask_max_cents))
    hi = int(max(ask_min_cents, ask_max_cents))

    in_band = []
    for q in quotes or []:
        if not q or getattr(q, "ticker", None) is None:
            continue
        if q.ticker == stopped_ticker:
            continue
        ask = q.yes_ask if s == "yes" else q.no_ask
        if ask is None:
            continue
        if lo <= int(ask) <= hi:
            in_band.append(q)
    if not in_band:
        return None

    ss = None
    if stopped_strike is not None:
        try:
            ss = float(stopped_strike)
        except Exception:
            ss = None

    if ss is not None:
        if s == "yes":
            directional = [q for q in in_band if float(q.strike or 0) < ss]
            if directional:
                # closest below stopped strike
                directional.sort(key=lambda q: float(q.strike or 0), reverse=True)
                return directional[0]
        else:
            directional = [q for q in in_band if float(q.strike or 0) > ss]
            if directional:
                # closest above stopped strike
                directional.sort(key=lambda q: float(q.strike or 0))
                return directional[0]

    # Fallback: pick the first in strike order
    in_band.sort(key=lambda q: float(q.strike or 0))
    return in_band[0]




def _get_spot_price(asset: str):
    """Fetch spot price for asset from Kraken."""
    client = KrakenClient()
    a = str(asset).lower()
    if a == "eth":
        return client.latest_eth_price().price
    if a == "sol":
        return client.latest_sol_price().price
    if a == "xrp":
        return client.latest_xrp_price().price
    return client.latest_btc_price().price


def _get_spot_window(config: dict, asset: str, interval: Optional[str] = None) -> int:
    """Get strike window for asset. interval: hourly (default), daily, weekly."""
    if interval == "daily":
        cfg = config.get("daily", {}) or {}
        by_asset = cfg.get("spot_window_by_asset") or {}
        return int(by_asset.get(asset) or cfg.get("spot_window", 3000))
    if interval == "weekly":
        cfg = config.get("weekly", {}) or {}
        by_asset = cfg.get("spot_window_by_asset") or {}
        return int(by_asset.get(asset) or cfg.get("spot_window", 5000))
    by_asset = config.get("spot_window_by_asset") or {}
    return int(by_asset.get(asset) or config.get("spot_window", 1500))


def _cap_state_for_window(
    config: dict,
    db_path: str,
    window_id: str,
    interval: str,
    ticker: Optional[str] = None,
    side: Optional[str] = None,
) -> dict:
    """Build cap_state dict for analysis logging."""
    caps = config.get("caps", {}) or {}
    cap_scope = caps.get("cap_scope", "combined")
    if interval == "fifteen_min" or interval == "15min":
        fm = caps.get("fifteen_min", {}) or {}
        max_per_ticker = fm.get("max_orders_per_ticker", caps.get("max_orders_per_ticker", 1))
        max_total = fm.get("max_total_orders_per_15min", 10)
    elif interval == "daily":
        dm = caps.get("daily", {}) or {}
        max_per_ticker = dm.get("max_orders_per_ticker", caps.get("max_orders_per_ticker", 3))
        max_total = dm.get("max_total_orders_per_window", 5)
    elif interval == "weekly":
        wm = caps.get("weekly", {}) or {}
        max_per_ticker = wm.get("max_orders_per_ticker", caps.get("max_orders_per_ticker", 2))
        max_total = wm.get("max_total_orders_per_window", 3)
    else:
        hm = caps.get("hourly", {}) or {}
        max_per_ticker = hm.get("max_orders_per_ticker", caps.get("max_orders_per_ticker", 5))
        max_total = hm.get("max_total_orders_per_hour", caps.get("max_total_orders_per_hour", 50))
    total = get_total_order_count(db_path, window_id)
    ticker_count = get_ticker_order_count(db_path, window_id, ticker or "", cap_scope, side=side) if ticker else None
    return {
        "max_orders_per_ticker": max_per_ticker,
        "max_total_orders_per_window": max_total,
        "current_total_orders": total,
        "current_ticker_orders": ticker_count,
    }


def _log_order_events(
    logger, execution_results, interval: str, window_id: str, asset: str,
    guard_eval_map: Optional[dict] = None,
) -> None:
    """Log ORDER_SUBMITTED, ORDER_FAILED, ORDER_SKIPPED for reconciliation and execution diagnostics."""
    guard_eval_map = guard_eval_map or {}
    for r in execution_results:
        if not getattr(r, "client_order_id", None):
            continue
        ticker = r.signal.ticker
        side = r.signal.side
        guard_eval = guard_eval_map.get((ticker, side))
        tob_kw = {}
        if hasattr(r, "yes_bid_cents") and (r.yes_bid_cents is not None or r.yes_ask_cents is not None or r.no_bid_cents is not None or r.no_ask_cents is not None):
            tob_kw = {
                "yes_bid_cents": getattr(r, "yes_bid_cents", None),
                "yes_ask_cents": getattr(r, "yes_ask_cents", None),
                "no_bid_cents": getattr(r, "no_bid_cents", None),
                "no_ask_cents": getattr(r, "no_ask_cents", None),
            }
        if r.status == "PLACED":
            ev = build_order_event(
                event_type="ORDER_SUBMITTED",
                interval=interval,
                window_id=window_id,
                asset=asset,
                ticker=ticker,
                side=side,
                client_order_id=r.client_order_id,
                order_id=r.order_id,
                price_cents=r.price_cents,
                contracts=r.contracts,
                guard_eval=guard_eval,
                **tob_kw,
            )
        elif r.status == "FAILED":
            ev = build_order_event(
                event_type="ORDER_FAILED",
                interval=interval,
                window_id=window_id,
                asset=asset,
                ticker=ticker,
                side=side,
                client_order_id=r.client_order_id,
                order_id=r.order_id,
                price_cents=r.price_cents,
                contracts=r.contracts,
                reason=r.error,
                guard_eval=guard_eval,
                **tob_kw,
            )
        else:  # SKIPPED_CAP_REACHED, WOULD_TRADE (OBSERVE)
            ev = build_order_event(
                event_type="ORDER_SKIPPED",
                interval=interval,
                window_id=window_id,
                asset=asset,
                ticker=ticker,
                side=side,
                client_order_id=r.client_order_id,
                reason=r.status,
                guard_eval=guard_eval,
            )
        logger.info(json.dumps(ev, default=str))


def run_bot_for_asset(
    asset: str,
    config: dict,
    db_path: str,
    logger,
    kalshi_client,
) -> None:
    """Run full bot cycle for one asset (btc, eth, sol, or xrp)."""
    intervals = config.get("intervals", {}) or {}
    mode = (intervals.get("hourly", {}).get("mode") or config.get("mode", "OBSERVE")).upper()
    if mode not in ("OBSERVE", "TRADE"):
        mode = "OBSERVE"
    schedule = config.get("schedule", {})
    late_window = schedule.get("late_window_minutes", 10)
    thresholds = config.get("thresholds", {})

    hour_market_id = get_current_hour_market_id(asset=asset)
    ctx = get_market_context(hour_market_id, late_window_minutes=int(late_window))

    # Risk guards: emit WINDOW_SUMMARY for previous hour if we transitioned
    last_hour = get_last_run_hour(db_path, asset=asset)
    if last_hour and last_hour != hour_market_id:
        schedule = config.get("schedule", {}) or {}
        hrg = schedule.get("hourly_risk_guards", {}) or {}
        if hrg.get("enabled", False):
            emit_window_summary(logger, "hourly", last_hour, asset=asset, build_summary_fn=build_window_summary_event)
            reset_window_on_expiry("hourly", last_hour)
        # Hourly report: trade rows + window rows + totals for the hour that just finished
        log_cfg = config.get("logging", {}) or {}
        log_path = log_cfg.get("file", "logs/bot.log")
        project_root = Path(__file__).resolve().parent.parent
        reports_cfg = config.get("reports", {}) or {}
        reports_db_path = reports_cfg.get("db_path", "data/kalshi_ingest.db")
        maybe_emit_hourly_report(last_hour, log_path, project_root, logger, db_path=reports_db_path)

    # Hour summary
    check_and_emit_hour_summary(
        current_hour_market_id=hour_market_id,
        db_path=db_path,
        kalshi_client=kalshi_client,
        logger=logger,
        asset=asset,
    )
    set_last_run_hour(db_path, hour_market_id, asset=asset)

    if ctx.minutes_to_close <= 0:
        logger.info("[%s] Skipping run: market closed (minutes_to_close <= 0)", asset.upper())
        return

    # Trade only in late window (last 10 min)
    if not ctx.is_late_window:
        logger.info("[%s] Skipping run: outside late window (minutes_to_close=%.1f > 10)", asset.upper(), ctx.minutes_to_close)
        return

    # Fetch spot price and tickers
    try:
        spot_price = _get_spot_price(asset)
    except Exception:
        spot_price = None
    window = _get_spot_window(config, asset)
    tickers = fetch_eligible_tickers(hour_market_id, spot_price=spot_price, window=window)
    if not tickers:
        logger.info("[%s] No eligible tickers for market %s", asset.upper(), hour_market_id)
        return

    ticker_to_strike = {t["ticker"]: float(t["strike"]) for t in tickers if t.get("strike")}
    seconds_to_close = ctx.minutes_to_close * 60.0
    hrg = schedule.get("hourly_risk_guards", {}) or {}
    hard_flip_cfg = hrg.get("hard_flip_exit") if isinstance(hrg.get("hard_flip_exit"), dict) else None

    # Exit criteria (per-ticker; cadence decoupled from bot run via evaluation_interval_minutes)
    run_exit_now, stop_override, profit_override = _exit_check_due("hourly", hour_market_id, config, db_path)
    if run_exit_now:
        exit_out = run_exit_criteria(
            client=kalshi_client,
            hour_market_id=hour_market_id,
            config=config,
            mode=mode,
            stop_loss_pct_override=stop_override,
            profit_target_pct_override=profit_override,
            spot_price=spot_price,
            ticker_to_strike=ticker_to_strike if ticker_to_strike else None,
            seconds_to_close=seconds_to_close,
            interval="hourly",
            hard_flip_cfg=hard_flip_cfg,
        )
    else:
        exit_out = {"exit_results": [], "evaluations": [], "positions_checked": 0}

    # If STOP_LOSS happens this run, attempt an immediate roll re-entry (same side, next strike).
    stoploss_roll_side = None
    stoploss_stopped_ticker = None
    stoploss_exit_info = None
    for r in exit_out.get("exit_results", []):
        action = r.get("action")
        if action in ("STOP_LOSS", "TAKE_PROFIT", "HARD_FLIP"):
            exit_reason = EXIT_STOPLOSS if action == "STOP_LOSS" else (EXIT_HARD_FLIP if action == "HARD_FLIP" else EXIT_TAKE_PROFIT)
            if hrg.get("enabled"):
                record_exit("hourly", hour_market_id, reason=exit_reason)
            pos = r.get("position", {})
            ticker_pos = pos.get("ticker")
            strike_val = ticker_to_strike.get(ticker_pos) if ticker_to_strike and ticker_pos else None
            dist = None
            if spot_price is not None and strike_val is not None and strike_val > 0:
                s = str(pos.get("side") or "").lower()
                dist = spot_price - strike_val if s == "yes" else (strike_val - spot_price if s == "no" else None)
            dist_cfg = hrg.get("distance_buffer", {}) or {}
            min_dist = get_min_distance_required(asset, spot_price, dist_cfg) if spot_price and dist_cfg else None
            exit_eval = r.get("exit_criteria_evaluated") or {}
            sr = r.get("sell_result") or {}
            exit_price = sr.get("exit_price_cents") if sr.get("exit_price_cents") is not None else exit_eval.get("mark_cents")
            dist_margin = (dist - min_dist) if (dist is not None and min_dist is not None) else None
            cutoff_secs = hrg.get("no_new_entry_cutoff_seconds")
            late_ov = hrg.get("late_persistence_override") or {}
            persist_req = hrg.get("persistence_polls", 2)
            if late_ov.get("enabled") and seconds_to_close is not None and late_ov.get("seconds_to_close_lte") is not None and seconds_to_close <= late_ov.get("seconds_to_close_lte"):
                persist_req = late_ov.get("persistence_polls", 1)
            exit_ev = build_exit_analysis_event(
                interval="hourly",
                window_id=hour_market_id,
                asset=asset,
                action=action,
                ticker=ticker_pos,
                side=pos.get("side"),
                pnl_pct=r.get("pnl_pct"),
                seconds_to_close=seconds_to_close,
                spot=spot_price,
                strike=strike_val,
                distance=dist,
                min_distance_required=min_dist,
                entry_price_cents=pos.get("entry_price_cents"),
                exit_price_cents=exit_price,
                dist_margin=dist_margin,
                mfe_pct=exit_eval.get("mfe_pct"),
                mae_pct=exit_eval.get("mae_pct"),
                mfe_ts=exit_eval.get("mfe_ts"),
                mae_ts=exit_eval.get("mae_ts"),
                top_of_book_yes_bid=exit_eval.get("top_of_book_yes_bid"),
                top_of_book_yes_ask=exit_eval.get("top_of_book_yes_ask"),
                top_of_book_no_bid=exit_eval.get("top_of_book_no_bid"),
                top_of_book_no_ask=exit_eval.get("top_of_book_no_ask"),
                yes_bid_cents=exit_eval.get("yes_bid_cents"),
                yes_ask_cents=exit_eval.get("yes_ask_cents"),
                no_bid_cents=exit_eval.get("no_bid_cents"),
                no_ask_cents=exit_eval.get("no_ask_cents"),
                target_sl_price_cents=exit_eval.get("target_sl_price_cents"),
                tob_spread_cents=exit_eval.get("tob_spread_cents"),
                guardrails_persistence_required=persist_req,
                guardrails_cutoff_seconds=float(cutoff_secs) if cutoff_secs is not None else None,
                exit_eval=exit_eval,
            )
            logger.info(json.dumps(exit_ev, default=str))
            if action == "STOP_LOSS" and hrg.get("enabled"):
                stop_cfg = hrg.get("stop_once_disable", {}) or {}
                if stop_cfg.get("enabled"):
                    record_stopout(
                        "hourly", hour_market_id,
                        r.get("position", {}).get("ticker", ""),
                        stop_cfg.get("max_stopouts_per_ticker", 1),
                    )
                # Hourly stop-loss roll: allow 1 re-entry same direction (different strike)
                pos_side = (pos.get("side") or "").lower()
                if pos_side in ("yes", "no"):
                    anchor_cfg = hrg.get("anchor_one_per_side", {}) or {}
                    record_hourly_stoploss_for_roll(
                        hour_market_id, ticker_pos or "", pos_side,
                        lock_on_stop_loss_only=anchor_cfg.get("lock_on_stop_loss_only", False),
                    )
                    stoploss_roll_side = pos_side
                    stoploss_stopped_ticker = ticker_pos or ""
                    stoploss_exit_info = {
                        "ticker": ticker_pos,
                        "side": pos_side,
                        "entry_price_cents": pos.get("entry_price_cents"),
                        "pnl_pct": r.get("pnl_pct"),
                    }
            # Mark roll exhausted when rolled position exits (any exit type)
            roll_used = get_roll_used_side(hour_market_id)
            pos_side = (pos.get("side") or "").lower()
            if roll_used and pos_side == roll_used:
                mark_hourly_roll_exhausted(hour_market_id, pos_side)
            logger.info(
                "[%s] Exit criteria: %s %s %s PnL=%.2f%%",
                asset.upper(), action,
                r.get("position", {}).get("ticker"),
                r.get("position", {}).get("side"),
                (r.get("pnl_pct") or 0) * 100,
            )
        elif action == "SL_WARNING":
            ev = r.get("exit_criteria_evaluated") or {}
            streak = ev.get("sl_warning_streak", 0)
            required = ev.get("sl_persistence_required", 2)
            logger.info(
                "[%s] Exit criteria: SL_WARNING %s %s PnL=%.2f%% (poll %d/%d, awaiting persistence)",
                asset.upper(),
                r.get("position", {}).get("ticker"),
                r.get("position", {}).get("side"),
                (r.get("pnl_pct") or 0) * 100,
                streak, required,
            )

    # Enrich quotes
    if kalshi_client is not None:
        try:
            quotes = enrich_with_orderbook(kalshi_client, tickers)
        except Exception as e:
            logger.warning("[%s] Orderbook unavailable, using market data: %s", asset.upper(), e)
            from bot.market import TickerQuote
            quotes = [
                TickerQuote(
                    ticker=t["ticker"], strike=t["strike"],
                    yes_ask=int(t["yes_ask"]) if t.get("yes_ask") is not None else None,
                    no_ask=int(t["no_ask"]) if t.get("no_ask") is not None else None,
                    yes_bid=None, no_bid=None, subtitle=t.get("subtitle", ""),
                )
                for t in tickers
            ]
    else:
        from bot.market import TickerQuote
        quotes = [
            TickerQuote(
                ticker=t["ticker"], strike=t["strike"],
                yes_ask=int(t["yes_ask"]) if t.get("yes_ask") is not None else None,
                no_ask=int(t["no_ask"]) if t.get("no_ask") is not None else None,
                yes_bid=None, no_bid=None, subtitle=t.get("subtitle", ""),
            )
            for t in tickers
        ]

    # STOPLOSS_COUNTERFACTUAL (hourly only): at STOP_LOSS time, evaluate candidate tickers
    if hrg.get("enabled") and stoploss_roll_side and stoploss_stopped_ticker and stoploss_exit_info and spot_price is not None and quotes:
        K = 5
        spot_win = config.get("spot_window_by_asset", {}).get(asset) or config.get("spot_window", 1500)
        entry_band_cf = f"{thresholds.get('yes_min', 94)}-{thresholds.get('yes_max', 99)}"
        opp_side = "no" if stoploss_roll_side == "yes" else "yes"
        same_cands = []
        opp_cands = []
        for q in quotes:
            if q.ticker == stoploss_stopped_ticker:
                continue
            strike_q = ticker_to_strike.get(q.ticker) if ticker_to_strike else getattr(q, "strike", None)
            if strike_q is None:
                continue
            dist_to_spot = abs(float(strike_q) - float(spot_price))
            if dist_to_spot > spot_win:
                continue
            same_cands.append((q, stoploss_roll_side, dist_to_spot))
            opp_cands.append((q, opp_side, dist_to_spot))
        same_cands.sort(key=lambda x: x[2])
        opp_cands.sort(key=lambda x: x[2])
        same_cands = same_cands[:K]
        opp_cands = opp_cands[:K]
        all_cands = [(q, side, "same") for q, side, _ in same_cands] + [(q, side, "opposite") for q, side, _ in opp_cands]
        cand_list = []
        for q, side, cand_type in all_cands:
            strike_q = ticker_to_strike.get(q.ticker) if ticker_to_strike else getattr(q, "strike", None)
            yes_p = q.yes_ask
            no_p = q.no_ask
            ask_p = yes_p if side == "yes" else no_p
            dist_cfg = hrg.get("distance_buffer", {}) or {}
            min_dist_req = get_min_distance_required(asset, spot_price, dist_cfg) if spot_price and dist_cfg else None
            dist_val = None
            if spot_price is not None and strike_q is not None and strike_q > 0:
                dist_val = spot_price - strike_q if side == "yes" else (strike_q - spot_price if side == "no" else None)
            dist_margin_cf = (dist_val - min_dist_req) if (dist_val is not None and min_dist_req is not None) else None
            late_ov = hrg.get("late_persistence_override") or {}
            persist_req_cf = hrg.get("persistence_polls", 2)
            if late_ov.get("enabled") and seconds_to_close is not None and late_ov.get("seconds_to_close_lte") is not None and seconds_to_close <= late_ov.get("seconds_to_close_lte"):
                persist_req_cf = late_ov.get("persistence_polls", 1)
            allowed, reason, payload = gate_allow_entry(
                interval="hourly", window_id=hour_market_id, asset=asset,
                ticker=q.ticker, side=side, yes_price=yes_p, no_price=no_p,
                spot=spot_price, strike=strike_q, seconds_to_close=seconds_to_close,
                entry_band=entry_band_cf, guards_cfg=hrg, is_hourly=True,
            )
            decision = "PASS" if allowed else "FAIL"
            fail_reason = None if allowed else reason
            streak = payload.get("streak")
            tob = {
                "top_of_book_yes_bid": getattr(q, "yes_bid", None) is not None,
                "top_of_book_yes_ask": q.yes_ask is not None,
                "top_of_book_no_bid": getattr(q, "no_bid", None) is not None,
                "top_of_book_no_ask": q.no_ask is not None,
            }
            cand_list.append({
                "ticker": q.ticker, "side": side, "ask_price_cents": ask_p,
                "strike": strike_q, "distance": dist_val, "min_distance_required": min_dist_req,
                "dist_margin": dist_margin_cf, "persistence_streak": streak,
                "persistence_required": persist_req_cf, "seconds_to_close": seconds_to_close,
                "decision": decision, "fail_reason": fail_reason,
                **tob,
            })
        ev_cf = build_stoploss_counterfactual_event(
            interval="hourly", window_id=hour_market_id, asset=asset,
            stopped_ticker=stoploss_stopped_ticker, stopped_side=stoploss_roll_side,
            stopped_entry_price_cents=stoploss_exit_info.get("entry_price_cents") or 0,
            pnl_pct=stoploss_exit_info.get("pnl_pct") or 0.0,
            candidates=cand_list,
        )
        logger.info(json.dumps(ev_cf, default=str))

    signals = generate_signals_farthest(quotes, spot_price, ctx.is_late_window, thresholds)
    roll_order_size_multiplier = None  # when set (e.g. 2 or 3), roll re-entry uses base max_cost_cents × this

    # STOP_LOSS roll re-entry: after STOP_LOSS, buy next available ticker (different strike)
    # in the same direction, using a separate ask band (e.g. 70-98c).
    if hrg.get("enabled") and stoploss_roll_side and stoploss_stopped_ticker:
        roll_cfg = hrg.get("stoploss_roll_reentry", {}) or {}
        if roll_cfg.get("enabled", False):
            ask_min = int(roll_cfg.get("ask_min_cents", 70))
            ask_max = int(roll_cfg.get("ask_max_cents", 98))
            stopped_strike = ticker_to_strike.get(stoploss_stopped_ticker) if ticker_to_strike else None
            cand = _select_hourly_stoploss_roll_ticker(
                quotes,
                stopped_ticker=stoploss_stopped_ticker,
                side=stoploss_roll_side,
                stopped_strike=stopped_strike,
                ask_min_cents=ask_min,
                ask_max_cents=ask_max,
            )
            if cand is not None:
                roll_price = cand.yes_ask if stoploss_roll_side == "yes" else cand.no_ask
                if roll_price is not None:
                    roll_sig = Signal(
                        ticker=cand.ticker,
                        side=stoploss_roll_side,
                        price=int(roll_price),
                        reason="STOPLOSS_ROLL_REENTRY",
                        late_window=ctx.is_late_window,
                    )
                    allowed_roll, roll_eval_logs = apply_guards_filter(
                        [roll_sig], "hourly", hour_market_id, asset, spot_price,
                        seconds_to_close, ticker_to_strike, hrg, is_hourly=True,
                        entry_band=f"{ask_min}-{ask_max}",
                    )
                    for g in roll_eval_logs:
                        cap_state = _cap_state_for_window(config, db_path, hour_market_id, "hourly", g.get("ticker"), g.get("side"))
                        ev = build_analysis_event(g, cap_state=cap_state, minutes_to_close=ctx.minutes_to_close)
                        logger.info(json.dumps(ev, default=str))
                    if allowed_roll:
                        # Prioritize the roll; do not also place the normal hourly entry this run.
                        signals = allowed_roll
                        roll_order_size_multiplier = roll_cfg.get("order_size_multiplier")

    # When executing roll-only signals, optionally scale order size (max_cost_cents) by multiplier.
    max_cost_override = None
    if signals and all(getattr(s, "reason", None) == "STOPLOSS_ROLL_REENTRY" for s in signals) and roll_order_size_multiplier is not None:
        try:
            mult = float(roll_order_size_multiplier)
        except (TypeError, ValueError):
            mult = 1.0
        if mult > 0 and mult != 1.0:
            order_cfg = (config.get("order", {}) or {}).get("hourly") or config.get("order", {}) or {}
            base_max = order_cfg.get("max_cost_cents", 2000)
            by_asset = order_cfg.get("max_cost_cents_by_asset") or {}
            base_max = by_asset.get(str(asset).lower(), base_max)
            max_cost_override = int(base_max * mult)

    guard_eval_map = {}
    if hrg.get("enabled"):
        allowed_signals, guard_eval_logs = apply_guards_filter(
            signals, "hourly", hour_market_id, asset, spot_price,
            seconds_to_close, ticker_to_strike, hrg, is_hourly=True,
        )
        for g in guard_eval_logs:
            cap_state = _cap_state_for_window(config, db_path, hour_market_id, "hourly", g.get("ticker"), g.get("side"))
            ev = build_analysis_event(g, cap_state=cap_state, minutes_to_close=ctx.minutes_to_close)
            logger.info(json.dumps(ev, default=str))
            t, s = g.get("ticker"), g.get("side")
            if t is not None and s is not None:
                guard_eval_map[(t, s)] = g
        signals = allowed_signals
    execution_results = execute_signals(
        signals, hour_market_id, db_path, kalshi_client, config, mode,
        max_cost_cents_override=max_cost_override,
        asset=asset,
    )
    _log_order_events(logger, execution_results, "hourly", hour_market_id, asset, guard_eval_map=guard_eval_map)
    if hrg.get("enabled"):
        anchor_cfg = hrg.get("anchor_one_per_side", {}) or {}
        for r in execution_results:
            if r.status == "PLACED":
                # Hourly stop-loss roll: consume roll and log if this entry was a roll
                if is_hourly_roll_available(hour_market_id, r.signal.side, r.signal.ticker):
                    original_ticker = get_roll_stopped_ticker(hour_market_id)
                    consume_hourly_roll(hour_market_id, r.signal.side, r.signal.ticker)
                    logger.info(
                        json.dumps({
                            "ROLL_AFTER_STOP": True,
                            "original_ticker": original_ticker,
                            "rolled_ticker": r.signal.ticker,
                            "side": r.signal.side,
                            "window_id": hour_market_id,
                            "asset": asset.upper(),
                        }, default=str),
                    )
                record_entry(
                    "hourly", hour_market_id, r.signal.ticker, r.signal.side,
                    lock_per_window=anchor_cfg.get("lock_per_window", False),
                    lock_on_stop_loss_only=anchor_cfg.get("lock_on_stop_loss_only", False),
                )

    log_run(
        logger, ctx, len(tickers), signals, execution_results,
        db_path, hour_market_id, exit_out=exit_out, asset=asset,
    )
    print_console_summary(
        ctx, len(tickers), signals, execution_results,
        db_path, hour_market_id, mode, exit_out=exit_out, asset=asset,
    )


def run_bot_15min_for_asset(
    asset: str,
    config: dict,
    db_path: str,
    logger,
    kalshi_client,
) -> None:
    """Run 15-min bot cycle for one asset."""
    intervals = config.get("intervals", {}) or {}
    mode = (intervals.get("fifteen_min", {}).get("mode") or config.get("mode", "OBSERVE")).upper()
    if mode not in ("OBSERVE", "TRADE"):
        mode = "OBSERVE"

    # Check previous 15-min market (just closed) - fetch and log outcome if now available
    prev_market_id = get_previous_15min_market_id(asset=asset)
    if prev_market_id:
        stored = get_stored_outcome(db_path, prev_market_id)
        if not stored:
            result = fetch_15min_market_result(prev_market_id)
            if result in ("yes", "no"):
                save_outcome(db_path, prev_market_id, result)
                logger.info(
                    "[%s 15m] Outcome logged: %s resolved %s",
                    asset.upper(), prev_market_id, result.upper(),
                )

    market_id = get_current_15min_market_id(asset=asset)
    minutes_to_close = get_minutes_to_close_15min(market_id)

    if minutes_to_close <= 0:
        logger.info("[%s 15m] Skipping: market closed (minutes_to_close <= 0)", asset.upper())
        return

    fm_cfg = config.get("fifteen_min", {}) or {}
    rg = fm_cfg.get("risk_guards", {}) or {}
    try:
        spot_price = _get_spot_price(asset)
    except Exception:
        spot_price = None

    quote = fetch_15min_quote(market_id, client=kalshi_client)
    if quote is None:
        logger.info("[%s 15m] No market data for %s", asset.upper(), market_id)
        return

    ticker_to_strike = {quote.ticker: float(quote.strike or 0)}
    seconds_to_close = minutes_to_close * 60.0
    hard_flip_cfg = {"enabled": bool(rg.get("hard_flip_exit", False)), "enable_only_if_seconds_to_close_lte": 999999} if rg.get("enabled") else None

    # Exit criteria (per-ticker; cadence decoupled from bot run via evaluation_interval_seconds)
    run_exit_now, stop_override, profit_override = _exit_check_due("15min", market_id, config, db_path)
    if run_exit_now:
        exit_out = run_exit_criteria(
            client=kalshi_client,
            hour_market_id=market_id,
            config=config,
            mode=mode,
            stop_loss_pct_override=stop_override,
            profit_target_pct_override=profit_override,
            spot_price=spot_price,
            ticker_to_strike=ticker_to_strike if ticker_to_strike else None,
            seconds_to_close=seconds_to_close,
            interval="15min",
            hard_flip_cfg=hard_flip_cfg,
        )
    else:
        exit_out = {"exit_results": [], "evaluations": [], "positions_checked": 0}
    for r in exit_out.get("exit_results", []):
        action = r.get("action")
        if action in ("STOP_LOSS", "TAKE_PROFIT", "HARD_FLIP"):
            exit_reason = EXIT_STOPLOSS if action == "STOP_LOSS" else (EXIT_HARD_FLIP if action == "HARD_FLIP" else EXIT_TAKE_PROFIT)
            if rg.get("enabled"):
                record_exit("15min", market_id, reason=exit_reason)
            pos = r.get("position", {})
            ticker_pos = pos.get("ticker")
            strike_val = ticker_to_strike.get(ticker_pos) if ticker_to_strike and ticker_pos else None
            dist = None
            if spot_price is not None and strike_val is not None and strike_val > 0:
                s = str(pos.get("side") or "").lower()
                dist = spot_price - strike_val if s == "yes" else (strike_val - spot_price if s == "no" else None)
            dist_cfg = rg.get("distance_buffer", {}) or {}
            min_dist = get_min_distance_required(asset, spot_price, dist_cfg) if spot_price and dist_cfg else None
            exit_eval_15 = r.get("exit_criteria_evaluated") or {}
            sr_15 = r.get("sell_result") or {}
            exit_price_15 = sr_15.get("exit_price_cents") if sr_15.get("exit_price_cents") is not None else exit_eval_15.get("mark_cents")
            dist_margin_15 = (dist - min_dist) if (dist is not None and min_dist is not None) else None
            cutoff_secs_15 = rg.get("no_new_entry_cutoff_seconds")
            late_ov_15 = rg.get("late_persistence_override") or {}
            persist_req_15 = rg.get("persistence_polls", 2)
            if late_ov_15.get("enabled") and seconds_to_close is not None and late_ov_15.get("seconds_to_close_lte") is not None and seconds_to_close <= late_ov_15.get("seconds_to_close_lte"):
                persist_req_15 = late_ov_15.get("persistence_polls", 1)
            exit_ev = build_exit_analysis_event(
                interval="15min",
                window_id=market_id,
                asset=asset,
                action=action,
                ticker=ticker_pos,
                side=pos.get("side"),
                pnl_pct=r.get("pnl_pct"),
                seconds_to_close=seconds_to_close,
                yes_price_cents=quote.yes_ask,
                no_price_cents=quote.no_ask,
                spot=spot_price,
                strike=strike_val,
                distance=dist,
                min_distance_required=min_dist,
                entry_price_cents=pos.get("entry_price_cents"),
                exit_price_cents=exit_price_15,
                dist_margin=dist_margin_15,
                mfe_pct=exit_eval_15.get("mfe_pct"),
                mae_pct=exit_eval_15.get("mae_pct"),
                mfe_ts=exit_eval_15.get("mfe_ts"),
                mae_ts=exit_eval_15.get("mae_ts"),
                top_of_book_yes_bid=exit_eval_15.get("top_of_book_yes_bid"),
                top_of_book_yes_ask=exit_eval_15.get("top_of_book_yes_ask"),
                top_of_book_no_bid=exit_eval_15.get("top_of_book_no_bid"),
                top_of_book_no_ask=exit_eval_15.get("top_of_book_no_ask"),
                yes_bid_cents=exit_eval_15.get("yes_bid_cents"),
                yes_ask_cents=exit_eval_15.get("yes_ask_cents"),
                no_bid_cents=exit_eval_15.get("no_bid_cents"),
                no_ask_cents=exit_eval_15.get("no_ask_cents"),
                target_sl_price_cents=exit_eval_15.get("target_sl_price_cents"),
                tob_spread_cents=exit_eval_15.get("tob_spread_cents"),
                guardrails_persistence_required=persist_req_15,
                guardrails_cutoff_seconds=float(cutoff_secs_15) if cutoff_secs_15 is not None else None,
                exit_eval=exit_eval_15,
            )
            logger.info(json.dumps(exit_ev, default=str))
            if action == "STOP_LOSS" and rg.get("enabled"):
                pos = r.get("position", {})
                ticker = pos.get("ticker", "")
                stopped_side = pos.get("side")
                stop_cfg = rg.get("stop_once_disable", {}) or {}
                rev_cfg = rg.get("reversal_after_stop", {}) or {}
                if rev_cfg.get("enabled"):
                    record_stopout(
                        "15min", market_id, ticker,
                        stop_cfg.get("max_stopouts_per_ticker", 1),
                        stopped_side=stopped_side,
                        reversal_after_stop_cfg=rev_cfg,
                    )
                    logger.info(json.dumps({
                        "event": "STOPLOSS_EXIT",
                        "interval": "15min",
                        "window_id": market_id,
                        "asset": asset,
                        "ticker": ticker,
                        "stopped_side": stopped_side,
                        "yes_price": quote.yes_ask,
                        "no_price": quote.no_ask,
                        "seconds_to_close": seconds_to_close,
                        "type": "risk_guard_reversal",
                    }, default=str))
                elif stop_cfg.get("enabled"):
                    record_stopout(
                        "15min", market_id, ticker,
                        stop_cfg.get("max_stopouts_per_ticker", 1),
                    )
            logger.info(
                "[%s 15m] Exit criteria: %s %s %s PnL=%.2f%%",
                asset.upper(), action,
                r.get("position", {}).get("ticker"),
                r.get("position", {}).get("side"),
                (r.get("pnl_pct") or 0) * 100,
            )
        elif action == "SL_WARNING":
            ev = r.get("exit_criteria_evaluated") or {}
            streak = ev.get("sl_warning_streak", 0)
            required = ev.get("sl_persistence_required", 2)
            logger.info(
                "[%s 15m] Exit criteria: SL_WARNING %s %s PnL=%.2f%% (poll %d/%d, awaiting persistence)",
                asset.upper(),
                r.get("position", {}).get("ticker"),
                r.get("position", {}).get("side"),
                (r.get("pnl_pct") or 0) * 100,
                streak, required,
            )

    # Risk guards: emit WINDOW_SUMMARY for previous 15-min window if transitioned
    last_15min = get_last_run_15min(db_path, asset=asset)
    if last_15min and last_15min != market_id and rg.get("enabled"):
        emit_window_summary(logger, "15min", last_15min, asset=asset, build_summary_fn=build_window_summary_event)
        reset_window_on_expiry("15min", last_15min)

    signals = generate_signals_15min(quote, minutes_to_close, config)
    guard_eval_map_15 = {}
    if rg.get("enabled"):
        allowed_signals, guard_eval_logs = apply_guards_filter(
            signals, "15min", market_id, asset, spot_price,
            seconds_to_close, ticker_to_strike, rg, is_hourly=False,
            yes_price_all=quote.yes_ask,
            no_price_all=quote.no_ask,
        )
        for g in guard_eval_logs:
            cap_state = _cap_state_for_window(config, db_path, market_id, "15min", g.get("ticker"), g.get("side"))
            ev = build_analysis_event(g, cap_state=cap_state, minutes_to_close=minutes_to_close)
            logger.info(json.dumps(ev, default=str))
            t, s = g.get("ticker"), g.get("side")
            if t is not None and s is not None:
                guard_eval_map_15[(t, s)] = g
        signals = allowed_signals
    execution_results = execute_signals(
        signals, market_id, db_path, kalshi_client, config, mode, interval="15min",
        asset=asset,
    )
    _log_order_events(logger, execution_results, "15min", market_id, asset, guard_eval_map=guard_eval_map_15)
    if rg.get("enabled"):
        rev_cfg = rg.get("reversal_after_stop", {}) or {}
        for r in execution_results:
            if r.status == "PLACED":
                is_rev = False
                for g in guard_eval_logs:
                    if (g.get("ticker") == r.signal.ticker and g.get("side") == r.signal.side
                            and g.get("reason_code") == "REVERSAL_ENTRY_ALLOWED"):
                        is_rev = True
                        logger.info(json.dumps({
                            "event": "DISABLE_AFTER_REVERSAL",
                            "interval": "15min",
                            "window_id": market_id,
                            "asset": asset,
                            "ticker": r.signal.ticker,
                            "side": r.signal.side,
                            "reversals_taken": 1,
                            "stopped_side": g.get("stopped_side"),
                            "type": "risk_guard_reversal",
                        }, default=str))
                        break
                record_entry(
                    "15min", market_id, r.signal.ticker, r.signal.side,
                    lock_per_window=False,
                    is_reversal=is_rev and rev_cfg.get("enabled", False),
                )

    set_last_run_15min(db_path, market_id, asset=asset)
    no_signal_reason = get_no_signal_reason(quote, minutes_to_close, config) if not signals else None

    log_run_15min(
        logger, market_id, minutes_to_close, quote.ticker,
        signals, execution_results, db_path, asset=asset,
        yes_ask=quote.yes_ask, no_ask=quote.no_ask, no_signal_reason=no_signal_reason,
        exit_out=exit_out,
    )
    print_console_summary_15min(
        market_id, minutes_to_close, signals, execution_results,
        db_path, mode, asset=asset,
        yes_ask=quote.yes_ask, no_ask=quote.no_ask, no_signal_reason=no_signal_reason,
        exit_out=exit_out,
    )


def _run_interval_daily_basket(
    asset: str,
    config: dict,
    db_path: str,
    logger,
    kalshi_client,
    markets: list,
    late_window_minutes: float,
    thresholds: dict,
    rg: dict,
    mode: str = "OBSERVE",
) -> None:
    """
    Daily basket (Iron Condor) flow: dual-sided YES+NO per window_id.
    Entry: both legs in 65-80¢, pass guards, not in cooldown.
    Exit: combined 10% TP or individual -35% SL.
    In TRADE mode: places real orders and sells via Kalshi; in OBSERVE uses paper positions.
    """
    try:
        spot_price = _get_spot_price(asset)
    except Exception:
        spot_price = None

    basket_cfg = config.get("daily", {}).get("basket_scalping") or {}
    if not basket_cfg.get("enabled"):
        return
    take_profit_pct = float(basket_cfg.get("take_profit_pct", 0.10))
    cooldown_minutes = int(basket_cfg.get("cooldown_minutes", 60))
    exit_cfg = (config.get("exit_criteria", {}) or {}).get("daily", {}) or {}
    stop_loss_pct = float(exit_cfg.get("stop_loss_pct", 0.35))
    panic_pct = float(exit_cfg.get("panic_stop_loss_pct", 0.60))

    asset_markets = [m for m in markets if str(m.get("asset", "")).lower() == str(asset).lower()]
    if not asset_markets:
        return

    active_tickers = [m.get("ticker") for m in asset_markets if m.get("ticker")]
    if mode == "OBSERVE":
        prune_stale_paper_positions(db_path, "daily", active_tickers)

    ticker_to_market = {m["ticker"]: m for m in asset_markets if m.get("ticker")}
    ticker_to_strike = {m["ticker"]: float(m.get("strike") or 0) for m in asset_markets if m.get("ticker")}

    is_late = any(0 < m.get("minutes_to_close", 999) <= late_window_minutes for m in asset_markets)
    basket_signals = generate_basket_signals(asset_markets, spot_price, thresholds, is_late, kalshi_client)

    by_window = {}
    for m in asset_markets:
        wid = m.get("window_id") or m.get("event_ticker") or ""
        if wid not in by_window:
            by_window[wid] = []
        by_window[wid].append(m)

    kalshi_positions_by_ticker = {}
    if mode == "TRADE" and kalshi_client:
        try:
            positions_resp = kalshi_client.get_positions(limit=200)
            positions_list = positions_resp.get("market_positions") or positions_resp.get("positions") or []
            from bot.exit_criteria import _normalize_position
            for p in positions_list:
                norm = _normalize_position(p)
                if norm and norm.get("ticker"):
                    kalshi_positions_by_ticker[norm["ticker"]] = {
                        "ticker": norm["ticker"],
                        "side": norm.get("side", "").lower(),
                        "average_price": (norm.get("entry_price_cents") or 0) / 100.0,
                        "count": norm.get("count", 1),
                    }
        except Exception:
            pass

    for wid, wmarkets in by_window.items():
        wtickers = [m["ticker"] for m in wmarkets if m.get("ticker")]
        paper_all = []
        if mode == "OBSERVE":
            for t in wtickers:
                paper_all.extend(get_paper_positions(db_path, "daily", t))
        else:
            for t in wtickers:
                kp = kalshi_positions_by_ticker.get(t)
                if kp and int(kp.get("count", 0)) > 0:
                    paper_all.append(kp)
        yes_pos = next((p for p in paper_all if str(p.get("side", "")).lower() == "yes"), None)
        no_pos = next((p for p in paper_all if str(p.get("side", "")).lower() == "no"), None)
        has_basket = yes_pos and no_pos

        if has_basket and kalshi_client:
            run_exit_yes, _, _ = _exit_check_due("daily", yes_pos["ticker"], config, db_path)
            run_exit_no, _, _ = _exit_check_due("daily", no_pos["ticker"], config, db_path)
            if run_exit_yes or run_exit_no:
                try:
                    top_yes = kalshi_client.get_top_of_book(yes_pos["ticker"])
                    top_no = kalshi_client.get_top_of_book(no_pos["ticker"])
                except Exception:
                    continue
                yes_bid = int(top_yes.get("yes_bid") or 0) if top_yes.get("yes_bid") is not None else 0
                no_bid = int(top_no.get("no_bid") or 0) if top_no.get("no_bid") is not None else 0
                yes_entry = int(yes_pos.get("average_price", 0) * 100)
                no_entry = int(no_pos.get("average_price", 0) * 100)
                yes_pnl = (yes_bid / 100.0 - yes_pos.get("average_price", 0)) / (yes_pos.get("average_price", 0.01) or 0.01) if yes_pos.get("average_price") else 0
                no_pnl = (no_bid / 100.0 - no_pos.get("average_price", 0)) / (no_pos.get("average_price", 0.01) or 0.01) if no_pos.get("average_price") else 0
                action, sell_both = evaluate_basket_exit(
                    yes_pos["ticker"], no_pos["ticker"],
                    yes_entry, no_entry, yes_bid, no_bid,
                    take_profit_pct, stop_loss_pct, yes_pnl, no_pnl, panic_pct,
                )
                if action == "BASKET_TAKE_PROFIT" and sell_both:
                    if mode == "TRADE" and kalshi_client:
                        from bot.exit_criteria import _place_sell
                        _place_sell(kalshi_client, {"ticker": yes_pos["ticker"], "side": "yes", "count": yes_pos.get("count", 1)}, "basket_take_profit")
                        _place_sell(kalshi_client, {"ticker": no_pos["ticker"], "side": "no", "count": no_pos.get("count", 1)}, "basket_take_profit")
                    else:
                        remove_paper_position(db_path, yes_pos["ticker"])
                        remove_paper_position(db_path, no_pos["ticker"])
                    set_basket_cooldown(db_path, wid)
                    basket_exit_value = yes_bid + no_bid
                    net_basket_pnl = basket_exit_value - (yes_entry + no_entry)
                    exit_ev = build_exit_analysis_event(
                        interval="daily", window_id=wid, asset=asset,
                        action="BASKET_TAKE_PROFIT", ticker=yes_pos["ticker"], side="yes",
                        pnl_pct=yes_pnl, seconds_to_close=0, spot=spot_price, strike=None, distance=None,
                        entry_price_cents=yes_entry, exit_price_cents=yes_bid,
                        mfe_pct=None, mae_pct=None, mfe_ts=None, mae_ts=None,
                        exit_eval={"exit_reason": "BASKET_TAKE_PROFIT", "basket_exit_value_cents": basket_exit_value,
                                   "net_basket_pnl_cents": net_basket_pnl, "combined_spread_at_exit": None},
                    )
                    logger.info(json.dumps(exit_ev, default=str))
                    exit_ev2 = build_exit_analysis_event(
                        interval="daily", window_id=wid, asset=asset,
                        action="BASKET_TAKE_PROFIT", ticker=no_pos["ticker"], side="no",
                        pnl_pct=no_pnl, seconds_to_close=0, spot=spot_price, strike=None, distance=None,
                        entry_price_cents=no_entry, exit_price_cents=no_bid,
                        mfe_pct=None, mae_pct=None, mfe_ts=None, mae_ts=None,
                        exit_eval={"exit_reason": "BASKET_TAKE_PROFIT", "basket_exit_value_cents": basket_exit_value,
                                   "net_basket_pnl_cents": net_basket_pnl, "combined_spread_at_exit": None},
                    )
                    logger.info(json.dumps(exit_ev2, default=str))
                elif action in ("YES_LEG_STOPLOSS", "NO_LEG_STOPLOSS") and sell_both:
                    if mode == "TRADE" and kalshi_client:
                        from bot.exit_criteria import _place_sell
                        _place_sell(kalshi_client, {"ticker": yes_pos["ticker"], "side": "yes", "count": yes_pos.get("count", 1)}, "leg_stoploss")
                        _place_sell(kalshi_client, {"ticker": no_pos["ticker"], "side": "no", "count": no_pos.get("count", 1)}, "leg_stoploss")
                    else:
                        remove_paper_position(db_path, yes_pos["ticker"])
                        remove_paper_position(db_path, no_pos["ticker"])
                elif action in ("YES_LEG_STOPLOSS", "NO_LEG_STOPLOSS") and not sell_both:
                    if mode == "TRADE" and kalshi_client:
                        from bot.exit_criteria import _place_sell
                        pos_to_sell = yes_pos if "YES" in action else no_pos
                        _place_sell(kalshi_client, {"ticker": pos_to_sell["ticker"], "side": pos_to_sell.get("side", "yes" if "YES" in action else "no"), "count": pos_to_sell.get("count", 1)}, "leg_stoploss")
                    else:
                        if "YES" in action:
                            remove_paper_position(db_path, yes_pos["ticker"])
                        else:
                            remove_paper_position(db_path, no_pos["ticker"])

        if has_basket:
            continue

        cooldown_until = get_basket_cooldown_until(db_path, wid, cooldown_minutes)
        if cooldown_until is not None:
            ev = build_analysis_event(
                {"event": "SKIP", "interval": "daily", "window_id": wid, "asset": asset,
                 "reason_code": "SKIP_BASKET_COOLDOWN", "cooldown_skipped": True},
                minutes_to_close=min(m.get("minutes_to_close", 999) for m in wmarkets),
            )
            logger.info(json.dumps(ev, default=str))
            continue

        basket = next((b for b in basket_signals if b.window_id == wid and b.asset == asset), None)
        if not basket:
            continue

        if kalshi_client:
            try:
                top_yes = kalshi_client.get_top_of_book(basket.yes_ticker)
                top_no = kalshi_client.get_top_of_book(basket.no_ticker)
                basket = BasketSignal(
                    window_id=basket.window_id, asset=basket.asset,
                    yes_ticker=basket.yes_ticker, no_ticker=basket.no_ticker,
                    yes_strike=basket.yes_strike, no_strike=basket.no_strike,
                    yes_price=basket.yes_price, no_price=basket.no_price,
                    yes_bid=int(top_yes.get("yes_bid") or 0) if top_yes.get("yes_bid") is not None else basket.yes_bid,
                    no_bid=int(top_no.get("no_bid") or 0) if top_no.get("no_bid") is not None else basket.no_bid,
                    minutes_to_close=basket.minutes_to_close, spot_price=basket.spot_price,
                    yes_distance=basket.yes_distance, no_distance=basket.no_distance,
                    combined_spread_at_entry=basket.combined_spread_at_entry,
                )
            except Exception:
                continue

        seconds_to_close = basket.minutes_to_close * 60.0
        allowed, reason = gate_allow_basket_entry(basket, seconds_to_close, rg)
        if not allowed:
            ev = build_analysis_event(
                {"event": "SKIP", "interval": "daily", "window_id": wid, "asset": asset,
                 "reason_code": reason, "ticker": basket.yes_ticker},
                minutes_to_close=basket.minutes_to_close,
            )
            logger.info(json.dumps(ev, default=str))
            continue

        basket_entry_cost = basket.yes_price + basket.no_price
        spread_at_entry = basket.combined_spread_at_entry
        ev = build_analysis_event(
            {"event": "ENTER_DECISION", "interval": "daily", "window_id": wid, "asset": asset,
             "ticker": basket.yes_ticker, "side": "yes", "yes_price": basket.yes_price,
             "reason_code": "BASKET_ENTRY",
             "basket_entry_cost_cents": basket_entry_cost,
             "yes_strike": basket.yes_strike, "no_strike": basket.no_strike,
             "yes_entry_price": basket.yes_price, "no_entry_price": basket.no_price,
             "yes_distance": basket.yes_distance, "no_distance": basket.no_distance,
             "combined_spread_at_entry": spread_at_entry},
            minutes_to_close=basket.minutes_to_close,
        )
        logger.info(json.dumps(ev, default=str))
        ev2 = build_analysis_event(
            {"event": "ENTER_DECISION", "interval": "daily", "window_id": wid, "asset": asset,
             "ticker": basket.no_ticker, "side": "no", "no_price": basket.no_price,
             "reason_code": "BASKET_ENTRY",
             "basket_entry_cost_cents": basket_entry_cost,
             "yes_strike": basket.yes_strike, "no_strike": basket.no_strike,
             "yes_entry_price": basket.yes_price, "no_entry_price": basket.no_price,
             "yes_distance": basket.yes_distance, "no_distance": basket.no_distance,
             "combined_spread_at_entry": spread_at_entry},
            minutes_to_close=basket.minutes_to_close,
        )
        logger.info(json.dumps(ev2, default=str))
        if mode == "TRADE" and kalshi_client:
            sig_yes = Signal(ticker=basket.yes_ticker, side="yes", price=basket.yes_price, reason="BASKET_ENTRY", late_window=is_late)
            sig_no = Signal(ticker=basket.no_ticker, side="no", price=basket.no_price, reason="BASKET_ENTRY", late_window=is_late)
            exec_results = execute_signals(
                [sig_yes, sig_no],
                hour_market_id=wid,
                db_path=db_path,
                kalshi_client=kalshi_client,
                config=config,
                mode="TRADE",
                interval="daily",
                asset=asset,
            )
            _log_order_events(logger, exec_results, interval="daily", window_id=wid, asset=asset)
        else:
            add_paper_position(db_path, basket.yes_ticker, "yes", basket.yes_price, "daily", asset)
            add_paper_position(db_path, basket.no_ticker, "no", basket.no_price, "daily", asset)


def _run_interval_daily_weekly(
    interval_key: str,
    asset: str,
    config: dict,
    db_path: str,
    logger,
    kalshi_client,
    markets: list,
    late_window_minutes: float,
    thresholds: dict,
    rg: dict,
) -> None:
    """
    Evaluate daily or weekly markets (OBSERVE only).
    - Paper ledger: records synthetic positions on ENTER_DECISION.
    - Exit evaluation: runs stop-loss/take-profit on paper positions using current prices.
    - Logs EVAL/ENTER_DECISION/SKIP and EXIT (paper) for analysis and fine-tuning.
    """
    from bot.market import TickerQuote

    try:
        spot_price = _get_spot_price(asset)
    except Exception:
        spot_price = None

    active_tickers = [m.get("ticker") for m in markets if m.get("ticker") and str(m.get("asset", "")).lower() == str(asset).lower()]
    prune_stale_paper_positions(db_path, interval_key, active_tickers)

    exit_cfg = (config.get("exit_criteria", {}) or {}).get(interval_key, {}) or {}
    sl_persistence_polls = int(exit_cfg.get("stop_loss_persistence_polls", 1))
    hard_flip_cfg = rg.get("hard_flip_exit") if isinstance(rg.get("hard_flip_exit"), dict) else None

    for m in markets:
        if str(m.get("asset", "")).lower() != str(asset).lower():
            continue
        ticker = m.get("ticker")
        if not ticker:
            continue
        minutes_to_close = float(m.get("minutes_to_close", 999))
        seconds_to_close = minutes_to_close * 60.0
        strike = m.get("strike") or 0
        ticker_to_strike = {ticker: float(strike)} if strike else {}
        yes_ask = m.get("yes_ask")
        no_ask = m.get("no_ask")
        if kalshi_client:
            try:
                top = kalshi_client.get_top_of_book(ticker)
                yes_ask = int(top["yes_ask"]) if top.get("yes_ask") is not None else yes_ask
                no_ask = int(top["no_ask"]) if top.get("no_ask") is not None else no_ask
            except Exception:
                ev = build_analysis_event(
                    {"event": "SKIP", "interval": interval_key, "window_id": ticker, "asset": asset,
                     "ticker": ticker, "reason_code": "ERR_TOP_OF_BOOK_MISSING"},
                    minutes_to_close=minutes_to_close,
                )
                logger.info(json.dumps(ev, default=str))
                continue

        run_exit_now, stop_override, profit_override = _exit_check_due(interval_key, ticker, config, db_path)
        if run_exit_now and kalshi_client:
            paper_positions = get_paper_positions(db_path, interval_key, ticker)
            if paper_positions:
                stop_pct = stop_override if stop_override is not None else exit_cfg.get("stop_loss_pct", 0.30)
                profit_pct = profit_override
                results = evaluate_positions(
                    positions=paper_positions,
                    client=kalshi_client,
                    stop_loss_pct=stop_pct,
                    profit_target_pct=profit_pct,
                    mode="OBSERVE",
                    hour_market_id=ticker,
                    spot_price=spot_price,
                    ticker_to_strike=ticker_to_strike,
                    seconds_to_close=seconds_to_close,
                    hard_flip_cfg=hard_flip_cfg,
                    sl_persistence_polls=sl_persistence_polls,
                )
                for r in results:
                    pos = r.position
                    ticker_pos = pos.get("ticker")
                    strike_val = ticker_to_strike.get(ticker_pos) if ticker_pos else None
                    dist = None
                    if spot_price is not None and strike_val is not None and strike_val > 0:
                        s = str(pos.get("side") or "").lower()
                        dist = spot_price - strike_val if s == "yes" else (strike_val - spot_price if s == "no" else None)
                    exit_eval = r.exit_criteria_evaluated or {}
                    exit_ev = build_exit_analysis_event(
                        interval=interval_key,
                        window_id=ticker,
                        asset=asset,
                        action=r.action,
                        ticker=ticker_pos,
                        side=pos.get("side"),
                        pnl_pct=r.pnl_pct,
                        seconds_to_close=seconds_to_close,
                        spot=spot_price,
                        strike=strike_val,
                        distance=dist,
                        entry_price_cents=pos.get("entry_price_cents"),
                        exit_price_cents=exit_eval.get("mark_cents"),
                        mfe_pct=exit_eval.get("mfe_pct"),
                        mae_pct=exit_eval.get("mae_pct"),
                        mfe_ts=exit_eval.get("mfe_ts"),
                        mae_ts=exit_eval.get("mae_ts"),
                        exit_eval=exit_eval,
                    )
                    logger.info(json.dumps(exit_ev, default=str))
                    if r.action in ("STOP_LOSS", "TAKE_PROFIT", "HARD_FLIP"):
                        remove_paper_position(db_path, ticker_pos)
                    elif r.action == "SL_WARNING":
                        logger.info(
                            "[%s %s] Paper SL_WARNING %s %s PnL=%.2f%% (poll awaiting persistence)",
                            asset.upper(), interval_key, ticker_pos, pos.get("side"),
                            (r.pnl_pct or 0) * 100,
                        )

        quote = TickerQuote(
            ticker=ticker,
            strike=strike,
            yes_ask=int(yes_ask) if yes_ask is not None else None,
            no_ask=int(no_ask) if no_ask is not None else None,
            yes_bid=None, no_bid=None,
            subtitle=m.get("subtitle", ""),
        )
        is_late = 0 < minutes_to_close <= late_window_minutes
        signals = generate_signals_farthest([quote], spot_price, is_late, thresholds)
        if not signals:
            continue
        sig = signals[0]
        entry_band = f"{94}-{99}"
        band = thresholds.get("late") if is_late else thresholds.get("normal")
        if band:
            entry_band = f"{band.get('yes_min', 94)}-{band.get('yes_max', 99)}"
        if rg.get("enabled"):
            allowed_signals, guard_eval_logs = apply_guards_filter(
                signals, interval_key, ticker, asset, spot_price,
                seconds_to_close, ticker_to_strike, rg, is_hourly=False,
                yes_price_all=quote.yes_ask, no_price_all=quote.no_ask,
                entry_band=entry_band,
            )
            for g in guard_eval_logs:
                cap_state = _cap_state_for_window(config, db_path, ticker, interval_key, g.get("ticker"), g.get("side"))
                ev = build_analysis_event(g, cap_state=cap_state, minutes_to_close=minutes_to_close)
                logger.info(json.dumps(ev, default=str))
                if g.get("event") == "ENTER_DECISION":
                    t = g.get("ticker") or ticker
                    if not get_paper_positions(db_path, interval_key, t):
                        side = str(g.get("side") or "").lower()
                        entry_cents = g.get("yes_price") if side == "yes" else g.get("no_price")
                        if entry_cents is not None and side in ("yes", "no"):
                            add_paper_position(db_path, t, side, int(entry_cents), interval_key, asset)
        else:
            allowed_signals = signals
            if allowed_signals and not get_paper_positions(db_path, interval_key, ticker):
                sig = allowed_signals[0]
                side = str(getattr(sig, "side", "")).lower()
                price = getattr(sig, "price", None)
                if side in ("yes", "no") and price is not None:
                    add_paper_position(db_path, ticker, side, int(price), interval_key, asset)


def run_bot_daily_for_asset(asset: str, config: dict, db_path: str, logger, kalshi_client) -> None:
    """Run daily bot for one asset (OBSERVE only). Discovers markets, evaluates, logs; no orders."""
    daily_cfg = config.get("daily", {}) or {}
    sched = daily_cfg.get("schedule", {}) or {}
    lookahead_hours = sched.get("lookahead_hours", 48)
    late_window_minutes = sched.get("late_window_minutes", 180)
    thresholds = daily_cfg.get("thresholds", {}) or {}
    rg = daily_cfg.get("risk_guards", {}) or {}
    assets = [asset]
    spot_price = None
    try:
        spot_price = _get_spot_price(asset)
    except Exception:
        pass
    spot_price_by_asset = {asset: spot_price} if spot_price is not None else None
    spot_window_by_asset = {}
    for a in assets:
        spot_window_by_asset[str(a).lower()] = _get_spot_window(config, a, "daily")
    markets = discover_daily_markets(
        assets,
        lookahead_hours=lookahead_hours,
        spot_price_by_asset=spot_price_by_asset,
        spot_window_by_asset=spot_window_by_asset,
    )
    if not markets:
        logger.info("[%s daily] No markets found closing in next %.0fh", asset.upper(), lookahead_hours)
        return
    intervals = config.get("intervals", {}) or {}
    basket_cfg = daily_cfg.get("basket_scalping") or {}
    mode = (intervals.get("daily", {}).get("mode") or config.get("mode", "OBSERVE")).upper()
    if mode not in ("OBSERVE", "TRADE"):
        mode = "OBSERVE"
    if basket_cfg.get("enabled"):
        _run_interval_daily_basket(
            asset, config, db_path, logger, kalshi_client,
            markets, late_window_minutes, thresholds, rg, mode=mode,
        )
    else:
        _run_interval_daily_weekly(
            "daily", asset, config, db_path, logger, kalshi_client,
            markets, late_window_minutes, thresholds, rg,
        )
    logger.info("[%s daily] Evaluated %d markets (%s)", asset.upper(), len([m for m in markets if str(m.get("asset")).lower() == str(asset).lower()]), mode)


def run_bot_weekly_for_asset(asset: str, config: dict, db_path: str, logger, kalshi_client) -> None:
    """Run weekly bot for one asset (OBSERVE only). Discovers markets, evaluates, logs; no orders."""
    weekly_cfg = config.get("weekly", {}) or {}
    sched = weekly_cfg.get("schedule", {}) or {}
    lookahead_days = sched.get("lookahead_days", 14)
    late_window_minutes = sched.get("late_window_minutes", 1440)
    thresholds = weekly_cfg.get("thresholds", {}) or {}
    rg = weekly_cfg.get("risk_guards", {}) or {}
    assets = [asset]
    spot_price = None
    try:
        spot_price = _get_spot_price(asset)
    except Exception:
        pass
    spot_price_by_asset = {asset: spot_price} if spot_price is not None else None
    spot_window_by_asset = {str(a).lower(): _get_spot_window(config, a, "weekly") for a in assets}
    markets = discover_weekly_markets(
        assets,
        lookahead_days=lookahead_days,
        spot_price_by_asset=spot_price_by_asset,
        spot_window_by_asset=spot_window_by_asset,
    )
    if not markets:
        logger.info("[%s weekly] No markets found closing in next %.0f days", asset.upper(), lookahead_days)
        return
    _run_interval_daily_weekly(
        "weekly", asset, config, db_path, logger, kalshi_client,
        markets, late_window_minutes, thresholds, rg,
    )
    logger.info("[%s weekly] Evaluated %d markets (OBSERVE)", asset.upper(), len([m for m in markets if str(m.get("asset")).lower() == str(asset).lower()]))


def run_bot(config: dict) -> None:
    schedule = config.get("schedule", {})
    start_offset = schedule.get("start_offset_minutes", 1)
    state_cfg = config.get("state", {})
    db_path = state_cfg.get("db_path", "data/bot_state.db")
    log_cfg = config.get("logging", {})
    log_file = log_cfg.get("file", "logs/bot.log")
    log_level = log_cfg.get("level", "INFO")

    ensure_state_db(db_path)
    logger = setup_logging(log_file, log_level)

    run_mode = config.get("run_mode", "hourly")
    intervals = config.get("intervals", {}) or {}
    run_hourly = run_mode in ("hourly", "both") and intervals.get("hourly", {}).get("enabled", True)
    run_15min = run_mode in ("fifteen_min", "both") and intervals.get("fifteen_min", {}).get("enabled", True)

    if run_hourly and not should_run(start_offset):
        logger.info("Skipping hourly run: before start_offset (H+1 min)")
        if not run_15min:
            return

    kalshi_client = None
    try:
        kalshi_client = KalshiClient()
    except Exception:
        pass

    if run_hourly:
        hourly_assets = intervals.get("hourly", {}).get("assets") or config.get("assets", ["btc"])
        for asset in hourly_assets:
            if str(asset).lower() not in ("btc", "eth", "sol", "xrp"):
                continue
            try:
                run_bot_for_asset(asset, config, db_path, logger, kalshi_client)
            except Exception as e:
                logger.exception("[%s] Hourly run error: %s", str(asset).upper(), e)

    if run_15min:
        fm_assets = intervals.get("fifteen_min", {}).get("assets") or config.get("assets", ["btc"])
        for asset in fm_assets:
            if str(asset).lower() not in ("btc", "eth", "sol", "xrp"):
                continue
            try:
                run_bot_15min_for_asset(asset, config, db_path, logger, kalshi_client)
            except Exception as e:
                logger.exception("[%s] 15-min run error: %s", str(asset).upper(), e)

    run_daily = intervals.get("daily", {}).get("enabled", False)
    run_weekly = intervals.get("weekly", {}).get("enabled", False)
    if run_daily:
        set_schedule_last_run(db_path, "daily", time.time())
        daily_assets = intervals.get("daily", {}).get("assets") or config.get("assets", ["btc"])
        for asset in daily_assets:
            if str(asset).lower() not in ("btc", "eth", "sol", "xrp"):
                continue
            try:
                run_bot_daily_for_asset(asset, config, db_path, logger, kalshi_client)
            except Exception as e:
                logger.exception("[%s] Daily run error: %s", str(asset).upper(), e)
    if run_weekly:
        set_schedule_last_run(db_path, "weekly", time.time())
        weekly_assets = intervals.get("weekly", {}).get("assets") or config.get("assets", ["btc"])
        for asset in weekly_assets:
            if str(asset).lower() not in ("btc", "eth", "sol", "xrp"):
                continue
            try:
                run_bot_weekly_for_asset(asset, config, db_path, logger, kalshi_client)
            except Exception as e:
                logger.exception("[%s] Weekly run error: %s", str(asset).upper(), e)


def main():
    parser = argparse.ArgumentParser(description="Hades Bot - BTC & ETH Hourly Markets")
    parser.add_argument("--config", "-c", default="config/config.yaml", help="Config file or dir (config/config.yaml for split)")
    parser.add_argument("--once", action="store_true", help="Run once and exit (no loop)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    os.chdir(project_root)
    load_dotenv(project_root / ".env")

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = project_root / config_path
    if not config_path.exists():
        fallback = project_root / "config.yaml" if args.config in ("config/config.yaml", "config/config.yml") else None
        if fallback and fallback.exists():
            config_path = fallback
        else:
            print(f"Config not found: {config_path}")
            sys.exit(1)

    config = load_config(str(config_path))
    # Setup console log tee (append mode) - captures all print() output
    log_cfg = config.get("logging", {}) or {}
    from bot.logging import setup_console_log
    setup_console_log(log_cfg.get("console_log_file"), project_root=project_root)

    run_mode = config.get("run_mode", "hourly")
    intervals = config.get("intervals", {}) or {}
    h_mode = (intervals.get("hourly", {}).get("mode") or config.get("mode", "OBSERVE")).upper()
    f_mode = (intervals.get("fifteen_min", {}).get("mode") or config.get("mode", "OBSERVE")).upper()
    d_enabled = intervals.get("daily", {}).get("enabled", False)
    w_enabled = intervals.get("weekly", {}).get("enabled", False)
    d_mode = (intervals.get("daily", {}).get("mode") or config.get("mode", "OBSERVE")).upper()
    w_mode = (intervals.get("weekly", {}).get("mode") or config.get("mode", "OBSERVE")).upper()
    extras = []
    if d_enabled:
        extras.append("daily=%s" % d_mode)
    if w_enabled:
        extras.append("weekly=%s" % w_mode)
    extra_str = " | " + " | ".join(extras) if extras else ""
    print(f"Run: {run_mode} | Hourly: {h_mode} | 15min: {f_mode}{extra_str} | Assets: {', '.join(config['assets'])}")

    if args.once:
        run_bot(config)
    else:
        schedule = config.get("schedule", {})
        interval = schedule.get("interval_minutes", 3)  # sleep (min) when market closed; only used as fallback
        run_mode = config.get("run_mode", "hourly")
        intervals = config.get("intervals", {}) or {}
        run_hourly = run_mode in ("hourly", "both") and intervals.get("hourly", {}).get("enabled", True)
        run_15min = run_mode in ("fifteen_min", "both") and intervals.get("fifteen_min", {}).get("enabled", True)
        hourly_assets = intervals.get("hourly", {}).get("assets") or config.get("assets", ["btc"])
        fm_assets = intervals.get("fifteen_min", {}).get("assets") or config.get("assets", ["btc"])
        fm_cfg = config.get("fifteen_min", {}) or {}
        late_window_secs = fm_cfg.get("late_window_seconds", 140)
        late_window_interval_secs = fm_cfg.get("late_window_interval_seconds", 15)

        # Hourly: late window and run interval (seconds preferred over minutes)
        late_window_min = schedule.get("late_window_minutes", 10)
        late_interval_sec = schedule.get("late_interval_seconds")  # optional: interval in seconds
        late_interval_min = schedule.get("late_interval_minutes", 1)  # fallback: interval in minutes

        def get_schedule_state_15min():
            return get_15min_schedule_state(
                assets=fm_assets,
                late_window_seconds=late_window_secs,
                late_window_interval_seconds=late_window_interval_secs,
                fallback_interval_minutes=interval,
            )

        def get_schedule_state_hourly():
            return get_hourly_schedule_state(
                assets=hourly_assets,
                late_window_minutes=late_window_min,
                late_interval_minutes=late_interval_min,
                late_interval_seconds=late_interval_sec,
                fallback_interval_minutes=interval,
            )

        run_daily = intervals.get("daily", {}).get("enabled", False)
        run_weekly = intervals.get("weekly", {}).get("enabled", False)
        daily_cfg = config.get("daily", {}) or {}
        weekly_cfg = config.get("weekly", {}) or {}
        daily_sched = daily_cfg.get("schedule", {}) or {}
        daily_normal_min = daily_sched.get("interval_minutes", 30) or 30
        daily_late_min = daily_sched.get("late_window_minutes", 240) or 240
        daily_late_interval = daily_sched.get("late_interval_minutes", 5) or 5
        daily_lookahead = daily_sched.get("lookahead_hours", 24) or 24
        daily_assets = intervals.get("daily", {}).get("assets") or config.get("assets", ["btc"])
        weekly_interval_sec = (weekly_cfg.get("schedule", {}).get("interval_minutes", 360) or 360) * 60
        db_path = config.get("state", {}).get("db_path", "data/bot_state.db")

        def _daily_interval_sec():
            """Two-speed: 5 min in late window, 30 min otherwise."""
            _, sleep = get_daily_schedule_state(
                assets=daily_assets,
                late_window_minutes=daily_late_min,
                late_interval_minutes=daily_late_interval,
                normal_interval_minutes=daily_normal_min,
                lookahead_hours=daily_lookahead,
            )
            return sleep

        def get_combined_schedule_state():
            """When both enabled: run if hourly, 15min, daily, or weekly wants to; sleep = min of all."""
            h_run, h_sleep = get_schedule_state_hourly()
            c_run, c_sleep = get_schedule_state_15min()
            now = time.time()
            last_daily = get_schedule_last_run(db_path, "daily")
            last_weekly = get_schedule_last_run(db_path, "weekly")
            daily_interval_sec = _daily_interval_sec() if run_daily else 3600
            daily_run = run_daily and (
                last_daily is None or (now - last_daily) >= daily_interval_sec
            )
            weekly_run = run_weekly and (
                last_weekly is None or (now - last_weekly) >= weekly_interval_sec
            )
            should_run = h_run or c_run or daily_run or weekly_run
            sleep = min(h_sleep, c_sleep)
            if run_daily and not daily_run and last_daily is not None:
                sec_until_daily = max(1, daily_interval_sec - (now - last_daily))
                sleep = min(sleep, sec_until_daily)
            if run_weekly and not weekly_run and last_weekly is not None:
                sec_until_weekly = max(1, weekly_interval_sec - (now - last_weekly))
                sleep = min(sleep, sec_until_weekly)
            if should_run:
                sleep = min(sleep, 60.0)  # cap wake interval when we will run
            return (should_run, sleep)

        def get_sleep_seconds() -> float:
            if run_hourly and run_15min:
                _, s = get_combined_schedule_state()
                return s
            if run_15min:
                _, s = get_schedule_state_15min()
                return s
            if run_hourly:
                _, s = get_schedule_state_hourly()
                return s
            return interval * 60.0

        if run_mode == "fifteen_min":
            use_schedule_state = True
            get_schedule_state = get_combined_schedule_state if (run_daily or run_weekly) else get_schedule_state_15min
        elif run_mode == "hourly":
            use_schedule_state = True
            get_schedule_state = get_combined_schedule_state if (run_daily or run_weekly) else get_schedule_state_hourly
        elif run_mode == "both":
            use_schedule_state = True
            get_schedule_state = get_combined_schedule_state
        else:
            use_schedule_state = run_daily or run_weekly
            get_schedule_state = get_combined_schedule_state if use_schedule_state else None

        run_loop(
            lambda: run_bot(config),
            interval_minutes=interval,
            start_offset_minutes=0 if use_schedule_state else schedule.get("start_offset_minutes", 1),
            get_sleep_seconds=get_sleep_seconds if (run_15min or run_hourly) and not use_schedule_state else None,
            get_schedule_state=get_schedule_state if use_schedule_state else None,
        )


if __name__ == "__main__":
    main()
