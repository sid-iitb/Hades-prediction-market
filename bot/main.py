#!/usr/bin/env python3
"""
Bot entrypoint - Phase 1: Verification Mode.
Default: OBSERVE (no trading). Use MODE=TRADE to enable orders.
Supports BTC and ETH hourly markets.

Usage:
  python -m bot.main --config config.yaml
"""
import argparse
import os
import sys
from pathlib import Path

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml
from dotenv import load_dotenv

from src.client.kalshi_client import KalshiClient
from src.client.kraken_client import KrakenClient

from bot.execution import execute_signals
from bot.exit_criteria import run_exit_criteria
from bot.hour_summary import check_and_emit_hour_summary
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
from bot.outcomes_15min import get_stored_outcome, save_outcome
from bot.scheduler import run_loop, should_run, sleep_until_next_interval
from bot.state import ensure_state_db, set_last_run_hour
from bot.strategy import generate_signals_farthest
from bot.strategy_15min import generate_signals_15min, get_no_signal_reason


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        cfg = yaml.safe_load(f) or {}
    mode = os.getenv("MODE", cfg.get("mode", "OBSERVE")).upper()
    if mode not in ("OBSERVE", "TRADE"):
        mode = "OBSERVE"
    cfg["mode"] = mode
    assets = cfg.get("assets", ["btc"])
    if isinstance(assets, str):
        assets = [assets]
    SUPPORTED_ASSETS = ("btc", "eth", "sol", "xrp")
    cfg["assets"] = [str(a).lower() for a in assets if str(a).lower() in SUPPORTED_ASSETS]
    if not cfg["assets"]:
        cfg["assets"] = ["btc"]
    run_mode = (cfg.get("run_mode") or "hourly").lower()
    if run_mode not in ("hourly", "fifteen_min", "both"):
        run_mode = "hourly"
    cfg["run_mode"] = run_mode
    intervals = cfg.get("intervals", {}) or {}
    if "hourly" not in intervals:
        intervals["hourly"] = {"enabled": True, "assets": cfg["assets"]}
    if "fifteen_min" not in intervals:
        intervals["fifteen_min"] = {"enabled": True, "assets": cfg["assets"]}
    cfg["intervals"] = intervals
    return cfg


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


def _get_spot_window(config: dict, asset: str) -> int:
    """Get strike window for asset."""
    by_asset = config.get("spot_window_by_asset") or {}
    return by_asset.get(asset) or config.get("spot_window", 1500)


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

    # Exit criteria
    exit_out = run_exit_criteria(
        client=kalshi_client,
        hour_market_id=hour_market_id,
        config=config,
        mode=mode,
    )
    for r in exit_out.get("exit_results", []):
        if r.get("action") in ("STOP_LOSS", "TAKE_PROFIT"):
            logger.info(
                "[%s] Exit criteria: %s %s %s PnL=%.2f%%",
                asset.upper(), r["action"],
                r.get("position", {}).get("ticker"),
                r.get("position", {}).get("side"),
                (r.get("pnl_pct") or 0) * 100,
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

    signals = generate_signals_farthest(quotes, spot_price, ctx.is_late_window, thresholds)
    execution_results = execute_signals(
        signals, hour_market_id, db_path, kalshi_client, config, mode
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

    # Exit criteria (stop-loss evaluation before new signals)
    fm_cfg = config.get("fifteen_min", {})
    stop_loss_15m = fm_cfg.get("stop_loss_pct", 0.30)
    exit_out = run_exit_criteria(
        client=kalshi_client,
        hour_market_id=market_id,
        config=config,
        mode=mode,
        stop_loss_pct_override=stop_loss_15m,
        profit_target_pct_override=None,  # 15-min: stop-loss only
    )
    for r in exit_out.get("exit_results", []):
        if r.get("action") in ("STOP_LOSS", "TAKE_PROFIT"):
            logger.info(
                "[%s 15m] Exit criteria: %s %s %s PnL=%.2f%%",
                asset.upper(), r["action"],
                r.get("position", {}).get("ticker"),
                r.get("position", {}).get("side"),
                (r.get("pnl_pct") or 0) * 100,
            )

    quote = fetch_15min_quote(market_id, client=kalshi_client)
    if quote is None:
        logger.info("[%s 15m] No market data for %s", asset.upper(), market_id)
        return

    signals = generate_signals_15min(quote, minutes_to_close, config)
    execution_results = execute_signals(
        signals, market_id, db_path, kalshi_client, config, mode, interval="15min"
    )
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


def main():
    parser = argparse.ArgumentParser(description="Hades Bot - BTC & ETH Hourly Markets")
    parser.add_argument("--config", "-c", default="config.yaml", help="Config file path")
    parser.add_argument("--once", action="store_true", help="Run once and exit (no loop)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    os.chdir(project_root)
    load_dotenv(project_root / ".env")

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = project_root / config_path
    if not config_path.exists():
        print(f"Config not found: {config_path}")
        sys.exit(1)

    config = load_config(str(config_path))
    run_mode = config.get("run_mode", "hourly")
    intervals = config.get("intervals", {}) or {}
    h_mode = (intervals.get("hourly", {}).get("mode") or config.get("mode", "OBSERVE")).upper()
    f_mode = (intervals.get("fifteen_min", {}).get("mode") or config.get("mode", "OBSERVE")).upper()
    print(f"Run: {run_mode} | Hourly: {h_mode} | 15min: {f_mode} | Assets: {', '.join(config['assets'])}")

    if args.once:
        run_bot(config)
    else:
        schedule = config.get("schedule", {})
        interval = schedule.get("interval_minutes", 5)
        start_offset = schedule.get("start_offset_minutes", 1)
        run_mode = config.get("run_mode", "hourly")
        intervals = config.get("intervals", {}) or {}
        run_hourly = run_mode in ("hourly", "both") and intervals.get("hourly", {}).get("enabled", True)
        run_15min = run_mode in ("fifteen_min", "both") and intervals.get("fifteen_min", {}).get("enabled", True)
        hourly_assets = intervals.get("hourly", {}).get("assets") or config.get("assets", ["btc"])
        fm_assets = intervals.get("fifteen_min", {}).get("assets") or config.get("assets", ["btc"])
        fm_cfg = config.get("fifteen_min", {}) or {}
        late_window_secs = fm_cfg.get("late_window_seconds", 140)
        late_window_interval_secs = fm_cfg.get("late_window_interval_seconds", 15)

        # Hourly: late window 10 min, run every 1 min (10-5 min) or 30 sec (5-0 min)
        late_window_min = schedule.get("late_window_minutes", 10)
        late_interval_min = schedule.get("late_interval_minutes", 1)
        late_interval_secs_under_5 = schedule.get("late_interval_seconds_under_5", 30)

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
                late_interval_seconds_under_5=late_interval_secs_under_5,
                fallback_interval_minutes=interval,
            )

        def get_combined_schedule_state():
            """When both enabled: run if either wants to; sleep = min of both."""
            h_run, h_sleep = get_schedule_state_hourly()
            c_run, c_sleep = get_schedule_state_15min()
            return (h_run or c_run, min(h_sleep, c_sleep))

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
            get_schedule_state = get_schedule_state_15min
        elif run_mode == "hourly":
            use_schedule_state = True
            get_schedule_state = get_schedule_state_hourly
        elif run_mode == "both":
            use_schedule_state = True
            get_schedule_state = get_combined_schedule_state
        else:
            use_schedule_state = False
            get_schedule_state = None

        run_loop(
            lambda: run_bot(config),
            interval_minutes=interval,
            start_offset_minutes=start_offset,
            get_sleep_seconds=get_sleep_seconds if (run_15min or run_hourly) and not use_schedule_state else None,
            get_schedule_state=get_schedule_state if use_schedule_state else None,
        )


if __name__ == "__main__":
    main()
