#!/usr/bin/env python3
"""
Standalone Data Bus engine: Oracle + Kalshi WebSockets and Director loop.

Runs the data pipeline (spot oracles, Kalshi orderbook WS, and 60s ticker rollover)
in isolation from the REST-based trading bot. Use this when you want the Data Bus
populated for strategies that read from it, without running the full bot.

Usage:
  python scripts/run_data_bus.py

Ctrl+C stops both WebSocket managers and exits cleanly.
"""
import logging
import sys
import time
from pathlib import Path

# Project root on path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from bot import data_bus
from bot.kalshi_ws_manager import start_kalshi_ws, stop_kalshi_ws, subscribe_to_tickers
from bot.oracle_ws_manager import start_ws_oracles, stop_ws_oracles
from bot.market import (
    fetch_15min_market,
    fetch_markets_for_event,
    get_current_15min_market_id,
    get_current_hour_market_id,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ASSETS = ["btc", "eth", "sol", "xrp"]


def _director_tick() -> None:
    """Fetch active 15-min and hourly tickers, subscribe Kalshi WS, cleanup data bus."""
    tickers = set()
    for asset in ASSETS:
        try:
            event_15 = get_current_15min_market_id(asset)
            m = fetch_15min_market(event_15)
            if m and m.get("ticker"):
                tickers.add(m["ticker"])
        except Exception:
            pass
        try:
            event_h = get_current_hour_market_id(asset)
            markets, _ = fetch_markets_for_event(event_h)
            for m in markets or []:
                t = m.get("ticker") if isinstance(m, dict) else None
                if t:
                    tickers.add(t)
        except Exception:
            pass
    list_of_tickers = list(tickers)
    if list_of_tickers:
        subscribe_to_tickers(list_of_tickers)
        logger.info("[Director] Rolled over WebSocket subscriptions: %s", list_of_tickers)
    data_bus.cleanup_old_data()


def main() -> None:
    logger.info("Data Bus engine starting (Oracle + Kalshi WS + Director)...")
    start_ws_oracles()
    start_kalshi_ws()
    logger.info("WebSockets started. Director loop every 60s. Ctrl+C to stop.")

    try:
        while True:
            try:
                _director_tick()
            except Exception as e:
                logger.error("[Director FATAL] Loop crashed: %s", e, exc_info=True)
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt: stopping WebSockets...")
        stop_ws_oracles()
        stop_kalshi_ws()
        logger.info("Data Bus engine stopped.")
        sys.exit(0)


if __name__ == "__main__":
    main()
