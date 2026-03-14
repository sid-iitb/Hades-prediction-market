"""
Bot V2 entry point: unified pipeline with configurable intervals (fifteen_min, hourly).
Loads v2 config, initializes v2_state.db, runs pipeline cycles per interval in separate threads.
DO NOT modify V1 code; this is greenfield.
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import time
from typing import List

from dotenv import load_dotenv

from src.client.kalshi_client import KalshiClient

from bot.pipeline.aggregator import OrderAggregator
from bot.pipeline.data_layer import DataLayer
from bot.pipeline.executor import PipelineExecutor
from bot.pipeline.registry import OrderRegistry, init_v2_db
from bot.pipeline.run_unified import run_pipeline_cycle
from bot.pipeline.strategies.atm_breakout import AtmBreakoutStrategy
from bot.pipeline.strategies.last_90s import Last90sStrategy
from bot.v2_config_loader import load_v2_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _resolve_dry_run(config: dict) -> bool:
    """Resolve dry_run: env V2_DRY_RUN overrides config. Default True (no real orders)."""
    raw = os.environ.get("V2_DRY_RUN", "").strip().lower()
    if raw in ("false", "0", "no"):
        return False
    if raw in ("true", "1", "yes"):
        return True
    return bool(config.get("dry_run", True))


def main() -> None:
    load_dotenv()
    logger.info("Environment variables loaded.")

    config = load_v2_config()
    init_v2_db()

    dry_run = _resolve_dry_run(config)
    kalshi_base = os.environ.get("KALSHI_BASE_URL", "")
    base_display = (kalshi_base or "NOT SET")[:50] + ("..." if len(kalshi_base or "") > 50 else "")

    if dry_run:
        logger.warning(
            "[V2] DRY RUN MODE — NO ORDERS WILL BE PLACED ON KALSHI. Set dry_run: false in config or V2_DRY_RUN=false to trade live."
        )
    else:
        logger.info("[V2] LIVE TRADING — Orders will be sent to Kalshi. Base URL: %s", base_display)

    kalshi_client = KalshiClient()
    registry = OrderRegistry()
    data_layer = DataLayer(kalshi_client=kalshi_client)
    aggregator = OrderAggregator()
    executor = PipelineExecutor(registry, dry_run=dry_run, kalshi_client=kalshi_client)

    strat_last90s = Last90sStrategy(config)
    strat_atm = AtmBreakoutStrategy(config)

    fifteen_min_strats = [strat_last90s, strat_atm]
    hourly_strats: list = []

    def run_interval_loop(interval: str, strategies: list, kalshi_client: KalshiClient) -> None:
        interval_cfg = (config.get(interval) or {}).get("pipeline") or {}
        sleep_secs = float(interval_cfg.get("run_interval_seconds", 1))
        while True:
            try:
                run_pipeline_cycle(
                    interval,
                    config,
                    data_layer,
                    strategies,
                    aggregator,
                    executor,
                    registry,
                    kalshi_client=kalshi_client,
                )
            except Exception as e:
                logger.exception("[%s] Pipeline cycle error: %s", interval, e)
            time.sleep(sleep_secs)

    threads: List[threading.Thread] = []
    intervals_cfg = config.get("intervals") or {}
    if intervals_cfg.get("fifteen_min", {}).get("enabled", False):
        t = threading.Thread(
            target=run_interval_loop,
            args=("fifteen_min", fifteen_min_strats, kalshi_client),
            name="v2_fifteen_min",
            daemon=True,
        )
        t.start()
        threads.append(t)
        logger.info("Started fifteen_min pipeline thread")
    if intervals_cfg.get("hourly", {}).get("enabled", False):
        t = threading.Thread(
            target=run_interval_loop,
            args=("hourly", hourly_strats, kalshi_client),
            name="v2_hourly",
            daemon=True,
        )
        t.start()
        threads.append(t)
        logger.info("Started hourly pipeline thread")

    if not threads:
        logger.warning("No intervals enabled in config; exiting")
        return

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt, shutting down")
    finally:
        registry.close()


if __name__ == "__main__":
    main()
