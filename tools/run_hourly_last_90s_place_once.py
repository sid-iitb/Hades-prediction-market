#!/usr/bin/env python3
"""
Run hourly_last_90s_limit_99 placement once (one iteration). Uses same logic as the
bot thread: only places when seconds_to_close <= window_seconds (default 75s) and
mode is TRADE. Optionally --force-window to treat current hour as "in window" so
all eligible tickers get an order attempt (real orders if --trade).

Usage (from repo root):
  # Dry run: OBSERVE mode, only place when in last 75s (shows would place / skips)
  python tools/run_hourly_last_90s_place_once.py --config config/config.yaml

  # Real orders: TRADE mode. Run in last 75s of hour, or use --force-window to run now
  python tools/run_hourly_last_90s_place_once.py --config config/config.yaml --trade
  python tools/run_hourly_last_90s_place_once.py --config config/config.yaml --trade --force-window
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main():
    # Load .env from project root so Kalshi/Kraken credentials are available
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
    except ImportError:
        pass

    ap = argparse.ArgumentParser(description="Run hourly_last_90s placement once (optional real orders)")
    ap.add_argument("--config", "-c", default="config/config.yaml", help="Config file or dir")
    ap.add_argument("--trade", action="store_true", help="Use TRADE mode (place real orders)")
    ap.add_argument("--force-window", action="store_true", help="Treat current hour as in window (window_seconds=99999)")
    args = ap.parse_args()

    from bot.config_loader import load_config
    from bot.hourly_last_90s_strategy import run_hourly_last_90s_once
    from src.client.kalshi_client import KalshiClient

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    if not config_path.exists():
        print("Config not found:", config_path, file=sys.stderr)
        return 1

    config = load_config(str(config_path))
    cfg = config.get("hourly_last_90s_limit_99") or (config.get("schedule") or {}).get("hourly_last_90s_limit_99")
    if not cfg or not isinstance(cfg, dict):
        print("hourly_last_90s_limit_99 not found or disabled in config.", file=sys.stderr)
        return 1

    if args.trade:
        config["mode"] = "TRADE"
        print("Mode: TRADE (real orders will be placed)", file=sys.stderr)
    else:
        config["mode"] = config.get("mode", "OBSERVE")
        print("Mode: %s (no real orders)" % config["mode"], file=sys.stderr)

    if args.force_window:
        if "hourly_last_90s_limit_99" not in config:
            config["hourly_last_90s_limit_99"] = dict(cfg)
        config["hourly_last_90s_limit_99"]["window_seconds"] = 99999
        print("Force-window: treating current hour as in window (all eligible markets will be attempted).", file=sys.stderr)

    logger = logging.getLogger("hourly_last_90s")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stderr)
        h.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(h)

    db_path = config.get("state", {}).get("db_path", "data/bot_state.db")
    try:
        client = KalshiClient()
    except Exception as e:
        print("Kalshi client failed:", e, file=sys.stderr)
        return 1

    run_hourly_last_90s_once(config, logger, client, db_path)
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
