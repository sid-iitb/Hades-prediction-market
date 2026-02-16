#!/usr/bin/env python3
"""
Analyze 15-min bot runs: group by market, aggregate YES/NO signals, compute win rate.
Reads bot log file and optionally fetches market resolution from Kalshi API.

Usage:
  python -m bot.analyze_15min [--config config.yaml] [--log logs/bot.log]
"""
import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure project root on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml
from dotenv import load_dotenv

from bot.market import fetch_15min_market_result
from bot.outcomes_15min import get_stored_outcome

PROJECT_ROOT = Path(__file__).resolve().parent.parent
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f) or {}


def parse_log_line(line: str) -> Optional[Dict]:
    """Parse a log line; return dict if it's a 15-min run, else None."""
    line = line.strip()
    if not line:
        return None
    # Log format: "YYYY-MM-DD HH:MM:SS | INFO | {...}"
    if " | " in line:
        parts = line.split(" | ", 2)
        if len(parts) >= 3:
            line = parts[2]
    try:
        data = json.loads(line)
        if data.get("interval") == "15min":
            return data
    except json.JSONDecodeError:
        pass
    return None


def get_market_result(market_id: str, db_path: str, fetch_from_api: bool = True) -> Optional[str]:
    """
    Get market resolution: prefer stored outcome (from bot's previous run), else fetch from Kalshi API.
    Returns 'yes', 'no', 'scalar', or None if not yet determined.
    """
    stored = get_stored_outcome(db_path, market_id)
    if stored:
        return stored
    if fetch_from_api:
        return fetch_15min_market_result(market_id)
    return None


def analyze_log(log_path: str, db_path: str, fetch_resolution: bool = True) -> Dict[str, Any]:
    """
    Parse 15-min runs from log, group by market_id, aggregate.
    """
    runs_by_market: Dict[str, List[Dict]] = defaultdict(list)

    if not os.path.exists(log_path):
        return {"markets": [], "error": f"Log file not found: {log_path}"}

    with open(log_path) as f:
        for line in f:
            data = parse_log_line(line)
            if data:
                market_id = data.get("market_id") or data.get("quote_ticker", "")
                if market_id:
                    runs_by_market[market_id].append(data)

    # Aggregate per market
    rows = []
    for market_id in sorted(runs_by_market.keys()):
        runs = runs_by_market[market_id]
        total_runs = len(runs)

        signals_yes = 0
        signals_no = 0
        placed_yes = 0
        placed_no = 0

        for r in runs:
            summary = r.get("summary", {}) or {}
            signals_yes += summary.get("signals_yes", 0)
            signals_no += summary.get("signals_no", 0)

            for ex in r.get("execution", []) or []:
                if ex.get("status") in ("PLACED", "WOULD_TRADE"):
                    side = (ex.get("side") or "").lower()
                    if side == "yes":
                        placed_yes += 1
                    elif side == "no":
                        placed_no += 1

        result = None
        if fetch_resolution:
            result = get_market_result(market_id, db_path, fetch_from_api=True)

        # Win rate: if result=yes, YES orders win; if result=no, NO orders win
        wins = 0
        total_orders = placed_yes + placed_no
        if result == "yes" and total_orders > 0:
            wins = placed_yes
        elif result == "no" and total_orders > 0:
            wins = placed_no

        win_rate = (wins / total_orders * 100) if total_orders > 0 else None

        rows.append({
            "market_id": market_id,
            "total_runs": total_runs,
            "signals_yes": signals_yes,
            "signals_no": signals_no,
            "placed_yes": placed_yes,
            "placed_no": placed_no,
            "total_orders": total_orders,
            "result": result,
            "wins": wins,
            "win_rate_pct": round(win_rate, 1) if win_rate is not None else None,
        })

    return {"markets": rows, "error": None}


def print_report(data: Dict[str, Any]) -> None:
    """Print human-readable report."""
    markets = data.get("markets", [])
    if data.get("error"):
        print(f"Error: {data['error']}")
        return

    if not markets:
        print("No 15-min runs found in log.")
        return

    # Summary totals
    total_runs = sum(m["total_runs"] for m in markets)
    total_signals_yes = sum(m["signals_yes"] for m in markets)
    total_signals_no = sum(m["signals_no"] for m in markets)
    total_placed_yes = sum(m["placed_yes"] for m in markets)
    total_placed_no = sum(m["placed_no"] for m in markets)
    total_orders = sum(m["total_orders"] for m in markets)
    total_wins = sum(m["wins"] for m in markets)
    resolved = [m for m in markets if m["result"]]
    overall_win_rate = (total_wins / total_orders * 100) if total_orders > 0 else None

    print("\n" + "=" * 90)
    print("  15-MIN BOT RUN ANALYSIS")
    print("=" * 90)
    print(f"  Total unique markets:  {len(markets)}")
    print(f"  Total run cycles:      {total_runs}")
    print(f"  Markets with result:   {len(resolved)}")
    print("-" * 90)
    print(f"  Aggregate signals:     YES={total_signals_yes}  NO={total_signals_no}")
    print(f"  Orders placed:         YES={total_placed_yes}  NO={total_placed_no}  Total={total_orders}")
    if overall_win_rate is not None:
        print(f"  Overall win rate:      {total_wins}/{total_orders} = {overall_win_rate:.1f}%")
    print("=" * 90)

    print("\n  Per-market breakdown:")
    print("-" * 90)
    print(f"  {'Market ID':<30} {'Runs':>5} {'SigY':>5} {'SigN':>5} {'PlY':>4} {'PlN':>4} {'Res':>4} {'Wins':>4} {'Win%':>6}")
    print("-" * 90)

    for m in markets:
        res = (m["result"] or "-")[:4]
        win_pct = f"{m['win_rate_pct']:.1f}%" if m["win_rate_pct"] is not None else "-"
        print(f"  {m['market_id']:<30} {m['total_runs']:>5} {m['signals_yes']:>5} {m['signals_no']:>5} "
              f"{m['placed_yes']:>4} {m['placed_no']:>4} {res:>4} {m['wins']:>4} {win_pct:>6}")

    print("=" * 90)
    print("  Legend: Runs=run cycles, SigY/SigN=signals YES/NO, PlY/PlN=orders placed,")
    print("          Res=Kalshi outcome (yes/no), Wins=winning orders, Win%=win rate")
    print("  Outcomes: from data/15min_outcomes.json (logged by bot) or Kalshi API")
    print("=" * 90 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Analyze 15-min bot runs")
    parser.add_argument("--config", "-c", default="config.yaml", help="Config file")
    parser.add_argument("--log", "-l", default=None, help="Log file (default from config)")
    parser.add_argument("--no-fetch", action="store_true", help="Skip fetching market resolution (faster)")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    os.chdir(project_root)
    load_dotenv(project_root / ".env")

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = project_root / config_path
    config = load_config(str(config_path)) if config_path.exists() else {}

    log_path = args.log
    if not log_path:
        log_path = config.get("logging", {}).get("file", "logs/bot.log")
    if not Path(log_path).is_absolute():
        log_path = str(project_root / log_path)

    db_path = config.get("state", {}).get("db_path", "data/bot_state.db")
    if not Path(db_path).is_absolute():
        db_path = str(project_root / db_path)

    data = analyze_log(log_path, db_path=db_path, fetch_resolution=not args.no_fetch)
    print_report(data)


if __name__ == "__main__":
    main()
