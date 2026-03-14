#!/usr/bin/env python3
"""
Validation script for hourly_last_90s_limit_99: lists tickers that WOULD be
selected for placement right now per asset and per market_id.

Market structure (as of implementation):
- BTC: 2 markets — KXBTCD-... (above/below), KXBTC-... (range)
- ETH: 2 — KXETHD-..., KXETH-...
- SOL: 2 — KXSOLD-..., KXSOL-...
- XRP: 2 — KXXRPD-..., KXXRP-...
- DOGE: 1 — KXDOGE-... (range only)

Selection: 2 tickers with strike < spot → YES; 2 with strike > spot → NO
(closest to spot first). Only runs when seconds_to_close is in (0, window_seconds].

Usage (from repo root):
  python tools/hourly_last_90s_validate_tickers.py
  python tools/hourly_last_90s_validate_tickers.py --config config/config.yaml
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Project root
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _pick_two_below_two_above(quotes, spot: float):
    """Same logic as hourly_last_90s_strategy: 2 below (YES), 2 above (NO), by distance to spot."""
    below = []
    above = []
    for q in quotes:
        s = float(getattr(q, "strike", 0) or 0)
        if s <= 0:
            continue
        dist = abs(spot - s)
        if s < spot:
            below.append((q, dist))
        elif s > spot:
            above.append((q, dist))
    below.sort(key=lambda x: x[1])
    above.sort(key=lambda x: x[1])
    out_below = [(q, "yes") for q, _ in below[:2]]
    out_above = [(q, "no") for q, _ in above[:2]]
    return out_below, out_above


def main():
    ap = argparse.ArgumentParser(description="List tickers hourly_last_90s would select per asset/market")
    ap.add_argument("--config", "-c", default="config/config.yaml", help="Config file or dir")
    args = ap.parse_args()

    from bot.config_loader import load_config
    from bot.market import (
        enrich_with_orderbook,
        fetch_eligible_tickers,
        get_current_hour_market_ids,
        get_minutes_to_close,
    )
    from bot.hourly_last_90s_strategy import _get_cfg, _get_spot_price, _assets, _spot_window

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    if not config_path.exists():
        print(f"Config not found: {config_path}", file=sys.stderr)
        return 1

    config = load_config(str(config_path))
    cfg = _get_cfg(config)
    if not cfg:
        print("hourly_last_90s_limit_99 not found in config.", file=sys.stderr)
        return 1

    window_seconds = int(cfg.get("window_seconds", 75))
    min_bid_cents = int(cfg.get("min_bid_cents", 95))
    assets = _assets(cfg, config)

    kalshi_client = None
    try:
        from src.client.kalshi_client import KalshiClient
        kalshi_client = KalshiClient()
    except Exception as e:
        print(f"[Kalshi client not available: {e}. Showing selection by strike only (no min_bid filter).]", file=sys.stderr)

    print("=== Hourly last_90s_limit_99 — tickers that would be selected NOW ===\n")
    print("Rule: 2 tickers below spot → YES, 2 above spot → NO (closest to spot).")
    print("Only markets with 0 < seconds_to_close <= %d are active.\n" % window_seconds)

    for asset in assets:
        spot = _get_spot_price(asset)
        if spot is None:
            print("[%s] No spot price (Kraken failed). Skipping." % asset.upper())
            continue

        market_ids = get_current_hour_market_ids(asset)
        print("[%s] spot = %.4f  |  %d market(s): %s" % (asset.upper(), spot, len(market_ids), ", ".join(market_ids)))

        for hour_market_id in market_ids:
            minutes_to_close = get_minutes_to_close(hour_market_id)
            seconds_to_close = minutes_to_close * 60.0

            if seconds_to_close <= 0:
                print("  — %s: closed (%.0fs), no placement." % (hour_market_id, seconds_to_close))
                continue
            if seconds_to_close > window_seconds:
                print("  — %s: %.0fs to close (outside window %ds), no placement." % (hour_market_id, seconds_to_close, window_seconds))
                continue

            window = _spot_window(config, asset)
            tickers_raw = fetch_eligible_tickers(hour_market_id, spot_price=spot, window=window)
            if not tickers_raw:
                print("  — %s: no eligible tickers in window (spot ± %d)." % (hour_market_id, window))
                continue

            if kalshi_client:
                quotes = enrich_with_orderbook(kalshi_client, tickers_raw)
            else:
                from bot.market import TickerQuote
                quotes = []
                for t in tickers_raw:
                    ticker = t.get("ticker")
                    if not ticker:
                        continue
                    strike = t.get("strike", 0)
                    quotes.append(TickerQuote(
                        ticker=ticker,
                        strike=strike,
                        yes_ask=t.get("yes_ask"),
                        no_ask=t.get("no_ask"),
                        yes_bid=t.get("yes_bid"),
                        no_bid=t.get("no_bid"),
                        subtitle=t.get("subtitle", ""),
                    ))

            below_picks, above_picks = _pick_two_below_two_above(quotes, spot)
            all_picks = below_picks + above_picks

            if not all_picks:
                print("  — %s: no tickers below/above spot in eligible set." % hour_market_id)
                continue

            print("  — %s (%.0fs to close): place %d bid(s):" % (hour_market_id, seconds_to_close, len(all_picks)))
            for q, side in all_picks:
                ticker = getattr(q, "ticker", getattr(q, "ticker", "?"))
                strike = getattr(q, "strike", None)
                bid = (q.yes_bid if side == "yes" else q.no_bid) if hasattr(q, "yes_bid") else None
                skip = ""
                if bid is not None and bid < min_bid_cents:
                    skip = "  [SKIP: bid %dc < min_bid_cents %d]" % (bid, min_bid_cents)
                print("      %s %s (strike %.2f)%s" % (side.upper(), ticker, float(strike or 0), skip))
        print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
