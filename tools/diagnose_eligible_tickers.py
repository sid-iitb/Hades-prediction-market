#!/usr/bin/env python3
"""
Diagnose why fetch_eligible_tickers returns [] for KXSOL and KXDOGE.
Prints: API market count, spot, window, per-market strike extraction and window pass/fail.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Load .env for Kraken spot
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass


def main():
    from bot.config_loader import load_config
    from bot.market import fetch_markets_for_event, extract_strike_from_market
    from bot.hourly_last_90s_strategy import _get_cfg, _get_spot_price, _assets, _spot_window

    config_path = ROOT / "config" / "config.yaml"
    if not config_path.exists():
        config_path = ROOT / "config.yaml"
    config = load_config(str(config_path))
    cfg = _get_cfg(config)
    spot_window_default = float((config.get("spot_window") or 1500))
    by_asset = (config.get("spot_window_by_asset") or {})

    def window_for(asset: str) -> float:
        return float(by_asset.get(asset.lower(), spot_window_default))

    # Events that had "no eligible tickers"
    to_check = [
        ("sol", "KXSOL-26FEB2818"),
        ("doge", "KXDOGE-26FEB2818"),
    ]

    for asset, event_ticker in to_check:
        print("\n" + "=" * 60)
        print("Asset: %s  Event: %s" % (asset.upper(), event_ticker))
        print("=" * 60)

        spot = _get_spot_price(asset)
        window = window_for(asset)
        print("Spot price: %s" % (spot if spot is not None else "FAILED (no Kraken)"))
        print("spot_window_by_asset[%s] = %s (same units as spot)" % (asset, window))

        markets, err = fetch_markets_for_event(event_ticker)
        if err:
            print("API error: %s" % err)
            continue
        print("Markets returned by API: %d" % len(markets))

        if not markets:
            print("-> No markets: event may not exist or slug wrong. Check event_ticker.")
            if "KXSOL" in event_ticker:
                print("   (Kalshi may not list SOL range events; only KXSOLD above/below exists.)")
            continue

        in_window = 0
        zero_strike = 0
        for i, m in enumerate(markets[:20]):  # first 20
            ticker = m.get("ticker", "")
            strike = extract_strike_from_market(m, ticker)
            floor_val = m.get("floor_strike")
            ceiling_val = m.get("ceiling_strike")
            if strike <= 0:
                zero_strike += 1
            if spot is not None and strike > 0:
                dist = abs(strike - spot)
                pass_window = dist <= window
                if pass_window:
                    in_window += 1
                if i < 10:  # show first 10 in detail
                    print("  ticker=%s  floor=%s ceiling=%s => strike=%.4f  |strike-spot|=%.4f  window=%.4f  pass=%s"
                          % (ticker, floor_val, ceiling_val, strike, dist, window, pass_window))
        if len(markets) > 20:
            for m in markets[20:]:
                strike = extract_strike_from_market(m, m.get("ticker", ""))
                if strike <= 0:
                    zero_strike += 1
                elif spot is not None and abs(strike - spot) <= window:
                    in_window += 1
        print("Total: %d markets, %d with strike<=0, %d in window (eligible)" % (len(markets), zero_strike, in_window))
        if in_window == 0 and zero_strike == len(markets):
            print("-> Cause: all markets had strike<=0 (extraction failed: API may not send floor_strike/ceiling_strike, ticker pattern may not match)")
        elif in_window == 0 and spot is not None:
            print("-> Cause: no market has |strike - spot| <= window (window may be too tight or strikes in different units)")

    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
