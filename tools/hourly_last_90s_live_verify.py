#!/usr/bin/env python3
"""
Live verification for hourly_last_90s_limit_99: run signal + placement logic for all
assets using live Kalshi orderbook and Kraken spot. NO ORDERS ARE PLACED.

Uses current strategy: generate_signals_farthest(..., pick_all_in_range=True, min_bid_cents)
then for each signal applies min_bid_cents and min_distance_at_placement. Reports
WOULD_PLACE / WOULD_SKIP per (asset, market, ticker, side). Runs for all current-hour
markets regardless of seconds_to_close (we do not place; we only verify logic).

Usage (from repo root; requires .env with KALSHI_API_KEY, KALSHI_PRIVATE_KEY, and Kraken):
  python tools/hourly_last_90s_live_verify.py
  python tools/hourly_last_90s_live_verify.py --config config/config.yaml
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main():
    ap = argparse.ArgumentParser(
        description="Verify hourly_last_90s signal + min_bid logic on live markets (no orders placed)"
    )
    ap.add_argument("--config", "-c", default="config/config.yaml", help="Config file or dir")
    args = ap.parse_args()

    from bot.config_loader import load_config
    from bot.market import (
        enrich_with_orderbook,
        fetch_eligible_tickers,
        get_current_hour_market_ids,
        get_minutes_to_close,
        TickerQuote,
    )
    from bot.hourly_last_90s_strategy import (
        _get_cfg,
        _get_spot_price,
        _assets,
        _spot_window,
        _min_distance_at_placement,
    )
    from bot.strategy import Signal, generate_signals_farthest

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
    limit_price_cents = int(cfg.get("limit_price_cents", 99))
    assets = _assets(cfg, config)
    thresholds = (config.get("thresholds") or {}).copy()
    if not thresholds:
        thresholds = {"yes_min": 92, "yes_max": 99, "no_min": 92, "no_max": 99}

    try:
        from src.client.kalshi_client import KalshiClient
        kalshi_client = KalshiClient()
    except Exception as e:
        print(f"Kalshi client failed (need KALSHI_API_KEY, KALSHI_PRIVATE_KEY): {e}", file=sys.stderr)
        return 1

    print("=== Hourly last_90s_limit_99 — LIVE VERIFY (no orders placed) ===\n")
    print("Strategy: generate_signals_farthest(pick_all_in_range=True, min_bid_cents=%d)" % min_bid_cents)
    print("Place rule: bid >= %dc → WOULD_PLACE limit %dc; else WOULD_SKIP (min_bid).\n" % (min_bid_cents, limit_price_cents))

    total_signals = 0
    would_place = 0
    would_skip_bid = 0
    would_skip_distance = 0
    would_skip_sanity = 0
    all_eligible: list[dict] = []  # collect every eligible signal for final table

    for asset in assets:
        spot = _get_spot_price(asset)
        if spot is None:
            print("[%s] No spot price (Kraken). Skip." % asset.upper())
            continue

        market_ids = get_current_hour_market_ids(asset)
        print("[%s] spot = %s  |  %d market(s): %s" % (
            asset.upper(),
            _fmt_strike(spot),
            len(market_ids),
            ", ".join(market_ids),
        ))

        for hour_market_id in market_ids:
            minutes_to_close = get_minutes_to_close(hour_market_id)
            seconds_to_close = minutes_to_close * 60.0
            in_window = 0 < seconds_to_close <= window_seconds

            window = _spot_window(config, asset)
            tickers_raw = fetch_eligible_tickers(hour_market_id, spot_price=spot, window=window)
            if not tickers_raw:
                print("  — %s: no eligible tickers (window=%s)." % (hour_market_id, window))
                continue

            quotes = enrich_with_orderbook(kalshi_client, tickers_raw)
            if not quotes:
                print("  — %s: no quotes after orderbook." % hour_market_id)
                continue

            above_below_prefixes = ("KXBTCD", "KXETHD", "KXSOLD", "KXXRPD")
            is_above_below = any(hour_market_id.upper().startswith(p) for p in above_below_prefixes)
            signals: list[Signal] = generate_signals_farthest(
                quotes, spot, ctx_late_window=True, thresholds=thresholds,
                pick_all_in_range=True, min_bid_cents=min_bid_cents,
                filter_side_by_spot_strike=is_above_below,
            )
            quote_by_ticker = {q.ticker: q for q in quotes}
            min_dist_required = _min_distance_at_placement(cfg, asset)

            if not signals:
                print("  — %s (%.0fs to close): 0 signals." % (hour_market_id, seconds_to_close))
                continue

            print("  — %s (%.0fs to close, in_window=%s): %d signal(s)" % (
                hour_market_id, seconds_to_close, in_window, len(signals),
            ))
            for sig in signals:
                total_signals += 1
                side = (sig.side or "").lower()
                if side not in ("yes", "no"):
                    continue
                quote = quote_by_ticker.get(sig.ticker)
                if not quote:
                    continue
                strike = float(quote.strike or 0)
                # B (range): distance to nearest boundary; else distance to strike (D markets)
                range_low = getattr(quote, "range_low", None)
                range_high = getattr(quote, "range_high", None)
                if range_low is not None and range_high is not None:
                    if spot < range_low:
                        distance = range_low - spot
                    elif spot > range_high:
                        distance = spot - range_high
                    else:
                        distance = min(spot - range_low, range_high - spot)
                else:
                    distance = abs(spot - strike) if strike > 0 else None
                bid = (quote.yes_bid if side == "yes" else quote.no_bid) or 0
                ask = (quote.yes_ask if side == "yes" else quote.no_ask) or None

                skip_reason = None
                if min_dist_required > 0 and distance is not None and distance <= min_dist_required:
                    skip_reason = "distance_to_boundary (%.4f <= %s)" % (distance, min_dist_required)
                    would_skip_distance += 1
                elif range_low is not None and range_high is not None:
                    spot_in_bucket = range_low <= spot <= range_high
                    if side == "yes" and not spot_in_bucket:
                        skip_reason = "YES but spot outside bucket (range %s..%s)" % (range_low, range_high)
                        would_skip_sanity += 1
                    elif side == "no" and spot_in_bucket:
                        skip_reason = "NO but spot inside bucket (range %s..%s)" % (range_low, range_high)
                        would_skip_sanity += 1
                if skip_reason is None and bid < min_bid_cents:
                    skip_reason = "%s bid=%dc < min_bid_cents=%d" % (side, bid, min_bid_cents)
                    would_skip_bid += 1
                if skip_reason is None:
                    would_place += 1

                action = "WOULD_PLACE" if skip_reason is None else "WOULD_SKIP"
                detail = ("limit %dc" % limit_price_cents) if skip_reason is None else skip_reason
                print("      %s %s %s  strike=%s  %s bid=%dc  ask=%s  → %s (%s)" % (
                    action, side.upper(), sig.ticker, _fmt_strike(strike), side, bid, ask, action, detail,
                ))
                all_eligible.append({
                    "asset": asset.upper(),
                    "spot": spot,
                    "market": hour_market_id,
                    "ticker": sig.ticker,
                    "side": side.upper(),
                    "strike": strike,
                    "bid": bid,
                    "ask": ask,
                    "action": action,
                    "detail": detail,
                })
        print()

    # --- All eligible signals (consolidated) ---
    print("=== ALL ELIGIBLE SIGNALS (%d total) ===" % len(all_eligible))
    print("asset\tspot\tmarket\tticker\tside\tstrike\tbid\task\taction\tdetail")
    print("-" * 140)
    out_dir = ROOT / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    tsv_path = out_dir / "hourly_last_90s_eligible_signals.tsv"
    with open(tsv_path, "w", encoding="utf-8") as f:
        f.write("asset\tspot\tmarket\tticker\tside\tstrike\tbid\task\taction\tdetail\n")
        for r in all_eligible:
            ask_str = "%dc" % r["ask"] if r["ask"] is not None else "n/a"
            line = "%s\t%s\t%s\t%s\t%s\t%s\t%dc\t%s\t%s\t%s\n" % (
                r["asset"],
                _fmt_strike(r.get("spot")),
                r["market"],
                r["ticker"],
                r["side"],
                _fmt_strike(r["strike"]),
                r["bid"],
                ask_str,
                r["action"],
                (r["detail"] or "")[:80],
            )
            f.write(line)
            print(line.rstrip())
    print("\n[Written: %s]" % tsv_path)

    print("--- Summary ---")
    print("Total signals: %d  |  WOULD_PLACE: %d  |  WOULD_SKIP (min_bid): %d  |  WOULD_SKIP (distance): %d  |  WOULD_SKIP (sanity): %d" % (
        total_signals, would_place, would_skip_bid, would_skip_distance, would_skip_sanity,
    ))
    print("\nNo orders were placed. Run inside last %ds of the hour to actually place." % window_seconds)
    return 0


def _fmt_strike(v) -> str:
    if v is None:
        return "n/a"
    try:
        x = float(v)
        if 0 < x < 20:
            return "%.4f" % x
        return "%.2f" % x
    except (TypeError, ValueError):
        return str(v)


if __name__ == "__main__":
    sys.exit(main())
