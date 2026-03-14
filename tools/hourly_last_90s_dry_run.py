#!/usr/bin/env python3
"""
Dry run for hourly_last_90s_limit_99: fetches current-hour markets from Kalshi (uses your .env
for auth if needed), spot from Kraken, and prints the exact list of tickers that would be
picked (YES/NO) per asset. Outputs one row per ticker: asset, market, ticker, side, distance_from_spot.

Usage (from repo root):
  python tools/hourly_last_90s_dry_run.py
  python tools/hourly_last_90s_dry_run.py --config config/config.yaml
  python tools/hourly_last_90s_dry_run.py --output reports/hourly_last_90s_dry_run.tsv
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Load .env so Kalshi/Kraken use your credentials
def _load_env():
    env_path = ROOT / ".env"
    if env_path.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_path)
        except ImportError:
            pass

KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"


def _slug_for_prefix(prefix: str, target_et: datetime) -> str:
    y = target_et.strftime("%y")
    m = target_et.strftime("%b").lower()
    d = target_et.strftime("%d")
    h = target_et.strftime("%H")
    return f"{prefix}-{y}{m}{d}{h}"


def get_current_hour_event_tickers() -> Dict[str, List[str]]:
    """Current hour (next close) event tickers per asset. 9 total: 2 for btc/eth/sol/xrp, 1 for doge."""
    import pytz
    et = datetime.now(pytz.timezone("US/Eastern"))
    # Next hour close = current "active" market
    next_hour = et.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    out: Dict[str, List[str]] = {}
    # above/below prefixes
    ab = {"btc": "kxbtcd", "eth": "kxethd", "sol": "kxsold", "xrp": "kxxrpd", "doge": "kxdoge"}
    # range prefixes
    rng = {"btc": "kxbtc", "eth": "kxeth", "sol": "kxsol", "xrp": "kxxrp", "doge": "kxdoge"}
    for asset in ("btc", "eth", "sol", "xrp", "doge"):
        ids: List[str] = []
        if asset in ("btc", "eth", "sol", "xrp"):
            ids.append(_slug_for_prefix(ab[asset], next_hour).upper())
            ids.append(_slug_for_prefix(rng[asset], next_hour).upper())
        else:
            ids.append(_slug_for_prefix(rng["doge"], next_hour).upper())
        out[asset] = ids
    return out


def fetch_markets(event_ticker: str, session: Optional[requests.Session] = None) -> List[Dict]:
    """List markets for event. Uses your auth if KALSHI_* env vars set (session with auth)."""
    try:
        if session is not None:
            r = session.get(KALSHI_API_URL, params={"limit": 100, "event_ticker": event_ticker}, timeout=15)
        else:
            r = requests.get(KALSHI_API_URL, params={"limit": 100, "event_ticker": event_ticker}, timeout=15)
        r.raise_for_status()
        return r.json().get("markets", [])
    except Exception:
        return []


def parse_strike_from_text(text: str) -> float:
    """First $ amount or first 3+ digit number."""
    if not text or not isinstance(text, str):
        return 0.0
    match = re.search(r"\$([\d,]+(?:\.[\d]+)?)", text)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if 0 < v < 1e9:
                return v
        except (ValueError, TypeError):
            pass
    match = re.search(r"([\d,]{3,}(?:\.[\d]+)?)", text)
    if match:
        try:
            v = float(match.group(1).replace(",", ""))
            if 0 < v < 1e9:
                return v
        except (ValueError, TypeError):
            pass
    return 0.0


def extract_strike(m: Dict[str, Any]) -> float:
    """Single strike for ordering (floor_strike, strike, subtitle, etc.)."""
    for key in ("floor_strike", "strike", "ceiling_strike"):
        v = m.get(key)
        if v is not None:
            try:
                f = float(v)
                if f > 0:
                    return f
            except (ValueError, TypeError):
                pass
    v = parse_strike_from_text(m.get("subtitle", ""))
    if v > 0:
        return v
    ticker = m.get("ticker", "")
    match = re.search(r"-T?([\d.]+)$", ticker, re.I) or re.search(r"-(\d{3,}\.?\d*)$", ticker)
    if match:
        try:
            f = float(match.group(1))
            if 0 < f < 1e9:
                return f
        except (ValueError, TypeError):
            pass
    return 0.0


def _parse_range_from_subtitle(subtitle: str) -> str:
    """Parse 'X - Y' or '$X - $Y' from subtitle; return 'X-Y' or ''."""
    if not subtitle or not isinstance(subtitle, str):
        return ""
    # $65,500 - $65,625 or 65500 - 65625 or 0.10 - 0.11
    match = re.search(r"\$?([\d,.]+)\s*[-–]\s*\$?([\d,.]+)", subtitle)
    if match:
        try:
            a = match.group(1).replace(",", "").strip()
            b = match.group(2).replace(",", "").strip()
            return "%s-%s" % (a, b)
        except Exception:
            pass
    return ""


def build_ticker_to_range(markets: List[Dict]) -> Dict[str, str]:
    """Map ticker -> range string (floor-ceiling) for range-type; empty for above/below."""
    out: Dict[str, str] = {}
    for m in markets:
        ticker = m.get("ticker", "")
        if not ticker:
            continue
        floor = m.get("floor_strike")
        ceiling = m.get("ceiling_strike") or m.get("cap_strike")  # API uses cap_strike
        if floor is not None and ceiling is not None:
            try:
                fl = float(floor)
                ce = float(ceiling)
                out[ticker] = "%s-%s" % (fl, ce)
            except (ValueError, TypeError):
                r = _parse_range_from_subtitle(m.get("subtitle", ""))
                out[ticker] = r
        else:
            r = _parse_range_from_subtitle(m.get("subtitle", ""))
            out[ticker] = r
    return out


def eligible_tickers(markets: List[Dict], spot: float, window: float) -> List[Tuple[str, float]]:
    """(ticker, strike) within spot ± window."""
    out = []
    for m in markets:
        strike = extract_strike(m)
        if strike <= 0:
            continue
        if abs(strike - spot) > window:
            continue
        out.append((m.get("ticker", ""), strike))
    out.sort(key=lambda x: x[1])
    return out


def pick_two_below_two_above(tickers: List[Tuple[str, float]], spot: float) -> Tuple[List[Tuple[str, str, float]], List[Tuple[str, str, float]]]:
    """(ticker, side, strike). Below = YES, above = NO; closest first, max 2 each."""
    below = [(t, s) for t, s in tickers if s < spot]
    above = [(t, s) for t, s in tickers if s > spot]
    below.sort(key=lambda x: abs(spot - x[1]))
    above.sort(key=lambda x: abs(spot - x[1]))
    yes_list = [(t, "YES", s) for t, s in below[:2]]
    no_list = [(t, "NO", s) for t, s in above[:2]]
    return (yes_list, no_list)


def get_spot(asset: str) -> Optional[float]:
    try:
        from src.client.kraken_client import KrakenClient
        c = KrakenClient()
        a = asset.lower()
        if a == "eth": return c.latest_eth_price().price
        if a == "sol": return c.latest_sol_price().price
        if a == "xrp": return c.latest_xrp_price().price
        if a == "doge": return c.latest_doge_price().price
        return c.latest_btc_price().price
    except Exception:
        return None


def main():
    _load_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", "-c", default="config/config.yaml")
    ap.add_argument("--spot-window", type=float, default=None, help="Override spot window (default from config)")
    ap.add_argument("--output", "-o", default=None, help="Write TSV to this path (e.g. reports/hourly_last_90s_dry_run.tsv)")
    args = ap.parse_args()

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = ROOT / config_path
    spot_window_by_asset = {"btc": 1500, "eth": 200, "sol": 50, "xrp": 1, "doge": 1}
    min_distance_at_placement: Dict[str, float] = {"btc": 40, "eth": 3, "sol": 0.25, "xrp": 0.0075, "doge": 0}
    if config_path.exists():
        try:
            from bot.config_loader import load_config
            cfg = load_config(str(config_path))
            sw = (cfg.get("spot_window_by_asset") or {})
            for k, v in sw.items():
                if isinstance(v, (int, float)):
                    spot_window_by_asset[k.lower()] = int(v)
            h90 = cfg.get("hourly_last_90s_limit_99") or {}
            md = h90.get("min_distance_at_placement")
            if isinstance(md, dict):
                for k, v in md.items():
                    try:
                        min_distance_at_placement[k.lower()] = float(v)
                    except (TypeError, ValueError):
                        pass
        except Exception:
            pass
    window_override = args.spot_window
    if window_override is not None:
        for k in spot_window_by_asset:
            spot_window_by_asset[k] = int(window_override)

    # Optional: authenticated session for Kalshi (if env has KALSHI_API_KEY_ID etc.)
    session = None
    try:
        from src.client.kalshi_client import KalshiClient
        _ = KalshiClient()
        # Use authenticated GET for markets (same base URL)
        session = requests.Session()
        session.headers.update({"Accept": "application/json"})
        # KalshiClient signs requests; for GET /markets we use public or add auth later
    except Exception:
        pass

    events = get_current_hour_event_tickers()
    print("=== Hourly last_90s_limit_99 — DRY RUN (actual API calls, .env loaded) ===\n")
    print("Rule: 2 tickers with strike < spot → YES; 2 with strike > spot → NO (closest to spot).")
    print("Skip if distance_from_spot <= min_distance_at_placement (by asset).")
    print("distance_from_spot = |spot - strike|\n")

    # Rows for final table: (asset, market, ticker, side, distance_from_spot, range)
    table_rows: List[Tuple[str, str, str, str, float, str]] = []
    all_results: List[Tuple[str, str, List[Tuple[str, str, float]], List[Tuple[str, str, float]]]] = []

    for asset in ("btc", "eth", "sol", "xrp", "doge"):
        spot = get_spot(asset)
        if spot is None:
            print("[%s] No spot (Kraken failed). Skipping." % asset.upper())
            continue
        window = spot_window_by_asset.get(asset.lower(), 1500)
        market_ids = events[asset]
        print("[%s] spot = %.4f  window = %s" % (asset.upper(), spot, window))
        for event_ticker in market_ids:
            markets = fetch_markets(event_ticker, session=session)
            if not markets:
                print("  — %s: no markets returned." % event_ticker)
                continue
            ticker_to_range = build_ticker_to_range(markets)
            tickers_with_strike = eligible_tickers(markets, spot, window)
            if not tickers_with_strike:
                print("  — %s: no tickers in spot ± %s." % (event_ticker, window))
                continue
            yes_picks, no_picks = pick_two_below_two_above(tickers_with_strike, spot)
            min_dist = min_distance_at_placement.get(asset.lower(), 0.0)
            all_results.append((asset.upper(), event_ticker, yes_picks, no_picks))
            print("  — %s (min_distance_at_placement=%s):" % (event_ticker, min_dist))
            for t, side, s in yes_picks + no_picks:
                dist = abs(spot - s)
                range_str = ticker_to_range.get(t, "")
                if min_dist > 0 and dist <= min_dist:
                    rng = "  range %s" % range_str if range_str else ""
                    print("      SKIP (distance %.4f <= min %.4f)  %s  %s (strike %.2f)%s" % (dist, min_dist, side, t, s, rng))
                    continue
                table_rows.append((asset.upper(), event_ticker, t, side, round(dist, 6), range_str))
                extra = "  range %s" % range_str if range_str else ""
                print("      %s  %s  (strike %.2f  distance %.4f%s)" % (side, t, s, dist, extra))
        print()

    # One row per ticker: asset, market, ticker, side, distance_from_spot, range
    header = ("asset", "market", "ticker", "side", "distance_from_spot", "range")
    lines = ["\t".join(header)]
    for row in table_rows:
        lines.append("\t".join(str(x) for x in row))

    print("--- Table (one row per ticker): asset, market, ticker, side, distance_from_spot, range ---\n")
    print("\n".join(lines))

    if args.output:
        out_path = Path(args.output)
        if not out_path.is_absolute():
            out_path = ROOT / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print("\n[Written: %s]" % out_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
