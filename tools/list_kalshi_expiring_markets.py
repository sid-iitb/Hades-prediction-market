#!/usr/bin/env python3
"""
List Kalshi markets expiring within configurable time windows.

Fetches markets from the Kalshi API and reports counts (and optionally tickers)
for markets expiring in the next 1h, 12h, 24h, 48h (or custom windows).

Usage:
  python tools/list_kalshi_expiring_markets.py
  python tools/list_kalshi_expiring_markets.py --windows 1 6 12 24 48
  python tools/list_kalshi_expiring_markets.py --list  # include ticker list per window
  python tools/list_kalshi_expiring_markets.py --max-hours 72 --windows 24 48 72
"""
import argparse
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests


KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
DEFAULT_WINDOWS_HOURS = [1, 12, 24, 48]


def _fetch_window(
    min_ts: int,
    max_ts: int,
    page_limit: int = 500,
) -> List[dict]:
    """
    Fetch all markets from Kalshi API that close between min_ts and max_ts.
    Uses per-window queries because the API pagination returns skewed results
    when querying a wide range (e.g. 0-48h returns mostly later-expiring markets).
    """
    params = {"limit": 200, "min_close_ts": min_ts, "max_close_ts": max_ts}
    all_markets: List[dict] = []
    cursor = None
    while len(all_markets) < page_limit:
        if cursor is not None:
            params = dict(params)
            params["cursor"] = cursor
        resp = requests.get(KALSHI_API_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        markets = data.get("markets", [])
        all_markets.extend(markets)
        cursor = data.get("cursor")
        if not cursor or not markets:
            break
    return all_markets


def fetch_markets_by_windows(
    windows_hours: List[int],
    now_ts: int,
    page_limit_per_window: int = 2000,
) -> Dict[int, List[Tuple[dict, int]]]:
    """
    Fetch markets per non-overlapping time bucket to get accurate counts.
    Kalshi API pagination with a wide range (0-48h) skews toward later markets.
    Returns: {window_hours: [(market, secs_remaining), ...]} - cumulative.
    """
    sorted_h = sorted(set(windows_hours))
    if not sorted_h:
        return {}

    # Non-overlapping buckets: [0,1), [1,12), [12,24), [24,48), ...
    bucket_starts = [0] + sorted_h[:-1]
    bucket_ends = sorted_h

    bucket_markets: Dict[Tuple[int, int], List[Tuple[dict, int]]] = {}
    for start_h, end_h in zip(bucket_starts, bucket_ends):
        min_ts = now_ts + start_h * 3600
        max_ts = now_ts + end_h * 3600
        raw = _fetch_window(min_ts, max_ts, page_limit=page_limit_per_window)
        items: List[Tuple[dict, int]] = []
        for m in raw:
            close_ts = get_close_ts(m)
            if close_ts is None or close_ts <= now_ts:
                continue
            secs = close_ts - now_ts
            items.append((m, secs))
        items.sort(key=lambda x: x[1])
        bucket_markets[(start_h, end_h)] = items

    # Build cumulative buckets: next Nh = all markets in buckets up to N
    result: Dict[int, List[Tuple[dict, int]]] = {}
    for h in sorted_h:
        combined: List[Tuple[dict, int]] = []
        ticker_set = set()
        for (start_h, end_h), items in bucket_markets.items():
            if end_h <= h:
                for m, secs in items:
                    t = m.get("ticker")
                    if t and t not in ticker_set:
                        ticker_set.add(t)
                        combined.append((m, secs))
        combined.sort(key=lambda x: x[1])
        result[h] = combined
    return result


def get_close_ts(market: dict) -> Optional[int]:
    """Extract close timestamp from market (seconds since epoch)."""
    for key in ("close_time", "expected_expiration_time", "expiration_time"):
        val = market.get(key)
        if val is None:
            continue
        try:
            if isinstance(val, (int, float)):
                return int(val)
            dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except (ValueError, TypeError):
            pass
    return None


def fmt_remaining(secs: int) -> str:
    if secs < 60:
        return f"{secs}s"
    elif secs < 3600:
        m = secs // 60
        s = secs % 60
        return f"{m}m" if s == 0 else f"{m}m {s}s"
    elif secs < 86400:
        h = secs // 3600
        m = (secs % 3600) // 60
        return f"{h}h {m}m"
    else:
        d = secs // 86400
        h = (secs % 86400) // 3600
        return f"{d}d {h}h"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List Kalshi markets expiring within configurable time windows."
    )
    parser.add_argument(
        "--windows",
        type=int,
        nargs="+",
        default=DEFAULT_WINDOWS_HOURS,
        help=f"Time windows in hours (default: {DEFAULT_WINDOWS_HOURS})",
    )
    parser.add_argument(
        "--max-hours",
        type=int,
        default=48,
        help="Fetch markets expiring within this many hours (default: 48)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List tickers per window (otherwise only counts)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="When --list, show at most N tickers per window (default: 10)",
    )
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    now_ts = int(now.timestamp())

    # Restrict windows to max_hours
    windows = [h for h in args.windows if h <= args.max_hours]
    if not windows:
        windows = [args.max_hours]

    try:
        buckets = fetch_markets_by_windows(windows, now_ts)
    except requests.RequestException as e:
        print(f"Error fetching markets: {e}", file=sys.stderr)
        sys.exit(1)

    print("Kalshi markets expiring within time windows")
    print("=" * 60)
    print(f"Now (UTC): {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    max_window = max(windows) if windows else args.max_hours
    total_in_max = len(buckets.get(max_window, []))
    print(f"Total markets expiring in next {args.max_hours}h: {total_in_max}")
    print()

    for h in sorted(windows):
        items = buckets.get(h, [])
        items.sort(key=lambda x: x[1])  # shortest first
        count = len(items)
        print(f"--- Next {h}h: {count} markets ---")
        if args.list and items:
            for m, secs in items[: args.top]:
                ticker = (m.get("ticker") or "")[:44]
                title = (m.get("title") or "")[:50]
                print(f"  {ticker:44} | {fmt_remaining(secs):>10} | {title}")
            if count > args.top:
                print(f"  ... and {count - args.top} more")
        print()


if __name__ == "__main__":
    main()
