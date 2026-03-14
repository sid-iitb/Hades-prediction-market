#!/usr/bin/env python3
"""
Query strategy_report_last_90s and verify whether we placed in the 85-65 sec window.

Usage:
  python -m tools.verify_last_90s_85_65_placement [--db PATH] [--windows N]

- Default db: data/bot_state.db (or state.db_path from config)
- --windows N: analyze last N windows (default 5)
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

# Project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

LAST_90S_TABLE = "strategy_report_last_90s"


def get_db_path(path: str | None) -> str:
    if path:
        return path
    from bot.state import get_default_db_path
    return get_default_db_path()


def fetch_rows(conn: sqlite3.Connection, limit_windows: int | None) -> list[dict]:
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT order_id, window_id, asset, ts_utc, seconds_to_close, placed,
               skip_reason, skip_details, limit_or_market, bid, price_cents
        FROM {LAST_90S_TABLE}
        ORDER BY ts_utc DESC
        """
    )
    rows = cur.fetchall()
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r)) for r in rows]


def analyze_placement(rows: list[dict], limit_windows: int | None) -> list[dict]:
    """Group by (window_id, asset). For each group determine: placed in 85-65?, blocker, etc."""
    # Group by (window_id, asset)
    by_key: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        w = r.get("window_id") or ""
        a = (r.get("asset") or "").lower()
        if not w or not a:
            continue
        key = (w, a)
        if key not in by_key:
            by_key[key] = []
        by_key[key].append(r)

    # For each group: find placement row (placed=1, real order_id) or use skip row
    results = []
    for (window_id, asset), group in by_key.items():
        placement_row = None
        skip_row = None
        for r in group:
            if r.get("placed") == 1 and not (r.get("order_id") or "").startswith("agg:"):
                placement_row = r
            if (r.get("skip_reason") or r.get("skip_details")) and not skip_row:
                skip_row = r
            if placement_row and skip_row:
                break

        sec = None
        if placement_row is not None:
            try:
                sec = float(placement_row.get("seconds_to_close") or 0)
            except (TypeError, ValueError):
                pass

        in_85_65 = False
        if sec is not None and 65 < sec <= 85:
            in_85_65 = True

        primary_blocker = None
        if skip_row and not placement_row:
            details = (skip_row.get("skip_details") or "") or ""
            if "Primary blocker:" in details:
                start = details.find("Primary blocker:") + len("Primary blocker:")
                end = details.find("|", start)
                if end == -1:
                    end = len(details)
                primary_blocker = details[start:end].strip()
            else:
                primary_blocker = skip_row.get("skip_reason") or ""

        results.append({
            "window_id": window_id,
            "asset": asset,
            "placed": placement_row is not None,
            "seconds_to_close": sec,
            "placed_in_85_65": in_85_65,
            "ts_utc": (placement_row or skip_row or {}).get("ts_utc"),
            "limit_or_market": (placement_row or {}).get("limit_or_market"),
            "bid": (placement_row or skip_row or {}).get("bid"),
            "primary_blocker": primary_blocker or (skip_row.get("skip_reason") if skip_row else None),
        })

    return results


def run(db_path: str, limit_windows: int) -> None:
    if not Path(db_path).exists():
        print(f"DB not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (LAST_90S_TABLE,),
        )
        if not cur.fetchone():
            print(f"Table {LAST_90S_TABLE} not found in {db_path}")
            sys.exit(1)

        rows = fetch_rows(conn, limit_windows)
        if not rows:
            print("No rows in strategy_report_last_90s.")
            return

        results = analyze_placement(rows, limit_windows)

        # Unique windows by latest ts_utc for this window
        window_order: dict[str, str] = {}
        for r in results:
            w = r["window_id"]
            ts = r.get("ts_utc") or ""
            if w not in window_order or (ts > window_order[w]):
                window_order[w] = ts
        sorted_windows = sorted(window_order.keys(), key=lambda w: window_order[w], reverse=True)
        if limit_windows:
            sorted_windows = sorted_windows[: limit_windows]

        print("=" * 72)
        print("LAST-90s: 85–65 sec placement verification")
        print("  Placed in 85–65 = order placed when 65 < seconds_to_close <= 85")
        print("  DB: %s" % db_path)
        print("=" * 72)

        any_in_85_65 = False
        for window_id in sorted_windows:
            win_results = [r for r in results if r["window_id"] == window_id]
            if not win_results:
                continue
            ts_sample = win_results[0].get("ts_utc") or ""
            print("\nWindow: %s  (ts_utc sample: %s)" % (window_id, ts_sample[:19] if ts_sample else ""))

            for r in sorted(win_results, key=lambda x: x["asset"]):
                asset = r["asset"].upper()
                if r["placed"]:
                    sec = r["seconds_to_close"]
                    in_85_65 = r["placed_in_85_65"]
                    if in_85_65:
                        any_in_85_65 = True
                    sec_str = "%.1f" % sec if sec is not None else "n/a"
                    band = "YES (85–65 sec)" if in_85_65 else "NO (placed at sec_to_close=%s)" % sec_str
                    print("  %s: PLACED  |  In 85–65? %s  |  limit_or_market=%s  bid=%s" % (
                        asset, band, r.get("limit_or_market"), r.get("bid")))
                else:
                    blocker = r.get("primary_blocker") or "—"
                    print("  %s: NO PLACE  |  Blocker: %s" % (asset, blocker))

        print("\n" + "-" * 72)
        if any_in_85_65:
            print("Summary: At least one order was placed in the 85–65 sec window.")
        else:
            print("Summary: No order in this run was placed in the 85–65 sec window (all were placed after 65s or not placed).")
        print("-" * 72)
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify last_90s 85–65 sec placement from DB")
    ap.add_argument("--db", default=None, help="Path to bot_state.db (default: data/bot_state.db)")
    ap.add_argument("--windows", type=int, default=5, help="Analyze last N windows (default 5)")
    args = ap.parse_args()
    db_path = get_db_path(args.db)
    run(db_path, args.windows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
