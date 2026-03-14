#!/usr/bin/env python3
"""
Query the strategy report DB for stop-loss rows (last_90s and hourly_last_90s).

Stop-loss rows are identified by is_stop_loss = 1. stop_loss_sell_price is the exit price.
final_outcome is always WIN or LOSS from Kalshi resolution (separate from stop-loss).
Option --by-price also lists rows with resolution_price in (1..99) for backward compatibility.

Usage:
  python tools/query_stop_loss_from_report.py [--db PATH] [--hours 24]
  python tools/query_stop_loss_from_report.py --by-price   # also show rows matching price heuristic
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_db_path(config_path: str | None) -> str:
    root = _project_root()
    if config_path and os.path.isfile(config_path):
        try:
            import yaml
            with open(config_path) as f:
                cfg = yaml.safe_load(f) or {}
            return (cfg.get("state") or {}).get("db_path", "data/bot_state.db")
        except Exception:
            pass
    return "data/bot_state.db"


def main() -> None:
    ap = argparse.ArgumentParser(description="Query report DB for stop-loss rows")
    ap.add_argument("--db", default=None, help="Report/state DB path (default: from config or data/bot_state.db)")
    ap.add_argument("--hours", type=float, default=24 * 7, help="Only rows with ts_utc in last N hours (default: 168 = 1 week). Ignored if --all.")
    ap.add_argument("--all", action="store_true", help="Fetch all stop-loss rows (no time filter)")
    ap.add_argument("--by-price", action="store_true", help="Also list rows matching price heuristic (resolution_price 1-99) even if final_outcome != STOP_LOSS")
    args = ap.parse_args()

    root = _project_root()
    sys.path.insert(0, str(root))

    db_path = args.db
    if not db_path:
        config_path = os.environ.get("CONFIG_PATH", str(root / "config" / "hourly.yaml"))
        db_path = get_db_path(config_path)
    if not os.path.isabs(db_path):
        db_path = str(root / db_path)
    if not os.path.isfile(db_path):
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    if args.all:
        cutoff = None
        where_ts = ""
        params_last: tuple = ()
        params_hourly: tuple = ()
    else:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=args.hours)).isoformat()
        where_ts = " AND ts_utc >= ?"
        params_last = (cutoff,)
        params_hourly = (cutoff,)

    # 15-min last_90s
    print("=== strategy_report_last_90s (15-min) is_stop_loss=1 ===")
    cur.execute(
        """
        SELECT window_id, asset, ts_utc, ticker, side, price_cents, fill_price, resolution_price, final_outcome, pnl_cents, filled, is_stop_loss, stop_loss_sell_price
        FROM strategy_report_last_90s
        WHERE placed = 1 AND (is_stop_loss = 1 OR (is_stop_loss IS NULL AND final_outcome = 'STOP_LOSS'))"""
        + where_ts + "\nORDER BY ts_utc DESC",
        params_last,
    )
    rows = cur.fetchall()
    print(f"Count (stop loss): {len(rows)}")
    for r in rows:
        entry = r["fill_price"] or r["price_cents"]
        sell = r["stop_loss_sell_price"] if r["stop_loss_sell_price"] is not None else r["resolution_price"]
        ts = r["ts_utc"] or ""
        print(f"  {r['window_id']} | {r['asset']} | entry={entry}c sell={sell}c | final_outcome={r['final_outcome']} pnl={r['pnl_cents']} | {ts[:19]}")

    if args.by_price:
        cur.execute(
            """
            SELECT window_id, asset, ts_utc, ticker, side, price_cents, fill_price, resolution_price, final_outcome, pnl_cents
            FROM strategy_report_last_90s
            WHERE placed = 1 AND resolution_price IS NOT NULL AND resolution_price > 0 AND resolution_price < 100"""
            + where_ts + "\nORDER BY ts_utc DESC",
            params_last,
        )
        price_rows = cur.fetchall()
        print(f"Count (price heuristic: resolution_price 1-99): {len(price_rows)}")

    print()
    print("=== strategy_report_hourly_last_90s is_stop_loss=1 ===")
    cur.execute(
        """
        SELECT window_id, asset, ticker, side, ts_utc, price_cents, fill_price, resolution_price, final_outcome, pnl_cents, filled, is_stop_loss, stop_loss_sell_price
        FROM strategy_report_hourly_last_90s
        WHERE placed = 1 AND (is_stop_loss = 1 OR (is_stop_loss IS NULL AND final_outcome = 'STOP_LOSS'))"""
        + where_ts + "\nORDER BY ts_utc DESC",
        params_hourly,
    )
    rows = cur.fetchall()
    print(f"Count (stop loss): {len(rows)}")
    for r in rows:
        entry = r["fill_price"] or r["price_cents"]
        sell = r["stop_loss_sell_price"] if r["stop_loss_sell_price"] is not None else r["resolution_price"]
        ts = r["ts_utc"] or ""
        print(f"  {r['window_id']} | {r['asset']} | {r['ticker']} | {r['side']} | entry={entry}c sell={sell}c | final_outcome={r['final_outcome']} pnl={r['pnl_cents']} | {ts[:19]}")

    if args.by_price:
        cur.execute(
            """
            SELECT window_id, asset, ticker, side, ts_utc, price_cents, fill_price, resolution_price, final_outcome, pnl_cents
            FROM strategy_report_hourly_last_90s
            WHERE placed = 1 AND resolution_price IS NOT NULL AND resolution_price > 0 AND resolution_price < 100"""
            + where_ts + "\nORDER BY ts_utc DESC",
            params_hourly,
        )
        price_rows = cur.fetchall()
        print(f"Count (price heuristic: resolution_price 1-99): {len(price_rows)}")

    conn.close()


if __name__ == "__main__":
    main()
