#!/usr/bin/env python3
"""
Inspect ATM stop-loss for SOL 26MAR122100: registry, telemetry, and strategy reports.
Run from project root: python3 tools/inspect_atm_stoploss_sol_26MAR122100.py
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "v2_state.db"

# Window of interest: 26MAR122100 (SOL, 12 Mar 2026 21:00 ET)
WINDOW = "26MAR122100"
ASSET = "sol"
STRATEGY = "atm_breakout_strategy"


def main() -> int:
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    print("=== 1. v2_order_registry: ATM orders for SOL with market_id containing 26MAR122100 ===\n")
    cur = conn.execute(
        """
        SELECT order_id, strategy_id, interval, market_id, asset, ticker, side, status,
               filled_count, count, limit_price_cents, placed_at
        FROM v2_order_registry
        WHERE strategy_id = ? AND asset = ? AND (market_id LIKE ? OR ticker LIKE ?)
        ORDER BY placed_at DESC
        """,
        (STRATEGY, ASSET, f"%{WINDOW}%", f"%{WINDOW}%"),
    )
    rows = cur.fetchall()
    if not rows:
        print("  (no rows)\n")
    else:
        for r in rows:
            print(dict(r))
        print()

    print("=== 2. v2_telemetry_atm: rows for window containing 26MAR122100, asset SOL ===\n")
    try:
        cur = conn.execute(
            """
            SELECT * FROM v2_telemetry_atm
            WHERE window_id LIKE ? AND asset = ?
            ORDER BY timestamp ASC
            LIMIT 50
            """,
            (f"%{WINDOW}%", ASSET),
        )
        rows = cur.fetchall()
        if not rows:
            cur = conn.execute(
                "SELECT * FROM v2_telemetry_atm WHERE asset = ? ORDER BY timestamp DESC LIMIT 15",
                (ASSET,),
            )
            rows = cur.fetchall()
            print("  (no rows for this window; showing last 15 SOL telemetry rows)\n")
        for r in rows:
            print(dict(r))
    except sqlite3.OperationalError as e:
        print(f"  Error: {e}\n")
    print()

    print("=== 3. v2_strategy_reports: ATM SOL (any window) ===\n")
    try:
        cur = conn.execute(
            """
            SELECT * FROM v2_strategy_reports
            WHERE strategy_id = ? AND asset = ?
            ORDER BY resolved_at DESC
            LIMIT 20
            """,
            (STRATEGY, ASSET),
        )
        rows = cur.fetchall()
        if not rows:
            print("  (no rows)\n")
        else:
            for r in rows:
                d = dict(r)
                if WINDOW in (d.get("window_id") or ""):
                    print("  ^^^ 26MAR122100 ^^^")
                print(d)
    except sqlite3.OperationalError as e:
        print(f"  Error: {e}\n")

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
