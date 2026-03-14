#!/usr/bin/env python3
"""
Inspect ATM breakout strategy_report_atm_breakout row(s) for a given 15-min window.
Prints when we took the bid (entry), exit reason, and full post-placement telemetry
(yes_bid, no_bid, our_bid per tick) to verify stop-loss / safe-zone behavior.

Usage:
  python tools/inspect_atm_breakout_trade.py --market 26MAR061615 --asset btc
  python tools/inspect_atm_breakout_trade.py --market 26MAR061615 --asset btc --db data/bot_state.db
  python tools/inspect_atm_breakout_trade.py --market 26MAR061615 --asset btc --output atm_breakout_26MAR061615_btc.csv

4:15 PM EST March 6 → market id pattern 26MAR061615 (KXBTC15M-26MAR061615).
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from bot.state import ensure_state_db

ATM_BREAKOUT_TABLE = "strategy_report_atm_breakout"


def get_db_path(project_root: Path, cli_db: str | None, config_path: str | None) -> str:
    if cli_db and Path(cli_db).exists():
        return cli_db
    if config_path and Path(config_path).exists():
        try:
            from bot.config_loader import load_config
            config = load_config(config_path)
            return (config.get("state") or {}).get("db_path", str(project_root / "data" / "bot_state.db"))
        except Exception:
            pass
    return str(project_root / "data" / "bot_state.db")


def main() -> None:
    ap = argparse.ArgumentParser(description="Inspect ATM breakout DB rows for a 15-min window (e.g. 4:15 PM = 26MAR061615)")
    ap.add_argument("--market", default="26MAR061615", help="Window pattern e.g. 26MAR061615 for 4:15 PM EST Mar 6")
    ap.add_argument("--asset", default="btc", help="Asset (btc, eth, sol, xrp)")
    ap.add_argument("--db", help="Path to bot_state.db (default: config state.db_path or data/bot_state.db)")
    ap.add_argument("--config", default=None, help="Config file to read state.db_path from")
    ap.add_argument("--output", "-o", help="Write table dump to CSV (column names same as DB table)")
    args = ap.parse_args()

    db_path = get_db_path(PROJECT_ROOT, args.db, args.config or str(PROJECT_ROOT / "config" / "common.yaml"))
    if not Path(db_path).exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    ensure_state_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            f"SELECT * FROM {ATM_BREAKOUT_TABLE} WHERE market_id LIKE ? AND LOWER(asset) = LOWER(?) ORDER BY ts_utc ASC",
            (f"%{args.market.strip()}%", args.asset.strip()),
        )
        rows = cur.fetchall()
        col_names = [d[0] for d in cur.description]
    finally:
        conn.close()

    if not rows:
        print(f"No rows for market_id ~ {args.market}, asset={args.asset} in {db_path}")
        return

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=col_names, extrasaction="ignore")
            w.writeheader()
            for row in rows:
                w.writerow(dict(row))
        print(f"Wrote {len(rows)} row(s) to {out_path} (columns: {col_names})")

    for row in rows:
        r = dict(row)
        print("=" * 60)
        print("ORDER_ID:", r.get("order_id"))
        print("MARKET_ID:", r.get("market_id"))
        print("ASSET:", r.get("asset"))
        print("TS_UTC:", r.get("ts_utc"))
        print("DIRECTION:", r.get("direction"))
        print("ENTRY_PRICE (cents):", r.get("entry_price"))
        print("EXIT_PRICE (cents):", r.get("exit_price"))
        print("EXIT_REASON:", r.get("exit_reason"))
        print("TIME_IN_TRADE_SECONDS:", r.get("time_in_trade_seconds"))
        print()

        telemetry_raw = r.get("trade_telemetry_history")
        if telemetry_raw:
            try:
                telemetry = json.loads(telemetry_raw)
                direction = (r.get("direction") or "YES").upper()
                print(f"POST-PLACEMENT TELEMETRY (direction={direction}, so our_bid = {'yes_bid' if direction == 'YES' else 'no_bid'}):")
                print("-" * 60)
                for i, tick in enumerate(telemetry):
                    elapsed = tick.get("elapsed")
                    yb = tick.get("yes_bid")
                    nb = tick.get("no_bid")
                    our = tick.get("our_bid")
                    print(f"  tick {i+1:3d}  elapsed={elapsed:6.2f}s  yes_bid={yb:3}  no_bid={nb:3}  our_bid={our:3}")
                print()
                # Summary for stop-loss check: if direction NO, no_bid <= 35 should have triggered
                if direction == "NO":
                    low_no = min((t.get("no_bid") for t in telemetry if t.get("no_bid") is not None), default=None)
                    if low_no is not None:
                        print(f"[Stop-loss check] Min no_bid in telemetry: {low_no}  (stop_loss 35 would trigger if <= 35)")
                else:
                    low_yes = min((t.get("yes_bid") for t in telemetry if t.get("yes_bid") is not None), default=None)
                    if low_yes is not None:
                        print(f"[Stop-loss check] Min yes_bid in telemetry: {low_yes}  (stop_loss 35 would trigger if <= 35)")
            except Exception as e:
                print("TELEMETRY (raw, parse failed):", telemetry_raw[:500], "...", e)
        else:
            print("No trade_telemetry_history (e.g. NoTrade row or pre-placement only).")

        pre_placement = r.get("pre_placement_history")
        if pre_placement and pre_placement != "[]":
            try:
                pre = json.loads(pre_placement)
                print()
                print(f"PRE-PLACEMENT HISTORY (last 5 of {len(pre)} ticks):")
                for tick in pre[-5:]:
                    print("  ", tick)
            except Exception:
                pass
        print()

    print(f"Total rows: {len(rows)}")


if __name__ == "__main__":
    main()
