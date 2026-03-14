#!/usr/bin/env python3
"""
Backfill is_stop_loss=1 and stop_loss_sell_price in the strategy report DB from bot log.

Parses log for JSON events only (plain lines do not contain window_id/hour_market_id):
  - LAST_90S_STOP_LOSS_REPORT (market_id, asset, sell_price_cents)
  - HOURLY_LAST_90S_STOP_LOSS_REPORT (hour_market_id, asset, ticker, side, sell_price_cents)

For each event we UPDATE the existing report row (by window_id+asset or window_id+asset+ticker+side)
with is_stop_loss=1 and stop_loss_sell_price, preserving filled/fill_price/final_outcome/pnl_cents.

Usage:
  python tools/backfill_stop_loss_from_log.py [--log logs/bot.log] [--db data/bot_state.db] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

EVENT_LAST_90S = "LAST_90S_STOP_LOSS_REPORT"
EVENT_HOURLY = "HOURLY_LAST_90S_STOP_LOSS_REPORT"
LAST_90S_TABLE = "strategy_report_last_90s"
HOURLY_LAST_90S_TABLE = "strategy_report_hourly_last_90s"

def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _get_db_path(config_path: str | None) -> str:
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


def _parse_log_events(log_path: str):
    """Yield (kind, payload) where kind is 'last_90s'|'hourly', payload has window_id, asset, sell_price_cents; hourly has ticker, side."""
    path = Path(log_path)
    if not path.exists():
        return
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Prefer JSON
            if EVENT_LAST_90S in line and "{" in line:
                idx = line.find("{")
                try:
                    obj = json.loads(line[idx:])
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict) and obj.get("event") == EVENT_LAST_90S:
                    mid = obj.get("market_id")
                    asset = (obj.get("asset") or "").strip().lower()
                    sell = obj.get("sell_price_cents")
                    if mid and asset and sell is not None:
                        yield ("last_90s", {"window_id": mid, "asset": asset, "sell_price_cents": int(sell)})
                    continue
            if EVENT_HOURLY in line and "{" in line:
                idx = line.find("{")
                try:
                    obj = json.loads(line[idx:])
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict) and obj.get("event") == EVENT_HOURLY:
                    mid = obj.get("hour_market_id")
                    asset = (obj.get("asset") or "").strip().lower()
                    ticker = (obj.get("ticker") or "").strip()
                    side = (obj.get("side") or "yes").strip().lower()
                    sell = obj.get("sell_price_cents")
                    if mid and asset and ticker and sell is not None:
                        yield ("hourly", {"window_id": mid, "asset": asset, "ticker": ticker, "side": side, "sell_price_cents": int(sell)})
                    continue


def _get_last_90s_row(conn: sqlite3.Connection, window_id: str, asset: str) -> dict | None:
    cur = conn.cursor()
    cur.execute(
        f"SELECT filled, fill_price, resolution_price, final_outcome, pnl_cents, order_id, is_stop_loss FROM {LAST_90S_TABLE} WHERE window_id = ? AND asset = ?",
        (window_id, asset.lower()),
    )
    row = cur.fetchone()
    if not row:
        return None
    if row[6] == 1:  # already backfilled
        return "skip"
    return {
        "filled": row[0] or 0,
        "fill_price": row[1],
        "resolution_price": row[2],
        "final_outcome": row[3],
        "pnl_cents": row[4],
        "order_id": row[5],
    }


def _get_hourly_row(conn: sqlite3.Connection, window_id: str, asset: str, ticker: str, side: str) -> dict | None:
    cur = conn.cursor()
    cur.execute(
        f"SELECT filled, fill_price, resolution_price, final_outcome, pnl_cents, order_id, is_stop_loss FROM {HOURLY_LAST_90S_TABLE} WHERE window_id = ? AND asset = ? AND ticker = ? AND side = ?",
        (window_id, asset.lower(), ticker, (side or "yes").lower()),
    )
    row = cur.fetchone()
    if not row:
        return None
    if row[6] == 1:
        return "skip"
    return {
        "filled": row[0] or 0,
        "fill_price": row[1],
        "resolution_price": row[2],
        "final_outcome": row[3],
        "pnl_cents": row[4],
        "order_id": row[5],
    }


def _update_last_90s_stop_loss(conn: sqlite3.Connection, window_id: str, asset: str, sell_price_cents: int, dry_run: bool) -> str:
    """Return 'updated' | 'skipped_no_row' | 'skipped_already'."""
    row = _get_last_90s_row(conn, window_id, asset)
    if row is None:
        return "skipped_no_row"
    if row == "skip":
        return "skipped_already"
    if dry_run:
        return "updated"
    cur = conn.cursor()
    set_parts = ["filled = ?", "fill_price = ?", "resolution_price = ?", "pnl_cents = ?", "is_stop_loss = ?", "stop_loss_sell_price = ?"]
    params = [row["filled"], row["fill_price"], row["resolution_price"], row["pnl_cents"], 1, sell_price_cents]
    if row["final_outcome"] is not None:
        set_parts.append("final_outcome = ?")
        params.append(row["final_outcome"])
    cur.execute(
        f"UPDATE {LAST_90S_TABLE} SET {', '.join(set_parts)} WHERE window_id = ? AND asset = ?",
        params + [window_id, asset.lower()],
    )
    return "updated" if cur.rowcount > 0 else "skipped_no_row"


def _update_hourly_stop_loss(
    conn: sqlite3.Connection,
    window_id: str,
    asset: str,
    ticker: str,
    side: str,
    sell_price_cents: int,
    dry_run: bool,
) -> str:
    row = _get_hourly_row(conn, window_id, asset, ticker, side)
    if row is None:
        return "skipped_no_row"
    if row == "skip":
        return "skipped_already"
    if dry_run:
        return "updated"
    cur = conn.cursor()
    set_parts = ["filled = ?", "fill_price = ?", "resolution_price = ?", "pnl_cents = ?", "is_stop_loss = ?", "stop_loss_sell_price = ?"]
    params = [row["filled"], row["fill_price"], row["resolution_price"], row["pnl_cents"], 1, sell_price_cents]
    if row["final_outcome"] is not None:
        set_parts.append("final_outcome = ?")
        params.append(row["final_outcome"])
    cur.execute(
        f"UPDATE {HOURLY_LAST_90S_TABLE} SET {', '.join(set_parts)} WHERE window_id = ? AND asset = ? AND ticker = ? AND side = ?",
        params + [window_id, asset.lower(), ticker, (side or "yes").lower()],
    )
    return "updated" if cur.rowcount > 0 else "skipped_no_row"


def main() -> None:
    ap = argparse.ArgumentParser(description="Backfill is_stop_loss and stop_loss_sell_price from bot log")
    ap.add_argument("--log", default=None, help="Bot log path (default: config logging.file or logs/bot.log)")
    ap.add_argument("--db", default=None, help="Report/state DB path")
    ap.add_argument("--dry-run", action="store_true", help="Only print what would be updated")
    args = ap.parse_args()

    root = _project_root()
    sys.path.insert(0, str(root))

    log_path = args.log
    if not log_path:
        config_path = os.environ.get("CONFIG_PATH", str(root / "config" / "hourly.yaml"))
        try:
            import yaml
            with open(config_path) as f:
                log_path = (yaml.safe_load(f) or {}).get("logging", {}) or {}
                log_path = log_path.get("file", "logs/bot.log")
        except Exception:
            log_path = "logs/bot.log"
    if not Path(log_path).is_absolute():
        log_path = str(root / log_path)
    if not os.path.isfile(log_path):
        print(f"Log file not found: {log_path}", file=sys.stderr)
        sys.exit(1)

    db_path = args.db
    if not db_path:
        config_path = os.environ.get("CONFIG_PATH", str(root / "config" / "hourly.yaml"))
        db_path = _get_db_path(config_path)
    if not os.path.isabs(db_path):
        db_path = str(root / db_path)
    if not os.path.isfile(db_path):
        print(f"DB not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    raw = list(_parse_log_events(log_path))
    # Dedupe by key (last occurrence wins)
    seen_last_90s: dict[tuple[str, str], dict] = {}
    seen_hourly: dict[tuple[str, str, str, str], dict] = {}
    for kind, payload in raw:
        if kind == "last_90s":
            key = (payload["window_id"], payload["asset"])
            seen_last_90s[key] = payload
        else:
            key = (payload["window_id"], payload["asset"], payload["ticker"], payload["side"])
            seen_hourly[key] = payload
    events = [("last_90s", p) for p in seen_last_90s.values()] + [("hourly", p) for p in seen_hourly.values()]
    if not events:
        print("No stop-loss events found in log.")
        return

    conn = sqlite3.connect(db_path)
    try:
        from bot.strategy_report_db import _ensure_hourly_last_90s_table, _ensure_last_90s_table
        _ensure_last_90s_table(conn)
        _ensure_hourly_last_90s_table(conn)
    except Exception as e:
        print(f"Warning: could not ensure report tables: {e}", file=sys.stderr)
    updated_last_90s = 0
    updated_hourly = 0
    skipped_no_row_last_90s = 0
    skipped_no_row_hourly = 0
    skipped_already_last_90s = 0
    skipped_already_hourly = 0
    try:
        for kind, payload in events:
            if kind == "last_90s":
                w, a, sell = payload["window_id"], payload["asset"], payload["sell_price_cents"]
                status = _update_last_90s_stop_loss(conn, w, a, sell, args.dry_run)
                if status == "updated":
                    updated_last_90s += 1
                    print(f"  last_90s  {w} | {a} | stop_loss_sell_price={sell}c")
                elif status == "skipped_no_row":
                    skipped_no_row_last_90s += 1
                else:
                    skipped_already_last_90s += 1
            else:
                w, a, t, s, sell = payload["window_id"], payload["asset"], payload["ticker"], payload["side"], payload["sell_price_cents"]
                status = _update_hourly_stop_loss(conn, w, a, t, s, sell, args.dry_run)
                if status == "updated":
                    updated_hourly += 1
                    print(f"  hourly    {w} | {a} | {t} | {s} | stop_loss_sell_price={sell}c")
                elif status == "skipped_no_row":
                    skipped_no_row_hourly += 1
                else:
                    skipped_already_hourly += 1
        if not args.dry_run:
            conn.commit()
    finally:
        conn.close()

    print()
    print(f"Events parsed (deduped): {len(events)}")
    print(f"Last_90s:  updated={updated_last_90s}  no_row={skipped_no_row_last_90s}  already_set={skipped_already_last_90s}")
    print(f"Hourly:    updated={updated_hourly}  no_row={skipped_no_row_hourly}  already_set={skipped_already_hourly}")
    if args.dry_run:
        print("(dry-run: no changes written)")


if __name__ == "__main__":
    main()
