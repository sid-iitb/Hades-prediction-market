#!/usr/bin/env python3
"""
6-hour sandbox phase review for last_90s strategy.

Queries strategy_report_last_90s for the last 6 hours and writes a clean
summary to sandbox_6hr_summary.txt:
  1. Execution breakdown (placed count, limit vs market, avg execution price for market)
  2. Skip breakdown (skipped count, primary skip reasons from skip_details)
  3. Stop-loss & PNL (stop-loss count, avg stop_loss_sell_price, WIN vs LOSS)

Usage:
  python tools/sandbox_6hr_review.py
  python tools/sandbox_6hr_review.py --hours 6 --out sandbox_6hr_summary.txt
"""
from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path


def _since_ts(hours: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")


def _extract_primary_blocker(skip_details: str | None) -> str | None:
    """Extract 'Primary blocker: X' from skip_details; else return skip_reason or 'unknown'."""
    if not skip_details:
        return None
    m = re.search(r"Primary blocker:\s*(\S+)", skip_details, re.IGNORECASE)
    return m.group(1).strip() if m else None


def main() -> int:
    ap = argparse.ArgumentParser(description="6-hour sandbox review from strategy_report_last_90s")
    ap.add_argument("--db", default=None, help="Path to bot_state.db (default: data/bot_state.db)")
    ap.add_argument("--hours", type=float, default=6.0, help="Look back hours (default 6)")
    ap.add_argument("--out", default="sandbox_6hr_summary.txt", help="Output report path")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent.parent
    db_path = Path(args.db) if args.db else root / "data" / "bot_state.db"
    out_path = root / args.out if not Path(args.out).is_absolute() else Path(args.out)

    since = _since_ts(args.hours)
    out_lines: list[str] = []

    def log(s: str = "") -> None:
        out_lines.append(s)

    log("=" * 60)
    log("  SANDBOX 6-HOUR PHASE REVIEW — last_90s strategy")
    log("  Window: last %.1f hours (since %s UTC)" % (args.hours, since))
    log("  DB: %s" % db_path)
    log("=" * 60)

    if not db_path.is_file():
        log("ERROR: DB not found: %s" % db_path)
        out_path.write_text("\n".join(out_lines), encoding="utf-8")
        return 1

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='strategy_report_last_90s'"
    )
    if not cur.fetchone():
        log("ERROR: Table strategy_report_last_90s not found.")
        conn.close()
        out_path.write_text("\n".join(out_lines), encoding="utf-8")
        return 1

    # --- 1. Execution breakdown ---
    log("")
    log("--- 1. Execution breakdown ---")
    cur.execute(
        """
        SELECT placed, limit_or_market, price_cents, fill_price
        FROM strategy_report_last_90s
        WHERE ts_utc >= ?
        """,
        (since,),
    )
    rows = cur.fetchall()
    placed_rows = [r for r in rows if r["placed"] == 1]
    total_placed = len(placed_rows)
    limit_count = sum(1 for r in placed_rows if (r["limit_or_market"] or "").lower() == "limit")
    market_count = sum(1 for r in placed_rows if (r["limit_or_market"] or "").lower() == "market")

    log("  Total placed orders (placed == 1): %d" % total_placed)
    log("  By type: limit = %d, market = %d" % (limit_count, market_count))

    market_rows = [r for r in placed_rows if (r["limit_or_market"] or "").lower() == "market"]
    if market_rows:
        prices = []
        for r in market_rows:
            p = r["fill_price"] if r["fill_price"] is not None else r["price_cents"]
            if p is not None:
                prices.append(int(p))
        if prices:
            avg_price = sum(prices) / len(prices)
            log("  Market orders: count = %d, avg execution price = %.2f¢ (fill_price when set, else price_cents)" % (len(market_rows), avg_price))
        else:
            log("  Market orders: count = %d, avg execution price = (no price data)" % len(market_rows))
    else:
        log("  Market orders: none in window; avg execution price = N/A")

    # --- 2. Skip breakdown ---
    log("")
    log("--- 2. Skip breakdown ---")
    total_skipped = sum(1 for r in rows if (r["placed"] or 0) == 0)
    log("  Total skipped opportunities (placed == 0): %d" % total_skipped)

    cur.execute(
        """
        SELECT skip_reason, skip_details
        FROM strategy_report_last_90s
        WHERE ts_utc >= ? AND (placed = 0 OR placed IS NULL)
        """,
        (since,),
    )
    reason_tally: Counter = Counter()
    for r in cur.fetchall():
        primary = _extract_primary_blocker(r["skip_details"])
        if primary:
            reason_tally[primary] += 1
        elif (r["skip_details"] or "").strip().startswith("No skip history"):
            reason_tally["no_history_recorded"] += 1
        else:
            reason_tally[r["skip_reason"] or "unknown"] += 1

    if reason_tally:
        log("  Primary skip reasons (from skip_details / skip_reason):")
        for reason, count in reason_tally.most_common():
            log("    %s: %d" % (reason, count))
    else:
        log("  No skip reason tally (no skipped rows or no parseable details).")

    # --- 3. Stop-loss & PNL health ---
    log("")
    log("--- 3. Stop-loss & PNL health ---")
    cur.execute(
        """
        SELECT order_id, stop_loss_sell_price, final_outcome, pnl_cents
        FROM strategy_report_last_90s
        WHERE ts_utc >= ? AND is_stop_loss = 1
        """,
        (since,),
    )
    sl_rows = cur.fetchall()
    sl_count = len(sl_rows)
    log("  Stop-losses triggered (is_stop_loss == 1): %d" % sl_count)
    if sl_rows:
        prices = [r["stop_loss_sell_price"] for r in sl_rows if r["stop_loss_sell_price"] is not None]
        if prices:
            avg_sl = sum(prices) / len(prices)
            log("  Average stop_loss_sell_price: %.2f¢" % avg_sl)
        else:
            log("  Average stop_loss_sell_price: (no price data)")
    else:
        log("  Average stop_loss_sell_price: N/A (no stop-losses)")

    cur.execute(
        """
        SELECT final_outcome
        FROM strategy_report_last_90s
        WHERE ts_utc >= ? AND placed = 1 AND final_outcome IS NOT NULL AND final_outcome != ''
        """,
        (since,),
    )
    outcomes = [r["final_outcome"].strip().upper() for r in cur.fetchall() if r["final_outcome"]]
    win_count = sum(1 for o in outcomes if o == "WIN")
    loss_count = sum(1 for o in outcomes if o == "LOSS")
    log("  Completed windows (final_outcome): WIN = %d, LOSS = %d" % (win_count, loss_count))

    conn.close()

    log("")
    log("=" * 60)
    log("  End of report.")
    log("=" * 60)

    out_path.write_text("\n".join(out_lines), encoding="utf-8")
    print("Report written to %s" % out_path, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
