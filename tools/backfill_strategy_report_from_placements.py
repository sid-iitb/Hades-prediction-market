#!/usr/bin/env python3
"""
Backfill strategy_report table from existing placement TSV files so the hourly
strategy report email shows historical data (instead of all zeros).

The strategy_report table is normally populated when the main bot runs with
last_90s_limit_99 and hourly_last_90s_limit_99 and processes signals. If you
only run the report daemon or haven't run the bot since that feature was added,
the table is empty. This script imports from:
  - reports/last_90s_placements.tsv + last_90s_skips.tsv  -> last_90s_limit_99
  - reports/hourly_last_90s_placements.tsv + hourly_last_90s_skips.tsv -> hourly_last_90s_limit_99
  Sets filled=1 when status is executed (from placement TSV). Win/Loss/Stop loss from final_outcome when present.

Usage:
  python tools/backfill_strategy_report_from_placements.py [--config config/config.yaml] [--dry-run]
"""
from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _db_path(project_root: Path, config_path: str) -> str:
    try:
        from bot.config_loader import load_config, resolve_config_path
        cfg_path = Path(config_path) if config_path and Path(config_path).exists() else resolve_config_path(project_root)
        if not cfg_path.exists():
            return str(project_root / "data" / "bot_state.db")
        config = load_config(str(cfg_path))
        return (config.get("state") or {}).get("db_path", str(project_root / "data" / "bot_state.db"))
    except Exception:
        return str(project_root / "data" / "bot_state.db")


def _window_id_15m(ts_utc: str) -> str:
    """Round ts_utc down to 15-minute boundary for use as window_id (e.g. 2026-02-27T21:28:57 -> 2026-02-27T21:15)."""
    try:
        dt = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        mins = dt.hour * 60 + dt.minute
        slot = (mins // 15) * 15
        h, m = slot // 60, slot % 60
        return dt.strftime("%Y-%m-%d") + f"T{h:02d}:{m:02d}"
    except Exception:
        return ts_utc[:16] if len(ts_utc) >= 16 else ts_utc


def _backfill_last_90s(db_path: str, reports_dir: Path, dry_run: bool) -> int:
    path = reports_dir / "last_90s_placements.tsv"
    if not path.exists():
        print(f"[backfill] Skip (not found): {path}")
        return 0
    from bot.strategy_report_db import ensure_report_db, upsert_candidate
    if not dry_run:
        ensure_report_db(db_path)
    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            ts = (row.get("ts_utc") or "").strip()
            asset = (row.get("asset") or "btc").strip().lower()
            ticker = (row.get("ticker") or "").strip()
            side = (row.get("side") or "yes").strip().lower()
            order_id = (row.get("order_id") or "").strip()
            price_cents = row.get("price_cents")
            if not ts or not ticker:
                continue
            try:
                price_cents = int(price_cents) if price_cents not in (None, "", "n/a") else 99
            except (TypeError, ValueError):
                price_cents = 99
            # Use ts_utc as window_id so each placement is one row (unique per order)
            window_id = ts
            if dry_run:
                count += 1
                continue
            try:
                upsert_candidate(
                    db_path,
                    "last_90s_limit_99",
                    window_id,
                    asset,
                    ticker,
                    side,
                    market=window_id,
                    price_cents=price_cents,
                    placed=1,
                    order_id=order_id or None,
                )
                count += 1
                from bot.strategy_report_db import update_resolution
                final_outcome = (row.get("final_outcome") or "").strip()
                executed = row.get("executed") in (1, "1", "1.0", True) or (row.get("status") or "").strip().lower() == "executed"
                if order_id and executed:
                    update_resolution(db_path, order_id, filled=1, final_outcome=final_outcome if final_outcome and final_outcome.lower() not in ("n/a", "") else None)
                elif order_id and final_outcome and final_outcome.lower() not in ("n/a", ""):
                    update_resolution(db_path, order_id, filled=1, final_outcome=final_outcome)
            except Exception as e:
                print(f"[backfill] last_90s row error: {e}", file=sys.stderr)
    return count


def _backfill_hourly(db_path: str, reports_dir: Path, dry_run: bool) -> int:
    path = reports_dir / "hourly_last_90s_placements.tsv"
    if not path.exists():
        print(f"[backfill] Skip (not found): {path}")
        return 0
    from bot.strategy_report_db import ensure_report_db, upsert_candidate
    if not dry_run:
        ensure_report_db(db_path)
    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            ts = (row.get("ts_utc") or "").strip()
            asset = (row.get("asset") or "btc").strip().lower()
            ticker = (row.get("ticker") or "").strip()
            side = (row.get("side") or "yes").strip().lower()
            order_id = (row.get("order_id") or "").strip()
            hour_market_id = (row.get("hour_market_id") or "").strip()
            price_cents = row.get("price_cents")
            if not ts or not ticker:
                continue
            # Prefer hour_market_id; fallback to base ticker; use ts for uniqueness if needed
            base_id = hour_market_id or re.sub(r"-T\d.*", "", ticker) or ticker
            window_id = f"{base_id}_{ts}" if base_id else ts
            try:
                price_cents = int(price_cents) if price_cents not in (None, "", "n/a") else 99
            except (TypeError, ValueError):
                price_cents = 99
            if dry_run:
                count += 1
                continue
            try:
                upsert_candidate(
                    db_path,
                    "hourly_last_90s_limit_99",
                    window_id,
                    asset,
                    ticker,
                    side,
                    market=window_id,
                    price_cents=price_cents,
                    placed=1,
                    order_id=order_id or None,
                )
                count += 1
                from bot.strategy_report_db import update_resolution
                final_outcome = (row.get("final_outcome") or "").strip()
                executed = row.get("executed") in (1, "1", "1.0", True) or (row.get("status") or "").strip().lower() == "executed"
                if order_id and executed:
                    update_resolution(db_path, order_id, filled=1, final_outcome=final_outcome if final_outcome and final_outcome.lower() not in ("n/a", "") else None)
                elif order_id and final_outcome and final_outcome.lower() not in ("n/a", ""):
                    update_resolution(db_path, order_id, filled=1, final_outcome=final_outcome)
            except Exception as e:
                print(f"[backfill] hourly row error: {e}", file=sys.stderr)
    return count


def _backfill_last_90s_skips(db_path: str, reports_dir: Path, dry_run: bool) -> int:
    path = reports_dir / "last_90s_skips.tsv"
    if not path.exists():
        return 0
    from bot.strategy_report_db import ensure_report_db, upsert_candidate
    if not dry_run:
        ensure_report_db(db_path)
    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            ts = (row.get("ts_utc") or "").strip()
            asset = (row.get("asset") or "btc").strip().lower()
            ticker = (row.get("ticker") or "").strip()
            window_id = (row.get("window_id") or ts or "").strip()
            skip_reason = (row.get("skip_reason") or "").strip() or "unknown"
            skip_details = (row.get("details") or "").strip() or None
            if not window_id:
                continue
            side = "yes"
            if dry_run:
                count += 1
                continue
            try:
                upsert_candidate(
                    db_path,
                    "last_90s_limit_99",
                    window_id,
                    asset,
                    ticker,
                    side,
                    market=window_id,
                    skip_reason=skip_reason,
                    skip_details=skip_details,
                    placed=0,
                )
                if ts:
                    conn = sqlite3.connect(db_path)
                    try:
                        conn.execute(
                            "UPDATE strategy_report SET ts_utc = ? WHERE strategy_name = ? AND window_id = ? AND asset = ? AND ticker = ? AND side = ?",
                            (ts, "last_90s_limit_99", window_id, asset, ticker, side),
                        )
                        conn.commit()
                    finally:
                        conn.close()
                count += 1
            except Exception as e:
                print(f"[backfill] last_90s skip row error: {e}", file=sys.stderr)
    return count


def _backfill_hourly_skips(db_path: str, reports_dir: Path, dry_run: bool) -> int:
    path = reports_dir / "hourly_last_90s_skips.tsv"
    if not path.exists():
        return 0
    from bot.strategy_report_db import ensure_report_db, upsert_candidate
    if not dry_run:
        ensure_report_db(db_path)
    count = 0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            ts = (row.get("ts_utc") or "").strip()
            asset = (row.get("asset") or "btc").strip().lower()
            ticker = (row.get("ticker") or "").strip()
            window_id = (row.get("window_id") or "").strip()
            skip_reason = (row.get("skip_reason") or "").strip() or "unknown"
            skip_details = (row.get("details") or "").strip() or None
            if not window_id:
                continue
            side = "yes"
            window_id_uniq = f"{window_id}_{ts}" if ts else window_id
            if dry_run:
                count += 1
                continue
            try:
                upsert_candidate(
                    db_path,
                    "hourly_last_90s_limit_99",
                    window_id_uniq,
                    asset,
                    ticker,
                    side,
                    market=window_id,
                    skip_reason=skip_reason,
                    skip_details=skip_details,
                    placed=0,
                )
                if ts:
                    conn = sqlite3.connect(db_path)
                    try:
                        conn.execute(
                            "UPDATE strategy_report SET ts_utc = ? WHERE strategy_name = ? AND window_id = ? AND asset = ? AND ticker = ? AND side = ?",
                            (ts, "hourly_last_90s_limit_99", window_id_uniq, asset, ticker, side),
                        )
                        conn.commit()
                    finally:
                        conn.close()
                count += 1
            except Exception as e:
                print(f"[backfill] hourly skip row error: {e}", file=sys.stderr)
    return count


def main() -> int:
    ap = argparse.ArgumentParser(description="Backfill strategy_report from placement TSVs")
    ap.add_argument("--config", default="", help="Config path (for db_path)")
    ap.add_argument("--dry-run", action="store_true", help="Only count rows, do not write")
    args = ap.parse_args()
    project_root = PROJECT_ROOT
    reports_dir = project_root / "reports"
    db_path = _db_path(project_root, args.config or str(project_root / "config" / "config.yaml"))
    print(f"DB: {db_path}")
    if args.dry_run:
        print("Dry run: no writes")
    n1 = _backfill_last_90s(db_path, reports_dir, args.dry_run)
    n2 = _backfill_hourly(db_path, reports_dir, args.dry_run)
    n3 = _backfill_last_90s_skips(db_path, reports_dir, args.dry_run)
    n4 = _backfill_hourly_skips(db_path, reports_dir, args.dry_run)
    print(f"last_90s_limit_99: {n1} placements, {n3} skips")
    print(f"hourly_last_90s_limit_99: {n2} placements, {n4} skips")
    return 0


if __name__ == "__main__":
    sys.exit(main())
