"""
Canonical strategy report DB: one row per candidate signal (before filters).
- last_90s_limit_99: uses strategy_report_last_90s (lean schema, one row per (window_id, asset)).
- hourly_last_90s_limit_99: uses strategy_report_hourly_last_90s (lean schema, one row per (window_id, asset, ticker, side)).
Resolution updates by order_id or (window_id, asset[, ticker, side]).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

REPORT_TABLE = "strategy_report"
LAST_90S_TABLE = "strategy_report_last_90s"
HOURLY_LAST_90S_TABLE = "strategy_report_hourly_last_90s"

# Lean columns for last_90s (one-row-per-window guarantee)
# final_outcome = WIN | LOSS from Kalshi resolution; is_stop_loss = 1 if we exited via stop loss; stop_loss_sell_price = cents when stopped out.
LAST_90S_COLUMNS = [
    "ts_utc",
    "window_id",
    "asset",
    "ticker",
    "side",
    "spot",
    "strike",
    "resolution_price",
    "seconds_to_close",
    "distance",
    "min_distance_threshold",
    "bid",
    "limit_or_market",
    "price_cents",
    "placed",
    "filled",
    "fill_price",
    "final_outcome",
    "pnl_cents",
    "is_stop_loss",
    "stop_loss_sell_price",
    "skip_reason",
    "skip_details",
    "order_id",
]

# Same lean columns for hourly_last_90s (one row per (window_id, asset, ticker, side))
HOURLY_LAST_90S_COLUMNS = list(LAST_90S_COLUMNS)

# Canonical columns for legacy report (strategy_report table only)
COLUMNS = [
    "ts_utc",
    "strategy_name",
    "window_id",
    "asset",
    "market",
    "ticker",
    "market_type",
    "side",
    "spot",
    "strike",
    "range_low",
    "range_high",
    "distance",
    "min_distance_threshold",
    "bid",
    "limit_or_market",
    "price_cents",
    "seconds_to_close",
    "signal_identified",
    "skip_reason",
    "skip_details",
    "placed",
    "order_id",
    "filled",
    "fill_price",
    "resolution_price",
    "final_outcome",
    "pnl_cents",
]


def _ensure_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {REPORT_TABLE} (
            strategy_name TEXT NOT NULL,
            window_id TEXT NOT NULL,
            asset TEXT NOT NULL,
            ticker TEXT NOT NULL,
            side TEXT NOT NULL,
            ts_utc TEXT,
            market TEXT,
            market_type TEXT,
            spot REAL,
            strike REAL,
            range_low REAL,
            range_high REAL,
            distance REAL,
            min_distance_threshold REAL,
            bid INTEGER,
            limit_or_market TEXT,
            price_cents INTEGER,
            seconds_to_close REAL,
            signal_identified INTEGER DEFAULT 1,
            skip_reason TEXT,
            skip_details TEXT,
            placed INTEGER DEFAULT 0,
            order_id TEXT,
            filled INTEGER DEFAULT 0,
            fill_price INTEGER,
            resolution_price INTEGER,
            final_outcome TEXT,
            pnl_cents INTEGER,
            updated_at TEXT,
            PRIMARY KEY (strategy_name, window_id, asset, ticker, side)
        )
        """
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{REPORT_TABLE}_order_id ON {REPORT_TABLE}(order_id)"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{REPORT_TABLE}_ts ON {REPORT_TABLE}(ts_utc)"
    )
    conn.commit()


def _add_column_if_missing(cur: sqlite3.Cursor, table: str, column: str, col_def: str) -> None:
    cur.execute(f"PRAGMA table_info({table})")
    if any(row[1] == column for row in cur.fetchall()):
        return
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_def}")


def _ensure_last_90s_table(conn: sqlite3.Connection) -> None:
    """Lean table for last_90s.

    Historical note:
    - v1 schema enforced a one-row-per-(window_id, asset) guarantee via PRIMARY KEY (window_id, asset).
      That prevented storing multiple orders per window/asset.
    - v2 schema uses PRIMARY KEY (order_id) so we can record multiple rows per window/asset while still
      resolving by order_id or (window_id, asset).
    """
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {LAST_90S_TABLE} (
            order_id TEXT PRIMARY KEY,
            window_id TEXT NOT NULL,
            asset TEXT NOT NULL,
            ts_utc TEXT,
            ticker TEXT,
            side TEXT,
            spot REAL,
            strike REAL,
            resolution_price INTEGER,
            seconds_to_close REAL,
            distance REAL,
            min_distance_threshold REAL,
            bid INTEGER,
            limit_or_market TEXT,
            price_cents INTEGER,
            placed INTEGER DEFAULT 0,
            filled INTEGER DEFAULT 0,
            fill_price INTEGER,
            final_outcome TEXT,
            pnl_cents INTEGER,
            is_stop_loss INTEGER DEFAULT 0,
            stop_loss_sell_price INTEGER,
            skip_reason TEXT,
            skip_details TEXT,
            is_filled INTEGER DEFAULT 0
        )
        """
    )
    _add_column_if_missing(cur, LAST_90S_TABLE, "is_filled", "INTEGER DEFAULT 0")
    # Dual-oracle (Kraken + Coinbase): store both and official spot/distance used for decision
    _add_column_if_missing(cur, LAST_90S_TABLE, "spot_kraken", "REAL")
    _add_column_if_missing(cur, LAST_90S_TABLE, "spot_coinbase", "REAL")
    _add_column_if_missing(cur, LAST_90S_TABLE, "distance_kraken", "REAL")
    _add_column_if_missing(cur, LAST_90S_TABLE, "distance_coinbase", "REAL")
    # Post-placement telemetry (MAE & full trajectory)
    _add_column_if_missing(cur, LAST_90S_TABLE, "min_dist_after_placement", "REAL")
    _add_column_if_missing(cur, LAST_90S_TABLE, "min_bid_after_placement", "INTEGER")
    _add_column_if_missing(cur, LAST_90S_TABLE, "post_placement_history", "TEXT")
    _add_column_if_missing(cur, LAST_90S_TABLE, "pre_placement_history", "TEXT")
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{LAST_90S_TABLE}_window_asset ON {LAST_90S_TABLE}(window_id, asset)"
    )
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{LAST_90S_TABLE}_ts ON {LAST_90S_TABLE}(ts_utc)")
    conn.commit()


def _migrate_last_90s_to_multi_order_table(conn: sqlite3.Connection) -> None:
    """If LAST_90S_TABLE still uses (window_id, asset) as PK, migrate to PK(order_id).

    This keeps existing data and synthesizes an order_id for rows that do not have one
    (e.g. aggregated skip rows) so that we can insert multiple rows per (window_id, asset).
    """
    cur = conn.cursor()
    # Detect existing schema
    cur.execute(f"PRAGMA table_info({LAST_90S_TABLE})")
    cols = cur.fetchall()
    if not cols:
        return
    pk_by_col = {row[1]: row[5] for row in cols}  # name -> pk index (0 = not PK)
    # New schema has order_id as the single PK.
    if pk_by_col.get("order_id"):
        return
    # Old schema: window_id / asset primary key, possibly without is_stop_loss/stop_loss_sell_price.
    cur.execute(f"ALTER TABLE {LAST_90S_TABLE} RENAME TO {LAST_90S_TABLE}_old")
    # Create new table with desired schema.
    _ensure_last_90s_table(conn)
    # Copy data over, synthesizing order_id when missing.
    cur.execute(
        f"""
        SELECT window_id, asset, ts_utc, ticker, side, spot, strike, resolution_price,
               seconds_to_close, distance, min_distance_threshold, bid, limit_or_market,
               price_cents, placed, filled, fill_price, final_outcome, pnl_cents,
               skip_reason, skip_details, order_id,
               COALESCE(is_stop_loss, 0) AS is_stop_loss,
               stop_loss_sell_price
        FROM {LAST_90S_TABLE}_old
        """
    )
    rows = cur.fetchall()
    for idx, row in enumerate(rows):
        (
            window_id,
            asset,
            ts_utc,
            ticker,
            side,
            spot,
            strike,
            resolution_price,
            seconds_to_close,
            distance,
            min_distance_threshold,
            bid,
            limit_or_market,
            price_cents,
            placed,
            filled,
            fill_price,
            final_outcome,
            pnl_cents,
            skip_reason,
            skip_details,
            order_id,
            is_stop_loss,
            stop_loss_sell_price,
        ) = row
        oid = (order_id or "").strip()
        if not oid:
            # Synthetic but deterministic ID to keep legacy aggregated rows addressable.
            oid = f"agg:{window_id}:{asset}:{idx}"
        cur.execute(
            f"""
            INSERT OR IGNORE INTO {LAST_90S_TABLE} (
                order_id, window_id, asset, ts_utc, ticker, side, spot, strike, resolution_price,
                seconds_to_close, distance, min_distance_threshold, bid, limit_or_market,
                price_cents, placed, filled, fill_price, final_outcome, pnl_cents,
                is_stop_loss, stop_loss_sell_price, skip_reason, skip_details
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                oid,
                window_id,
                asset,
                ts_utc,
                ticker,
                side,
                spot,
                strike,
                resolution_price,
                seconds_to_close,
                distance,
                min_distance_threshold,
                bid,
                limit_or_market,
                price_cents,
                placed,
                filled,
                fill_price,
                final_outcome,
                pnl_cents,
                is_stop_loss,
                stop_loss_sell_price,
                skip_reason,
                skip_details,
            ),
        )
    cur.execute(f"DROP TABLE IF EXISTS {LAST_90S_TABLE}_old")
    conn.commit()


def _ensure_hourly_last_90s_table(conn: sqlite3.Connection) -> None:
    """Lean table for hourly_last_90s: one row per (window_id, asset, ticker, side)."""
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HOURLY_LAST_90S_TABLE} (
            window_id TEXT NOT NULL,
            asset TEXT NOT NULL,
            ticker TEXT NOT NULL,
            side TEXT NOT NULL,
            ts_utc TEXT,
            spot REAL,
            strike REAL,
            resolution_price INTEGER,
            seconds_to_close REAL,
            distance REAL,
            min_distance_threshold REAL,
            bid INTEGER,
            limit_or_market TEXT,
            price_cents INTEGER,
            placed INTEGER DEFAULT 0,
            filled INTEGER DEFAULT 0,
            fill_price INTEGER,
            final_outcome TEXT,
            pnl_cents INTEGER,
            skip_reason TEXT,
            skip_details TEXT,
            order_id TEXT,
            PRIMARY KEY (window_id, asset, ticker, side)
        )
        """
    )
    _add_column_if_missing(cur, HOURLY_LAST_90S_TABLE, "is_stop_loss", "INTEGER DEFAULT 0")
    _add_column_if_missing(cur, HOURLY_LAST_90S_TABLE, "stop_loss_sell_price", "INTEGER")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{HOURLY_LAST_90S_TABLE}_order_id ON {HOURLY_LAST_90S_TABLE}(order_id)")
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{HOURLY_LAST_90S_TABLE}_ts ON {HOURLY_LAST_90S_TABLE}(ts_utc)")
    conn.commit()


def _migrate_hourly_last_90s_to_lean_table(conn: sqlite3.Connection) -> None:
    """One-time: copy hourly_last_90s_limit_99 rows from strategy_report into strategy_report_hourly_last_90s, then delete from strategy_report."""
    cur = conn.cursor()
    cur.execute(
        f"SELECT window_id, asset, ticker, side, ts_utc, spot, strike, resolution_price, seconds_to_close, "
        f"distance, min_distance_threshold, bid, limit_or_market, price_cents, placed, filled, fill_price, "
        f"final_outcome, pnl_cents, skip_reason, skip_details, order_id "
        f"FROM {REPORT_TABLE} WHERE strategy_name = ?",
        ("hourly_last_90s_limit_99",),
    )
    rows = cur.fetchall()
    if not rows:
        return
    cols = [
        "window_id", "asset", "ticker", "side", "ts_utc", "spot", "strike", "resolution_price", "seconds_to_close",
        "distance", "min_distance_threshold", "bid", "limit_or_market", "price_cents", "placed", "filled", "fill_price",
        "final_outcome", "pnl_cents", "skip_reason", "skip_details", "order_id",
    ]
    cur.executemany(
        f"""
        INSERT INTO {HOURLY_LAST_90S_TABLE} (
            window_id, asset, ticker, side, ts_utc, spot, strike, resolution_price, seconds_to_close,
            distance, min_distance_threshold, bid, limit_or_market, price_cents, placed, filled, fill_price,
            final_outcome, pnl_cents, skip_reason, skip_details, order_id
        ) VALUES ({",".join("?" for _ in cols)})
        ON CONFLICT(window_id, asset, ticker, side) DO UPDATE SET
            ts_utc = excluded.ts_utc,
            spot = excluded.spot,
            strike = excluded.strike,
            resolution_price = excluded.resolution_price,
            seconds_to_close = excluded.seconds_to_close,
            distance = excluded.distance,
            min_distance_threshold = excluded.min_distance_threshold,
            bid = excluded.bid,
            limit_or_market = excluded.limit_or_market,
            price_cents = excluded.price_cents,
            placed = excluded.placed,
            filled = excluded.filled,
            fill_price = excluded.fill_price,
            final_outcome = excluded.final_outcome,
            pnl_cents = excluded.pnl_cents,
            skip_reason = excluded.skip_reason,
            skip_details = excluded.skip_details,
            order_id = COALESCE(excluded.order_id, {HOURLY_LAST_90S_TABLE}.order_id)
        """,
        rows,
    )
    cur.execute(f"DELETE FROM {REPORT_TABLE} WHERE strategy_name = ?", ("hourly_last_90s_limit_99",))
    conn.commit()


def ensure_report_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        _ensure_table(conn)
        _ensure_last_90s_table(conn)
        _migrate_last_90s_to_multi_order_table(conn)
        _ensure_hourly_last_90s_table(conn)
        _migrate_hourly_last_90s_to_lean_table(conn)
    finally:
        conn.close()


def upsert_candidate(
    db_path: str,
    strategy_name: str,
    window_id: str,
    asset: str,
    ticker: str,
    side: str,
    *,
    market: Optional[str] = None,
    market_type: Optional[str] = None,
    spot: Optional[float] = None,
    strike: Optional[float] = None,
    range_low: Optional[float] = None,
    range_high: Optional[float] = None,
    distance: Optional[float] = None,
    min_distance_threshold: Optional[float] = None,
    bid: Optional[int] = None,
    limit_or_market: str = "limit",
    price_cents: Optional[int] = None,
    seconds_to_close: Optional[float] = None,
    skip_reason: Optional[str] = None,
    skip_details: Optional[str] = None,
    placed: int = 0,
    order_id: Optional[str] = None,
) -> None:
    """Insert or replace one candidate row. Call when signal is identified; then call record_skip/record_place or update_resolution."""
    ts = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        _ensure_table(conn)
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO {REPORT_TABLE} (
                strategy_name, window_id, asset, ticker, side,
                ts_utc, market, market_type, spot, strike, range_low, range_high,
                distance, min_distance_threshold, bid, limit_or_market, price_cents,
                seconds_to_close, signal_identified, skip_reason, skip_details, placed, order_id,
                filled, fill_price, resolution_price, final_outcome, pnl_cents, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 0, NULL, NULL, NULL, NULL, ?)
            ON CONFLICT(strategy_name, window_id, asset, ticker, side) DO UPDATE SET
                ts_utc = excluded.ts_utc,
                market = excluded.market,
                market_type = excluded.market_type,
                spot = excluded.spot,
                strike = excluded.strike,
                range_low = excluded.range_low,
                range_high = excluded.range_high,
                distance = excluded.distance,
                min_distance_threshold = excluded.min_distance_threshold,
                bid = excluded.bid,
                limit_or_market = excluded.limit_or_market,
                price_cents = excluded.price_cents,
                seconds_to_close = excluded.seconds_to_close,
                updated_at = excluded.updated_at,
                skip_reason = CASE WHEN {REPORT_TABLE}.skip_reason IS NOT NULL THEN {REPORT_TABLE}.skip_reason ELSE excluded.skip_reason END,
                skip_details = COALESCE(excluded.skip_details, {REPORT_TABLE}.skip_details),
                placed = CASE WHEN {REPORT_TABLE}.placed = 1 THEN 1 ELSE excluded.placed END,
                order_id = COALESCE({REPORT_TABLE}.order_id, excluded.order_id)
            """,
            (
                strategy_name,
                window_id,
                asset.lower(),
                ticker,
                (side or "yes").lower(),
                ts,
                market,
                market_type,
                spot,
                strike,
                range_low,
                range_high,
                distance,
                min_distance_threshold,
                bid,
                limit_or_market or "limit",
                price_cents,
                seconds_to_close,
                skip_reason,
                skip_details,
                placed,
                order_id,
                ts,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def record_skip(
    db_path: str,
    strategy_name: str,
    window_id: str,
    asset: str,
    ticker: str,
    side: str,
    skip_reason: str,
    skip_details: Optional[str] = None,
) -> None:
    """Set skip_reason and optional skip_details for an existing candidate row."""
    ts = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE {REPORT_TABLE}
            SET skip_reason = ?, skip_details = ?, updated_at = ?
            WHERE strategy_name = ? AND window_id = ? AND asset = ? AND ticker = ? AND side = ?
            """,
            (skip_reason, skip_details, ts, strategy_name, window_id, asset.lower(), ticker, (side or "yes").lower()),
        )
        conn.commit()
    finally:
        conn.close()


def record_place(
    db_path: str,
    strategy_name: str,
    window_id: str,
    asset: str,
    ticker: str,
    side: str,
    order_id: str,
) -> None:
    """Set placed=1 and order_id for an existing candidate row."""
    ts = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE {REPORT_TABLE}
            SET placed = 1, order_id = ?, skip_reason = NULL, updated_at = ?
            WHERE strategy_name = ? AND window_id = ? AND asset = ? AND ticker = ? AND side = ?
            """,
            (order_id, ts, strategy_name, window_id, asset.lower(), ticker, (side or "yes").lower()),
        )
        conn.commit()
    finally:
        conn.close()


def update_resolution(
    db_path: str,
    order_id: str,
    *,
    filled: int = 0,
    fill_price: Optional[int] = None,
    resolution_price: Optional[int] = None,
    final_outcome: Optional[str] = None,
    pnl_cents: Optional[int] = None,
    strategy_name: Optional[str] = None,
    window_id: Optional[str] = None,
    asset: Optional[str] = None,
    ticker: Optional[str] = None,
    side: Optional[str] = None,
) -> None:
    """Update the row with this order_id with resolution fields (filled, final_outcome, pnl_cents, etc.).
    If order_id update affects 0 rows and (strategy_name, window_id, asset, ticker, side) are provided,
    try updating by those keys (fallback when record_place failed or row was created without order_id)."""
    if not order_id and not all((strategy_name, window_id, asset, ticker, side)):
        return
    ts = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        if order_id:
            cur.execute(
                f"""
                UPDATE {REPORT_TABLE}
                SET filled = ?, fill_price = ?, resolution_price = ?, final_outcome = ?, pnl_cents = ?, updated_at = ?
                WHERE order_id = ?
                """,
                (filled, fill_price, resolution_price, final_outcome, pnl_cents, ts, order_id),
            )
        if cur.rowcount == 0 and strategy_name and window_id and asset is not None and ticker is not None and side is not None:
            cur.execute(
                f"""
                UPDATE {REPORT_TABLE}
                SET filled = ?, fill_price = ?, resolution_price = ?, final_outcome = ?, pnl_cents = ?, order_id = ?, updated_at = ?
                WHERE strategy_name = ? AND window_id = ? AND asset = ? AND ticker = ? AND side = ?
                """,
                (filled, fill_price, resolution_price, final_outcome, pnl_cents, order_id or "", ts,
                 strategy_name, window_id, asset.lower(), ticker, (side or "yes").lower()),
            )
        conn.commit()
    finally:
        conn.close()


def write_row_last_90s(
    db_path: str,
    window_id: str,
    asset: str,
    *,
    ts_utc: Optional[str] = None,
    ticker: Optional[str] = None,
    side: Optional[str] = None,
    spot: Optional[float] = None,
    strike: Optional[float] = None,
    resolution_price: Optional[int] = None,
    seconds_to_close: Optional[float] = None,
    distance: Optional[float] = None,
    min_distance_threshold: Optional[float] = None,
    bid: Optional[int] = None,
    limit_or_market: Optional[str] = None,
    price_cents: Optional[int] = None,
    placed: int = 0,
    filled: int = 0,
    fill_price: Optional[int] = None,
    final_outcome: Optional[str] = None,
    pnl_cents: Optional[int] = None,
    is_stop_loss: int = 0,
    stop_loss_sell_price: Optional[int] = None,
    skip_reason: Optional[str] = None,
    skip_details: Optional[str] = None,
    order_id: Optional[str] = None,
    spot_kraken: Optional[float] = None,
    spot_coinbase: Optional[float] = None,
    distance_kraken: Optional[float] = None,
    distance_coinbase: Optional[float] = None,
    pre_placement_history: Optional[str] = None,
) -> None:
    """Insert or replace one row keyed by order_id for last_90s.

    For rows without a real order_id (e.g. aggregated skip rows), we synthesize a stable
    ID so the row can still be updated later.

    Dual-oracle: spot/distance are the official values used for the trading decision
    (official_spot = Kraken or Coinbase depending on which had the min distance;
    official_distance = min(distance_kraken, distance_coinbase) with Kraken fallback).
    spot_kraken, spot_coinbase, distance_kraken, distance_coinbase store both exchanges;
    when Coinbase fails, spot_coinbase and distance_coinbase are NULL.
    """
    ts = ts_utc or datetime.now(timezone.utc).isoformat()
    # Use provided order_id when present; otherwise synthesize a deterministic ID per window/asset.
    base_order_id = (order_id or "").strip()
    if not base_order_id:
        base_order_id = f"agg:{window_id}:{asset.lower()}"
    conn = sqlite3.connect(db_path)
    try:
        _ensure_last_90s_table(conn)
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO {LAST_90S_TABLE} (
                order_id, window_id, asset, ts_utc, ticker, side, spot, strike, resolution_price,
                seconds_to_close, distance, min_distance_threshold, bid, limit_or_market, price_cents,
                placed, filled, fill_price, final_outcome, pnl_cents, is_stop_loss, stop_loss_sell_price,
                skip_reason, skip_details,
                spot_kraken, spot_coinbase, distance_kraken, distance_coinbase,
                pre_placement_history
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                ts_utc = excluded.ts_utc,
                ticker = excluded.ticker,
                side = excluded.side,
                spot = excluded.spot,
                strike = excluded.strike,
                resolution_price = excluded.resolution_price,
                seconds_to_close = excluded.seconds_to_close,
                distance = excluded.distance,
                min_distance_threshold = excluded.min_distance_threshold,
                bid = excluded.bid,
                limit_or_market = excluded.limit_or_market,
                price_cents = excluded.price_cents,
                placed = excluded.placed,
                filled = excluded.filled,
                fill_price = excluded.fill_price,
                final_outcome = excluded.final_outcome,
                pnl_cents = excluded.pnl_cents,
                is_stop_loss = excluded.is_stop_loss,
                stop_loss_sell_price = excluded.stop_loss_sell_price,
                skip_reason = excluded.skip_reason,
                skip_details = excluded.skip_details,
                spot_kraken = excluded.spot_kraken,
                spot_coinbase = excluded.spot_coinbase,
                distance_kraken = excluded.distance_kraken,
                distance_coinbase = excluded.distance_coinbase,
                pre_placement_history = excluded.pre_placement_history
            """,
            (
                base_order_id,
                window_id,
                asset.lower(),
                ts,
                ticker,
                (side or "yes").lower(),
                spot,
                strike,
                resolution_price,
                seconds_to_close,
                distance,
                min_distance_threshold,
                bid,
                limit_or_market,
                price_cents,
                placed,
                filled,
                fill_price,
                final_outcome,
                pnl_cents,
                1 if is_stop_loss else 0,
                stop_loss_sell_price,
                skip_reason,
                skip_details,
                spot_kraken,
                spot_coinbase,
                distance_kraken,
                distance_coinbase,
                pre_placement_history,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_resolution_last_90s(
    db_path: str,
    *,
    order_id: Optional[str] = None,
    window_id: Optional[str] = None,
    asset: Optional[str] = None,
    filled: int = 0,
    fill_price: Optional[int] = None,
    resolution_price: Optional[int] = None,
    final_outcome: Optional[str] = None,
    pnl_cents: Optional[int] = None,
    is_stop_loss: Optional[int] = None,
    stop_loss_sell_price: Optional[int] = None,
    is_filled: Optional[int] = None,
    respect_stop_loss: bool = False,
) -> None:
    """Update the existing last_90s row (no insert). Match by order_id or (window_id, asset). Used for stop-loss and resolve.
    final_outcome = WIN | LOSS from Kalshi resolution. When we trigger stop loss pass is_stop_loss=1, stop_loss_sell_price=<cents>."""
    if not order_id and not (window_id and asset is not None):
        return
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        # Optional guard: when respect_stop_loss is True and the existing row is marked as a
        # stop-loss exit, preserve its resolution_price/final_outcome/pnl_cents so later resolve
        # passes (e.g. from market settlement) do not overwrite the STOP_LOSS outcome.
        if respect_stop_loss and (order_id or (window_id and asset is not None)):
            if order_id:
                cur.execute(
                    f"SELECT is_stop_loss, resolution_price, final_outcome, pnl_cents FROM {LAST_90S_TABLE} WHERE order_id = ? LIMIT 1",
                    (order_id,),
                )
            else:
                cur.execute(
                    f"SELECT is_stop_loss, resolution_price, final_outcome, pnl_cents FROM {LAST_90S_TABLE} WHERE window_id = ? AND asset = ? LIMIT 1",
                    (window_id, str(asset).lower()),
                )
            row = cur.fetchone()
            if row:
                existing_is_sl, existing_res_price, existing_final, existing_pnl = row
                if int(existing_is_sl or 0) == 1 and (is_stop_loss is None or int(is_stop_loss or 0) == 0):
                    # Respect prior stop-loss data: keep its resolution_price/final_outcome/pnl_cents.
                    if existing_res_price is not None:
                        resolution_price = existing_res_price
                    if existing_final is not None:
                        final_outcome = existing_final
                    if existing_pnl is not None:
                        pnl_cents = existing_pnl
        set_parts = ["filled = ?", "fill_price = ?", "resolution_price = ?", "pnl_cents = ?"]
        params: List[Any] = [filled, fill_price, resolution_price, pnl_cents]
        if final_outcome is not None:
            set_parts.append("final_outcome = ?")
            params.append(final_outcome)
        if is_stop_loss is not None:
            set_parts.append("is_stop_loss = ?")
            params.append(is_stop_loss)
        if stop_loss_sell_price is not None:
            set_parts.append("stop_loss_sell_price = ?")
            params.append(stop_loss_sell_price)
        if is_filled is not None:
            set_parts.append("is_filled = ?")
            params.append(is_filled)
        set_sql = ", ".join(set_parts)
        if order_id:
            cur.execute(
                f"UPDATE {LAST_90S_TABLE} SET {set_sql} WHERE order_id = ?",
                tuple(params) + (order_id,),
            )
        if cur.rowcount == 0 and window_id and asset is not None:
            params_wi = list(params) + [order_id, window_id, asset.lower()]
            cur.execute(
                f"UPDATE {LAST_90S_TABLE} SET {set_sql}, order_id = COALESCE(?, order_id) WHERE window_id = ? AND asset = ?",
                tuple(params_wi),
            )
        conn.commit()
    finally:
        conn.close()


def update_post_placement_telemetry(
    db_path: str,
    order_id: str,
    *,
    min_dist_after_placement: Optional[float] = None,
    min_bid_after_placement: Optional[int] = None,
    post_placement_history: Optional[str] = None,
) -> None:
    """Update a single last_90s row by order_id with post-placement telemetry (MAE & trajectory).
    Does not insert; only UPDATE. Call once per order when the monitoring loop exits (window close or stop-loss)."""
    if not order_id:
        return
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE {LAST_90S_TABLE} SET min_dist_after_placement = ?, min_bid_after_placement = ?, post_placement_history = ? WHERE order_id = ?",
            (
                min_dist_after_placement,
                min_bid_after_placement,
                post_placement_history,
                order_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def write_row_hourly_last_90s(
    db_path: str,
    window_id: str,
    asset: str,
    ticker: str,
    side: str,
    *,
    ts_utc: Optional[str] = None,
    spot: Optional[float] = None,
    strike: Optional[float] = None,
    resolution_price: Optional[int] = None,
    seconds_to_close: Optional[float] = None,
    distance: Optional[float] = None,
    min_distance_threshold: Optional[float] = None,
    bid: Optional[int] = None,
    limit_or_market: Optional[str] = None,
    price_cents: Optional[int] = None,
    placed: int = 0,
    filled: int = 0,
    fill_price: Optional[int] = None,
    final_outcome: Optional[str] = None,
    pnl_cents: Optional[int] = None,
    is_stop_loss: int = 0,
    stop_loss_sell_price: Optional[int] = None,
    skip_reason: Optional[str] = None,
    skip_details: Optional[str] = None,
    order_id: Optional[str] = None,
) -> None:
    """Insert or replace exactly one row for (window_id, asset, ticker, side). One-row guarantee for hourly_last_90s."""
    ts = ts_utc or datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        _ensure_hourly_last_90s_table(conn)
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO {HOURLY_LAST_90S_TABLE} (
                window_id, asset, ticker, side, ts_utc, spot, strike, resolution_price,
                seconds_to_close, distance, min_distance_threshold, bid, limit_or_market, price_cents,
                placed, filled, fill_price, final_outcome, pnl_cents, is_stop_loss, stop_loss_sell_price,
                skip_reason, skip_details, order_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(window_id, asset, ticker, side) DO UPDATE SET
                ts_utc = excluded.ts_utc,
                spot = excluded.spot,
                strike = excluded.strike,
                resolution_price = excluded.resolution_price,
                seconds_to_close = excluded.seconds_to_close,
                distance = excluded.distance,
                min_distance_threshold = excluded.min_distance_threshold,
                bid = excluded.bid,
                limit_or_market = excluded.limit_or_market,
                price_cents = excluded.price_cents,
                placed = excluded.placed,
                filled = excluded.filled,
                fill_price = excluded.fill_price,
                final_outcome = excluded.final_outcome,
                pnl_cents = excluded.pnl_cents,
                is_stop_loss = excluded.is_stop_loss,
                stop_loss_sell_price = excluded.stop_loss_sell_price,
                skip_reason = excluded.skip_reason,
                skip_details = excluded.skip_details,
                order_id = COALESCE(excluded.order_id, {HOURLY_LAST_90S_TABLE}.order_id)
            """,
            (
                window_id,
                asset.lower(),
                ticker,
                (side or "yes").lower(),
                ts,
                spot,
                strike,
                resolution_price,
                seconds_to_close,
                distance,
                min_distance_threshold,
                bid,
                limit_or_market,
                price_cents,
                placed,
                filled,
                fill_price,
                final_outcome,
                pnl_cents,
                1 if is_stop_loss else 0,
                stop_loss_sell_price,
                skip_reason,
                skip_details,
                order_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_resolution_hourly_last_90s(
    db_path: str,
    *,
    order_id: Optional[str] = None,
    window_id: Optional[str] = None,
    asset: Optional[str] = None,
    ticker: Optional[str] = None,
    side: Optional[str] = None,
    filled: int = 0,
    fill_price: Optional[int] = None,
    resolution_price: Optional[int] = None,
    final_outcome: Optional[str] = None,
    pnl_cents: Optional[int] = None,
    is_stop_loss: Optional[int] = None,
    stop_loss_sell_price: Optional[int] = None,
) -> None:
    """Update the existing hourly_last_90s row (no insert). Match by order_id or (window_id, asset, ticker, side).
    When we trigger stop loss pass is_stop_loss=1, stop_loss_sell_price=<cents>."""
    if not order_id and not all((window_id, asset is not None, ticker is not None, side is not None)):
        return
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        set_parts = ["filled = ?", "fill_price = ?", "resolution_price = ?", "pnl_cents = ?"]
        params: List[Any] = [filled, fill_price, resolution_price, pnl_cents]
        if final_outcome is not None:
            set_parts.append("final_outcome = ?")
            params.append(final_outcome)
        if is_stop_loss is not None:
            set_parts.append("is_stop_loss = ?")
            params.append(is_stop_loss)
        if stop_loss_sell_price is not None:
            set_parts.append("stop_loss_sell_price = ?")
            params.append(stop_loss_sell_price)
        set_sql = ", ".join(set_parts)
        if order_id:
            cur.execute(
                f"UPDATE {HOURLY_LAST_90S_TABLE} SET {set_sql} WHERE order_id = ?",
                tuple(params) + (order_id,),
            )
        if cur.rowcount == 0 and window_id and asset is not None and ticker is not None and side is not None:
            params_wi = list(params) + [order_id, window_id, asset.lower(), ticker, (side or "yes").lower()]
            cur.execute(
                f"UPDATE {HOURLY_LAST_90S_TABLE} SET {set_sql}, order_id = COALESCE(?, order_id) WHERE window_id = ? AND asset = ? AND ticker = ? AND side = ?",
                tuple(params_wi),
            )
        conn.commit()
    finally:
        conn.close()


def get_all_rows_last_90s(
    db_path: str,
    since_ts_utc: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return all last_90s rows as list of dicts with LAST_90S_COLUMNS keys."""
    conn = sqlite3.connect(db_path)
    try:
        _ensure_last_90s_table(conn)
        cur = conn.cursor()
        q = f"SELECT * FROM {LAST_90S_TABLE} WHERE 1=1"
        params: List[Any] = []
        if since_ts_utc:
            q += " AND ts_utc >= ?"
            params.append(since_ts_utc)
        q += " ORDER BY ts_utc ASC"
        cur.execute(q, params)
        rows = cur.fetchall()
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in rows]
    finally:
        conn.close()


def get_all_rows_hourly_last_90s(
    db_path: str,
    since_ts_utc: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return all hourly_last_90s rows as list of dicts with HOURLY_LAST_90S_COLUMNS keys."""
    conn = sqlite3.connect(db_path)
    try:
        _ensure_hourly_last_90s_table(conn)
        cur = conn.cursor()
        q = f"SELECT * FROM {HOURLY_LAST_90S_TABLE} WHERE 1=1"
        params: List[Any] = []
        if since_ts_utc:
            q += " AND ts_utc >= ?"
            params.append(since_ts_utc)
        q += " ORDER BY ts_utc ASC"
        cur.execute(q, params)
        rows = cur.fetchall()
        names = [d[0] for d in cur.description]
        return [dict(zip(names, r)) for r in rows]
    finally:
        conn.close()


def get_all_rows(
    db_path: str,
    strategy_name: Optional[str] = None,
    since_ts_utc: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return all report rows. last_90s from lean table; hourly_last_90s from lean table; other strategies from strategy_report."""
    out: List[Dict[str, Any]] = []
    sn = (strategy_name or "").strip()
    want_last_90s = strategy_name is None or sn == "last_90s_limit_99"
    want_hourly_lean = strategy_name is None or sn == "hourly_last_90s_limit_99"
    want_legacy = strategy_name is None or (sn and sn not in ("last_90s_limit_99", "hourly_last_90s_limit_99"))
    if want_last_90s:
        rows = get_all_rows_last_90s(db_path, since_ts_utc=since_ts_utc)
        for r in rows:
            r["strategy_name"] = "last_90s_limit_99"
        out.extend(rows)
    if want_hourly_lean:
        rows = get_all_rows_hourly_last_90s(db_path, since_ts_utc=since_ts_utc)
        for r in rows:
            r["strategy_name"] = "hourly_last_90s_limit_99"
        out.extend(rows)
    if want_legacy:
        conn = sqlite3.connect(db_path)
        try:
            _ensure_table(conn)
            cur = conn.cursor()
            q = f"SELECT * FROM {REPORT_TABLE} WHERE 1=1"
            params: List[Any] = []
            if strategy_name and sn not in ("last_90s_limit_99", "hourly_last_90s_limit_99"):
                q += " AND strategy_name = ?"
                params.append(strategy_name)
            elif strategy_name is None:
                q += " AND strategy_name NOT IN (?, ?)"
                params.extend(["last_90s_limit_99", "hourly_last_90s_limit_99"])
            if since_ts_utc:
                q += " AND ts_utc >= ?"
                params.append(since_ts_utc)
            q += " ORDER BY ts_utc ASC"
            cur.execute(q, params)
            rows = cur.fetchall()
            names = [d[0] for d in cur.description]
            out.extend([dict(zip(names, r)) for r in rows])
        finally:
            conn.close()
    out.sort(key=lambda r: (r.get("ts_utc") or "", r.get("window_id") or "", r.get("asset") or "", r.get("ticker") or ""))
    return out
