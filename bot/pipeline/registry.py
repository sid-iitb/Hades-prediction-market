"""
Central order registry and strategy reports for Bot V2 (v2_state.db).
Tracks every V2 order for ownership, exits, and caps; records trade outcomes.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import List, Optional

from bot.pipeline.intents import OrderRecord

logger = logging.getLogger(__name__)

REGISTRY_TABLE = "v2_order_registry"
REPORTS_TABLE = "v2_strategy_reports"


def _default_db_path() -> Path:
    return Path(__file__).resolve().parents[1].parent / "data" / "v2_state.db"


def init_v2_db(db_path: Optional[Path] = None) -> None:
    """
    Create v2_state.db and the two tables if they do not exist.
    Safe to call multiple times (idempotent).
    """
    path = db_path or _default_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (
                order_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                interval TEXT NOT NULL,
                market_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                ticker TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                filled_count INTEGER DEFAULT 0,
                count INTEGER NOT NULL,
                limit_price_cents INTEGER,
                placed_at REAL NOT NULL,
                client_order_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_registry_strategy_interval_market_asset
                ON {REGISTRY_TABLE}(strategy_id, interval, market_id, asset);
            CREATE INDEX IF NOT EXISTS idx_registry_status ON {REGISTRY_TABLE}(status);

            CREATE TABLE IF NOT EXISTS {REPORTS_TABLE} (
                order_id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                interval TEXT NOT NULL,
                window_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price_cents INTEGER,
                exit_price_cents INTEGER,
                outcome TEXT,
                is_stop_loss INTEGER DEFAULT 0,
                pnl_cents INTEGER,
                resolved_at REAL
            );
            CREATE INDEX IF NOT EXISTS idx_reports_strategy_interval ON {REPORTS_TABLE}(strategy_id, interval);
            """
        )
        conn.commit()
        logger.info("V2 DB initialized at %s", path)
    finally:
        conn.close()


class OrderRegistry:
    """
    CRUD for v2_order_registry and v2_strategy_reports.
    Uses a single connection with check_same_thread=False for use from multiple threads.
    """

    def __init__(self, db_path: Optional[Path] = None) -> None:
        self._path = Path(db_path) if db_path else _default_db_path()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.row_factory = sqlite3.Row

    def register_order(
        self,
        order_id: str,
        strategy_id: str,
        interval: str,
        market_id: str,
        asset: str,
        ticker: str,
        side: str,
        count: int,
        placed_at: float,
        limit_price_cents: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> None:
        """Insert a new order into v2_order_registry (status=resting, filled_count=0)."""
        self._conn.execute(
            f"""
            INSERT INTO {REGISTRY_TABLE}
            (order_id, strategy_id, interval, market_id, asset, ticker, side, status, filled_count, count, limit_price_cents, placed_at, client_order_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'resting', 0, ?, ?, ?, ?)
            """,
            (
                order_id,
                strategy_id,
                interval,
                market_id,
                asset,
                ticker,
                side,
                count,
                limit_price_cents,
                placed_at,
                client_order_id,
            ),
        )
        self._conn.commit()
        logger.debug("Registered order %s for %s/%s", order_id, strategy_id, interval)

    def update_order_status(
        self,
        order_id: str,
        status: str,
        filled_count: Optional[int] = None,
    ) -> None:
        """Update status and optionally filled_count for an order."""
        if filled_count is not None:
            self._conn.execute(
                f"UPDATE {REGISTRY_TABLE} SET status = ?, filled_count = ? WHERE order_id = ?",
                (status, filled_count, order_id),
            )
        else:
            self._conn.execute(
                f"UPDATE {REGISTRY_TABLE} SET status = ? WHERE order_id = ?",
                (status, order_id),
            )
        self._conn.commit()
        logger.debug("Updated order %s -> status=%s filled_count=%s", order_id, status, filled_count)

    def get_orders_by_strategy(
        self,
        strategy_id: str,
        interval: str,
        market_id: Optional[str] = None,
        asset: Optional[str] = None,
        active_only: bool = True,
    ) -> List[OrderRecord]:
        """Return OrderRecords for the given strategy/interval, optionally filtered by market_id/asset."""
        query = f"""
            SELECT order_id, strategy_id, interval, market_id, asset, ticker, side, status,
                   filled_count, count, limit_price_cents, placed_at
            FROM {REGISTRY_TABLE}
            WHERE strategy_id = ? AND interval = ?
            """
        params: List[object] = [strategy_id, interval]
        if market_id is not None:
            query += " AND market_id = ?"
            params.append(market_id)
        if asset is not None:
            query += " AND asset = ?"
            params.append(asset)
        if active_only:
            query += " AND status = 'resting'"
        query += " ORDER BY placed_at DESC"
        cur = self._conn.execute(query, params)
        rows = cur.fetchall()
        return [self._row_to_order_record(dict(r)) for r in rows]

    def get_all_active_orders_for_cap_check(
        self,
        interval: str,
        market_id: Optional[str] = None,
    ) -> List[OrderRecord]:
        """
        Return all orders with status='resting' for the given interval (and optionally market_id).
        Used by aggregator to enforce caps (max_orders_per_ticker, max_total_orders_per_interval).
        """
        query = f"""
            SELECT order_id, strategy_id, interval, market_id, asset, ticker, side, status,
                   filled_count, count, limit_price_cents, placed_at
            FROM {REGISTRY_TABLE}
            WHERE interval = ? AND status = 'resting'
            """
        params: List[object] = [interval]
        if market_id is not None:
            query += " AND market_id = ?"
            params.append(market_id)
        cur = self._conn.execute(query, params)
        rows = cur.fetchall()
        return [self._row_to_order_record(dict(r)) for r in rows]

    def record_trade_outcome(
        self,
        order_id: str,
        strategy_id: str,
        interval: str,
        window_id: str,
        asset: str,
        side: str,
        entry_price_cents: Optional[int],
        exit_price_cents: Optional[int],
        outcome: str,  # win | loss | resolved_yes | resolved_no
        is_stop_loss: bool,
        pnl_cents: Optional[int],
        resolved_at: float,
    ) -> None:
        """Insert a row into v2_strategy_reports for a closed trade."""
        self._conn.execute(
            f"""
            INSERT OR REPLACE INTO {REPORTS_TABLE}
            (order_id, strategy_id, interval, window_id, asset, side, entry_price_cents, exit_price_cents, outcome, is_stop_loss, pnl_cents, resolved_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                strategy_id,
                interval,
                window_id,
                asset,
                side,
                entry_price_cents,
                exit_price_cents,
                outcome,
                1 if is_stop_loss else 0,
                pnl_cents,
                resolved_at,
            ),
        )
        self._conn.commit()
        logger.debug("Recorded trade outcome for order %s: outcome=%s pnl_cents=%s", order_id, outcome, pnl_cents)

    @staticmethod
    def _row_to_order_record(row: dict) -> OrderRecord:
        return OrderRecord(
            order_id=row["order_id"],
            strategy_id=row["strategy_id"],
            interval=row["interval"],
            market_id=row["market_id"],
            asset=row["asset"],
            ticker=row["ticker"],
            side=row["side"],
            status=row["status"],
            filled_count=row["filled_count"] or 0,
            count=row["count"] or 0,
            limit_price_cents=row.get("limit_price_cents"),
            placed_at=row["placed_at"],
        )

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
        logger.debug("OrderRegistry closed for %s", self._path)
