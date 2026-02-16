"""
State persistence for the bot - tracks order counts and caps.
Uses SQLite for durability across restarts.
"""
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass
class TickerOrderCount:
    ticker: str
    count: int
    last_action_time: str


def get_default_db_path() -> str:
    base = Path(__file__).resolve().parent.parent
    return str(base / "data" / "bot_state.db")


def ensure_state_db(db_path: str) -> None:
    """Create tables if they don't exist."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS bot_state (
                hour_market_id TEXT NOT NULL,
                ticker TEXT NOT NULL,
                side TEXT NOT NULL,
                placed_order_count INTEGER DEFAULT 0,
                last_action_time TEXT,
                order_ids TEXT,
                PRIMARY KEY (hour_market_id, ticker, side)
            );
            CREATE TABLE IF NOT EXISTS bot_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hour_market_id TEXT NOT NULL,
                ticker TEXT NOT NULL,
                side TEXT NOT NULL,
                order_id TEXT,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_bot_state_hour ON bot_state(hour_market_id);
            CREATE INDEX IF NOT EXISTS idx_bot_orders_hour ON bot_orders(hour_market_id);
        """)
        conn.commit()
    finally:
        conn.close()


def get_ticker_order_count(
    db_path: str,
    hour_market_id: str,
    ticker: str,
    cap_scope: str = "combined",
    side: Optional[str] = None,
) -> int:
    """
    Get order count for a ticker in this hour market.
    cap_scope=combined: sum YES+NO. cap_scope=per_side: count for given side only (side required).
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        if cap_scope == "per_side" and side:
            side = str(side).lower()
            if side in ("yes", "no"):
                cur.execute(
                    """
                    SELECT COALESCE(SUM(placed_order_count), 0)
                    FROM bot_state
                    WHERE hour_market_id = ? AND ticker = ? AND side = ?
                    """,
                    (hour_market_id, ticker, side),
                )
                return cur.fetchone()[0] or 0
        cur.execute(
            """
            SELECT COALESCE(SUM(placed_order_count), 0)
            FROM bot_state
            WHERE hour_market_id = ? AND ticker = ?
            """,
            (hour_market_id, ticker),
        )
        return cur.fetchone()[0] or 0
    finally:
        conn.close()


def get_total_order_count(db_path: str, hour_market_id: str) -> int:
    """Total orders placed for this hour market."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(SUM(placed_order_count), 0)
            FROM bot_state
            WHERE hour_market_id = ?
            """,
            (hour_market_id,),
        )
        return cur.fetchone()[0] or 0
    finally:
        conn.close()


def get_side_order_count(
    db_path: str, hour_market_id: str, side: str
) -> int:
    """Total orders placed for this hour market for a given side (yes/no)."""
    side = str(side or "").lower()
    if side not in ("yes", "no"):
        return 0
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(SUM(placed_order_count), 0)
            FROM bot_state
            WHERE hour_market_id = ? AND side = ?
            """,
            (hour_market_id, side),
        )
        return cur.fetchone()[0] or 0
    finally:
        conn.close()


def get_last_run_hour(db_path: str, asset: Optional[str] = None) -> Optional[str]:
    """Get the hour_market_id we last ran for (for hour transition detection)."""
    meta_path = _meta_path(db_path)
    if not os.path.exists(meta_path):
        return None
    try:
        with open(meta_path) as f:
            data = json.load(f)
        if asset:
            last_by_asset = data.get("last_hour_by_asset") or {}
            return last_by_asset.get(str(asset).lower())
        return data.get("last_hour_market_id")
    except Exception:
        return None


def set_last_run_hour(db_path: str, hour_market_id: str, asset: Optional[str] = None) -> None:
    """Store the hour_market_id we just ran for. Supports per-asset tracking."""
    meta_path = _meta_path(db_path)
    os.makedirs(os.path.dirname(meta_path) or ".", exist_ok=True)
    data = {}
    if os.path.exists(meta_path):
        try:
            with open(meta_path) as f:
                data = json.load(f)
        except Exception:
            pass
    if asset:
        last_by_asset = data.get("last_hour_by_asset") or {}
        last_by_asset[str(asset).lower()] = hour_market_id
        data["last_hour_by_asset"] = last_by_asset
    else:
        data["last_hour_market_id"] = hour_market_id
    with open(meta_path, "w") as f:
        json.dump(data, f)


def _meta_path(db_path: str) -> str:
    base = os.path.dirname(db_path) or "."
    return os.path.join(base, "bot_run_meta.json")


def get_per_ticker_counts(
    db_path: str, hour_market_id: str, limit: int = 10
) -> List[Tuple[str, int]]:
    """Top N tickers by order count for this hour market."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ticker, SUM(placed_order_count) as total
            FROM bot_state
            WHERE hour_market_id = ?
            GROUP BY ticker
            ORDER BY total DESC
            LIMIT ?
            """,
            (hour_market_id, limit),
        )
        return cur.fetchall()
    finally:
        conn.close()


def increment_order_count(
    db_path: str,
    hour_market_id: str,
    ticker: str,
    side: str,
    order_id: Optional[str] = None,
) -> None:
    """Record a new order and increment count."""
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bot_state (hour_market_id, ticker, side, placed_order_count, last_action_time, order_ids)
            VALUES (?, ?, ?, 1, ?, ?)
            ON CONFLICT(hour_market_id, ticker, side) DO UPDATE SET
                placed_order_count = placed_order_count + 1,
                last_action_time = excluded.last_action_time,
                order_ids = CASE
                    WHEN order_ids IS NULL OR order_ids = '' THEN excluded.order_ids
                    ELSE order_ids || ',' || excluded.order_ids
                END
            """,
            (hour_market_id, ticker, side, now, order_id or ""),
        )
        cur.execute(
            """
            INSERT INTO bot_orders (hour_market_id, ticker, side, order_id, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (hour_market_id, ticker, side, order_id or "", now),
        )
        conn.commit()
    finally:
        conn.close()
