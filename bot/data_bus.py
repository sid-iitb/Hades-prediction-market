"""
Event-Driven Data Bus: SQLite-backed market data store for spot prices and Kalshi order books.
WebSocket feeders write here; strategies (Phase 3) will read from here.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

# Default DB path: data/market_data.db under repo root
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_DB_PATH = _REPO_ROOT / "data" / "market_data.db"
_conn: Optional[sqlite3.Connection] = None
_lock = threading.Lock()


def _get_connection() -> sqlite3.Connection:
    global _conn
    with _lock:
        if _conn is not None:
            return _conn
        path = os.environ.get("MARKET_DATA_DB_PATH", str(_DEFAULT_DB_PATH))
        path = str(Path(path).resolve())
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS spot_prices (
            asset TEXT PRIMARY KEY,
            spot REAL NOT NULL,
            timestamp REAL NOT NULL
        )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS kalshi_books (
            ticker TEXT PRIMARY KEY,
            yes_bid INTEGER,
            yes_ask INTEGER,
            no_bid INTEGER,
            no_ask INTEGER,
            timestamp REAL NOT NULL
        )"""
        )
        conn.commit()
        _conn = conn
        return conn


def write_spot(asset: str, spot: float) -> None:
    """Write latest spot price for an asset (e.g. BTC, ETH). Overwrites previous."""
    if not asset or spot is None:
        return
    ts = time.time()
    conn = _get_connection()
    conn.execute(
        "INSERT OR REPLACE INTO spot_prices (asset, spot, timestamp) VALUES (?, ?, ?)",
        (str(asset).strip().upper(), float(spot), ts),
    )
    conn.commit()


def write_book(
    ticker: str,
    yes_bid: Optional[int],
    yes_ask: Optional[int],
    no_bid: Optional[int],
    no_ask: Optional[int],
) -> None:
    """Write latest top-of-book for a Kalshi ticker. Overwrites previous."""
    if not ticker:
        return
    ts = time.time()
    conn = _get_connection()
    conn.execute(
        """INSERT OR REPLACE INTO kalshi_books
        (ticker, yes_bid, yes_ask, no_bid, no_ask, timestamp) VALUES (?, ?, ?, ?, ?, ?)""",
        (
            str(ticker).strip(),
            int(yes_bid) if yes_bid is not None else None,
            int(yes_ask) if yes_ask is not None else None,
            int(no_bid) if no_bid is not None else None,
            int(no_ask) if no_ask is not None else None,
            ts,
        ),
    )
    conn.commit()


def get_spot(asset: str) -> Optional[float]:
    """Return latest spot price for asset, or None if missing."""
    if not asset:
        return None
    conn = _get_connection()
    row = conn.execute(
        "SELECT spot FROM spot_prices WHERE asset = ?",
        (str(asset).strip().upper(),),
    ).fetchone()
    return float(row[0]) if row else None


def get_book(ticker: str) -> Optional[Dict[str, Any]]:
    """Return latest book row for ticker (yes_bid, yes_ask, no_bid, no_ask, timestamp), or None."""
    if not ticker:
        return None
    conn = _get_connection()
    row = conn.execute(
        "SELECT yes_bid, yes_ask, no_bid, no_ask, timestamp FROM kalshi_books WHERE ticker = ?",
        (str(ticker).strip(),),
    ).fetchone()
    if not row:
        return None
    return {
        "yes_bid": row[0],
        "yes_ask": row[1],
        "no_bid": row[2],
        "no_ask": row[3],
        "timestamp": row[4],
    }


def cleanup_old_data(max_age_seconds: float = 3600) -> None:
    """Delete rows where timestamp is older than max_age_seconds (default 1 hour)."""
    conn = _get_connection()
    cutoff = time.time() - max_age_seconds
    conn.execute("DELETE FROM spot_prices WHERE timestamp < ?", (cutoff,))
    conn.execute("DELETE FROM kalshi_books WHERE timestamp < ?", (cutoff,))
    conn.commit()
