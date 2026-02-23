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
            CREATE TABLE IF NOT EXISTS exit_check_last_run (
                interval_key TEXT NOT NULL,
                market_id TEXT NOT NULL,
                last_run_sec REAL NOT NULL,
                PRIMARY KEY (interval_key, market_id)
            );
            CREATE TABLE IF NOT EXISTS schedule_last_run (
                interval_key TEXT PRIMARY KEY,
                last_run_sec REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS paper_positions (
                ticker TEXT NOT NULL,
                interval_key TEXT NOT NULL,
                asset TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_price_cents INTEGER NOT NULL,
                count INTEGER DEFAULT 1,
                entry_ts TEXT NOT NULL,
                PRIMARY KEY (ticker)
            );
            CREATE TABLE IF NOT EXISTS basket_cooldowns (
                window_id TEXT NOT NULL PRIMARY KEY,
                ts_exit REAL NOT NULL
            );
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


def get_last_run_15min(db_path: str, asset: Optional[str] = None) -> Optional[str]:
    """Get the 15-min market_id we last ran for (for window transition detection)."""
    meta_path = _meta_path(db_path)
    if not os.path.exists(meta_path):
        return None
    try:
        with open(meta_path) as f:
            data = json.load(f)
        if asset:
            last_by_asset = data.get("last_15min_by_asset") or {}
            return last_by_asset.get(str(asset).lower())
        return data.get("last_15min_market_id")
    except Exception:
        return None


def set_last_run_15min(db_path: str, market_id: str, asset: Optional[str] = None) -> None:
    """Store the 15-min market_id we just ran for."""
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
        last_by_asset = data.get("last_15min_by_asset") or {}
        last_by_asset[str(asset).lower()] = market_id
        data["last_15min_by_asset"] = last_by_asset
    else:
        data["last_15min_market_id"] = market_id
    with open(meta_path, "w") as f:
        json.dump(data, f)


def get_exit_check_last_run(db_path: str, interval_key: str, market_id: str) -> float:
    """Get last exit-check run time (unix sec) for (interval_key, market_id). Returns 0 if never run."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT last_run_sec FROM exit_check_last_run WHERE interval_key = ? AND market_id = ?",
            (interval_key, market_id),
        )
        row = cur.fetchone()
        return float(row[0]) if row else 0.0
    finally:
        conn.close()


def set_exit_check_last_run(db_path: str, interval_key: str, market_id: str, sec: float) -> None:
    """Record that we ran exit criteria for (interval_key, market_id) at given unix time."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO exit_check_last_run (interval_key, market_id, last_run_sec)
            VALUES (?, ?, ?)
            ON CONFLICT(interval_key, market_id) DO UPDATE SET last_run_sec = excluded.last_run_sec
            """,
            (interval_key, market_id, sec),
        )
        conn.commit()
    finally:
        conn.close()


def get_schedule_last_run(db_path: str, interval_key: str) -> Optional[float]:
    """Get last schedule run time (unix sec) for daily/weekly. Returns None if never run."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT last_run_sec FROM schedule_last_run WHERE interval_key = ?",
            (interval_key,),
        )
        row = cur.fetchone()
        return float(row[0]) if row else None
    finally:
        conn.close()


def set_schedule_last_run(db_path: str, interval_key: str, sec: float) -> None:
    """Record that we ran daily/weekly at given unix time."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO schedule_last_run (interval_key, last_run_sec)
            VALUES (?, ?)
            ON CONFLICT(interval_key) DO UPDATE SET last_run_sec = excluded.last_run_sec
            """,
            (interval_key, sec),
        )
        conn.commit()
    finally:
        conn.close()


def get_paper_positions(
    db_path: str, interval_key: str, ticker: Optional[str] = None
) -> List[dict]:
    """
    Get paper (synthetic) positions for OBSERVE-mode exit evaluation.
    Returns list of position dicts in format expected by evaluate_positions.
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        if ticker:
            cur.execute(
                """
                SELECT ticker, side, entry_price_cents, count, entry_ts, interval_key, asset
                FROM paper_positions
                WHERE interval_key = ? AND ticker = ?
                """,
                (interval_key, ticker),
            )
        else:
            cur.execute(
                """
                SELECT ticker, side, entry_price_cents, count, entry_ts, interval_key, asset
                FROM paper_positions
                WHERE interval_key = ?
                """,
                (interval_key,),
            )
        rows = cur.fetchall()
        out = []
        for r in rows:
            out.append({
                "ticker": r[0],
                "side": r[1],
                "count": int(r[3] or 1),
                "average_price": (r[2] or 0) / 100.0,
            })
        return out
    finally:
        conn.close()


def add_paper_position(
    db_path: str,
    ticker: str,
    side: str,
    entry_price_cents: int,
    interval_key: str,
    asset: str,
    count: int = 1,
) -> None:
    """Record a synthetic position (would-have-bought in OBSERVE mode)."""
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO paper_positions (ticker, interval_key, asset, side, entry_price_cents, count, entry_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                side = excluded.side,
                entry_price_cents = excluded.entry_price_cents,
                count = excluded.count,
                entry_ts = excluded.entry_ts,
                interval_key = excluded.interval_key,
                asset = excluded.asset
            """,
            (ticker, interval_key, asset, str(side).lower(), entry_price_cents, count, now),
        )
        conn.commit()
    finally:
        conn.close()


def remove_paper_position(db_path: str, ticker: str) -> None:
    """Remove a synthetic position (e.g. after STOP_LOSS or TAKE_PROFIT)."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM paper_positions WHERE ticker = ?", (ticker,))
        conn.commit()
    finally:
        conn.close()


def get_basket_cooldown_until(db_path: str, window_id: str, cooldown_minutes: int) -> Optional[float]:
    """Return epoch sec when cooldown ends for window_id, or None if not in cooldown."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT ts_exit FROM basket_cooldowns WHERE window_id = ?",
            (window_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        ts_exit = row[0]
        end_ts = ts_exit + (cooldown_minutes * 60.0)
        return end_ts if end_ts > datetime.now(timezone.utc).timestamp() else None
    finally:
        conn.close()


def set_basket_cooldown(db_path: str, window_id: str) -> None:
    """Record basket take-profit exit time for window_id (starts cooldown)."""
    now = datetime.now(timezone.utc).timestamp()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO basket_cooldowns (window_id, ts_exit) VALUES (?, ?)
            """,
            (window_id, now),
        )
        conn.commit()
    finally:
        conn.close()


def prune_stale_paper_positions(db_path: str, interval_key: str, active_tickers: List[str]) -> None:
    """Remove paper positions for tickers no longer in the active markets list (expired)."""
    if not active_tickers:
        return
    placeholders = ",".join("?" * len(active_tickers))
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            DELETE FROM paper_positions
            WHERE interval_key = ? AND ticker NOT IN ({placeholders})
            """,
            [interval_key] + list(active_tickers),
        )
        conn.commit()
    finally:
        conn.close()


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
