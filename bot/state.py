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
            CREATE TABLE IF NOT EXISTS last_90s_placed (
                order_id TEXT PRIMARY KEY,
                market_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                placed_at TEXT NOT NULL,
                ticker TEXT,
                count INTEGER,
                limit_price_cents INTEGER,
                side TEXT,
                resolved_at TEXT,
                executed_count INTEGER,
                market_result TEXT,
                outcome TEXT,
                stop_loss_triggered_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_last_90s_placed_market_asset ON last_90s_placed(market_id, asset);
            CREATE TABLE IF NOT EXISTS hourly_limit_99_placed (
                hour_market_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                ticker TEXT NOT NULL,
                side TEXT NOT NULL,
                placed_at TEXT NOT NULL,
                order_id TEXT,
                count INTEGER,
                limit_price_cents INTEGER,
                resolved_at TEXT,
                executed_count INTEGER,
                market_result TEXT,
                outcome TEXT,
                stop_loss_triggered_at TEXT,
                PRIMARY KEY (hour_market_id, asset, ticker, side)
            );
            CREATE TABLE IF NOT EXISTS last_90s_skip_aggregator (
                market_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (market_id, asset)
            );
            CREATE TABLE IF NOT EXISTS hourly_last_90s_skip_aggregator (
                hour_market_id TEXT NOT NULL,
                asset TEXT NOT NULL,
                ticker TEXT NOT NULL,
                side TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (hour_market_id, asset, ticker, side)
            );
            CREATE TABLE IF NOT EXISTS strategy_report_market_maker (
                order_id TEXT PRIMARY KEY,
                market_id TEXT,
                asset TEXT,
                ts_utc TEXT,
                spot_kraken REAL,
                spot_coinbase REAL,
                distance_kraken REAL,
                distance_coinbase REAL,
                leg1_side TEXT,
                leg1_fill_price INTEGER,
                leg2_fill_price INTEGER,
                time_to_leg_seconds REAL,
                scratch_triggered INTEGER,
                scratch_reason TEXT,
                pre_placement_history TEXT,
                legging_telemetry_history TEXT
            );
            CREATE TABLE IF NOT EXISTS strategy_report_atm_breakout (
                order_id TEXT PRIMARY KEY,
                market_id TEXT,
                asset TEXT,
                ts_utc TEXT,
                direction TEXT,
                entry_price INTEGER,
                exit_price INTEGER,
                exit_reason TEXT,
                time_in_trade_seconds REAL,
                pre_placement_history TEXT,
                trade_telemetry_history TEXT
            );
        """)
        conn.commit()
        _migrate_last_90s_placed(conn)
        _migrate_last_90s_placed_pk_to_order_id(conn)
        _migrate_hourly_limit_99_placed(conn)
    finally:
        conn.close()


def _migrate_last_90s_placed(conn: sqlite3.Connection) -> None:
    """Add outcome columns to last_90s_placed if missing (for existing DBs)."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(last_90s_placed)")
    cols = {row[1] for row in cur.fetchall()}
    for col, typ in [
        ("order_id", "TEXT"),
        ("ticker", "TEXT"),
        ("count", "INTEGER"),
        ("limit_price_cents", "INTEGER"),
        ("side", "TEXT"),
        ("resolved_at", "TEXT"),
        ("executed_count", "INTEGER"),
        ("market_result", "TEXT"),
        ("outcome", "TEXT"),
        ("stop_loss_triggered_at", "TEXT"),
    ]:
        if col not in cols:
            try:
                cur.execute(f"ALTER TABLE last_90s_placed ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass
    conn.commit()


def _migrate_last_90s_placed_pk_to_order_id(conn: sqlite3.Connection) -> None:
    """If last_90s_placed still uses (market_id, asset) as PK, migrate to PK(order_id).

    This allows multiple orders per (market_id, asset) while preserving legacy rows.
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(last_90s_placed)")
    rows = cur.fetchall()
    if not rows:
        return
    pk_by_col = {r[1]: r[5] for r in rows}  # name -> pk index (0 = not PK)
    # New schema: order_id is the sole primary key.
    if pk_by_col.get("order_id"):
        return
    # Old schema: PRIMARY KEY (market_id, asset).
    cur.execute("ALTER TABLE last_90s_placed RENAME TO last_90s_placed_old")
    conn.commit()
    # Create new table with desired schema.
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS last_90s_placed (
            order_id TEXT PRIMARY KEY,
            market_id TEXT NOT NULL,
            asset TEXT NOT NULL,
            placed_at TEXT NOT NULL,
            ticker TEXT,
            count INTEGER,
            limit_price_cents INTEGER,
            side TEXT,
            resolved_at TEXT,
            executed_count INTEGER,
            market_result TEXT,
            outcome TEXT,
            stop_loss_triggered_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_last_90s_placed_market_asset ON last_90s_placed(market_id, asset);
        """
    )
    # Copy existing data, synthesizing order_id when missing.
    cur = conn.cursor()
    cur.execute(
        """
        SELECT market_id, asset, placed_at, order_id, ticker, count, limit_price_cents,
               side, resolved_at, executed_count, market_result, outcome, stop_loss_triggered_at
        FROM last_90s_placed_old
        """
    )
    existing = cur.fetchall()
    for idx, row in enumerate(existing):
        (
            market_id,
            asset,
            placed_at,
            order_id,
            ticker,
            count,
            limit_price_cents,
            side,
            resolved_at,
            executed_count,
            market_result,
            outcome,
            stop_loss_triggered_at,
        ) = row
        oid = (order_id or "").strip()
        if not oid:
            oid = f"{market_id}:{asset}:{idx}"
        cur.execute(
            """
            INSERT OR IGNORE INTO last_90s_placed (
                order_id, market_id, asset, placed_at, ticker, count, limit_price_cents,
                side, resolved_at, executed_count, market_result, outcome, stop_loss_triggered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                oid,
                market_id,
                str(asset).lower(),
                placed_at,
                ticker,
                count,
                limit_price_cents,
                (side or "yes").lower() if side else "yes",
                resolved_at,
                executed_count,
                market_result,
                outcome,
                stop_loss_triggered_at,
            ),
        )
    cur.execute("DROP TABLE IF EXISTS last_90s_placed_old")
    conn.commit()


def _migrate_hourly_limit_99_placed(conn: sqlite3.Connection) -> None:
    """Add stop_loss_triggered_at to hourly_limit_99_placed if missing."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(hourly_limit_99_placed)")
    cols = {row[1] for row in cur.fetchall()}
    if "stop_loss_triggered_at" not in cols:
        try:
            cur.execute("ALTER TABLE hourly_limit_99_placed ADD COLUMN stop_loss_triggered_at TEXT")
        except sqlite3.OperationalError:
            pass
    conn.commit()


def get_last_90s_placement(
    db_path: str, market_id: str, asset: str
) -> Optional[Tuple[str, str, int, int, str]]:
    """
    Return (order_id, ticker, count, limit_price_cents, side) for the current market's
    placement if it exists and is not yet resolved. None otherwise.
    """
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT order_id, ticker, count, limit_price_cents, side
            FROM last_90s_placed
            WHERE market_id = ? AND asset = ? AND resolved_at IS NULL
              AND order_id IS NOT NULL AND order_id != ''
            ORDER BY placed_at ASC
            LIMIT 1
            """,
            (market_id, str(asset).lower()),
        )
        row = cur.fetchone()
        if not row:
            return None
        return (row[0], row[1] or "", int(row[2] or 0), int(row[3] or 99), (row[4] or "yes").lower())
    finally:
        conn.close()


def get_last_90s_stop_loss_triggered(db_path: str, market_id: str, asset: str) -> bool:
    """True if we already triggered stop loss for this (market_id, asset) this market."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM last_90s_placed WHERE market_id = ? AND asset = ? AND stop_loss_triggered_at IS NOT NULL",
            (market_id, str(asset).lower()),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def set_last_90s_stop_loss_triggered(db_path: str, market_id: str, asset: str) -> None:
    """Record that we triggered stop loss (sold to close) for this (market_id, asset)."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE last_90s_placed SET stop_loss_triggered_at = ? WHERE market_id = ? AND asset = ?",
            (datetime.now(timezone.utc).isoformat(), market_id, str(asset).lower()),
        )
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


def get_last_90s_placed(db_path: str, market_id: str, asset: str) -> bool:
    """True if we already placed a last-90s limit-99 order for this (market_id, asset)."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM last_90s_placed WHERE market_id = ? AND asset = ?",
            (market_id, str(asset).lower()),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def set_last_90s_placed(
    db_path: str,
    market_id: str,
    asset: str,
    order_id: Optional[str] = None,
    ticker: Optional[str] = None,
    count: Optional[int] = None,
    limit_price_cents: Optional[int] = None,
    side: Optional[str] = None,
) -> None:
    """Record that we placed a last-90s limit-99 order. Allows multiple orders per (market_id, asset)."""
    if not order_id:
        return
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO last_90s_placed
            (order_id, market_id, asset, placed_at, ticker, count, limit_price_cents, side)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                market_id,
                str(asset).lower(),
                datetime.now(timezone.utc).isoformat(),
                ticker,
                count,
                limit_price_cents,
                (side or "yes").lower() if side else "yes",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_last_90s_unresolved_for_market(
    db_path: str, market_id: str
) -> List[Tuple[str, str, Optional[str], Optional[str], Optional[int], Optional[int], Optional[str]]]:
    """Return list of (market_id, asset, order_id, ticker, count, limit_price_cents, side) for unresolved orders in this market."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT market_id, asset, order_id, ticker, count, limit_price_cents, side
            FROM last_90s_placed
            WHERE market_id = ? AND order_id IS NOT NULL AND order_id != '' AND resolved_at IS NULL
            """,
            (market_id,),
        )
        return [tuple(row) for row in cur.fetchall()]
    finally:
        conn.close()


def update_last_90s_resolved(
    db_path: str,
    market_id: str,
    asset: str,
    executed_count: int,
    market_result: Optional[str],
    outcome: str,
    order_id: Optional[str] = None,
) -> None:
    """Mark last-90s order as resolved with execution and outcome."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        if order_id:
            cur.execute(
                """
                UPDATE last_90s_placed
                SET resolved_at = ?, executed_count = ?, market_result = ?, outcome = ?
                WHERE order_id = ?
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    executed_count,
                    market_result,
                    outcome,
                    order_id,
                ),
            )
        else:
            cur.execute(
                """
                UPDATE last_90s_placed
                SET resolved_at = ?, executed_count = ?, market_result = ?, outcome = ?
                WHERE market_id = ? AND asset = ?
                """,
                (
                    datetime.now(timezone.utc).isoformat(),
                    executed_count,
                    market_result,
                    outcome,
                    market_id,
                    str(asset).lower(),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def get_last_90s_placements_for_stoploss(
    db_path: str, market_id: str, asset: str
) -> List[Tuple[str, str, int, int, str]]:
    """Return (order_id, ticker, count, limit_price_cents, side) for unresolved and not yet stop-lossed orders."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT order_id, ticker, count, limit_price_cents, side
            FROM last_90s_placed
            WHERE market_id = ? AND asset = ? AND order_id IS NOT NULL AND order_id != ''
              AND resolved_at IS NULL
              AND (stop_loss_triggered_at IS NULL OR stop_loss_triggered_at = '')
            """,
            (market_id, str(asset).lower()),
        )
        rows = cur.fetchall()
        out: List[Tuple[str, str, int, int, str]] = []
        for r in rows:
            out.append(
                (
                    r[0],
                    r[1] or "",
                    int(r[2] or 0),
                    int(r[3] or 99),
                    (r[4] or "yes").lower(),
                )
            )
        return out
    finally:
        conn.close()


def get_last_90s_skip_aggregator(db_path: str, market_id: str, asset: str) -> Optional[Dict]:
    """Load persisted skip-aggregator record for (market_id, asset). Returns None if not found or invalid."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT payload_json FROM last_90s_skip_aggregator WHERE market_id = ? AND asset = ?",
            (market_id, str(asset).lower()),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        return json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return None
    finally:
        conn.close()


def persist_last_90s_skip_aggregator(db_path: str, market_id: str, asset: str, rec: Dict) -> None:
    """Persist skip-aggregator record so resolve can read it after process restart."""
    if not rec:
        return
    ensure_state_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO last_90s_skip_aggregator (market_id, asset, payload_json)
            VALUES (?, ?, ?)
            """,
            (market_id, str(asset).lower(), json.dumps(rec)),
        )
        conn.commit()
    finally:
        conn.close()


def clear_last_90s_skip_aggregator(db_path: str, market_id: str, asset: str) -> None:
    """Remove persisted skip-aggregator for (market_id, asset) after place or after resolve."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM last_90s_skip_aggregator WHERE market_id = ? AND asset = ?",
            (market_id, str(asset).lower()),
        )
        conn.commit()
    finally:
        conn.close()


def get_hourly_last_90s_skip_aggregator(
    db_path: str, hour_market_id: str, asset: str, ticker: str, side: str
) -> Optional[Dict]:
    """Load persisted skip-aggregator for (hour_market_id, asset, ticker, side). Returns None if not found."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT payload_json FROM hourly_last_90s_skip_aggregator
             WHERE hour_market_id = ? AND asset = ? AND ticker = ? AND side = ?""",
            (hour_market_id, str(asset).lower(), ticker, (side or "yes").lower()),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        return json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return None
    finally:
        conn.close()


def persist_hourly_last_90s_skip_aggregator(
    db_path: str, hour_market_id: str, asset: str, ticker: str, side: str, rec: Dict
) -> None:
    """Persist skip-aggregator so resolve can read it after process restart."""
    if not rec:
        return
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO hourly_last_90s_skip_aggregator
            (hour_market_id, asset, ticker, side, payload_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (hour_market_id, str(asset).lower(), ticker, (side or "yes").lower(), json.dumps(rec)),
        )
        conn.commit()
    finally:
        conn.close()


def clear_hourly_last_90s_skip_aggregator(
    db_path: str, hour_market_id: str, asset: str, ticker: str, side: str
) -> None:
    """Remove persisted skip-aggregator after place or after resolve."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """DELETE FROM hourly_last_90s_skip_aggregator
             WHERE hour_market_id = ? AND asset = ? AND ticker = ? AND side = ?""",
            (hour_market_id, str(asset).lower(), ticker, (side or "yes").lower()),
        )
        conn.commit()
    finally:
        conn.close()


def get_all_hourly_last_90s_skip_aggregator_for_windows(
    db_path: str, prev_hour_market_ids: List[str]
) -> List[Tuple[str, str, str, str, Dict]]:
    """Load all persisted aggregator rows for the given window ids. Returns [(hour_market_id, asset, ticker, side, rec), ...]. Does not delete."""
    if not prev_hour_market_ids:
        return []
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        placeholders = ",".join("?" * len(prev_hour_market_ids))
        cur.execute(
            f"""SELECT hour_market_id, asset, ticker, side, payload_json
             FROM hourly_last_90s_skip_aggregator
             WHERE hour_market_id IN ({placeholders})""",
            prev_hour_market_ids,
        )
        out = []
        for row in cur.fetchall():
            try:
                rec = json.loads(row[4]) if row[4] else {}
                out.append((row[0], row[1], row[2], row[3], rec))
            except (json.JSONDecodeError, TypeError):
                pass
        return out
    finally:
        conn.close()


def clear_hourly_last_90s_skip_aggregator_for_windows(
    db_path: str, prev_hour_market_ids: List[str]
) -> None:
    """Delete all persisted aggregator rows for the given window ids."""
    if not prev_hour_market_ids:
        return
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        placeholders = ",".join("?" * len(prev_hour_market_ids))
        cur.execute(
            f"DELETE FROM hourly_last_90s_skip_aggregator WHERE hour_market_id IN ({placeholders})",
            prev_hour_market_ids,
        )
        conn.commit()
    finally:
        conn.close()


# --- Hourly last-90s style (limit 99, 2 YES below spot + 2 NO above spot per asset) ---


def get_hourly_limit_99_placed(
    db_path: str, hour_market_id: str, asset: str, ticker: str, side: str
) -> bool:
    """True if we already placed for this (hour_market_id, asset, ticker, side)."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT 1 FROM hourly_limit_99_placed
             WHERE hour_market_id = ? AND asset = ? AND ticker = ? AND side = ?""",
            (hour_market_id, str(asset).lower(), ticker, (side or "yes").lower()),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def set_hourly_limit_99_placed(
    db_path: str,
    hour_market_id: str,
    asset: str,
    ticker: str,
    side: str,
    order_id: Optional[str] = None,
    count: Optional[int] = None,
    limit_price_cents: Optional[int] = None,
) -> None:
    """Record one hourly limit-99 placement (per ticker, side)."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO hourly_limit_99_placed
            (hour_market_id, asset, ticker, side, placed_at, order_id, count, limit_price_cents)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                hour_market_id,
                str(asset).lower(),
                ticker,
                (side or "yes").lower(),
                datetime.now(timezone.utc).isoformat(),
                order_id,
                count,
                limit_price_cents,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_hourly_limit_99_unresolved_for_market(
    db_path: str, hour_market_id: str
) -> List[Tuple[str, str, str, str, Optional[str], Optional[int], Optional[int]]]:
    """Return (hour_market_id, asset, ticker, side, order_id, count, limit_price_cents) for unresolved."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT hour_market_id, asset, ticker, side, order_id, count, limit_price_cents
            FROM hourly_limit_99_placed
            WHERE hour_market_id = ? AND order_id IS NOT NULL AND order_id != '' AND resolved_at IS NULL
            """,
            (hour_market_id,),
        )
        return [tuple(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_hourly_limit_99_placements_for_stoploss(
    db_path: str, hour_market_id: str
) -> List[Tuple[str, str, str, str, Optional[str], Optional[int], Optional[int]]]:
    """Return (hour_market_id, asset, ticker, side, order_id, count, limit_price_cents) for unresolved and not yet stop-lossed."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT hour_market_id, asset, ticker, side, order_id, count, limit_price_cents
            FROM hourly_limit_99_placed
            WHERE hour_market_id = ? AND order_id IS NOT NULL AND order_id != ''
              AND resolved_at IS NULL AND (stop_loss_triggered_at IS NULL OR stop_loss_triggered_at = '')
            """,
            (hour_market_id,),
        )
        return [tuple(row) for row in cur.fetchall()]
    finally:
        conn.close()


def get_hourly_limit_99_stop_loss_triggered(
    db_path: str, hour_market_id: str, asset: str, ticker: str, side: str
) -> bool:
    """True if we already triggered stop loss for this (hour_market_id, asset, ticker, side)."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT 1 FROM hourly_limit_99_placed
             WHERE hour_market_id = ? AND asset = ? AND ticker = ? AND side = ?
             AND stop_loss_triggered_at IS NOT NULL AND stop_loss_triggered_at != ''""",
            (hour_market_id, str(asset).lower(), ticker, (side or "yes").lower()),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def set_hourly_limit_99_stop_loss_triggered(
    db_path: str, hour_market_id: str, asset: str, ticker: str, side: str
) -> None:
    """Record that we triggered stop loss for this (hour_market_id, asset, ticker, side)."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """UPDATE hourly_limit_99_placed SET stop_loss_triggered_at = ?
             WHERE hour_market_id = ? AND asset = ? AND ticker = ? AND side = ?""",
            (
                datetime.now(timezone.utc).isoformat(),
                hour_market_id,
                str(asset).lower(),
                ticker,
                (side or "yes").lower(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_hourly_limit_99_resolved(
    db_path: str,
    hour_market_id: str,
    asset: str,
    ticker: str,
    side: str,
    executed_count: int,
    market_result: Optional[str],
    outcome: str,
) -> None:
    """Mark hourly limit-99 order as resolved."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE hourly_limit_99_placed
            SET resolved_at = ?, executed_count = ?, market_result = ?, outcome = ?
            WHERE hour_market_id = ? AND asset = ? AND ticker = ? AND side = ?
            """,
            (
                datetime.now(timezone.utc).isoformat(),
                executed_count,
                market_result,
                outcome,
                hour_market_id,
                str(asset).lower(),
                ticker,
                (side or "yes").lower(),
            ),
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
