import os
import sqlite3
import time
from datetime import datetime, timezone, timedelta

from src.utils.fetch_current_predictions_kalshi import (
    fetch_kalshi_data_struct,
    select_markets_within_range,
)

DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "data",
    "kalshi_ingest.db",
)


def ensure_db(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            event_ticker TEXT,
            current_price REAL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS kalshi_markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            strike REAL,
            yes_bid REAL,
            yes_ask REAL,
            no_bid REAL,
            no_ask REAL,
            subtitle TEXT,
            ticker TEXT,
            FOREIGN KEY (run_id) REFERENCES ingest_runs(id)
        )
        """
    )
    conn.commit()


def open_db(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    ensure_db(conn)
    return conn


def insert_run(conn, event_ticker, current_price, markets):
    ts = datetime.now(timezone.utc).isoformat()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO ingest_runs (ts, event_ticker, current_price) VALUES (?, ?, ?)",
        (ts, event_ticker, current_price),
    )
    run_id = cur.lastrowid
    if markets:
        cur.executemany(
            """
            INSERT INTO kalshi_markets (
                run_id, strike, yes_bid, yes_ask, no_bid, no_ask, subtitle, ticker
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    m.get("strike"),
                    m.get("yes_bid"),
                    m.get("yes_ask"),
                    m.get("no_bid"),
                    m.get("no_ask"),
                    m.get("subtitle"),
                    m.get("ticker")
                )
                for m in markets
            ],
        )
    conn.commit()
    print("Pushed Record to Storage " + str(ts) + "BTC" + str(current_price))


def purge_old(conn, hours=1):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    cutoff_iso = cutoff.isoformat()
    cur = conn.cursor()
    # Delete markets for old runs first
    cur.execute(
        """
        DELETE FROM kalshi_markets
        WHERE run_id IN (
            SELECT id FROM ingest_runs WHERE ts < ?
        )
        """,
        (cutoff_iso,),
    )
    cur.execute(
        "DELETE FROM ingest_runs WHERE ts < ?",
        (cutoff_iso,),
    )
    conn.commit()


def ingest_loop(db_path=None, window=1500, interval_sec=1):
    db_path = db_path or os.getenv("KALSHI_DB_PATH") or DEFAULT_DB_PATH
    conn = open_db(db_path)
    try:
        while True:
            data, err = fetch_kalshi_data_struct()
            if err:
                print(f"Kalshi ingest error: {err}")
                time.sleep(interval_sec)
                continue

            current_price = data.get("current_price")
            markets = select_markets_within_range(
                data.get("markets", []),
                current_price,
                window=window,
            )
            insert_run(
                conn,
                data.get("event_ticker"),
                current_price,
                markets,
            )
            purge_old(conn, hours=24)
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("Ingest stopped.")
    finally:
        conn.close()


def main():
    ingest_loop()


if __name__ == "__main__":
    main()
