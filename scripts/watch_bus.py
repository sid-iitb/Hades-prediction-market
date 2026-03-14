#!/usr/bin/env python3
"""
Read-only dashboard for the SQLite Data Bus (market_data.db).
Run in a separate terminal to monitor live spot prices and Kalshi order books
without interfering with the main bot.
"""
from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_DB_PATH = _REPO_ROOT / "data" / "market_data.db"


def get_db_path() -> str:
    path = os.environ.get("MARKET_DATA_DB_PATH", str(_DEFAULT_DB_PATH))
    return str(Path(path).resolve())


def main() -> None:
    db_path = get_db_path()

    conn = None
    try:
        while True:
            if not Path(db_path).exists():
                os.system("clear" if os.name == "posix" else "cls")
                print("=== 🔴 LIVE DATA BUS MONITOR ===")
                print()
                print("Waiting for bot to create database...")
                time.sleep(1)
                continue

            if conn is None:
                try:
                    conn = sqlite3.connect(
                        f"file:{db_path}?mode=ro",
                        uri=True,
                    )
                    conn.row_factory = sqlite3.Row
                except sqlite3.OperationalError:
                    os.system("clear" if os.name == "posix" else "cls")
                    print("=== 🔴 LIVE DATA BUS MONITOR ===")
                    print()
                    print("Waiting for bot to create database...")
                    time.sleep(1)
                    continue

            try:
                os.system("clear" if os.name == "posix" else "cls")
                print("=== 🔴 LIVE DATA BUS MONITOR ===")
                print()

                now = time.time()

                # Oracle Data (spot prices)
                print("--- Oracle (Spot Prices) ---")
                cur = conn.execute("SELECT * FROM spot_prices")
                rows = cur.fetchall()
                if rows:
                    for row in rows:
                        asset = row["asset"]
                        spot = row["spot"]
                        ts = row["timestamp"]
                        age_ms = int((now - ts) * 1000)
                        print(f"  {asset}: ${spot:.2f}  (age: {age_ms} ms)")
                else:
                    print("  (no data)")
                print()

                # Kalshi Data (order books)
                print("--- Kalshi (Order Books) ---")
                cur = conn.execute(
                    "SELECT * FROM kalshi_books ORDER BY timestamp DESC LIMIT 10"
                )
                rows = cur.fetchall()
                if rows:
                    for row in rows:
                        ticker = row["ticker"]
                        yes_bid = row["yes_bid"]
                        yes_ask = row["yes_ask"]
                        no_bid = row["no_bid"]
                        no_ask = row["no_ask"]
                        ts = row["timestamp"]
                        age_ms = int((now - ts) * 1000)
                        print(
                            f"  {ticker} | yes: bid={yes_bid} ask={yes_ask} | no: bid={no_bid} ask={no_ask} | age: {age_ms} ms"
                        )
                else:
                    print("  (no data)")

            except sqlite3.OperationalError:
                os.system("clear" if os.name == "posix" else "cls")
                print("=== 🔴 LIVE DATA BUS MONITOR ===")
                print()
                print("Waiting for table data...")

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nStopping monitor...")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
