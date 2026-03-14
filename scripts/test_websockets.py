#!/usr/bin/env python3
"""
Standalone read-only WebSocket diagnostic: Kraken + Coinbase (all 4 assets) + Kalshi portfolio balance.
Runs for 1 hour, logs timestamp + kraken/cb spot for btc, eth, sol, xrp + cash_balance to ws_diagnostic_log.csv.
Run from repo root: python scripts/test_websockets.py
"""
from __future__ import annotations

import csv
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from pathlib import Path

# Add repo root so we can import bot modules
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

CSV_PATH = REPO_ROOT / "ws_diagnostic_log.csv"
DEBUG_LOG_PATH = REPO_ROOT / "ws_diagnostic_debug.txt"
RUN_SECONDS = 3600  # 1 hour; set to 10 for a quick test
ASSETS = ("btc", "eth", "sol", "xrp")
ROW_TIMEOUT_SEC = 8.0

# CSV structure (ws_diagnostic_log.csv):
#   timestamp          | Kraken (4 assets)     | Coinbase (4 assets)    | Kalshi
#   ISO UTC             | kraken_btc, _eth,     | cb_btc, cb_eth,        | cash_balance
#                       | _sol, kraken_xrp      | cb_sol, cb_xrp         | (cents)


def _harvest_one_row(client, start: float, end: float) -> list:
    """Fetch one row of data (spot for all assets + balance). Used with executor timeout."""
    _debug_log("harvest_start")
    from bot.oracle_ws_manager import get_safe_spot_prices_sync
    t0 = time.time()
    ts = datetime.now(timezone.utc).isoformat()
    kraken_row = []
    cb_row = []
    for asset in ASSETS:
        _debug_log(f"oracle_{asset}_before")
        spot = get_safe_spot_prices_sync(asset, max_age_seconds=10.0, require_both=False)
        _debug_log(f"oracle_{asset}_after")
        kraken_row.append((spot.get("kraken") if spot else None) or "")
        cb_row.append((spot.get("cb") if spot else None) or "")
    t_oracle = time.time() - t0
    _debug_log(f"oracle_done {t_oracle:.2f}s")
    cash_balance = ""
    if client:
        try:
            bal = client.get_balance(timeout=5)
            cash_balance = bal.get("cash_available") or bal.get("balance")
            cash_balance = cash_balance if cash_balance is not None else ""
        except Exception as e:
            cash_balance = f"err:{e!s}"[:64]
        t_balance = time.time() - t0 - t_oracle
        _debug_log(f"balance_done {t_balance:.2f}s")
    return [ts] + kraken_row + cb_row + [cash_balance]


def _debug_log(msg: str) -> None:
    """Append one line to debug log (to see where timeout occurs)."""
    try:
        with open(DEBUG_LOG_PATH, "a") as f:
            f.write(f"{datetime.now(timezone.utc).isoformat()} {msg}\n")
            f.flush()
    except Exception:
        pass


def main() -> None:
    print("[test_websockets] Starting Oracle (Kraken + Coinbase) WebSockets and Kalshi client (portfolio)...")
    from bot.oracle_ws_manager import get_safe_spot_prices_sync, start_ws_oracles

    start_ws_oracles()

    client = None
    try:
        from src.client.kalshi_client import KalshiClient
        client = KalshiClient()
        print("[test_websockets] Kalshi client OK (will log cash_balance)", flush=True)
    except Exception as e:
        print(f"[test_websockets] WARNING: Kalshi client init failed (balance will be empty): {e}", flush=True)
    # Give WS time to connect and receive first data
    print("[test_websockets] Waiting 5s for first WS data...")
    time.sleep(5)
    # Clear debug log so we see only this run
    try:
        DEBUG_LOG_PATH.write_text("")
        print(f"[test_websockets] Debug log: {DEBUG_LOG_PATH}", flush=True)
    except Exception as e:
        print(f"[test_websockets] WARNING: could not clear debug log: {e}", flush=True)

    header = ["timestamp"] + [f"kraken_{a}" for a in ASSETS] + [f"cb_{a}" for a in ASSETS] + ["cash_balance"]
    with open(CSV_PATH, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)

    start = time.time()
    end = start + RUN_SECONDS
    row_count = 0

    harvest_executor = ThreadPoolExecutor(max_workers=1)
    try:
        while time.time() < end:
            try:
                fut = harvest_executor.submit(_harvest_one_row, client, start, end)
                row = fut.result(timeout=ROW_TIMEOUT_SEC)
                with open(CSV_PATH, "a", newline="") as f:
                    w = csv.writer(f)
                    w.writerow(row)
                row_count += 1
                if row_count % 60 == 0:
                    print(f"[test_websockets] {row_count} rows written, {(time.time() - start):.0f}s elapsed", flush=True)
            except FuturesTimeoutError:
                with open(CSV_PATH, "a", newline="") as f:
                    w = csv.writer(f)
                    w.writerow([datetime.now(timezone.utc).isoformat()] + ["timeout"] * (len(ASSETS) * 2) + ["timeout"])
                row_count += 1
                # Show last debug lines so user can see where it got stuck
                try:
                    lines = DEBUG_LOG_PATH.read_text().strip().split("\n")
                    last5 = lines[-5:] if len(lines) >= 5 else lines
                    snippet = " | ".join(last5) if last5 else "(no lines yet)"
                except Exception as e:
                    snippet = f"(read err: {e})"
                print(f"[test_websockets] Row timeout after {ROW_TIMEOUT_SEC}s. Debug: {snippet}", flush=True)
            except Exception as e:
                print("\n" + "!" * 60, flush=True)
                print("[test_websockets] ERROR IN HARVEST LOOP (WebSocket thread may have died):", flush=True)
                print(e, flush=True)
                import traceback
                traceback.print_exc()
                print("!" * 60 + "\n", flush=True)
                sys.exit(1)
            time.sleep(1)

        harvest_executor.shutdown(wait=False)
        print(f"[test_websockets] Done. {row_count} rows written to {CSV_PATH} over {RUN_SECONDS}s.", flush=True)
    except Exception as e:
        print("\n" + "!" * 60, flush=True)
        print("[test_websockets] FATAL ERROR:", flush=True)
        print(e, flush=True)
        import traceback
        traceback.print_exc()
        print("!" * 60 + "\n", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
