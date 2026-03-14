#!/usr/bin/env python3
"""
Check that WebSocket oracles are running and receiving prices.
Run: python3 -m tools.check_oracle_ws
Starts the oracles, waits ~5 seconds for data, then prints status.
Optional: --out file.json writes status JSON to file (e.g. for CI).
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bot.oracle_ws_manager import get_ws_status, is_ws_running, start_ws_oracles, stop_ws_oracles


def main() -> int:
    out_file = None
    if len(sys.argv) > 1 and sys.argv[1] == "--out" and len(sys.argv) > 2:
        out_file = Path(sys.argv[2])
    print("Starting WebSocket oracles (Kraken + Coinbase)...")
    start_ws_oracles()
    if not is_ws_running():
        print("ERROR: Oracles did not start (is 'websockets' installed?).")
        if out_file:
            out_file.write_text(json.dumps({"running": False, "error": "oracles did not start"}))
        return 1
    print("Waiting 2 seconds for ticker data...")
    time.sleep(2)
    status = get_ws_status()
    print("\n--- Oracle WS Status ---")
    print(f"Running: {status['running']}")
    for asset, data in status.get("assets", {}).items():
        k = data.get("kraken")
        k_age = data.get("kraken_age_s")
        cb = data.get("cb")
        cb_age = data.get("cb_age_s")
        if k is not None or cb is not None:
            print(
                f"  {asset}: kraken={k} (age_s={k_age})  cb={cb} (age_s={cb_age})"
            )
        else:
            print(f"  {asset}: no data yet")
    if not status.get("assets"):
        print("  (no assets in state yet)")
    print("---")
    if out_file:
        out_file.write_text(json.dumps(status, indent=2))
        print(f"Wrote {out_file}")
    stop_ws_oracles()
    return 0


if __name__ == "__main__":
    sys.exit(main())
