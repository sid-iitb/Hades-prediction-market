#!/usr/bin/env python3
"""
Export last 48 hours of last_90s_limit_99 telemetry to CSV for distance buffer and min_bid analysis.

- Source: v2_telemetry_last_90s in data/v2_state.db
- Only rows from the "last 85" window (seconds_to_close in [0, 85])
- Excludes corrupted rows where yes_bid=0 AND no_bid=0 (wrong parsing before V2 orderbook fix)
- Keeps table column names; adds yes_bid, no_bid from pre_data and config columns for thresholds

Columns in CSV:
  id, window_id, asset, placed, seconds_to_close, bid, distance, reason,
  yes_bid, no_bid, yes_ask, no_ask, spot_kraken,   (from pre_data)
  min_bid_cents_cfg, min_distance_at_placement_cfg,   (current config for that asset)
  pre_data, timestamp

Run from project root: python tools/export_last_90s_telemetry_csv.py [--hours 48] [--out file.csv]
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path

# Project root
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _db_path() -> Path:
    return ROOT / "data" / "v2_state.db"


def _config_dir() -> Path:
    return ROOT / "config"


def _load_last_90s_config() -> tuple[int | dict, dict]:
    """Load min_bid_cents and min_distance_at_placement from v2_fifteen_min.yaml.
    min_bid_cents may be a scalar (int) or per-asset dict { btc: 85, eth: 80, ... }.
    """
    min_bid_cents: int | dict = 95
    min_distance_at_placement = {}
    try:
        from bot.v2_config_loader import load_v2_config
        config = load_v2_config(_config_dir())
        strat = (config.get("fifteen_min") or {}).get("strategies") or {}
        last90 = strat.get("last_90s_limit_99") or {}
        raw_mb = last90.get("min_bid_cents", 95)
        if isinstance(raw_mb, dict):
            min_bid_cents = {k.strip().lower(): int(v) for k, v in raw_mb.items() if v is not None}
        else:
            min_bid_cents = int(raw_mb) if raw_mb is not None else 95
        mdp = last90.get("min_distance_at_placement")
        if isinstance(mdp, dict):
            min_distance_at_placement = {k.strip().lower(): float(v) for k, v in mdp.items() if v is not None}
        elif mdp is not None:
            v = float(mdp)
            min_distance_at_placement = {"btc": v, "eth": v, "sol": v, "xrp": v}
    except Exception:
        min_distance_at_placement = {"btc": 40.0, "eth": 2.5, "sol": 0.16, "xrp": 0.0025}
    return min_bid_cents, min_distance_at_placement


def _parse_pre_data(pre_data: str | None) -> dict:
    if not pre_data:
        return {}
    try:
        return json.loads(pre_data)
    except (json.JSONDecodeError, TypeError):
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Export last_90s_limit_99 telemetry to CSV (last 48h, last 85s window).")
    parser.add_argument("--hours", type=float, default=48.0, help="Hours of history (default 48)")
    parser.add_argument("--out", type=Path, default=None, help="Output CSV path (default: data/last_90s_telemetry_48h.csv)")
    parser.add_argument("--window-max", type=float, default=85.0, help="Max seconds_to_close for 'last 85' window (default 85)")
    args = parser.parse_args()

    db_path = _db_path()
    if not db_path.exists():
        print(f"DB not found: {db_path}", file=sys.stderr)
        return 1

    out_path = args.out or (ROOT / "data" / "last_90s_telemetry_48h.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    min_bid_cents_cfg, min_dist_cfg = _load_last_90s_config()
    cutoff_ts = __import__("time").time() - (args.hours * 3600.0)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT id, window_id, asset, placed, seconds_to_close, bid, distance, reason, pre_data, timestamp
        FROM v2_telemetry_last_90s
        WHERE timestamp >= ?
          AND seconds_to_close >= 0
          AND seconds_to_close <= ?
        ORDER BY timestamp ASC, window_id, asset, id
        """,
        (cutoff_ts, args.window_max),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    # Exclude corrupted: yes_bid=0 and no_bid=0 (wrong parsing)
    out_rows = []
    for r in rows:
        pre = _parse_pre_data(r.get("pre_data"))
        quote = pre.get("quote") or {}
        yes_bid = int(quote.get("yes_bid", 0) or 0)
        no_bid = int(quote.get("no_bid", 0) or 0)
        if yes_bid == 0 and no_bid == 0:
            continue
        asset = (r.get("asset") or "").strip().lower()
        r["yes_bid"] = yes_bid
        r["no_bid"] = no_bid
        r["yes_ask"] = quote.get("yes_ask")
        r["no_ask"] = quote.get("no_ask")
        r["spot_kraken"] = pre.get("spot_kraken")
        r["min_bid_cents_cfg"] = min_bid_cents_cfg.get(asset, 95) if isinstance(min_bid_cents_cfg, dict) else min_bid_cents_cfg
        r["min_distance_at_placement_cfg"] = min_dist_cfg.get(asset)

        # Keep table columns + extra; ensure pre_data and timestamp remain
        out_rows.append(r)

    # CSV column order: table cols first, then derived, then pre_data, timestamp
    fieldnames = [
        "id", "window_id", "asset", "placed", "seconds_to_close", "bid", "distance", "reason",
        "yes_bid", "no_bid", "yes_ask", "no_ask", "spot_kraken",
        "min_bid_cents_cfg", "min_distance_at_placement_cfg",
        "pre_data", "timestamp",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in out_rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

    print(f"Wrote {len(out_rows)} rows to {out_path} (excluded {len(rows) - len(out_rows)} corrupted yes_bid=0&no_bid=0)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
