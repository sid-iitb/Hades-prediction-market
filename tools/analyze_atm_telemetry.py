#!/usr/bin/env python3
"""
Analyze v2_telemetry_atm for tuning momentum_trigger_3s and min_distance_at_placement.

Reports:
  - Counts by asset and reason (SOL/XRP vs others).
  - For SOL/XRP: distribution of momentum_delta and distance from pre_data.
  - Suggests what data to use for tuning.

We only write telemetry when momentum has been evaluated (ref_spot exists). Rows:
  - entry_intent: we placed an order (momentum + distance + spread + price band all passed).
  - low_distance_at_placement: momentum passed, distance < min_distance_at_placement.
  - entry_price_band: momentum passed, ask outside [min_entry_price, max_entry_price].
  - entry_spread: momentum passed, spread > max_entry_spread_cents.

We do NOT currently write when momentum is below threshold (no row). So "firing very less"
could mean: (1) momentum rarely exceeds trigger, or (2) momentum often passes but blocked by
distance/band/spread. This script shows what we have.

Run from project root: python3 tools/analyze_atm_telemetry.py [--hours 168]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "v2_state.db"


def _parse_pre_data(pre_data: str | None) -> dict:
    if not pre_data:
        return {}
    try:
        return json.loads(pre_data)
    except (json.JSONDecodeError, TypeError):
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze ATM telemetry for SOL/XRP tuning.")
    parser.add_argument("--hours", type=float, default=168.0, help="Hours of history (default 168 = 7 days)")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH}", file=sys.stderr)
        return 1

    cutoff_ts = __import__("time").time() - (args.hours * 3600.0)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    # --- 1. Counts by asset and reason ---
    print("=== 1. v2_telemetry_atm: counts by asset and reason (last %.0f hours) ===\n" % args.hours)
    cur = conn.execute(
        """
        SELECT asset, reason, COUNT(*) AS cnt
        FROM v2_telemetry_atm
        WHERE timestamp >= ?
        GROUP BY asset, reason
        ORDER BY asset, reason
        """,
        (cutoff_ts,),
    )
    rows = cur.fetchall()
    by_asset_reason: dict[str, dict[str, int]] = {}
    for r in rows:
        asset = (r["asset"] or "").strip().lower()
        reason = (r["reason"] or "").strip()
        cnt = int(r["cnt"] or 0)
        if asset not in by_asset_reason:
            by_asset_reason[asset] = {}
        by_asset_reason[asset][reason] = cnt

    assets_order = ["btc", "eth", "sol", "xrp"]
    for asset in assets_order:
        if asset not in by_asset_reason:
            print("  %s: (no rows)\n" % asset.upper())
            continue
        total = sum(by_asset_reason[asset].values())
        print("  %s: total = %d" % (asset.upper(), total))
        for reason in sorted(by_asset_reason[asset].keys()):
            print("    %s: %d" % (reason, by_asset_reason[asset][reason]))
        print()

    # --- 2. SOL and XRP: sample and distribution from pre_data ---
    for asset in ["sol", "xrp"]:
        print("=== 2.%s %s: momentum_delta and distance from pre_data (last %.0f hours) ===\n" % (
            "a" if asset == "sol" else "b", asset.upper(), args.hours))
        cur = conn.execute(
            """
            SELECT reason, distance, pre_data, timestamp
            FROM v2_telemetry_atm
            WHERE asset = ? AND timestamp >= ?
            ORDER BY timestamp DESC
            LIMIT 500
            """,
            (asset, cutoff_ts),
        )
        rows = cur.fetchall()
        if not rows:
            print("  (no rows)\n")
            continue

        momentum_deltas_by_reason: dict[str, list[float]] = {}
        distances_by_reason: dict[str, list[float]] = {}
        distance_buffers: list[float] = []

        for r in rows:
            reason = (r["reason"] or "").strip()
            pre = _parse_pre_data(r["pre_data"])
            md = pre.get("momentum_delta")
            dist = r["distance"] if r["distance"] is not None else None
            buf = pre.get("distance_buffer")
            if reason not in momentum_deltas_by_reason:
                momentum_deltas_by_reason[reason] = []
                distances_by_reason[reason] = []
            if md is not None:
                try:
                    momentum_deltas_by_reason[reason].append(float(md))
                except (TypeError, ValueError):
                    pass
            if dist is not None and dist >= 0:
                distances_by_reason[reason].append(float(dist))
            if buf is not None:
                try:
                    distance_buffers.append(float(buf))
                except (TypeError, ValueError):
                    pass

        print("  Rows: %d" % len(rows))
        for reason in sorted(momentum_deltas_by_reason.keys()):
            mds = momentum_deltas_by_reason[reason]
            dists = distances_by_reason.get(reason, [])
            if mds:
                print("  %s: momentum_delta count=%d min=%.4f max=%.4f avg=%.4f" % (
                    reason, len(mds), min(mds), max(mds), sum(mds) / len(mds)))
            if dists:
                print("           distance    count=%d min=%.4f max=%.4f avg=%.4f" % (
                    len(dists), min(dists), max(dists), sum(dists) / len(dists)))
        if distance_buffers:
            print("  distance_buffer (config) seen: min=%.4f max=%.4f" % (min(distance_buffers), max(distance_buffers)))
        print()

    # --- 3. What to use for tuning ---
    print("=== 3. Data to use for tuning momentum_trigger_3s and min_distance_at_placement ===\n")
    print("  - entry_intent count: how often we actually placed (all gates passed).")
    print("  - low_distance_at_placement: momentum passed but distance < buffer.")
    print("    If many: consider LOWERING min_distance_at_placement for that asset so more qualify.")
    print("    Use pre_data.distance and pre_data.distance_buffer to see how close we were.")
    print("  - entry_price_band / entry_spread: momentum passed but price or spread blocked.")
    print("  - momentum_delta in pre_data: actual $ move over the window when we wrote a row.")
    print("    If SOL/XRP have few rows and when they do, momentum_delta is often just above trigger:")
    print("    consider LOWERING momentum_trigger_3s so more ticks qualify (more rows will appear).")
    print("  - reason 'momentum_below_threshold' (if present): we had enough lookback but $ move was below")
    print("    momentum_trigger_3s. Use pre_data.momentum_delta distribution vs config to decide if you")
    print("    should LOWER the trigger (e.g. SOL 0.15 -> 0.12) to get more entries.")
    print()
    print("  Current config (v2_fifteen_min.yaml):")
    print("    momentum_trigger_3s: sol: 0.15, xrp: 0.002")
    print("    min_distance_at_placement (ATM): sol: 0.15, xrp: 0.0020")
    print()

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
