#!/usr/bin/env python3
"""
Quick sanity check for hourly_last_90s_limit_99 from bot log.

Scans log for [hourly_last_90s] and reports:
  - Loop started?
  - OBSERVE (would place) count
  - SKIP count (min_bid, distance_buffer, other)
  - PLACED count (actual orders)
  - OUTCOME count
  - Errors

Explains why no orders might be placed (mode=OBSERVE, never in window, only skips, etc.).

Usage:
  python tools/hourly_last_90s_sanity_check.py [path/to/bot.log]
  python tools/hourly_last_90s_sanity_check.py   # uses logs/bot.log
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
PREFIX = "[hourly_last_90s]"

# Patterns
RE_LOOP = re.compile(r"\[hourly_last_90s\]\s+Loop started")
RE_OBSERVE = re.compile(r"\[hourly_last_90s\].*OBSERVE:\s+would place")
RE_PLACED = re.compile(r"\[hourly_last_90s\].*PLACED\s+\w+\s+\S+\s+@")
RE_SKIP_MIN_BID = re.compile(r"\[hourly_last_90s\].*SKIP:.*min_bid_cents.*too risky")
RE_SKIP_DISTANCE = re.compile(r"\[hourly_last_90s\].*SKIP:.*distance_from_strike=.*min_distance_at_placement")
RE_SKIP_OTHER = re.compile(r"\[hourly_last_90s\].*SKIP:")
RE_OUTCOME = re.compile(r"\[hourly_last_90s\]\s+OUTCOME\s+")
RE_ERR = re.compile(r"\[hourly_last_90s\].*Error|Exception|failed")
RE_NO_SPOT = re.compile(r"\[hourly_last_90s\].*No spot price")
RE_NO_TICKERS = re.compile(r"\[hourly_last_90s\].*No eligible tickers")


def scan(log_path: Path) -> dict:
    counts = defaultdict(int)
    lines_found = 0
    if not log_path.exists():
        return {"exists": False, "counts": dict(counts), "lines_found": 0}
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if PREFIX not in line:
                continue
            lines_found += 1
            if RE_LOOP.search(line):
                counts["loop_started"] += 1
            if RE_OBSERVE.search(line):
                counts["observe"] += 1
            if RE_PLACED.search(line):
                counts["placed"] += 1
            if RE_SKIP_DISTANCE.search(line):
                counts["skip_distance"] += 1
            elif RE_SKIP_MIN_BID.search(line):
                counts["skip_min_bid"] += 1
            elif RE_SKIP_OTHER.search(line):
                counts["skip_other"] += 1
            if RE_OUTCOME.search(line):
                counts["outcome"] += 1
            if RE_ERR.search(line):
                counts["errors"] += 1
            if RE_NO_SPOT.search(line):
                counts["no_spot"] += 1
            if RE_NO_TICKERS.search(line):
                counts["no_tickers"] += 1
    return {"exists": True, "counts": dict(counts), "lines_found": lines_found}


def main():
    log_path = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "logs" / "bot.log"
    if not log_path.is_absolute():
        log_path = ROOT / log_path

    print("HOURLY LAST-90S SANITY CHECK")
    print("Log: %s" % log_path)
    print()

    out = scan(log_path)
    if not out["exists"]:
        print("Log file not found.")
        print()
        print("Reasons you might see no orders:")
        print("  1. Bot log is elsewhere – run: python tools/hourly_last_90s_sanity_check.py /path/to/bot.log")
        print("  2. config: mode must be TRADE (not OBSERVE) to place real orders")
        print("  3. Placement only runs when 0 < seconds_to_close <= 75 (last 75s of each hour)")
        print("  4. hourly_last_90s_limit_99.enabled must be true and bot run without --once")
        return 0

    c = out["counts"]
    lines_found = out["lines_found"]

    print("--- Log summary ---")
    print("Lines with [hourly_last_90s]: %d" % lines_found)
    print("Loop started:              %d" % c.get("loop_started", 0))
    print("OBSERVE (would place):      %d" % c.get("observe", 0))
    print("PLACED (real orders):       %d" % c.get("placed", 0))
    print("SKIP (distance_buffer):     %d" % c.get("skip_distance", 0))
    print("SKIP (min_bid):             %d" % c.get("skip_min_bid", 0))
    print("SKIP (other):               %d" % c.get("skip_other", 0))
    print("OUTCOME:                   %d" % c.get("outcome", 0))
    print("Errors:                    %d" % c.get("errors", 0))
    print("No spot price:             %d" % c.get("no_spot", 0))
    print("No eligible tickers:       %d" % c.get("no_tickers", 0))
    print()

    if lines_found == 0:
        # Check if 15-min last_90s has activity (same log file)
        fifteen_min_lines = 0
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    if "[last_90s]" in line and "[hourly_last_90s]" not in line:
                        fifteen_min_lines += 1
                        if fifteen_min_lines >= 3:
                            break
        except Exception:
            pass
        print("No [hourly_last_90s] lines in log. Possible causes:")
        print("  - Hourly last-90s thread not started:")
        print("      • Run without --once (thread is only started when not --once)")
        print("      • config/hourly.yaml hourly_last_90s_limit_99.enabled: true")
        print("      • Config merge includes hourly config (e.g. run_mode / intervals)")
        if fifteen_min_lines > 0:
            print("  - (15-min [last_90s] has activity in this log → same file; hourly thread was simply not started in this run.)")
        print("  - Placement only runs in last 75s of each hour; ensure bot was running then.")
    elif c.get("placed", 0) == 0:
        print("Why no orders placed?")
        if c.get("observe", 0) > 0:
            print("  - You have OBSERVE lines → config mode is OBSERVE. Set mode: TRADE in config to place real orders.")
        elif c.get("skip_min_bid", 0) + c.get("skip_distance", 0) + c.get("skip_other", 0) > 0:
            print("  - All candidates were SKIPped (min_bid, distance, or other). Check min_bid_cents and min_distance_at_placement.")
        elif c.get("no_tickers", 0) > 0 and c.get("observe", 0) == 0:
            print("  - No eligible tickers in window, or never in placement window (last 75s of hour).")
        else:
            print("  - Placement window is last 75 seconds of each hour. Bot must be running in that window.")
            print("  - Check config: mode: TRADE (common.yaml or merged config)")
    else:
        print("Orders were placed: %d PLACED, %d OUTCOME" % (c.get("placed", 0), c.get("outcome", 0)))

    return 0


if __name__ == "__main__":
    sys.exit(main())
