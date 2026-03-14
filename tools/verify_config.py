#!/usr/bin/env python3
"""
Verify merged config for bot.main. Use same path as: python -m bot.main --config config/config.yaml
Prints whether hourly_last_90s and 15-min last_90s threads would start (when not --once).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bot.config_loader import load_config


def main():
    config_path = ROOT / "config" / "config.yaml"
    if not config_path.exists():
        print("Config not found: %s" % config_path)
        return 1

    config = load_config(str(config_path))
    print("Loaded config: %s" % config_path)
    print("  mode: %s" % config.get("mode"))
    print("  run_mode: %s" % config.get("run_mode"))
    print("  assets: %s" % config.get("assets"))
    print()

    # 15-min last_90s
    last_90s_cfg = (config.get("fifteen_min") or {}).get("last_90s_limit_99") or config.get("last_90s_limit_99")
    last_90s_enabled = isinstance(last_90s_cfg, dict) and last_90s_cfg.get("enabled", False)
    print("15-min last_90s_limit_99: %s (enabled=%s)" % ("present" if last_90s_cfg else "MISSING", last_90s_enabled))

    # Hourly last_90s
    hourly_90s_cfg = config.get("hourly_last_90s_limit_99") or (config.get("schedule") or {}).get("hourly_last_90s_limit_99")
    hourly_90s_enabled = isinstance(hourly_90s_cfg, dict) and hourly_90s_cfg.get("enabled", False)
    print("hourly_last_90s_limit_99:  %s (enabled=%s)" % ("present" if hourly_90s_cfg else "MISSING", hourly_90s_enabled))
    print()

    if hourly_90s_cfg:
        print("Hourly last-90s thread will start: YES (when run without --once)")
    else:
        print("Hourly last-90s thread will start: NO - config block missing from merged config")
    if last_90s_cfg and last_90s_enabled:
        print("15-min last-90s thread will start: YES (when run without --once)")
    print()
    print("Run command: python -m bot.main --config config/config.yaml")
    print("Do NOT use --once if you want the hourly/15-min last-90s threads to run.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
