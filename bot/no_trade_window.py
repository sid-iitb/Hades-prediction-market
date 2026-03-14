"""
Global no-trade windows: skip placing new orders during configured volatile times (e.g. Pacific).
Exit criteria still run; only execute_signals (new entries) is skipped when inside a window.
"""
from __future__ import annotations

from datetime import datetime, time
from typing import Any, Dict, List

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # type: ignore

import pytz


def _parse_time(s: str) -> time:
    """Parse 'HH:MM' or 'H:MM' to time."""
    parts = s.strip().split(":")
    h, m = int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
    return time(hour=h, minute=m, second=0, microsecond=0)


def in_no_trade_window(config: Dict[str, Any]) -> bool:
    """
    Return True if current time (in configured timezone) falls inside any no_trade_windows window.
    Used to skip placing new orders during high-volatility periods; exit criteria still run.
    """
    cfg = config.get("no_trade_windows") or {}
    if not cfg.get("enabled", False):
        return False
    windows: List[Dict[str, str]] = cfg.get("windows") or []
    if not windows:
        return False
    tz_name = cfg.get("timezone", "America/Los_Angeles")
    try:
        if ZoneInfo is not None:
            tz = ZoneInfo(tz_name)
        else:
            tz = pytz.timezone(tz_name)
    except Exception:
        return False
    now = datetime.now(tz).time()
    for w in windows:
        start_s = w.get("start")
        end_s = w.get("end")
        if not start_s or not end_s:
            continue
        try:
            start_t = _parse_time(start_s)
            end_t = _parse_time(end_s)
        except (ValueError, TypeError):
            continue
        # Same-day window: start <= now < end
        if start_t <= end_t:
            if start_t <= now < end_t:
                return True
        else:
            # Overnight window: e.g. 23:00 - 01:00
            if now >= start_t or now < end_t:
                return True
    return False
