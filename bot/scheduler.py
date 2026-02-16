"""
Scheduler - run loop with time logic.
- Run every N minutes
- For each market hour H, only start after (H + start_offset_minutes) America/Los_Angeles
- Optional: dynamic sleep via get_sleep_seconds callback (e.g. 15 sec in 15-min late window)
"""
import time
from datetime import datetime, timedelta
from typing import Callable, Optional, Tuple

import pytz

LA_TZ = pytz.timezone("America/Los_Angeles")


def get_minutes_since_hour_start() -> float:
    """Minutes since the start of the current hour in LA."""
    now = datetime.now(LA_TZ)
    return now.minute + now.second / 60.0 + now.microsecond / 1e6


def should_run(
    start_offset_minutes: int = 1,
) -> bool:
    """
    True if we're past (hour + start_offset_minutes).
    E.g. if start_offset=1 and it's 14:00:30, we're past 14:01 -> True.
    """
    mins = get_minutes_since_hour_start()
    return mins >= start_offset_minutes


def sleep_until_next_interval(interval_minutes: int) -> None:
    """Sleep until the next 5-minute (or configured) boundary."""
    now = datetime.now(LA_TZ)
    min_elapsed = now.minute + now.second / 60.0
    remainder = min_elapsed % interval_minutes
    if remainder < 0.1:
        remainder = interval_minutes
    sleep_mins = interval_minutes - remainder
    sleep_secs = sleep_mins * 60 - now.second - now.microsecond / 1e6
    if sleep_secs > 0:
        time.sleep(sleep_secs)


def run_loop(
    run_fn: Callable[[], None],
    interval_minutes: int = 5,
    start_offset_minutes: int = 1,
    get_sleep_seconds: Optional[Callable[[], float]] = None,
    get_schedule_state: Optional[Callable[[], Tuple[bool, float]]] = None,
) -> None:
    """
    Main loop: wait for start offset, then run. Sleep interval:
    - If get_schedule_state is provided (15-min only): skip run when outside late window,
      sleep until 2 min remaining, then run every 15 sec in late window.
    - Elif get_sleep_seconds is provided: use it after each run.
    - Otherwise use sleep_until_next_interval(interval_minutes).
    """
    while True:
        if get_schedule_state is not None:
            should_run_now, sleep_secs = get_schedule_state()
            if not should_run_now:
                mins_sleep = sleep_secs / 60.0
                if mins_sleep >= 1:
                    print(f"Outside trading window, sleeping {mins_sleep:.1f} min until next run")
                time.sleep(max(1, sleep_secs))
                continue
            if should_run(start_offset_minutes):
                try:
                    run_fn()
                except Exception as e:
                    print(f"Run error: {e}")
            time.sleep(max(1, sleep_secs))
        else:
            if should_run(start_offset_minutes):
                try:
                    run_fn()
                except Exception as e:
                    print(f"Run error: {e}")
            if get_sleep_seconds is not None:
                sleep_secs = max(1, get_sleep_seconds())
                time.sleep(sleep_secs)
            else:
                sleep_until_next_interval(interval_minutes)
