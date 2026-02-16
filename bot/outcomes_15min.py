"""
Store and retrieve 15-min market outcomes (yes/no resolution from Kalshi).
Logged when the bot runs and detects a newly settled previous market.
"""
import json
import os
from pathlib import Path
from typing import Optional


def _outcomes_path(db_path: str) -> str:
    """Path to outcomes JSON file (same dir as state DB)."""
    base = os.path.dirname(db_path) or "."
    return os.path.join(base, "15min_outcomes.json")


def load_outcomes(db_path: str) -> dict:
    """Load outcomes: {market_id: {"result": "yes"|"no", "logged_at": "..."}}"""
    path = _outcomes_path(db_path)
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def get_stored_outcome(db_path: str, market_id: str) -> Optional[str]:
    """Get stored result for market_id, or None if not found."""
    data = load_outcomes(db_path)
    entry = data.get(market_id)
    if not entry or not isinstance(entry, dict):
        return None
    return entry.get("result")


def save_outcome(db_path: str, market_id: str, result: str) -> None:
    """Store outcome for market_id."""
    from datetime import datetime, timezone

    path = _outcomes_path(db_path)
    data = load_outcomes(db_path)
    data[market_id] = {
        "result": result,
        "logged_at": datetime.now(timezone.utc).isoformat(),
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
