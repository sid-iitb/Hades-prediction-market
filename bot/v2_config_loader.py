"""
V2 strict config loader: loads and validates v2_common.yaml, v2_fifteen_min.yaml, v2_hourly.yaml.
Refuses to boot if the pipeline + strategies hierarchy is missing (no legacy fallback).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict

import yaml

logger = logging.getLogger(__name__)

V2_COMMON = "v2_common.yaml"
V2_FIFTEEN_MIN = "v2_fifteen_min.yaml"
V2_HOURLY = "v2_hourly.yaml"


def _load_yaml(path: Path) -> Dict[str, Any]:
    """Load a single YAML file. Returns {} if file missing or invalid."""
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("Failed to load %s: %s", path, e)
        return {}


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> None:
    """Merge overlay into base in-place. Dicts merged recursively; other values overwrite."""
    for k, v in overlay.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


def _validate_common(config: Dict[str, Any]) -> None:
    """Require intervals.fifteen_min.enabled, intervals.hourly.enabled, caps, no_trade_windows."""
    intervals = config.get("intervals")
    if not isinstance(intervals, dict):
        raise ValueError("Invalid V2 Config Schema: missing or invalid 'intervals' in common")
    fifteen = intervals.get("fifteen_min")
    hourly = intervals.get("hourly")
    if not isinstance(fifteen, dict) or "enabled" not in fifteen:
        raise ValueError("Invalid V2 Config Schema: missing 'intervals.fifteen_min.enabled' in common")
    if not isinstance(hourly, dict) or "enabled" not in hourly:
        raise ValueError("Invalid V2 Config Schema: missing 'intervals.hourly.enabled' in common")
    if "caps" not in config:
        raise ValueError("Invalid V2 Config Schema: missing 'caps' in common")
    if "no_trade_windows" not in config:
        raise ValueError("Invalid V2 Config Schema: missing 'no_trade_windows' in common")


def _validate_fifteen_min(config: Dict[str, Any]) -> None:
    """Require fifteen_min.pipeline.strategy_priority and fifteen_min.strategies."""
    fifteen_min = config.get("fifteen_min")
    if not isinstance(fifteen_min, dict):
        raise ValueError("Invalid V2 Config Schema: missing or invalid 'fifteen_min' in v2_fifteen_min.yaml")
    pipeline = fifteen_min.get("pipeline")
    if not isinstance(pipeline, dict) or "strategy_priority" not in pipeline:
        raise ValueError(
            "Invalid V2 Config Schema: missing 'fifteen_min.pipeline.strategy_priority' in v2_fifteen_min.yaml"
        )
    if "strategies" not in fifteen_min or not isinstance(fifteen_min["strategies"], dict):
        raise ValueError(
            "Invalid V2 Config Schema: missing 'fifteen_min.strategies' in v2_fifteen_min.yaml"
        )


def _validate_hourly(config: Dict[str, Any]) -> None:
    """Require hourly.pipeline.strategy_priority and hourly.strategies."""
    hourly = config.get("hourly")
    if not isinstance(hourly, dict):
        raise ValueError("Invalid V2 Config Schema: missing or invalid 'hourly' in v2_hourly.yaml")
    pipeline = hourly.get("pipeline")
    if not isinstance(pipeline, dict) or "strategy_priority" not in pipeline:
        raise ValueError(
            "Invalid V2 Config Schema: missing 'hourly.pipeline.strategy_priority' in v2_hourly.yaml"
        )
    if "strategies" not in hourly or not isinstance(hourly["strategies"], dict):
        raise ValueError(
            "Invalid V2 Config Schema: missing 'hourly.strategies' in v2_hourly.yaml"
        )


def load_v2_config(config_dir: str | Path = "config") -> Dict[str, Any]:
    """
    Load and merge v2_common.yaml, v2_fifteen_min.yaml, and v2_hourly.yaml from config_dir.
    Validates strict V2 schema; raises ValueError("Invalid V2 Config Schema") if any required
    key or structure is missing.

    Required in common:
      - intervals.fifteen_min.enabled
      - intervals.hourly.enabled
      - caps
      - no_trade_windows

    Required in v2_fifteen_min.yaml:
      - fifteen_min.pipeline.strategy_priority
      - fifteen_min.strategies

    Required in v2_hourly.yaml:
      - hourly.pipeline.strategy_priority
      - hourly.strategies

    Returns merged config dict (common + fifteen_min + hourly).
    """
    root = Path(config_dir)
    common = _load_yaml(root / V2_COMMON)
    if not common:
        raise ValueError("Invalid V2 Config Schema: could not load v2_common.yaml or file is empty")

    _validate_common(common)

    fifteen_min = _load_yaml(root / V2_FIFTEEN_MIN)
    if not fifteen_min:
        raise ValueError("Invalid V2 Config Schema: could not load v2_fifteen_min.yaml or file is empty")
    _validate_fifteen_min(fifteen_min)
    _deep_merge(common, fifteen_min)

    hourly = _load_yaml(root / V2_HOURLY)
    if not hourly:
        raise ValueError("Invalid V2 Config Schema: could not load v2_hourly.yaml or file is empty")
    _validate_hourly(hourly)
    _deep_merge(common, hourly)

    logger.info("V2 config loaded from %s (common + fifteen_min + hourly)", root)
    return common
