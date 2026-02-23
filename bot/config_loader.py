"""
Config loader: supports single-file and split config (config/ with common + interval files).
Merges common.yaml + hourly.yaml + fifteen_min.yaml + daily.yaml + weekly.yaml.
Missing interval files are skipped without error.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


def resolve_config_path(project_root: Optional[Path] = None) -> Path:
    """Return first existing path: config/config.yaml, then config.yaml."""
    root = Path(project_root) if project_root else Path.cwd()
    for name in ("config/config.yaml", "config.yaml"):
        p = root / name
        if p.exists():
            return p
    return root / "config/config.yaml"


def _load_yaml(path: Path) -> Dict[str, Any]:
    """Load YAML file. Returns {} on error or empty."""
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _deep_merge(base: Dict[str, Any], overlay: Dict[str, Any]) -> None:
    """Merge overlay into base in-place. Dicts are merged recursively; other values overwrite."""
    for k, v in overlay.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


def _load_split_config(config_dir: Path) -> Dict[str, Any]:
    """Load and merge config from config/ directory."""
    cfg = _load_yaml(config_dir / "common.yaml")
    if not cfg:
        return {}

    # Ensure exit_criteria exists with global defaults
    if "exit_criteria" not in cfg:
        cfg["exit_criteria"] = {}

    # hourly: merge schedule, thresholds, exit_criteria, spot_window into top level
    hourly = _load_yaml(config_dir / "hourly.yaml")
    if hourly:
        if "schedule" in hourly:
            if "schedule" not in cfg:
                cfg["schedule"] = {}
            _deep_merge(cfg["schedule"], hourly["schedule"])
            # Flatten schedule.entry_window so schedule has late_window_minutes, late_interval_seconds (or late_interval_minutes)
            sched = cfg["schedule"]
            entry_window = sched.pop("entry_window", None)
            if isinstance(entry_window, dict):
                for k, v in entry_window.items():
                    sched.setdefault(k, v)
            # If exit_criteria was nested under schedule, promote to cfg["exit_criteria"]["hourly"]
            if "exit_criteria" in sched:
                cfg["exit_criteria"]["hourly"] = sched.pop("exit_criteria")
        if "thresholds" in hourly:
            cfg["thresholds"] = hourly["thresholds"]
        if "exit_criteria" in hourly:
            cfg["exit_criteria"]["hourly"] = hourly["exit_criteria"]
        if "spot_window" in hourly:
            cfg["spot_window"] = hourly["spot_window"]
        if "spot_window_by_asset" in hourly:
            if "spot_window_by_asset" not in cfg:
                cfg["spot_window_by_asset"] = {}
            _deep_merge(cfg["spot_window_by_asset"], hourly["spot_window_by_asset"])

    # fifteen_min, daily, weekly: set cfg[key] = content; pull exit_criteria into cfg["exit_criteria"][key]
    for name in ("fifteen_min", "daily", "weekly"):
        data = _load_yaml(config_dir / f"{name}.yaml")
        if data:
            block = data.get(name, data)
            if name in data:
                cfg[name] = data[name]
            else:
                cfg[name] = data
            if isinstance(block, dict) and "exit_criteria" in block:
                cfg["exit_criteria"][name] = block["exit_criteria"]

    return cfg


def load_config(config_path: str) -> dict:
    """
    Load config from path. Supports:
    - Single file: config.yaml (or any .yaml path)
    - Split config: when path is config/config.yaml or path's directory contains common.yaml,
      merges common + hourly + fifteen_min + daily + weekly
    """
    path = Path(config_path)
    if not path.is_absolute():
        path = Path.cwd() / path

    config_dir = path if path.is_dir() else path.parent
    common_path = config_dir / "common.yaml"

    if common_path.exists():
        cfg = _load_split_config(config_dir)
    elif path.is_file():
        cfg = _load_yaml(path)
        if cfg.get("_legacy_root_config_deprecated"):
            raise ValueError(
                "Root config.yaml is deprecated and must not be used. "
                "It caused $50 order sizes (order.hourly.max_cost_cents: 5000). "
                "Use split config instead: run with --config config/config.yaml (default). "
                "Original file archived as config/config.yaml.archived"
            )
    else:
        cfg = {}

    return _post_process(cfg or {})


def _post_process(cfg: dict) -> dict:
    """Apply env overrides and validation (mode, assets, run_mode, intervals)."""
    mode = os.getenv("MODE", cfg.get("mode", "OBSERVE")).upper()
    if mode not in ("OBSERVE", "TRADE"):
        mode = "OBSERVE"
    cfg["mode"] = mode

    assets = cfg.get("assets", ["btc"])
    if isinstance(assets, str):
        assets = [assets]
    SUPPORTED_ASSETS = ("btc", "eth", "sol", "xrp")
    cfg["assets"] = [str(a).lower() for a in assets if str(a).lower() in SUPPORTED_ASSETS]
    if not cfg["assets"]:
        cfg["assets"] = ["btc"]

    run_mode = (cfg.get("run_mode") or "hourly").lower()
    if run_mode not in ("hourly", "fifteen_min", "both"):
        run_mode = "hourly"
    cfg["run_mode"] = run_mode

    intervals = cfg.get("intervals", {}) or {}
    if "hourly" not in intervals:
        intervals["hourly"] = {"enabled": True, "assets": cfg["assets"]}
    if "fifteen_min" not in intervals:
        intervals["fifteen_min"] = {"enabled": True, "assets": cfg["assets"]}
    cfg["intervals"] = intervals

    return cfg
