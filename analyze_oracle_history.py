#!/usr/bin/env python3
"""
Historical Oracle Performance Analyzer.

Reads v2_telemetry_last_90s from data/v2_state.db. Aggregation: group by window_id, then asset.
Per (window_id, asset): max divergence, opportunity cost, consistency %, additional trades gained.
Output: one section per window with separator, asset table, and window summary (total ticks,
total additional trades gained, worst-case divergence). Thresholds from V2 last_90s_limit_99.
Run from project root: python analyze_oracle_history.py
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

CONSISTENCY_PCT = 0.0005  # 0.05% = both oracles within this fraction of each other

# Fallback thresholds when V2 config is unavailable
_FALLBACK_THRESHOLDS: dict[str, float] = {
    "btc": 40.0,
    "eth": 2.5,
    "sol": 0.15,
    "xrp": 0.003,
}


def _config_dir() -> Path:
    root = Path(__file__).resolve().parent
    return root / "config"


def _load_thresholds_from_v2() -> dict[str, float]:
    """Load min_distance_at_placement from V2 last_90s_limit_99 config."""
    try:
        from bot.v2_config_loader import load_v2_config
        config = load_v2_config(_config_dir())
        strat = (config.get("fifteen_min") or {}).get("strategies") or {}
        mdp = (strat.get("last_90s_limit_99") or {}).get("min_distance_at_placement")
        if isinstance(mdp, dict):
            return {k.strip().lower(): float(v) for k, v in mdp.items() if v is not None}
        if mdp is not None:
            return {"btc": float(mdp), "eth": float(mdp), "sol": float(mdp), "xrp": float(mdp)}
    except Exception:
        pass
    return dict(_FALLBACK_THRESHOLDS)


def _db_path() -> Path:
    root = Path(__file__).resolve().parent
    return root / "data" / "v2_state.db"


def _threshold_for_asset(asset: str, thresholds: dict[str, float]) -> float:
    a = (asset or "").strip().lower()
    return thresholds.get(a, _FALLBACK_THRESHOLDS.get(a, 10.0))


def _parse_pre_data(pre_data: str | None) -> dict[str, Any]:
    if not pre_data:
        return {}
    try:
        return json.loads(pre_data)
    except (json.JSONDecodeError, TypeError):
        return {}


def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def run_analysis(
    db_path: Path | None = None,
    thresholds: dict[str, float] | None = None,
) -> list[dict[str, Any]]:
    db_path = db_path or _db_path()
    thresholds = thresholds or _load_thresholds_from_v2()

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT id, window_id, asset, placed, distance, pre_data
        FROM v2_telemetry_last_90s
        ORDER BY window_id, asset, id
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    # Group by (window_id, asset)
    groups: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        key = (r["window_id"] or "", (r["asset"] or "").strip().lower())
        groups.setdefault(key, []).append(r)

    results = []
    for (window_id, asset), group in sorted(groups.items()):
        thresh = _threshold_for_asset(asset, thresholds)
        distances = []
        avg_distances: list[float] = []
        divergences: list[float] = []
        within_005_pct = 0
        total_with_oracles = 0

        for r in group:
            dist = _safe_float(r.get("distance"))
            if dist is not None and dist >= 0:
                distances.append(dist)
            pd = _parse_pre_data(r.get("pre_data"))
            sk = _safe_float(pd.get("spot_kraken"))
            sc = _safe_float(pd.get("spot_coinbase"))
            dk = _safe_float(pd.get("distance_kraken"))
            dc = _safe_float(pd.get("distance_coinbase"))

            if dk is not None and dc is not None and dk >= 0 and dc >= 0:
                avg_d = (dk + dc) / 2.0
                avg_distances.append(avg_d)

            if sk is not None and sc is not None:
                div = abs(sk - sc)
                divergences.append(div)
                mid = (sk + sc) / 2.0
                if mid and mid > 0:
                    total_with_oracles += 1
                    if div / mid <= CONSISTENCY_PCT:
                        within_005_pct += 1

        # Opportunity cost / Additional trades: only computable when we have per-tick distance_kraken & distance_coinbase
        additional_trades: int | None = None
        if len(avg_distances) == len(group):
            additional_trades = 0
            for i, r in enumerate(group):
                dist = _safe_float(r.get("distance"))
                if dist is not None and dist >= 0 and dist < thresh and i < len(avg_distances):
                    if avg_distances[i] >= thresh:
                        additional_trades += 1

        max_divergence = max(divergences) if divergences else None
        consistency_pct = (100.0 * within_005_pct / total_with_oracles) if total_with_oracles else None

        results.append({
            "window_id": window_id,
            "asset": asset or "—",
            "threshold": thresh,
            "max_divergence": max_divergence,
            "opportunity_cost": additional_trades,
            "consistency_score_pct": consistency_pct,
            "additional_trades_gained": additional_trades,
            "ticks": len(group),
        })

    return results


def _group_by_window(results: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Group result rows by window_id (order preserved per window)."""
    by_window: dict[str, list[dict[str, Any]]] = {}
    for r in results:
        wid = r.get("window_id") or ""
        by_window.setdefault(wid, []).append(r)
    return by_window


# Column widths for aligned terminal table (asset table only, no window_id)
_TABLE_HEADERS = ["asset", "threshold", "max_div", "opp_cost", "consist%", "add_trades", "ticks"]
_COL_WIDTHS = [8, 10, 12, 10, 10, 12, 6]


def _cell_str(key: str, value: Any) -> str:
    if value is None:
        return "—"
    if key == "max_div" and isinstance(value, (int, float)):
        return f"{float(value):.4f}"
    if key == "consist%" and isinstance(value, (int, float)):
        return f"{float(value):.1f}"
    if key == "threshold" and isinstance(value, (int, float)):
        v = float(value)
        return f"{v:.4f}" if 0 < v < 1 else f"{v:.2f}"
    return str(value)


def _format_asset_table(asset_rows: list[dict[str, Any]]) -> str:
    """Format a single window's asset rows as an aligned table (no window_id column)."""
    if not asset_rows:
        return "(no assets)"
    key_map = {
        "asset": "asset",
        "threshold": "threshold",
        "max_div": "max_divergence",
        "opp_cost": "opportunity_cost",
        "consist%": "consistency_score_pct",
        "add_trades": "additional_trades_gained",
        "ticks": "ticks",
    }
    widths = list(_COL_WIDTHS)
    for r in asset_rows:
        for i, key in enumerate(_TABLE_HEADERS):
            k = key_map[key]
            v = r.get(k, 0 if key == "ticks" else None)
            s = _cell_str(key, v)
            widths[i] = max(widths[i], min(len(s), 18))
    sep = "+" + "+".join("-" * (widths[i] + 2) for i in range(len(widths))) + "+"

    def row_vals(r: dict[str, Any]) -> list[str]:
        return [
            _cell_str("asset", r.get("asset", "")),
            _cell_str("threshold", r.get("threshold")),
            _cell_str("max_div", r.get("max_divergence")),
            _cell_str("opp_cost", r.get("opportunity_cost")),
            _cell_str("consist%", r.get("consistency_score_pct")),
            _cell_str("add_trades", r.get("additional_trades_gained")),
            _cell_str("ticks", r.get("ticks", 0)),
        ]

    lines = [sep, "| " + " | ".join(h.ljust(widths[i]) for i, h in enumerate(_TABLE_HEADERS)) + " |", sep]
    for r in asset_rows:
        vals = row_vals(r)
        lines.append("| " + " | ".join((vals[i][: widths[i]]).ljust(widths[i]) for i in range(len(vals))) + " |")
    lines.append(sep)
    return "\n".join(lines)


def _window_summary(asset_rows: list[dict[str, Any]]) -> str:
    total_ticks = sum(r.get("ticks", 0) or 0 for r in asset_rows)
    total_add_trades = sum(r.get("additional_trades_gained") or 0 for r in asset_rows)
    divergences = [r.get("max_divergence") for r in asset_rows if r.get("max_divergence") is not None]
    worst_div = max(divergences) if divergences else None
    worst_str = f"{worst_div:.4f}" if worst_div is not None else "—"
    return f"Window Summary: total_ticks={total_ticks}  total_additional_trades_gained={total_add_trades}  worst_case_divergence={worst_str}"


def format_window_centric(results: list[dict[str, Any]]) -> str:
    """Format results grouped by window_id with separator and summary per window."""
    by_window = _group_by_window(results)
    if not by_window:
        return "(no data)"
    out: list[str] = []
    for window_id in sorted(by_window.keys()):
        asset_rows = by_window[window_id]
        # Sort by asset for stable order
        asset_rows = sorted(asset_rows, key=lambda r: (r.get("asset") or "").lower())
        out.append("")
        out.append("--- WINDOW: " + window_id + " ---")
        out.append("")
        out.append(_format_asset_table(asset_rows))
        out.append("")
        out.append(_window_summary(asset_rows))
    return "\n".join(out).lstrip("\n")


def main() -> int:
    try:
        results = run_analysis()
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        return 1

    print("Historical Oracle Performance (grouped by window_id, then asset)")
    print("Additional Trades Gained = ticks where MIN distance was below threshold but AVG would have been above.")
    print(format_window_centric(results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
