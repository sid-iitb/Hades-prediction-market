#!/usr/bin/env python3
"""
Pre-processing validation filter for last_90s strategy_report TSV (lean schema).
Reads strategy_report_last_90s_limit_99.tsv, applies validation rules, and writes
clean_strategy_report.tsv and (if any failures) corrupted_logs.tsv.

Usage:
  python tools/audit_tsv.py [--input reports/strategy_report_last_90s_limit_99.tsv]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = PROJECT_ROOT / "reports" / "strategy_report_last_90s_limit_99.tsv"

# Rule names for reporting
RULE_1 = "Distance Guard (placed==1 and distance <= min_distance_threshold)"
RULE_2 = "Market-Style Limit (limit_or_market=='market' and price_cents >= 99)"
RULE_3 = "Passive Limit Price (limit_or_market=='limit' and price_cents != 99)"
RULE_4 = "Phantom Fills (placed==0 and filled==1)"
RULE_5 = "Missing Spot (placed==1 and (spot null/NaN or 0))"
RULE_6 = "Time Window bypassed (placed==1 and (seconds_to_close > 120 or < 0))"


def _coerce_numeric(s: pd.Series, default: float = float("nan")) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(default)


def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """
    Apply validation rules. Returns (clean_df, corrupted_df, failure_counts_by_rule).
    A row is corrupted if it fails any rule.
    """
    if df.empty:
        return df.copy(), pd.DataFrame(), {r: 0 for r in [RULE_1, RULE_2, RULE_3, RULE_4, RULE_5, RULE_6]}

    failure_counts = {RULE_1: 0, RULE_2: 0, RULE_3: 0, RULE_4: 0, RULE_5: 0, RULE_6: 0}
    idx = df.index
    placed = _coerce_numeric(df.get("placed", pd.Series(0, index=idx)), 0).reindex(idx, fill_value=0).astype(int)
    distance = _coerce_numeric(df.get("distance", pd.Series(index=idx))).reindex(idx, fill_value=float("nan"))
    min_dist = _coerce_numeric(df.get("min_distance_threshold", pd.Series(index=idx))).reindex(idx, fill_value=0)
    limit_or_market = df.get("limit_or_market", pd.Series("", index=idx)).fillna("").astype(str).str.strip().str.lower()
    price_cents = _coerce_numeric(df.get("price_cents", pd.Series(index=idx)), -1).reindex(idx, fill_value=-1)
    filled = _coerce_numeric(df.get("filled", pd.Series(0, index=idx)), 0).reindex(idx, fill_value=0).astype(int)
    spot = _coerce_numeric(df.get("spot", pd.Series(index=idx))).reindex(idx, fill_value=float("nan"))
    seconds_to_close = _coerce_numeric(df.get("seconds_to_close", pd.Series(index=idx))).reindex(idx, fill_value=float("nan"))
    failed_mask = pd.Series(False, index=idx)

    # Rule 1: placed == 1 AND distance <= min_distance_threshold
    r1 = (placed == 1) & (min_dist > 0) & (distance <= min_dist)
    failed_mask |= r1
    failure_counts[RULE_1] = int(r1.sum())

    # Rule 2: limit_or_market == 'market' AND price_cents >= 99
    r2 = (limit_or_market == "market") & (price_cents >= 99)
    failed_mask |= r2
    failure_counts[RULE_2] = int(r2.sum())

    # Rule 3: limit_or_market == 'limit' AND price_cents != 99
    r3 = (limit_or_market == "limit") & (price_cents != 99)
    failed_mask |= r3
    failure_counts[RULE_3] = int(r3.sum())

    # Rule 4: placed == 0 AND filled == 1
    r4 = (placed == 0) & (filled == 1)
    failed_mask |= r4
    failure_counts[RULE_4] = int(r4.sum())

    # Rule 5: placed == 1 AND (spot is null/NaN or 0)
    r5 = (placed == 1) & (spot.isna() | (spot == 0))
    failed_mask |= r5
    failure_counts[RULE_5] = int(r5.sum())

    # Rule 6: placed == 1 AND (seconds_to_close > 120 OR seconds_to_close < 0)
    r6 = (placed == 1) & ((seconds_to_close > 120) | (seconds_to_close < 0))
    failed_mask |= r6
    failure_counts[RULE_6] = int(r6.sum())

    clean_df = df.loc[~failed_mask].copy()
    corrupted_df = df.loc[failed_mask].copy()
    return clean_df, corrupted_df, failure_counts


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit last_90s TSV: validate and split clean vs corrupted")
    ap.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Input TSV path (default: {DEFAULT_INPUT})",
    )
    args = ap.parse_args()
    input_path = args.input
    if not input_path.is_absolute():
        input_path = PROJECT_ROOT / input_path

    if not input_path.exists():
        print(f"Reading {input_path}...", file=sys.stderr)
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        return 1

    print(f"Reading {input_path}...")
    try:
        df = pd.read_csv(input_path, sep="\t", dtype=str, keep_default_na=False)
    except Exception as e:
        print(f"Error reading TSV: {e}", file=sys.stderr)
        return 1

    total = len(df)
    print(f"Total rows processed: {total}")

    clean_df, corrupted_df, failure_counts = validate(df)

    print("\nValidation Results:")
    for rule_name, count in failure_counts.items():
        print(f"  {rule_name}: {count} failures")

    n_clean = len(clean_df)
    n_corrupted = len(corrupted_df)

    if n_corrupted == 0:
        print("\nAUDIT PASSED! All rows perfectly clean.")
    else:
        print(f"\nAUDIT: {n_clean} clean, {n_corrupted} corrupted.")

    out_dir = input_path.parent
    clean_path = out_dir / "clean_strategy_report.tsv"
    clean_df.to_csv(clean_path, sep="\t", index=False)
    print(f"Clean file saved to {clean_path}.")

    if n_corrupted > 0:
        corrupted_path = out_dir / "corrupted_logs.tsv"
        corrupted_df.to_csv(corrupted_path, sep="\t", index=False)
        print(f"Corrupted file saved to {corrupted_path}.")

    return 0 if n_corrupted == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
