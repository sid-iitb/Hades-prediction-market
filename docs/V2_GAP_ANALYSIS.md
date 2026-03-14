# V2 Spec vs V1 Codebase — Gap Analysis

This document lists gaps between the **Unified Strategy Pipeline (Bot V2)** specification and the **V1 codebase** reality. Each gap is followed by the **exact edits** to apply to the main migration plan so V2 does not miss business logic.

**Guardrails (unchanged):** No legacy config fallback; no sharing V1 DBs; no modifications to V1 code.

---

## 1. Config Keys

### 1.1 last_90s_limit_99

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| **panic_seconds** | `last_90s_strategy.py`: `cfg.get("panic_seconds", 30)` — used to treat final N seconds as "panic zone" (e.g. market sell path). | Add to `last_90s_limit_99` (and hourly_last_90s) in target YAML: `panic_seconds: 30`. |
| **Dynamic bid floor** | V1 hardcodes: when `seconds_to_close > 65` use bid floor **98**; when ≤ 65 use `min_bid_cents` (e.g. 96). | In spec "Pipeline Data Structures" or strategy port notes, add: *"Last-90s strategies may use a time-weighted bid floor: e.g. require bid ≥ 98 when seconds_to_close > 65, else require bid ≥ min_bid_cents (config)."* Or add optional config: `early_bid_floor_cents: 98`, `early_bid_floor_seconds: 65`. |
| **monitor_window_seconds** | `fifteen_min.yaml`: `monitor_window_seconds: 120` — pre-placement telemetry window. | Ensure target layout lists `monitor_window_seconds` under `strategies.last_90s_limit_99`. |

### 1.2 market_maker_strategy

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| Full key set not enumerated | `fifteen_min.yaml` + `market.py` / strategy code. | In the section that shows target YAML layout for fifteen_min, add an explicit list under `market_maker_strategy`: `mode`, `max_cost_cents`, `min_seconds_to_close`, `max_seconds_to_close`, `max_mm_distance`, `min_price_parity`, `max_price_parity`, `min_spread_cents`, `safe_velocity_threshold_5s`, `scratch_timeout_seconds`, `max_acceptable_loss_cents`, `scratch_spot_movement_threshold`, `zero_fill_timeout_seconds`. |

### 1.3 atm_breakout_strategy

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| Full key set not enumerated | `fifteen_min.yaml` + `atm_breakout_strategy.py`. | Add under `atm_breakout_strategy`: `mode`, `min_seconds_to_close`, `max_seconds_to_close`, `window_seconds`, `contracts`, `max_cost_cents`, `min_price_parity`, `max_price_parity`, `momentum_trigger_3s`, `take_profit_cents`, `stop_loss_cents`, `trailing_stop_cents`, `safe_distance_threshold`, `safe_zone_window_seconds`. |

### 1.4 signals_15min / risk_guards (15-min)

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| **exit_criteria** full keys | `fifteen_min.yaml`: `panic_stop_loss_pct`, `stop_loss_persistence_polls`, `evaluation_interval_seconds`. | In target layout, under `signals_15min.exit_criteria` list: `stop_loss_pct`, `panic_stop_loss_pct`, `stop_loss_persistence_polls`, `evaluation_interval_seconds`, and optionally `profit_target_pct`. |
| **risk_guards** nested keys | `fifteen_min.yaml` + `risk_guards.py`: `enabled`, `no_new_entry_cutoff_seconds`, `persistence_polls`, `late_persistence_override` (enabled, seconds_to_close_lte, persistence_polls), `hard_flip_exit`, `stop_once_disable` (enabled, max_stopouts_per_ticker), `recent_cross` (enabled, from_below_cents, to_at_least_cents, extra_persistence_polls), `distance_buffer` (enabled, assets with time-tiered or pct/floor_usd), `reversal_after_stop` (enabled, max_reversals_per_ticker, allow_inside_cutoff, require_opposite_price_at_least_cents). | In spec, add a short subsection or table: "Risk guards (15-min and hourly) must support these keys under `strategies.<name>.risk_guards` (or `hourly_risk_guards`): …" with the list above. |

### 1.5 v2_common.yaml (global)

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| **no_trade_windows** | `common.yaml`: `no_trade_windows.enabled`, `.timezone`, `.windows` (start/end). Used in `no_trade_window.in_no_trade_window(config)` to block new entry; exits still run. | State explicitly: **v2_common.yaml** must include **no_trade_windows** (enabled, timezone, windows). Pipeline must skip new entry when current time falls inside a no_trade_window; exit criteria may still run. |
| **caps** | `common.yaml`: `caps.cap_scope` (combined | per_side), `caps.fifteen_min.max_orders_per_ticker`, `max_total_orders_per_15min`, `caps.hourly.*`, etc. `main.py` and `execution.py` use these with `get_ticker_order_count` / `get_total_order_count`. | State explicitly: **v2_common.yaml** must include **caps** (cap_scope, per-interval max_orders_per_ticker, max_total_orders_per_15min, max_total_orders_per_hour). Aggregator (or executor) enforces these using **v2_order_registry** counts (no V1 bot_state). |

### 1.6 Hourly

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| **schedule.entry_window** | `hourly.yaml`: `late_window_minutes`, `late_interval_seconds`. | Already covered in existing doc; ensure v2_hourly target shows `signals_hourly.entry_window.late_window_minutes` and `late_interval_seconds`. |
| **spot_window / spot_window_by_asset** | `hourly.yaml` + main.py. | Ensure target layout shows these under `signals_hourly` (or `hourly.pipeline`). |

---

## 2. State & DB

### 2.1 v2_order_registry

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| **count** (contracts) | `last_90s_placed.count`, `hourly_limit_99_placed`: number of contracts. Executor needs this for exit (how many to sell). | Add column **count** (INTEGER) to **v2_order_registry**. |
| **limit_price_cents** | `last_90s_placed.limit_price_cents` — used for reporting and for knowing at what limit the order was placed. | Add column **limit_price_cents** (INTEGER, optional) to **v2_order_registry**. |

### 2.2 v2_strategy_reports

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| Optional telemetry columns | V1 `strategy_report_last_90s`: ticker, skip_reason, skip_details, resolution_price, spot_kraken, spot_coinbase, distance_kraken, distance_coinbase, seconds_to_close, etc. | Spec is fine with minimal PnL columns. Optional (for parity/backtest): add **ticker**, **skip_reason**, **skip_details**, **resolution_price** as optional columns in v2_strategy_reports, or state that skip reporting can be log-only in V2. |

### 2.3 Caps enforcement

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| Cap enforcement source | V1 uses **bot_state** (hour_market_id, ticker, side, placed_order_count) and cap_scope. | State explicitly: V2 enforces caps using **v2_order_registry** only (counts per interval, market_id, ticker, side when cap_scope=per_side; combined when cap_scope=combined). No bot_state table in V2. |

### 2.4 Risk guard / exit in-memory state

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| Keying | `risk_guards._ticker_states`, `_window_states`; `exit_criteria._position_excursion`, `_sl_warning_streak` keyed by window_id or hour_market_id|ticker|side. | Add one sentence to spec: "Risk guard and exit-criteria in-memory state (e.g. persistence streak, anchor ticker, stop-once-disable, reversal count, MFE/MAE) must be keyed by **(interval, window_id)** and (ticker, side) where applicable so 15-min and hourly do not collide." |

---

## 3. Kalshi API / Oracles — WindowContext

### 3.1 Quote (orderbook)

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| Explicit fields | `last_90s_strategy` and others use **yes_bid**, **yes_ask**, **no_bid**, **no_ask** from orderbook. | In **WindowContext** (or "Pipeline Data Structures"), state that **quote** includes (or is an object with) **yes_bid**, **yes_ask**, **no_bid**, **no_ask** (integers, cents). |

### 3.2 Market (strike source)

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| Strike derivation | `market.py` `extract_strike_from_market`: uses **floor_strike**, **ceiling_strike**, **strike**, **subtitle** from Kalshi market object. | In data layer / WindowContext description, add: "Strike is derived from market API fields: **floor_strike**, **ceiling_strike**, **strike**, **subtitle** (same logic as V1 extract_strike_from_market)." |

### 3.3 Positions (for exit)

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| Position data for exit | `exit_criteria._normalize_position`: ticker, side, count, entry_price_cents. Mark price and PnL% from **get_top_of_book** (yes_bid/yes_ask/no_bid/no_ask). | State explicitly: **WindowContext** (or the context passed to **evaluate_exit**) must include **positions** for the current asset/window — list of normalized positions with **ticker**, **side**, **count**, **entry_price_cents** — so strategies can compute mark-to-market and return ExitActions. Quote (orderbook) provides mark price. |

### 3.4 Optional dual-oracle reporting

| Gap | V1 location | Proposed spec change |
|-----|-------------|----------------------|
| distance_kraken / distance_coinbase | V1 strategy_report_last_90s stores distance_kraken, distance_coinbase for analysis. | Existing §9 already has distance_kraken, distance_coinbase on WindowContext; no change. Optional: v2_strategy_reports may add columns for telemetry if needed later. |

---

## 4. Exact Edits to Apply to the Migration Plan .md

- **§1.3 (v2_state.db)**  
  - **v2_order_registry:** Add two columns: **count** (INTEGER), **limit_price_cents** (INTEGER, nullable).  
  - Add one sentence after the Aggregation section: "Caps (max_orders_per_ticker, max_total_orders_per_interval, cap_scope) are read from v2_common and enforced by the aggregator using v2_order_registry counts."

- **§1.2 (Pipeline Data Structures) / WindowContext**  
  - **WindowContext:** Require **quote** with **yes_bid**, **yes_ask**, **no_bid**, **no_ask**; require **positions** (list of {ticker, side, count, entry_price_cents}) for exit evaluation.  
  - **Data layer:** State that strike is computed from market fields **floor_strike**, **ceiling_strike**, **strike**, **subtitle** (per V1 extract_strike_from_market).

- **§2 (YAML Config)**  
  - **v2_common:** Require **no_trade_windows** (enabled, timezone, windows) and **caps** (cap_scope, per-interval max_orders_per_ticker, max_total_orders_per_15min / max_total_orders_per_hour).  
  - **last_90s_limit_99:** Add **panic_seconds** (default 30); add **monitor_window_seconds**; add note: "Last-90s may use time-weighted bid floor: bid ≥ 98 when seconds_to_close > 65, else ≥ min_bid_cents."  
  - **market_maker_strategy / atm_breakout_strategy:** Add bullet list of all config keys (see 1.2, 1.3 above).  
  - **signals_15min / signals_hourly:** Ensure **exit_criteria** includes panic_stop_loss_pct, stop_loss_persistence_polls, evaluation_interval_seconds; **risk_guards** (or hourly_risk_guards) key list as in 1.4.

- **§4 (Concurrency & Edge Cases)** or new subsection  
  - Add: "Risk guard and exit-criteria in-memory state must be keyed by (interval, window_id) and (ticker, side) where applicable."

---

## 5. Summary

- **Config:** No_trade_windows and caps in v2_common; panic_seconds, monitor_window_seconds, and time-weighted bid floor for last_90s; full key lists for market_maker, atm_breakout, risk_guards, exit_criteria.  
- **State/DB:** v2_order_registry gains **count** and **limit_price_cents**; caps enforced via v2_order_registry only; in-memory state keying by (interval, window_id).  
- **WindowContext/API:** Quote with yes_bid, yes_ask, no_bid, no_ask; strike from floor_strike/ceiling_strike/strike/subtitle; **positions** on context for evaluate_exit.

Once these edits are applied to the main migration plan, the spec captures all V1 business logic within the strict V2 rules.
