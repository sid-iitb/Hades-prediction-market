# Architecture Spec: Unified Strategy Pipeline (Bot V2)

## Executive Summary

**Bot V2** is a **greenfield deployment**: a new, unified pipeline that runs **in parallel** to the existing Bot V1. We do **not** modify V1 code, config, or databases. V2 is built from scratch with its own entry point, config schema, and database.

V2 delivers:

1. **Two threads** (15-min, hourly), each invoking the **same** pipeline with `interval` (window type). At any time at most one 15-min and one hourly window; strategies are tied to interval via config. **Fetches once per window/asset per tick**: market, orderbook, spot, open orders, positions.
2. **Single pipeline implementation**: One code path `run_unified_pipeline(interval)`; data layer, strategy eval, aggregator, executor shared; no separate 15-min vs hourly pipeline code.
3. **Central executor**: Produces order intents per (window, asset); central executor places and records strategy ownership; evaluates exits per strategy; uses Kalshi batch APIs where supported (§7).
4. **Strict YAML config** (§8): V2 uses **v2_common.yaml**, **v2_fifteen_min.yaml**, **v2_hourly.yaml** with the `pipeline` + `strategies.<name>` hierarchy natively. If config does not match this layout, V2 **refuses to boot**.
5. **Unified Kraken/Coinbase spot and distance** (§9): single place for spot fetch (WS preferred, REST fallback); strike cached per window_id; distance-from-strike computed once per tick.

**Deployment:** V2 has its own **entry point** (e.g. `bot/v2_main.py`), its own **strict config files** (v2_common.yaml, v2_fifteen_min.yaml, v2_hourly.yaml), and a **fresh database** (**v2_state.db**) for the central order registry and V2 strategy reports. V1 remains untouched. When V2 goes live for trading, **V1 must be paused** so both bots do not compete on the same Kalshi account.

A **gap analysis** (V2 spec vs V1 code) is in **docs/V2_GAP_ANALYSIS.md**. All identified gaps have been patched into this plan (registry columns, WindowContext quote/positions, v2_common no_trade_windows/caps, last_90s panic_seconds/monitor_window_seconds/bid floor, config key checklists, caps enforcement via v2_order_registry only, in-memory state keying).



---

## Scope: In / Out, Pros and Cons

### In scope (explicit)

- **15-min and hourly intervals only**: Unified pipeline and two threads (15-min, hourly); config and logic for these two intervals. Strategies are **ported** from V1 into the V2 pipeline (evaluate_entry / evaluate_exit); V1 code is not changed.
- **Single pipeline implementation**: One code path `run_unified_pipeline(interval)`; data layer, strategy eval, aggregator, executor shared.
- **Unified data fetch per tick per (interval, asset)**: Market, orderbook (Kalshi WS or REST), spot (Kraken + Coinbase, §9), open orders; strike cache per window_id and distance in one place (§9).
- **Central order registry** in **v2_state.db** (§2.6): order_id, strategy_id, interval, market_id, asset, side, status, filled_count; one shared Kalshi client; batch create/cancel where supported.
- **V2 config**: Strict schema **v2_common.yaml**, **v2_fifteen_min.yaml**, **v2_hourly.yaml** with `pipeline` + `strategies.<name>`; no legacy fallback; V2 refuses to boot if config does not match.
- **Strategy interface**: Each strategy exposes evaluate_entry(ctx) and evaluate_exit(ctx, my_orders); no direct place/cancel or own Kalshi client. **V2 strategy reports** table in v2_state.db holds final PnL outcome per trade (§2.6).
- **Risk guards / no_trade_window**: Applied in pipeline or inside strategy; in-memory state keyed by (interval, window_id); caps enforced via v2_state.db; outcome/resolve flow after window close; last_90s dual-strike preserved in aggregator/executor rules.
- **Data Bus / Oracle**: V2 reads spot (and optionally orderbook) from REST or from Data Bus when `run_data_bus.py` is used; Kraken and Coinbase spot fetch unified in data layer (§9).

### Out of scope (explicit)

- **Daily and weekly intervals**: No change to how daily/weekly run (separate code paths, not part of the unified pipeline). They remain as today unless a future project extends the pipeline to them.
- **run_data_bus.py process**: Remains a separate process; this plan does not merge it into the bot process or change its contract. The bot only consumes data (REST or Data Bus); unified spot/distance logic is **inside the bot’s data layer**.
- **Kalshi API contract**: No change to Kalshi REST/WS API usage beyond using one client and optional batch endpoints. No new Kalshi features (e.g. new order types) unless already planned.
- **V1 strategy_report_* tables**: V1 tables (strategy_report_last_90s, ATM reporting, etc.) are unchanged; V2 uses its own **v2 strategy reports** table in v2_state.db (§2.6).
- **New strategies or new assets**: Adding new strategy logic or new assets is out of scope for this spec; V2 ports existing 15-min and hourly strategies only.
- **Changing strategy logic (gates, thresholds)**: Entry/exit rules (e.g. min_distance_at_placement, stop_loss_pct) are ported as configured; they run inside evaluate_entry/evaluate_exit with data from WindowContext.

### Pros of this migration

- **Single source of truth for data**: One fetch per tick per (interval, asset); all strategies see the same market, quote, spot, strike, and distance — no duplicate API calls or inconsistent snapshots.
- **Easier to reason about**: One pipeline, two threads, interval-based; clear ownership (registry) and one order per market per interval; aggregator handles conflict.
- **Fewer rate-limit and connection issues**: One Kalshi client; optional batch create/cancel; fewer Kraken/Coinbase calls when spot and distance are unified and strike is cached (§9).
- **Unified Kraken/Coinbase and distance (§9)**: Spot fetch and distance-from-strike in one place; strike cached per window_id (unchanged for the whole window); fallback for null/stale reduces duplicate logic and inconsistencies.
- **Testability**: Pipeline can be tested with a mock client and mock context; strategy eval is pure (intents/exits) and easy to unit test.
- **Config clarity**: Explicit strategy names and pipeline/strategies layout; per-strategy enabled and priority in one place.

### Cons / risks

Each risk below has a short mitigation so the migration stays controllable and reversible.

- **Refactor surface** — Many files and call paths change; risk of behavior regressions (wrong intent, exit, or priority). **Mitigation:** Phased rollout with feature flag; Phase 2 keeps existing code paths and only adds eval-only extraction; compare intents/exits against current behavior in tests; canary (one asset or one interval) before full cutover.
- **Single point of failure** — One shared executor and one Kalshi client; a bug or blockage there affects both 15-min and hourly. **Mitigation:** Keep executor small and well-tested; single lock or dedicated executor thread to avoid races; config/rollback flag to revert to legacy threads; monitor and alert on executor errors.
- **Strike cache / stale** — If strike is cached per window and market metadata is wrong or updated mid-window, we might use a stale strike. **Mitigation:** Cache keyed by (interval, market_id); invalidate on window end or market_id change; optional sanity check (e.g. |spot − strike| within asset-specific max) and fallback to recompute or skip; do not cache null/absurd strike.
- **Two threads, same process** — 15-min and hourly run concurrently; shared executor and registry must be thread-safe. **Mitigation:** Single lock around execute (or a single executor thread that both pipelines submit to); registry and shared structures accessed under the same lock or documented thread-safety; no shared mutable state without synchronization.
- **Migration effort** — Config loader, data layer, registry, run_unified, and each strategy’s eval extraction take time; YAML migration and testing are non-trivial. **Mitigation:** Break work into phases (Phase 1: strike cache + spot/distance helpers; Phase 2: eval extraction with legacy still running; Phase 3: data layer + run_unified + cutover); backward-compatible config loader first; automate regression tests and document rollout/rollback steps.

- **In-memory state keying** — risk_guards and exit_criteria use global in-memory state keyed by window_id (or market_id|ticker|side). With two threads (15-min, hourly), keys must not collide. **Mitigation:** Key all such state by (interval, window_id) and (ticker, side) where applicable; or document that window_id is globally unique; add regression test for no cross-interval leakage (§10.2).

#### Option: Bot V2 as a separate deployment (run alongside Bot V1)

Building the unified pipeline as **Bot V2** and running it as a **separate process** (or separate entrypoint) while **Bot V1 (current) remains in production** reduces several risks:

| Risk | How Bot V2 separate helps |
|------|---------------------------|
| **Refactor surface** | V1 is unchanged; no in-place refactor of production code. V2 is developed and tested independently. Regressions in V2 do not affect V1. |
| **Single point of failure** | Only one bot is "live" (places orders) at a time. If V2 fails, production can stay on V1; switch to V2 when ready. Rollback = run V1 again. |
| **Migration effort** | No big-bang cutover. V2 can run in **shadow mode** first (evaluate intents, log or compare to V1, do not place orders); then switch to V2 for live trading when confident. |
| **Two threads, same process** | Unchanged inside V2 (V2 still has two threads). But a crash or bug in V2 does not take down V1, since they are separate processes. |
| **Strike cache / stale** | Same mitigation in V2 either way (cache keyed by window_id, invalidate on window end); not specifically improved by separate deployment. |

**Operational model:** Run **one** of {V1, V2} for live order placement (same Kalshi account) to avoid double orders. Use the other for shadow/backtest or keep stopped. Optionally run both on the same host with a single "active bot" config or different entrypoints so only one connects to Kalshi for trading. This option keeps rollback trivial (point production back to Bot V1).

---

## 1. Current Architecture (As-Is)

### 1.1 Threading Model

| Component | Thread | What it does |
|-----------|--------|--------------|
| **Main scheduler** | Main thread | `run_loop(lambda: run_bot(config))` → calls `run_bot()` on schedule. `run_bot()` runs `run_bot_for_asset()` (hourly) and `run_bot_15min_for_asset()` (15min) **sequentially per asset**. Single `KalshiClient()` in `run_bot()`. |
| **last_90s** | Daemon thread | `run_last_90s_loop()` → `while True: run_last_90s_once(); sleep(interval)`. Inside `run_last_90s_once()`, **ThreadPoolExecutor** runs `_run_last_90s_for_asset()` per asset in parallel. **Own KalshiClient()** per loop. |
| **hourly_last_90s** | Daemon thread | Same pattern: own loop, **ThreadPoolExecutor** per asset, own **KalshiClient()**. |
| **market_maker** | Daemon thread | `run_market_maker_loop()` → `while True: run_market_maker_once(); sleep(interval)`. **Sequential** over assets. **Own KalshiClient()**. |
| **atm_breakout** | Daemon thread | `run_atm_breakout_loop()` → `while True: run_atm_breakout_once(); sleep(interval)`. **Sequential** over assets. **Own KalshiClient()**. |
| **Data Bus (optional)** | Separate process | `scripts/run_data_bus.py` runs Oracle WS + Kalshi WS + Director; not in main bot process. |

### 1.2 Kalshi: WS vs REST (current)

| Use | last_90s | ATM, market_maker, main 15min |
|-----|----------|-------------------------------|
| **Orderbook (quote)** | **WebSocket** via `get_safe_orderbook(ticker)` (Kalshi WS), with **REST fallback** `kalshi_client.get_top_of_book(ticker)` if WS has no data | **REST only**: `fetch_15min_quote(market_id, client)` → `client.get_top_of_book(ticker)` |
| **Orders** (place, cancel, get) | **REST** (`KalshiClient`) | **REST** (`KalshiClient`) |

So today: **orderbook** is mixed (last_90s can use Kalshi WS; others use REST). **Order placement and management are REST only.** The choice of orderbook source is **tied to the strategy** (last_90s opts into WS; others don’t).

### 1.3 Data Fetching (Duplicated Today)

- **last_90s**: `get_current_15min_market_id`, `fetch_15min_market`, `get_safe_orderbook(ticker)` (or `kalshi_client.get_top_of_book`), spot via REST or Oracle WS.
- **ATM**: `get_current_15min_market_id`, `fetch_15min_market`, `fetch_15min_quote(market_id, client)`, spot via Kraken/Coinbase REST.
- **market_maker**: `fetch_15min_market`, `fetch_15min_quote`, spot REST.
- **Main 15min**: `run_bot_15min_for_asset` uses `get_market_context`, `fetch_15min_quote`, `enrich_with_orderbook`, spot, etc.

Each strategy (and main 15min) fetches market + quote + spot independently → duplicate API calls and risk of inconsistent snapshot.

### 1.4 Order Placement and Ownership

- Each strategy calls `kalshi_client.place_limit_order` / `place_market_order` directly.
- Ownership is implicit: last_90s uses `client_order_id` prefix `last90s:`, ATM uses `atm:`, MM uses `mm:`.
- No central registry of “order_id → strategy_id”; each strategy tracks its own placements in DB (e.g. `set_last_90s_placed`, ATM `_in_trade`).

### 1.5 Exit / Stop-Loss

- Each strategy implements its own exit loop: last_90s `_maybe_stop_loss` and resolve; ATM `_execute_exit_loop`; MM scratch/zero-fill.
- No shared “position registry” per order; strategies read their own state and DB.

### 1.6 Pain Points

- **Multiple resting orders on same market**: Kalshi effectively one resting order per market → ATM unfilled entry blocked last_90s until we added cancel.
- **Duplicate fetches**: Same market/quote/spot fetched by several threads.
- **Multiple Kalshi clients**: 4+ clients (main + 4 strategy threads) → rate limits and connection churn.
- **Hard to add “one order per market” rule**: Would require cross-strategy coordination.
- **Testing**: Hard to test “all strategies on same context” without running full threads.

---

## 2. Target Architecture (Truly Unified Pipeline)

**One pipeline, two threads.** The same pipeline code runs for both 15-min and hourly; the only parameter is **interval** (window type: `fifteen_min` | `hourly`). At any given time there is at most **one** active 15-min window and **one** active hourly window, so we run two threads that each invoke the unified pipeline with their interval. Strategies are tied to interval via config (`fifteen_min.strategies.*` vs `hourly.strategies.*`).

### 2.1 High-Level Flow (same for 15-min and hourly; parameterized by interval)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Thread: 15-min OR hourly (two threads, same code)                           │
│  run_unified_pipeline(interval="fifteen_min" | "hourly")                     │
│  Tick every run_interval_seconds when in window for that interval            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  1. DATA LAYER (per asset, per tick) — unified, not strategy-tied           │
│     - interval = fifteen_min | hourly (from caller)                          │
│     - market_id = get_current_market_id(asset, interval); market = fetch     │
│     - quote    = single policy: get_safe_orderbook(ticker) or REST           │
│     - spot     = unified Kraken + Coinbase (§9): WS preferred, REST fallback │
│     - strike   = cached per window_id (unchanged for window); fallback if null│
│     - distance = single place: |spot - strike| (Kraken, Coinbase, conservative)│
│     - orders   = client.get_orders(ticker=ticker, status=…)  [optional]      │
│     - Build WindowContext(interval, market_id, ticker, asset, seconds_to_    │
│         close, quote, spot, spot_kraken, spot_coinbase, strike, distance, …)   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  2. STRATEGY LAYER (stateless evaluation)                                    │
│     Strategies for this interval only: config[interval].strategies.*         │
│     For each enabled strategy for this interval:                             │
│       - entry_intent = strategy.evaluate_entry(ctx) → OrderIntent | None     │
│       - exit_actions = strategy.evaluate_exit(ctx, my_orders) → [ExitAction] │
│     OrderIntent = { side, price_cents, count, order_type, client_order_id }  │
│     ExitAction  = { order_id, action: "stop_loss"|"take_profit"|"market_sell" }│
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  3. AGGREGATION / CONFLICT RESOLUTION                                        │
│     - At most one new resting order per (market_id, asset) per interval.    │
│     - Priority from config[interval].pipeline.strategy_priority.             │
│     - Output: list of (OrderIntent, strategy_id), list of ExitAction.        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  4. EXECUTION LAYER (single place that touches Kalshi)                       │
│     - One shared KalshiClient(); both 15-min and hourly use it.              │
│     - For each ExitAction: execute (cancel + market sell or limit sell).     │
│     - For selected OrderIntent: place_order(intent); record(order_id,        │
│       strategy_id, interval, market_id, asset) in central state/DB.         │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Window ID / interval:** The pipeline receives `interval` (e.g. `fifteen_min` or `hourly`). The data layer uses it to resolve the current market (e.g. `get_current_15min_market_id` vs `get_current_hourly_market_id`). The strategy layer loads only strategies enabled for that interval from `config[interval].strategies`. The registry stores `(order_id, strategy_id, interval, ...)` so exits and ownership are scoped per interval. No duplicate pipeline logic — one implementation, parameterized by interval.

**Place / cancel / exit: per interval, after evaluating all strategies for that interval.**  
We do **not** evaluate 15-min and hourly strategies together and then do one combined place/cancel/exit. Each pipeline run is **for one interval only**. So: **(1)** 15-min thread: for each tick (when in 15-min window), evaluate **all 15-min strategies** (last_90s, ATM, MM, signals_15min) → aggregate → **then** executor runs **exits first** (cancel + sell for 15-min orders), **then** places new orders from aggregated intents for 15-min. **(2)** Hourly thread: for each tick (when in hourly window), evaluate **all hourly strategies** (hourly_last_90s_limit_99, signals_hourly) → aggregate → **then** executor runs exits first for hourly orders, then places new orders for hourly. So **place, cancel, and exit all happen after evaluating all strategies for that interval**; 15-min and hourly each have their own evaluate → aggregate → execute sequence. The **executor** (one shared Kalshi client) is used by both threads but each invocation is scoped to one interval’s intents and exits.

**Run: defined at global level.**  
Whether the 15-min pipeline runs and whether the hourly pipeline runs is **global** (top-level) config: **run_mode** (e.g. `both` | `hourly` | `fifteen_min`) and **intervals.fifteen_min.enabled**, **intervals.hourly.enabled** in common.yaml. So “run” is at top level; per-interval enabled controls whether that interval’s thread is started (or whether it ticks when in window).

### 2.2 Unified Kalshi data source (not strategy-tied)

After migration, **how** we get the orderbook (and spot) is decided in **one place** — the **data layer** — and is **not tied to any strategy**:

- **Orderbook (quote)**: The data layer uses a single, configurable policy (e.g. “prefer Kalshi WS via `get_safe_orderbook(ticker)` when available, else REST `fetch_15min_quote(market_id, client)`”). All strategies receive the same `WindowContext` with that quote; none choose WS vs REST themselves.
- **Spot**: Same idea — one policy (e.g. Data Bus / Oracle WS vs REST) in the data layer; strategies only see `ctx.spot`.
- **Orders** (place, cancel, get): Remain **REST** via the single `KalshiClient` in the executor. No strategy touches the client.

So: **one** orderbook source policy, **one** spot source policy, **one** client for orders — all defined at pipeline/data-layer level, not per strategy.

### 2.3 Central State: Order Ownership

- **New (or extended) state**:
  - Table or in-memory map: `order_id → (strategy_id, interval, market_id, asset, ticker, side, placed_at)`.
  - **interval** identifies which window type (15-min vs hourly) the order belongs to; used to scope “my_orders” when evaluating exits (only orders for this interval and strategy).
  - On placement: executor writes this; strategies no longer write their own “placed” state for execution purposes (they can still write reporting rows).
- **Strategy_id**: e.g. `"last_90s"`, `"atm_breakout"`, `"market_maker"`, `"signals_15min"` (15-min); `"hourly_last_90s"`, `"signals_hourly"` (hourly). Strategies are tied to interval via config; registry lookup is always by (strategy_id, interval, market_id, asset).

### 2.4 Strategy Interface (Contract)

Each strategy exposes:

- **evaluate_entry(ctx: WindowContext) → Optional[OrderIntent]**  
  Returns one order intent if the strategy wants to place in this tick, else None. No side effects (no place, no DB write for placement).

- **evaluate_exit(ctx: WindowContext, my_orders: List[OrderRecord]) → List[ExitAction]**  
  For each of its orders that should exit (stop-loss, take-profit, window-end), returns an ExitAction. Executor will perform the exit.

- **Optional: post_placement_hook(order_id, ctx)** for reporting/telemetry only.

Strategies remain free to keep their own **reporting** tables (e.g. `strategy_report_last_90s`); only **placement and execution** go through the pipeline.

### 2.5 Threading Model (Target): Two threads, one pipeline

- **Two threads**: (1) **15-min thread** — runs when there is an active 15-min window; (2) **hourly thread** — runs when there is an active hourly window. At any time there is at most one 15-min and one hourly window, so each thread runs the **same** `run_unified_pipeline(interval=...)` with its interval.
- **Inside each thread** we can use **Option A** (single-threaded: for each asset → build context → evaluate strategies for this interval → aggregate → execute) or **Option B** (parallel eval per asset, serial executor). Recommendation: **Option B** so one slow strategy doesn’t block others; execution (place/cancel) remains serial with one shared Kalshi client.
- **Single pipeline implementation**: One code path for data layer, strategy layer, aggregator, executor. No separate “run_15min_pipeline” vs “run_hourly_pipeline” — only `run_unified_pipeline(interval)`; the executor and Kalshi client are shared across both threads (thread-safe: e.g. one lock around execute, or executor in main thread and both pipeline threads submit to it).

### 2.6 V2 database (v2_state.db)

V2 uses a **purpose-built database**, **v2_state.db**, separate from any V1 state or reporting DBs. We do not migrate or reuse V1 tables (e.g. `last_90s_placed` or V1 strategy_report_*). The following tables are defined natively for the event-driven pipeline.

**Central Order Registry table** — Tracks every order placed by V2 for ownership, exits, and caps. Columns: **order_id** (PK), **strategy_id**, **interval**, **market_id**, **asset**, **ticker**, **side**, **status** (resting | filled | canceled), **filled_count**, **count** (contracts), **limit_price_cents** (nullable), **placed_at**, **client_order_id**. The executor registers a row on place; updates status and filled_count on fill/cancel. Strategies receive "my_orders" by querying for (strategy_id, interval, market_id, asset). **Caps** (max_orders_per_ticker, max_total_orders_per_interval, cap_scope from v2_common) are enforced by the aggregator using v2_order_registry counts only (no V1 bot_state).

**V2 Strategy Reports table** — A **unified** reporting table where the **pipeline** writes the **final PnL outcome** of a trade, rather than each strategy maintaining its own bespoke table. One row per closed trade. Columns: **order_id**, **strategy_id**, **interval**, **window_id**, **asset**, **side**, **entry_price_cents**, **exit_price_cents**, **outcome** (win | loss | resolved_yes | resolved_no), **is_stop_loss**, **pnl_cents**, **resolved_at**. The pipeline (or executor) writes a row when a position is closed (exit or market resolution).

### 2.7 Async / Data Bus

- **No async in the main bot loop.** Keep the pipeline synchronous.  
- **Data Bus**: If `run_data_bus.py` is used, the bot reads spot/orderbook from Data Bus (or REST fallback) when building `WindowContext`; no need for the main process to run WebSockets.  
- If we later want WS in-process, we can read from `kalshi_ws_manager.get_safe_orderbook` and `oracle_ws_manager.get_safe_spot_prices_sync` in the data layer; still sync from the loop’s perspective.

---

## 3. What Gets Refactored

### 3.1 New Modules / Files

| Item | Purpose |
|------|--------|
| `bot/pipeline/context.py` | `WindowContext` dataclass: interval, market_id, ticker, asset, seconds_to_close, **quote** (yes_bid, yes_ask, no_bid, no_ask), spot, **spot_kraken**, **spot_coinbase**, **strike** (cached per window_id), **distance** / distance_kraken / distance_coinbase (§9), **positions** (list of {ticker, side, count, entry_price_cents} for exit evaluation), open_orders, config, etc. |
| `bot/pipeline/data_layer.py` | Build `WindowContext` per (asset, interval): resolve market_id, fetch market, quote; **unified spot** (Kraken + Coinbase, §9); **strike cache** per window_id; **distance** computed in single place; optional orders. One implementation for both intervals. |
| `bot/pipeline/intents.py` | `OrderIntent`, `ExitAction` types; optional `OrderRecord` for “my orders” passed to strategy. |
| `bot/pipeline/aggregator.py` | Given list of (OrderIntent, strategy_id) per asset (for this interval), pick at most one resting order per market; priority from config[interval].pipeline.strategy_priority. |
| `bot/pipeline/executor.py` | Place orders (from OrderIntent), record order_id → (strategy_id, interval, …); execute ExitActions (cancel, sell). **Single shared** Kalshi client (thread-safe for both 15-min and hourly threads). |
| `bot/pipeline/registry.py` | Central order ownership: register(order_id, strategy_id, interval, …), get_orders_by_strategy(strategy_id, interval, market_id, asset), resolve on fill/expiry. |
| `bot/pipeline/run_unified.py` | **Single** entry point: `run_unified_pipeline(interval: Literal["fifteen_min", "hourly"])`. For each tick (when in window for that interval), for each asset: data_layer(interval) → strategies for that interval → aggregator → executor. Option B: thread pool for eval, serial executor. Two threads in main.py call this with interval="fifteen_min" and interval="hourly" respectively. |

### 3.2 Strategy Refactors (Existing Files)

| Strategy | Current | Refactor |
|----------|---------|----------|
| **last_90s** | `run_last_90s_loop` + `run_last_90s_once` + ThreadPoolExecutor + `_run_last_90s_for_asset` (big function: gates, place, stop-loss, resolve). | Extract: (1) `evaluate_entry(ctx) -> OrderIntent | None` (gates + intent; no place). (2) `evaluate_exit(ctx, my_orders) -> List[ExitAction]` (stop-loss only). (3) Resolve/outcome writing: can stay in a “resolve” step called from pipeline after window close, or stay in strategy as a hook. (4) Remove `run_last_90s_loop`, ThreadPoolExecutor, and per-strategy Kalshi client from last_90s. |
| **ATM breakout** | `run_atm_breakout_loop` + `run_atm_breakout_once` + `_run_atm_for_asset` (HUNTING vs IN_TRADE, place entry, exit loop). | Extract: (1) `evaluate_entry(ctx) -> OrderIntent | None` when in HUNTING and gates pass; (2) `evaluate_exit(ctx, my_orders)` for IN_TRADE (take-profit, stop-loss, window-end). State: `_in_trade` becomes “my_orders” from registry for this strategy. (3) Remove loop and own client; keep reporting DB write. |
| **market_maker** | `run_market_maker_loop` + per-asset logic, two legs, scratch, cancel. | Extract: (1) `evaluate_entry(ctx) -> OrderIntent | None` (or two intents for YES/NO legs if executor supports multi); (2) `evaluate_exit(ctx, my_orders)` for scratch, zero-fill, window-end. (3) Remove loop and own client. |
| **signals_15min** (regular 15 min) | `run_bot_15min_for_asset`: generate_signals_15min, guards, execute_signals. Config: today `intervals.fifteen_min.enabled` + `entry_window`/`exit_criteria`/`risk_guards`. | Treat as another strategy: config name **signals_15min** (§8); returns OrderIntent(s) from signals; guards in pipeline or inside strategy. Execute via central executor. |
| **hourly_last_90s_limit_99** | Own loop + ThreadPoolExecutor + own client. | Same pattern as 15min last_90s: evaluate_entry / evaluate_exit; pipeline runs in “hourly” mode Config: `hourly.strategies.hourly_last_90s_limit_99`. |
| **signals_hourly** (regular hourly) | `run_bot_for_asset`: generate_signals, execute_signals, schedule (entry_window, exit_criteria, hourly_risk_guards), thresholds, spot_window. | Regular hourly strategy (§8.4): config **signals_hourly**; OrderIntent(s) from signals; entry/exit/risk guards from `hourly.strategies.signals_hourly`. Central executor. |

### 3.3 What Stays the Same

- **KalshiClient** API (place_limit_order, place_market_order, cancel_order, get_order, get_orders, get_top_of_book).
- **Config** is refactored per **§8**: target layout is `fifteen_min.pipeline` + `fifteen_min.strategies.<name>` (including **signals_15min**). Config_loader supports the new layout and, during migration, legacy keys. Strategies receive their slice in `WindowContext`.
- **DB**: state DB for placement tracking can be extended with a central “order_registry” table; strategy_report_* tables stay for reporting.
- **Risk guards / no_trade_window**: Applied in pipeline before calling strategies or inside strategy eval (same as today).
- **run_data_bus.py**: Unchanged; bot still reads from REST or Data Bus when building context.

### 3.4 What Is Removed or Deprecated

- **Per-strategy threads**: No more `run_last_90s_loop`, `run_atm_breakout_loop`, `run_market_maker_loop`, `hourly_last_90s` loop, or legacy `run_bot_15min_for_asset` / `run_bot_for_asset` as separate code paths. Replaced by **two threads** that both call `run_unified_pipeline(interval)`.
- **Per-strategy KalshiClient**: One **shared** client in executor (thread-safe); both 15-min and hourly use it.
- **Duplicate pipeline logic** for 15-min vs hourly: One pipeline implementation; interval selects market resolution and strategy set.
- **Duplicate fetch logic** inside each strategy: Replaced by data_layer(interval) building WindowContext once per tick per asset.
- **Strategy-internal “cancel my unfilled so others can place”**: Replaced by aggregator + “one order per market” rule and executor.

---

## 3.5 Coverage checklist: entry, exit, risk guards, stop loss, config (all code and config covered)

This checklist ensures **every** current code path and config key for 15-min and hourly is accounted for.

- **Entry:** 15-min: `fifteen_min.entry_window` (late_window_seconds, late_window_interval_seconds) and per-strategy windows (last_90s window_seconds, ATM min/max_seconds_to_close). Hourly: `schedule.entry_window` (late_window_minutes, late_interval_seconds). **Plan:** Pipeline runs when in window; each strategy's `evaluate_entry(ctx)` uses `ctx.seconds_to_close` and its config slice; entry_window under `fifteen_min.strategies.signals_15min` and `hourly.strategies.signals_hourly`.
- **Exit:** 15-min: `fifteen_min.exit_criteria`, `run_exit_criteria`, last_90s `_maybe_stop_loss`, ATM `_execute_exit_loop`, MM scratch/zero-fill. Hourly: schedule.exit_criteria, `_maybe_stop_loss_hourly`. **Plan:** `evaluate_exit(ctx, my_orders)` returns `List[ExitAction]`; executor runs them; DB (stop_loss_triggered_at, is_stop_loss, stop_loss_sell_price) preserved.
- **Risk guards:** 15-min: `fifteen_min.risk_guards` (all keys). Hourly: `schedule.hourly_risk_guards` (all keys including anchor_one_per_side, stoploss_roll_reentry). **Plan:** Applied in pipeline or inside signals_15min/signals_hourly; config under `strategies.signals_15min.risk_guards` and `strategies.signals_hourly.hourly_risk_guards`. risk_guards.py / exit_criteria.py unchanged.
- **Stop loss:** last_90s, ATM, hourly_last_90s — all via evaluate_exit → ExitAction → executor; DB/strategy_report_db columns unchanged.
- **Config refactor:** fifteen_min: four strategies (last_90s_limit_99, market_maker_strategy, atm_breakout_strategy, signals_15min). **Hourly: two strategies** — **hourly_last_90s_limit_99** (last-90s style) and **signals_hourly** (regular hourly, i.e. run_bot_for_asset path: schedule + thresholds + spot_window). hourly.yaml → hourly.pipeline + hourly.strategies.hourly_last_90s_limit_99 + hourly.strategies.signals_hourly. common.yaml unchanged.
- **Code files:** main.py (two pipeline threads; no_trade_window; remove/redirect run_bot_15min/run_bot_for_asset); config_loader (new layout + legacy); last_90s, hourly_last_90s, atm, market_maker (evaluate_entry/exit; remove loop/client); strategy_15min; exit_criteria; risk_guards; state; strategy_report_db; no_trade_window.

---

## 4. Implementation Phases (side-by-side deployment)

Rollout is **not** a phased code replacement in V1. It is a **side-by-side deployment**: V2 is built and run alongside V1. **When V2 goes live for trading, V1 must be paused** to prevent both bots from competing on the same Kalshi account (limits and capital).

### Phase 1 – Build V2 skeleton

1. Create **v2/** directories or **v2_** prefixed files (e.g. `bot/v2_main.py` as entry point). V1 code and config are **not** modified.
2. Add **data layer**: given (asset, interval), resolve market_id, fetch market, quote; unified spot (Kraken + Coinbase, §9); strike cache per window_id; distance computed once per tick. Return `WindowContext`. One implementation for both intervals.
3. Implement **v2_state.db** (§2.6): Central Order Registry table and V2 Strategy Reports table. Registry: register on place; lookup by (strategy_id, interval, market_id, asset) for "my_orders."
4. Implement **aggregator**: input list of (OrderIntent, strategy_id) per asset; output at most one OrderIntent per (market_id, asset) using priority from config[interval].pipeline.strategy_priority.
5. Implement **executor**: place orders from intents; register in Central Order Registry; execute ExitActions (cancel + sell). Support **DRY_RUN** mode (Phase 3). Optional: batch create/cancel (§7).
6. **Config**: V2 loads **v2_common.yaml**, **v2_fifteen_min.yaml**, **v2_hourly.yaml** only; strict schema (§8).

### Phase 2 – Port strategies

1. **Port** the logic from V1 into the new V2 interfaces: **evaluate_entry(ctx) -> Optional[OrderIntent]** and **evaluate_exit(ctx, my_orders) -> List[ExitAction]**.
2. Implement or adapt: last_90s_limit_99, atm_breakout_strategy, market_maker_strategy, signals_15min (15-min); hourly_last_90s_limit_99, signals_hourly (hourly). No changes to V1; ported code lives in V2 (e.g. under bot/v2/ or v2_ modules).
3. Add unit tests: given a WindowContext, strategy returns expected intent or exit actions.

### Phase 3 – Shadow mode

1. Run **V2 alongside V1**. V2 receives market data, evaluates strategies, and produces OrderIntents.
2. Set the V2 executor to **DRY_RUN**: it **does not** place or cancel Kalshi orders; it only **logs** OrderIntents and ExitActions to the logs.
3. Compare V2 intents/exits with V1 behavior (same config shape in v2_*.yaml) to validate parity. When confident, proceed to live.
4. **Go live**: Point production to V2 (e.g. run `bot/v2_main.py` with DRY_RUN=false). **Pause V1** so only V2 places orders on the Kalshi account. Rollback = stop V2 and resume V1.

---

## 5. Risk and Rollback

- **Risk**: Bugs in ported logic (wrong intent, wrong exit, wrong priority). Mitigation: Shadow mode (Phase 3) with DRY_RUN; compare V2 logs to V1 behavior before going live.
- **Rollback**: V2 and V1 are separate deployments. To roll back, **stop V2** and **resume V1**. No code revert in V1; no shared DB. When V2 goes live, V1 must already be paused, so rollback is simply "stop V2, start V1 again" (and ensure only one is live at a time).

---


## 6. Summary Table

| Aspect | Current | Target |
|--------|---------|--------|
| **Threads** | Main (scheduler) + 4 strategy daemon threads | Main (scheduler) + **2 pipeline threads** (15-min, hourly); each runs same `run_unified_pipeline(interval)` |
| **Kalshi clients** | 1 in run_bot + 1 per strategy thread (4+) | **1 shared** in executor (both 15-min and hourly use it; thread-safe) |
| **Data fetch** | Per strategy, duplicated | Once per tick per asset in data_layer |
| **Order placement** | Each strategy calls client.place_* | Executor only; strategies return intents |
| **Order ownership** | Implicit (client_order_id prefix) | Explicit registry order_id → strategy_id |
| **Exit/stop-loss** | Inside each strategy loop | Strategy returns ExitAction; executor runs it |
| **Parallelism** | ThreadPoolExecutor inside last_90s/hourly_last_90s | Optional: parallel eval per asset, serial execution |
| **Order API** | One place_limit_order / cancel_order per call | Batch create + batch cancel where supported (see §7) |
| **Config** | Flat keys per strategy; “regular 15 min” implied by interval enabled | `pipeline` + `strategies.<name>`; four strategies named (incl. **signals_15min**); same for hourly (§8) |
| **Spot / distance** | Per-strategy Kraken/Coinbase fetch; strike and distance recomputed in several places | **Unified** Kraken+Coinbase fetch (§9); **strike cached per window_id**; **distance in single place**; fallback for null/stale |

V2 is a **greenfield** pipeline: one implementation for both 15-min and hourly, two threads that differ only by interval, single data fetch per tick per interval, multi-strategy evaluation tied to window/interval, and central execution with one resting order per market per interval. V1 remains unchanged and can be paused when V2 goes live.

---

## 7. Batch order processing and rate limits

The original plan did **not** include batch order processing. This section adds it so the executor can reduce API call count and ease rate limits.

### 7.1 What Kalshi provides

Kalshi’s API supports **batch** operations:

| API | Endpoint | Limit | Purpose |
|-----|----------|--------|---------|
| **Batch create orders** | `POST /trade-api/v2/portfolio/orders` (batch body) or batch-specific path per [Kalshi docs](https://docs.kalshi.com/api-reference/portfolio/batch-create-orders) | Multiple orders in one request | Place several intents (e.g. one per asset) in a single HTTP call. |
| **Batch cancel orders** | `DELETE /trade-api/v2/portfolio/orders/batched` | **Up to 20 orders** per request | Cancel many orders (e.g. all exits for this tick) in one call. |

- **Batch create**: Request body is a list of order specs (ticker, side, action, count, type, yes_price/no_price, client_order_id, time_in_force, etc.). Response returns per-order result (order_id or error).
- **Batch cancel**: Request body: `{ "orders": [ { "order_id": "..." }, ... ] }`. Response: per-order `reduced_by`, `error` if any.

Using these reduces the number of HTTP requests per tick when the pipeline has multiple placements or multiple exits.

### 7.2 How the pipeline uses batch

- **Executor – placements**: After aggregation we have at most one new order per (market_id, asset). Across 4 assets that’s up to 4 orders per tick. Instead of 4× `place_limit_order`, the executor calls **one batch create** with a list of 1–4 order specs. If Kalshi returns per-order success/failure, register only successful order_ids in the central registry.
- **Executor – exits**: Each strategy can return multiple `ExitAction`s (e.g. stop-loss on 2 positions). Collect all order_ids to cancel; send them in chunks of 20 via **batch cancel**. Then place any market/limit sells (or batch if Kalshi supports batch for sells; otherwise keep one sell per order to avoid ambiguity).
- **Fallback**: If batch create/cancel is not implemented yet or fails, executor falls back to current behavior: one `place_limit_order` / `cancel_order` per intent/exit. Same logic, more requests.

### 7.3 Implementation in the plan

- **Phase 1 (optional)**: Add `KalshiClient.batch_create_orders(orders: List[OrderSpec])` and `KalshiClient.batch_cancel_orders(order_ids: List[str])` that call the Kalshi batch endpoints. Use official docs for exact path and request/response shape. Handle per-order errors (e.g. log and skip failed items; register only successes).
- **Phase 3**: Executor uses batch where applicable:
  - **Place**: Build list of OrderIntent from aggregator output; call `batch_create_orders` (or single-place in a loop if batch not available).
  - **Cancel**: Collect order_ids from ExitActions; call `batch_cancel_orders` in chunks of 20; then place sell orders as today (one per position unless batch sell exists).
- **Rate limits**: Fewer requests per tick (one or two batch calls instead of N single-order calls) reduces the chance of hitting rate limits when multiple strategies or assets are active. If Kalshi documents a rate limit (e.g. X requests/minute), the executor can optionally throttle or queue batch calls; the plan does not assume a specific limit.

### 7.4 References

- Batch Create: [Kalshi – Batch Create Orders](https://docs.kalshi.com/api-reference/portfolio/batch-create-orders), `BatchCreateOrdersRequest` / `CreateOrderRequest`.
- Batch Cancel: [Kalshi – Batch Cancel Orders](https://docs.kalshi.com/api-reference/orders/batch-cancel-orders) — `DELETE /portfolio/orders/batched`, up to 20 order_ids per request.

### 7.5 Worst-case Kalshi API calls per run (per pipeline tick, one interval)

“Per run” = one pipeline tick for **one interval** (either 15-min or hourly). Let **A** = number of assets for that interval (e.g. 4 for 15-min, 5 for hourly with doge). Assume one market / one ticker per asset per tick.

**Data-layer optimization:** Kalshi supports **one call for multiple markets**: `GET /markets?tickers=t1,t2,t3,...`. The data layer uses **1** request for all A markets (instead of A separate calls), saving **A − 1** calls per run. Orderbook has no REST batch (one call per ticker); orders use one `get_orders(status=resting)` and filter by ticker client-side.

| Phase | Call type | Worst case (no batch exec) | Worst case (with batch §7) |
|-------|-----------|------------------------|----------------------------|
| **Data layer** | fetch_markets (all tickers) | **1** (`GET /markets?tickers=t1,t2,...`) | 1 |
| | get orderbook / quote (REST) or WS | **A** (1 per ticker; no REST batch; if WS, 0 REST) | A or 0 |
| | get_orders (open orders) | **1** (one call, filter by tickers client-side) | 1 |
| **Execution – exits** | cancel_order | **A** (at most one resting order per market; A markets) | **ceil(A/20)** (batch cancel, max 20 per request) |
| | place (market/limit sell) | **A** (1 sell per exit) | A (no batch sell assumed) |
| **Execution – new orders** | place_order | **A** (at most 1 new order per asset after aggregate) | **1** (batch create) |
| **Total** | | **1 + A + 1 + 2A + A = 4A + 2** | **1 + A + 1 + ceil(A/20) + A + 1 = 2A + ceil(A/20) + 3** |

**Example (A = 4, 15-min):**

- **Without batch exec:** 4×4 + 2 = **18** calls (1 market + 4 quote + 1 orders + 4 cancel + 4 sell + 4 place). *Save 3 vs one-call-per-market.*
- **With batch:** 2×4 + 1 + 3 = **12** calls (1 market + 4 quote + 1 orders + 1 batch_cancel + 4 sells + 1 batch_create). *Save 3.*  
  (If orderbook from WS: 1 + 0 + 1 + 1 + 4 + 1 = **8**.)

**Example (A = 5, hourly):**

- **Without batch:** 4×5 + 2 = **22** calls. *Save 4.*
- **With batch:** 2×5 + 1 + 3 = **14** calls. *Save 4.*

**If both intervals tick in the same second:** worst case is the sum of the two (15-min tick + hourly tick), e.g. 18 + 22 = **40** without batch, or 12 + 14 = **26** with batch, per “logical” second (two pipeline runs). **Per run** worst case: **4A + 2** without batch exec, **2A + ceil(A/20) + 3** with batch (orderbook REST); lower if orderbook from WS. **Implementation:** Data layer calls **one** `GET /markets?tickers=ticker1,ticker2,...` and parses into one market per asset.

---

## 8. YAML config (V2 strict schema)

V2 uses a **strict, new config schema**. There is **no legacy fallback** and **no backward compatibility** with V1 flat-key structures. Config files are **v2_common.yaml**, **v2_fifteen_min.yaml**, and **v2_hourly.yaml**, which enforce the `pipeline` + `strategies.<name>` hierarchy natively. **If the config does not match this layout, V2 refuses to boot.**

### Config hierarchy: top level vs per-interval vs strategy level

| Level | Where it lives | What belongs there |
|-------|----------------|--------------------|
| **Top level (global)** | **v2_common.yaml** | mode, run_mode. **oracle_ws** (start_with_bot). **kalshi** (orderbook_source: ws_preferred \| rest_only). strategy, assets. **intervals** (enabled, assets, mode per interval). **caps**, **order** (global + per-interval). no_trade_windows. exit_criteria (global defaults). state (db_path: v2_state.db), reports, logging. |
| **Per fifteen_min** | **intervals.fifteen_min** (v2_common) + **fifteen_min.pipeline** (v2_fifteen_min.yaml) | Interval: enabled, assets, mode. Pipeline: run_interval_seconds, strategy_priority. |
| **Per hourly** | **intervals.hourly** (v2_common) + **hourly.pipeline** (v2_hourly.yaml) | Interval: enabled, assets, mode. Pipeline: run_interval_seconds, strategy_priority. |
| **Strategy level (15-min)** | **fifteen_min.strategies.&lt;name&gt;** | last_90s_limit_99, market_maker_strategy, atm_breakout_strategy, signals_15min. Each: enabled + all strategy params (window_seconds, limit_price_cents, stop_loss_pct, entry_window, exit_criteria, risk_guards, yes_min/yes_max, etc.). |
| **Strategy level (hourly)** | **hourly.strategies.&lt;name&gt;** | hourly_last_90s_limit_99, signals_hourly. Each: enabled + all strategy params (window_seconds, limit_price_cents, stop_loss_pct, entry_window, exit_criteria, hourly_risk_guards, thresholds, spot_window, etc.). |

**Summary:** Top = process-wide and interval toggles (intervals.*, **caps**, **order**, **no_trade_windows**, state, logging). **v2_common.yaml must include no_trade_windows** (enabled, timezone, windows): when current time falls inside a no_trade_window, the pipeline must skip new entry (exits still run). **v2_common must include caps** (cap_scope, per-interval max_orders_per_ticker, max_total_orders_per_15min, max_total_orders_per_hour); the aggregator enforces these using v2_order_registry counts only. Per interval = that interval's pipeline only (run_interval_seconds, strategy_priority). Strategy = one named strategy's enabled flag and all its parameters.

**Kalshi client: orderbook WS vs REST — global (top level), not implicit.**  
The choice of how to fetch the **orderbook** (quote) — Kalshi WebSocket vs REST — is **not** per strategy after migration; the data layer has a single policy. That policy should be **explicit at top level** so operators can turn WS on/off without touching strategy config. Add to **common.yaml** (top level), for example:

- **kalshi.orderbook_source**: `ws_preferred` (try `get_safe_orderbook` / Kalshi WS first, fall back to REST) or `rest_only` (always use `client.get_top_of_book`). Default can be `ws_preferred` when Kalshi WS is available, else REST.

**Orders** (place, cancel, get order) remain **REST only** via the single KalshiClient; no config needed for that. So: **Kalshi orderbook source = global config**; **Kalshi order API = always REST** (implicit). Optionally, **kalshi.ws_url** or connection params can also live at top level if we want them configurable.  
**Spot (Kraken/Coinbase)** is already covered: unified in data layer (§9) with policy (WS preferred, REST fallback); today `oracle_ws.start_with_bot` is global. Per-strategy `use_ws_oracles` goes away for **orderbook** (orderbook follows the single global/pipeline policy); for **spot**, the unified data layer can still respect a single global policy (e.g. use Oracle WS when `oracle_ws.start_with_bot` and Data Bus is running, else REST).

### 8.1 The four 15-min strategies (named)

| # | Config key (proposed) | Current location | Description |
|---|------------------------|------------------|-------------|
| 1 | **last_90s_limit_99** | `fifteen_min.last_90s_limit_99` | Last-90s limit-99c sweep; places in final ~85s. |
| 2 | **market_maker_strategy** | `fifteen_min.market_maker_strategy` | Coin-toss MM; two legs, scratch/zero-fill. |
| 3 | **atm_breakout_strategy** | `fifteen_min.atm_breakout_strategy` | ATM breakout sniper; 120–840s window. |
| 4 | **signals_15min** | `intervals.fifteen_min.enabled` + `fifteen_min.entry_window` / `exit_criteria` / `risk_guards` / `yes_min`–`yes_max` | Regular 15-min: `generate_signals_15min`, risk guards, execute_signals. Today it has no strategy name; it’s just “fifteen_min interval enabled.” |

The 4th strategy is named **`signals_15min`** in config so it’s a first-class strategy like the others (enabled/disabled per strategy, included in priority order, receives its slice in `WindowContext`).

### 8.2 Target layout (15-min)

We adopt the following layout. Pipeline-level settings live under `fifteen_min.pipeline`; per-strategy settings under `fifteen_min.strategies.<name>` with an `enabled` flag each.

```yaml
# common.yaml
intervals:
  fifteen_min:
    enabled: false   # master: run 15-min pipeline? (if true, pipeline runs; which strategies run is below)
    assets: [btc, eth]
    mode: TRADE
```

```yaml
# fifteen_min.yaml
fifteen_min:
  pipeline:
    run_interval_seconds: 1
    strategy_priority: [last_90s_limit_99, atm_breakout_strategy, market_maker_strategy, signals_15min]

  strategies:
    last_90s_limit_99:
      enabled: true
      use_ws_oracles: false
      monitor_window_seconds: 120   # pre-placement telemetry
      window_seconds: { btc: 85, eth: 85, sol: 85, xrp: 85 }
      limit_price_cents: 99
      min_bid_cents: 96
      panic_seconds: 30
      # Time-weighted bid floor (V1): when seconds_to_close > 65 require bid >= 98; else >= min_bid_cents
      side: auto
      stop_loss_pct: { btc: 30, eth: 30, sol: 40, xrp: 30 }
      stop_loss_distance_factor: { btc: 0.6, eth: 0.6, sol: 0.5, xrp: 0.5 }
      max_cost_cents: 10000
      max_cost_cents_by_asset: { btc: 10000, eth: 10000, sol: 10000, xrp: 10000 }
      min_distance_at_placement: { btc: 40, eth: 2.5, sol: 0.15, xrp: 0.003 }
      # ... rest unchanged

    market_maker_strategy:
      enabled: false
      # ...

    atm_breakout_strategy:
      enabled: false
      # ...

    signals_15min:
      enabled: false   # when true, runs generate_signals_15min + risk_guards + execute via executor
      entry_window:
        late_window_seconds: 10
        late_window_interval_seconds: 1
      exit_criteria:
        stop_loss_pct: 0.30
        panic_stop_loss_pct: 0.40
        stop_loss_persistence_polls: 1
        evaluation_interval_seconds: 1
      yes_min: 90
      yes_max: 99
      no_min: 90
      no_max: 99
      risk_guards:
        enabled: true
        # ...
```

- **Pipeline-level** settings (e.g. `run_interval_seconds`, `strategy_priority`) live under `fifteen_min.pipeline`.
- **Per-strategy** settings live under `fifteen_min.strategies.<name>`; each has `enabled`.
- **`signals_15min`** holds the current `entry_window`, `exit_criteria`, `risk_guards`, `yes_min`/`yes_max`, etc. so “regular 15 min” is just another strategy.

**V1 config keys that must have a home in V2 target layout (checklist):**  
- **market_maker_strategy:** mode, max_cost_cents, min/max_seconds_to_close, max_mm_distance, min/max_price_parity, min_spread_cents, safe_velocity_threshold_5s, scratch_timeout_seconds, max_acceptable_loss_cents, scratch_spot_movement_threshold, zero_fill_timeout_seconds.  
- **atm_breakout_strategy:** mode, min/max_seconds_to_close, window_seconds, contracts, max_cost_cents, min/max_price_parity, momentum_trigger_3s, take_profit_cents, stop_loss_cents, trailing_stop_cents, safe_distance_threshold, safe_zone_window_seconds.  
- **risk_guards** (15-min and hourly): enabled, no_new_entry_cutoff_seconds, persistence_polls, late_persistence_override, hard_flip_exit, stop_once_disable, recent_cross, distance_buffer, reversal_after_stop; hourly adds anchor_one_per_side, stoploss_roll_reentry, hard_flip_exit.

**V2 strict schema:** V2 config loader **only** accepts the layout above. Files must be named and structured as **v2_common.yaml**, **v2_fifteen_min.yaml**, **v2_hourly.yaml** with the `pipeline` and `strategies.<name>` hierarchy. If any required key or structure is missing, **V2 refuses to boot** (no fallback to V1 flat keys).
### 8.3 V2 config loading
- V2 loads **v2_common.yaml** plus **v2_fifteen_min.yaml** and/or **v2_hourly.yaml** according to enabled intervals.
- Strategy list and priority come from `fifteen_min.pipeline.strategy_priority` and `hourly.pipeline.strategy_priority`; per-strategy config from `fifteen_min.strategies.<name>` and `hourly.strategies.<name>`.
- No legacy key handling; no backward compatibility with V1 config.

### 8.4 Hourly: two strategies (both covered)

Hourly run has **two** strategies, both in scope and both use the same unified pipeline with `interval="hourly"`:

| # | Config key (proposed) | Current location | Description |
|---|------------------------|------------------|-------------|
| 1 | **hourly_last_90s_limit_99** | `hourly_last_90s_limit_99` (top-level in hourly.yaml) | Last-90s style limit-99 for hourly: places in last `window_seconds` (e.g. 75s), 2 YES below spot + 2 NO above (or per max_yes/max_no), stop_loss_pct, min_distance_at_placement. Own thread today; becomes one strategy in `hourly.strategies.*`. |
| 2 | **signals_hourly** | `schedule` (entry_window, exit_criteria, hourly_risk_guards, …) + `thresholds` + `spot_window` / `spot_window_by_asset` | **Regular hourly strategy**: same role as signals_15min for 15-min. Uses `run_bot_for_asset` today: generate_signals, entry_window (late_window_minutes), exit_criteria, hourly_risk_guards, anchor_one_per_side, stoploss_roll_reentry, hard_flip_exit, thresholds, spot_window. Has no strategy name today; it is just “hourly interval enabled” + schedule. Named **signals_hourly** so it is a first-class strategy with its own `enabled` and config under `hourly.strategies.signals_hourly`. |

So: **hourly_last_90s_limit_99** (last-90s style) and **signals_hourly** (regular hourly) are both covered. The same layout is used for hourly: **`hourly.pipeline`** (run_interval_seconds, strategy_priority) and **`hourly.strategies.<name>`** with these two names. Config refactor applies to both; strategy_priority e.g. `[hourly_last_90s_limit_99, signals_hourly]`.

### 8.5 Hourly strategies and config keys (full list)

Ensure no hourly config or code is missed:

| Strategy | Current config key / block | Target (new layout) | Notes |
|----------|----------------------------|---------------------|--------|
| **hourly_last_90s_limit_99** | `hourly_last_90s_limit_99` (top-level in hourly.yaml) | `hourly.strategies.hourly_last_90s_limit_99` | All keys: enabled, window_seconds, limit_price_cents, min_bid_cents, side, stop_loss_pct, stop_loss_distance_factor, run_interval_seconds, assets, max_yes_per_market, max_no_per_market, max_cost_cents, max_cost_cents_by_asset, min_distance_at_placement. |
| **signals_hourly** | `schedule` (entry_window, exit_criteria, hourly_risk_guards, anchor_one_per_side, stoploss_roll_reentry, hard_flip_exit); `thresholds`; `spot_window`, `spot_window_by_asset` | `hourly.strategies.signals_hourly` with .entry_window, .exit_criteria, .hourly_risk_guards, .anchor_one_per_side, .stoploss_roll_reentry, .hard_flip_exit, .thresholds, .spot_window, .spot_window_by_asset | Entry/exit/risk guards and thresholds in one strategy block; spot_window can live here or under hourly.pipeline. |
| **Pipeline** | — | `hourly.pipeline.run_interval_seconds`, `hourly.pipeline.strategy_priority` | strategy_priority e.g. [hourly_last_90s_limit_99, signals_hourly]. |

**Target layout example (hourly.yaml):**

```yaml
# hourly.yaml (new layout)
hourly:
  pipeline:
    run_interval_seconds: 1
    strategy_priority: [hourly_last_90s_limit_99, signals_hourly]

  strategies:
    hourly_last_90s_limit_99:
      enabled: false
      window_seconds: 75
      limit_price_cents: 99
      min_bid_cents: 94
      side: both
      stop_loss_pct: 15
      stop_loss_distance_factor: 0.75
      assets: [btc, eth, sol, xrp, doge]
      max_yes_per_market: 3
      max_no_per_market: 3
      max_cost_cents: 100
      max_cost_cents_by_asset: { btc: 100, eth: 100, sol: 100, xrp: 100, doge: 100 }
      min_distance_at_placement: { btc: 80, eth: 7.5, sol: 0.3, xrp: 0.07, doge: 0.0017 }
      # ... rest unchanged

    signals_hourly:
      enabled: false   # when true, runs regular hourly: generate_signals + entry_window + exit_criteria + hourly_risk_guards via executor
      entry_window:
        late_window_minutes: 20
        late_interval_seconds: 1
      exit_criteria:
        stop_loss_pct: 0.30
        panic_stop_loss_pct: 0.30
        stop_loss_persistence_polls: 1
        evaluation_interval_seconds: 1
      hourly_risk_guards:
        enabled: true
        # ... (no_new_entry_cutoff_seconds, persistence_polls, recent_cross, distance_buffer,
        #      anchor_one_per_side, stoploss_roll_reentry, hard_flip_exit)
      thresholds: { yes_min: 92, yes_max: 99, no_min: 92, no_max: 99 }
      spot_window: 1500
      spot_window_by_asset: { btc: 1500, eth: 200, sol: 50, xrp: 1, doge: 0.005 }
```

**Code:** V2 reads from **v2_hourly.yaml** only; config is under `config["hourly"]["pipeline"]` and `config["hourly"]["strategies"][...]`. No flattening or legacy key resolution.

---

## 9. Unified Kraken/Coinbase (Oracle) fetch and distance per window

Spot (Kraken and Coinbase) and distance-from-strike are used by multiple strategies (e.g. last_90s, ATM, market_maker) and today are fetched or computed in several places. This section makes them **unified** in the data layer and **per window_id** with **strike caching** and **fallback** for null/stale.

### 9.1 Why unify

- **Strike price** is fixed for the lifetime of a market/window: it comes from the Kalshi market (e.g. event series or title). Fetching or parsing it once per window and reusing it avoids redundant work and ensures all strategies see the same strike for that window.
- **Spot (Kraken, Coinbase)** should be fetched once per tick in one place: same policy (WS preferred, REST fallback), same staleness rules, and a single fallback path when one oracle is null or stale.
- **Distance** (e.g. `|spot - strike|`) is derived from spot and strike; if both are supplied in one place, distance can be computed once per tick per (window_id, asset) and attached to `WindowContext` so strategies do not recompute or use inconsistent values.

### 9.2 Single place: data layer

- **Spot (Kraken + Coinbase)**  
  - **Unified fetch**: One function (e.g. `get_spot_prices(asset)`) that returns `{ "kraken": float | None, "cb": float | None }`.  
  - **Policy**: Prefer Oracle WebSocket when available and fresh (`get_safe_spot_prices_sync(asset, max_age_seconds=...)`); else REST: Kraken via `KrakenClient` (or existing `_get_spot_price`), Coinbase via existing REST helper.  
  - **Fallback**: If Kraken is null or stale, use Coinbase if available (and optionally log). If both are null/stale, return None(s) and the pipeline can skip placement or use a conservative path (e.g. skip that tick or use last-known-good with a staleness cap). Configurable max age for “stale” (e.g. 3–5 seconds).

- **Strike (per window_id)**  
  - **Window_id**: Uniquely identifies the window: e.g. `(interval, market_id)` or `(interval, market_id, ticker)`. Strike is constant for this window.  
  - **Cache**: In the data layer (or a small `strike_cache` module), maintain a cache keyed by `window_id`: `strike_cache[window_id] = strike_float`.  
  - **Population**: When building `WindowContext` for (asset, interval), we have market and ticker; compute strike from Kalshi market API fields **floor_strike**, **ceiling_strike**, **strike**, **subtitle** (same logic as V1 `extract_strike_from_market(market, ticker)`). If not in cache, set `strike_cache[window_id] = strike`; if in cache, reuse.  
  - **Invalidation**: When the window ends (market closes or we move to a new market_id), clear or invalidate cache entries for that `window_id`. Optionally treat cache entry as valid for a TTL (e.g. window length) to avoid stale use if market_id is reused.  
  - **Fallback**: If strike is null or invalid (e.g. 0 or absurd vs spot), do not cache; strategies that need strike get `ctx.strike is None` and can skip or use existing sanity checks (e.g. `MAX_ABSURD_DISTANCE_USD`).

- **Distance (per window_id, per tick)**  
  - **Single computation**: In the data layer, after obtaining spot (Kraken, Coinbase) and strike (from cache or fresh), compute distance once:  
    - `distance_kraken = |spot_kraken - strike|` if both present; else None.  
    - `distance_coinbase = |spot_coinbase - strike|` if both present; else None.  
    - **Conservative distance** (for strategies that use “min of both oracles”): `distance = min(distance_kraken, distance_coinbase)` when both are not None; else the one that is not None; else None.  
  - **Attach to context**: Put `spot_kraken`, `spot_coinbase`, `strike`, `distance_kraken`, `distance_coinbase`, and `distance` (conservative) on `WindowContext`. Strategies read from `ctx` and do not compute distance themselves.  
  - **Fallback for null/stale**: If spot or strike is missing, distance fields are None; strategies already handle “no distance” (e.g. skip placement or ignore distance filter). Optional: if spot is stale, treat as null for that tick and log.

### 9.3 WindowContext fields (additions)

Extend `WindowContext` (in `bot/pipeline/context.py`) with:

- **quote** — orderbook for the current ticker: must expose **yes_bid**, **yes_ask**, **no_bid**, **no_ask** (integers, cents) so strategies can compare sides and compute mark price for exit.
- **positions** — list of normalized positions for the current asset/window for **exit evaluation**: each item has **ticker**, **side**, **count**, **entry_price_cents** (same as V1 `_normalize_position`). Data layer populates this from Kalshi positions (or registry + fill state) so `evaluate_exit(ctx, my_orders)` can compute mark-to-market and return ExitActions.
- `strike: Optional[float]` — cached per window_id; None if invalid or not available.  
- `spot_kraken: Optional[float]`, `spot_coinbase: Optional[float]` — from unified spot fetch.  
- `distance_kraken: Optional[float]`, `distance_coinbase: Optional[float]`, `distance: Optional[float]` — distance_from_strike (USD); `distance` is the conservative single value (e.g. min of the two when both present).

Existing `spot` can remain as the “primary” spot used for display or legacy (e.g. `spot_kraken` or the conservative choice); strategies that need dual-oracle logic use `spot_kraken`, `spot_coinbase`, and `distance` from context.

### 9.4 Phasing

- **Phase 1**: Add strike cache (keyed by window_id), unified `get_spot_prices(asset)` with WS + REST fallback, and a single `compute_distance(spot_kraken, spot_coinbase, strike)` helper. Data layer (when built) uses these; no behavior change yet if pipeline is not running.  
- **Phase 3**: When building `WindowContext`, data layer always fills spot (unified), strike (cached per window_id), and distance (single place). Strategies refactored to use `ctx.strike`, `ctx.spot_kraken` / `ctx.spot_coinbase`, `ctx.distance` instead of fetching or computing themselves.  
- **Fallback**: If unified spot returns both null, context has None spot/distance; pipeline can skip that asset for that tick or apply a safe default (e.g. skip placement). Strike cache miss: compute strike from market and store; if strike is null/absurd, do not cache and set `ctx.strike = None`.

---

## 10. Codebase audit: entry, exit, risk guards, state (double-check)

This section records a **verification pass against the current code** so the migration plan does not miss any entry/exit/risk-guard or state logic. Use it to confirm scope and to add any missing cons or mitigations.

### 10.0 What was traced in code

| Area | Current location | Notes |
|------|------------------|--------|
| **Entry – 15min** | `main.run_bot_15min_for_asset`: `generate_signals_15min` → `apply_guards_filter` (risk_guards) → `execute_signals`; then `record_entry` (risk_guards). | Also: last_90s thread (`_run_last_90s_for_asset`), ATM thread (`_run_atm_for_asset`), MM thread (`_run_mm_for_asset`). |
| **Entry – hourly** | `main.run_bot_for_asset`: `generate_signals_farthest` → `apply_guards_filter` (hourly_risk_guards) → `execute_signals`; `record_entry`, `consume_hourly_roll`. | hourly_last_90s thread: `generate_signals_farthest` (all 9 markets), then placement + stop loss. |
| **Exit – shared** | `exit_criteria.run_exit_criteria` / `evaluate_positions`: hard-flip, take-profit, stop-loss (with persistence, panic); MFE/MAE; `_place_sell`. Used by **main 15min and main hourly only** (signals path). | In-memory: `_position_excursion`, `_sl_warning_streak` (keyed by hour_market_id\|ticker\|side). |
| **Exit – last_90s** | `last_90s_strategy._maybe_stop_loss`: distance-based danger zone, `stop_loss_pct`, `stop_loss_distance_factor`; then resolve/outcome. | DB: `last_90s_placed.stop_loss_triggered_at`; strategy_report_last_90s. |
| **Exit – ATM** | `atm_breakout_strategy._execute_exit_loop`: `take_profit_cents`, `stop_loss_cents`, `trailing_stop_cents`, safe_zone; `_in_trade` in-memory. | strategy_report_atm_breakout. |
| **Exit – MM** | `market_maker_strategy`: scratch loop, zero-fill timeout, cancel both legs. | strategy_report_market_maker. |
| **Exit – hourly_last_90s** | Own stop-loss then resolve; `hourly_limit_99_placed.stop_loss_triggered_at`; strategy_report_hourly_last_90s. | Same pattern as 15min last_90s. |
| **Risk guards** | `risk_guards.gate_allow_entry`: cutoff_seconds, persistence (in_band_streak), recent_cross, distance_buffer (time-tiered 15min), disabled_until_expiry, anchor_one_per_side (hourly), roll (stoploss_roll_reentry), reversal_after_stop (15min). | In-memory: `_ticker_states`, `_window_states` (TickerState, WindowState) keyed by `window_id`. `record_stopout`, `record_entry`, `record_exit`, `reset_window_on_expiry`, `emit_window_summary`. |
| **Caps** | `state.get_ticker_order_count`, `get_total_order_count`, `get_side_order_count`; `caps.cap_scope` (per_side / combined). | **bot_state** table: hour_market_id, ticker, side, placed_order_count. Executor or pipeline must query/update for cap enforcement. |
| **No-trade window** | `no_trade_window.in_no_trade_window(config)`: skip new orders; exits still run. | Global; config `no_trade_windows.enabled`, `.windows`, `.timezone`. |
| **Outcome / resolve** | Main 15min: `get_previous_15min_market_id` → `fetch_15min_market_result`, `save_outcome`. last_90s/hourly_last_90s: resolve after close, write strategy_report. | Pipeline or strategy must run “resolve” after window close (fetch result, save outcome, update strategy_report_*). |
| **last_90s dual-strike** | In-memory `_last_90s_placed_windows`: at most one limit + one market per (window, asset). DB `last_90s_placed`. | Aggregator “one order per market” is **per strategy**: last_90s can have two orders per market (limit + market); other strategies one resting. |

### 10.1 Explicit scope additions (nothing missing)

The following are **in scope** and must be preserved or explicitly designed in the unified pipeline:

1. **Risk guard in-memory state** — `risk_guards._ticker_states` and `_window_states` are keyed by `window_id`. Pipeline must call `reset_window_on_expiry(interval, window_id)` when a window ends so state does not leak. If both 15-min and hourly run in the same process, key by `(interval, window_id)` to avoid collision, or document that window_id is unique across intervals.
2. **Exit_criteria in-memory state** — `exit_criteria._position_excursion` and `_sl_warning_streak` are keyed by `hour_market_id|ticker|side`. When exit runs per-interval, use the same key (market_id is interval-specific) or key by `(interval, window_id, ticker, side)`.
3. **Caps enforcement** — In V2, caps are enforced via **v2_order_registry** only (no bot_state): aggregator (or executor) counts orders per (interval, market_id, ticker, side) when cap_scope=per_side, or combined when cap_scope=combined, and blocks new intents when limits are reached. Config `caps.cap_scope` (per_side vs combined) and per-interval max_orders_per_ticker / max_total_orders_* from v2_common must be respected.
4. **last_90s “two orders per market”** — last_90s allows at most **one limit + one market** order per (window, asset). Aggregator/executor rule is “one resting order per market” **per strategy** where last_90s is explicitly allowed two (limit + market) per market; other strategies remain one per market.
5. **Outcome and resolve flow** — After a window closes, pipeline or strategy must: (a) fetch result (e.g. `fetch_15min_market_result`), (b) save outcome to state DB, (c) update strategy_report_* (resolve_row, is_stop_loss, stop_loss_sell_price, final_outcome). Current main 15min does (a)(b) at start of next tick; last_90s/hourly_last_90s do (c) in their resolve step. Preserve this ordering and DB contract.
6. **apply_guards_filter behavior** — For signals_15min and signals_hourly, risk guards (gate_allow_entry) must behave like current `apply_guards_filter`: same cutoff, persistence, recent_cross, distance_buffer, anchor, roll, reversal logic. Either pipeline calls `gate_allow_entry` for each candidate intent before aggregator, or the strategy’s `evaluate_entry` calls it and returns None when blocked.
7. **Previous-window outcome at tick start** — Current main 15min checks previous market and fetches/stores outcome at the start of the run. Data layer or pipeline tick should do the same so “resolved” state is available for reporting and for strategies that depend on it.

### 10.2 Cons / risks to add (from audit)

- **In-memory state keying** — risk_guards and exit_criteria use global in-memory dicts keyed by window_id (or market_id|ticker|side). With two threads (15-min, hourly), keys can collide if window_id is not unique across intervals. **Mitigation:** Key **all** such state (persistence streak, anchor ticker, stop-once-disable, reversal count, MFE/MAE) by **(interval, window_id)** and (ticker, side) where applicable; or document that window_id is globally unique (e.g. 15min uses event_ticker, hourly uses hour_market_id) and add a regression test that both intervals run without cross-leak.

### 10.3 Confirmation: pros and cons still accurate

- **Pros** — Single source of truth, fewer clients, unified spot/distance, testability, config clarity: all remain valid; current code has duplicate fetches and multiple clients as described.
- **Cons** — Refactor surface, single point of failure, strike cache, two threads, migration effort: all still apply. The **Bot V2 separate** option (§ Cons) and **in-memory state keying** (above) complete the risk list.

---

## 11. Verification steps (ensure everything is working)

Use these steps to verify entry, exit, risk guards, stop loss, config refactor, and both 15-min and hourly pipelines before and after migration.

### 11.1 Config and loader

- [ ] **Config load (new layout):** With YAML migrated to `fifteen_min.pipeline` + `fifteen_min.strategies.*` and `hourly.pipeline` + `hourly.strategies.*`, run config load and assert no errors; assert `config["fifteen_min"]["strategies"]["last_90s_limit_99"]["enabled"]`, `config["fifteen_min"]["strategies"]["signals_15min"]["risk_guards"]`, and equivalent hourly keys are present.
- [ ] **Config load (V2 strict):** With v2_common.yaml, v2_fifteen_min.yaml, v2_hourly.yaml in the strict pipeline + strategies.<name> layout, V2 loads and boots; missing or invalid structure causes V2 to refuse to boot.
- [ ] **All keys covered:** For fifteen_min.yaml and hourly.yaml, every key listed in §3.5 has a target location in the new layout; after migration, grep config usage and confirm no key is read from old path only.

### 11.2 Data layer and context

- [ ] **WindowContext:** Build WindowContext for (asset, interval) with mock market/quote; assert interval, market_id, ticker, asset, seconds_to_close, quote, spot_kraken, spot_coinbase, strike, distance, distance_kraken, distance_coinbase present; strike cached per window_id and reused on second call for same window_id.
- [ ] **Unified spot:** get_spot_prices(asset) returns kraken/cb; with WS down, REST fallback used; with both null, context has None and pipeline can skip or log.
- [ ] **Distance:** For given spot_kraken, spot_coinbase, strike, distance and distance_kraken/distance_coinbase are computed once and match min(|kraken-strike|, |cb-strike|) when both present.

### 11.3 Entry

- [ ] **15-min window:** Pipeline ticks only when seconds_to_close <= entry_window.late_window_seconds (or signals_15min.entry_window); last_90s returns intent only when seconds_to_close <= window_seconds; ATM only when min_seconds_to_close <= seconds_to_close <= max_seconds_to_close.
- [ ] **Hourly window:** Pipeline ticks when minutes_to_close <= late_window_minutes; hourly_last_90s returns intent only when in its window_seconds.
- [ ] **No-trade window:** When in_no_trade_window(config) is true, pipeline does not place new orders (exits still run).

### 11.4 Exit and stop loss

- [ ] **last_90s stop loss:** For a filled position, when loss >= stop_loss_pct and distance <= threshold, evaluate_exit returns ExitAction(stop_loss); executor sells; DB stop_loss_triggered_at set; strategy_report_db resolve_row(is_stop_loss=1, stop_loss_sell_price=…) called in resolve step.
- [ ] **ATM exit:** evaluate_exit_atm returns ExitAction for take_profit, stop_loss_cents, or window-end when conditions met; executor executes; no regression vs current _execute_exit_loop.
- [ ] **Hourly last_90s stop loss:** Same as 15min last_90s; set_hourly_limit_99_stop_loss_triggered and reporting updated.
- [ ] **signals_15min / signals_hourly exit:** run_exit_criteria (or equivalent) runs with correct exit_criteria config; stop_loss_pct and panic_stop_loss_pct produce correct ExitAction(s); executor runs them.

### 11.5 Risk guards

- [ ] **15-min risk_guards:** When risk_guards block entry (e.g. no_new_entry_cutoff_seconds, recent_cross), signals_15min evaluate_entry returns None or pipeline skips placement; behavior matches current run_bot_15min_for_asset with risk_guards enabled.
- [ ] **Hourly risk_guards:** hourly_risk_guards (including anchor_one_per_side, stoploss_roll_reentry) applied; behavior matches current run_bot_for_asset.

### 11.6 Aggregator and executor

- [ ] **One order per market:** With two strategies returning intents for same (market_id, asset), only the higher-priority (per strategy_priority) is placed; executor places one order and registers (order_id, strategy_id, interval, …).
- [ ] **Exits then entries:** In each tick, executor runs all ExitActions first, then places new orders from aggregated intents.
- [ ] **Registry:** get_orders_by_strategy(strategy_id, interval, market_id, asset) returns only orders for that strategy/interval; after placement, order appears in registry; after resolve/expiry, handled correctly.

### 11.7 Integration and regression

- [ ] **Unit tests:** For each strategy, unit test "given WindowContext X, evaluate_entry returns Y and evaluate_exit returns Z" for a few representative contexts (in window, out of window, stop-loss trigger, take-profit).
- [ ] **Integration test:** One tick of run_unified_pipeline(interval) with mock Kalshi client: build context for 2 assets, run 2 strategies, aggregator picks one intent, executor "places" (mock) and registers; assert registry and mock client state.
- [ ] **15-min vs hourly:** Run 15-min pipeline thread and hourly pipeline thread in same process; assert both use same executor (thread-safe) and different config slices (fifteen_min vs hourly); no cross-interval leakage in registry.
- [ ] **Feature flag rollback:** With use_unified_pipeline=false, main.py starts old threads (run_last_90s_loop, run_atm_breakout_loop, run_bot_15min_for_asset, run_bot_for_asset, etc.); with true, only two pipeline threads; restart and toggle flag, confirm behavior.

### 11.8 Post-migration sanity (production-like)

- [ ] **Dry run / OBSERVE:** Run bot in OBSERVE with unified pipeline and new config; verify logs show correct strategy_priority, entry windows, and no placement; exit/stop-loss logic paths logged as expected.
- [ ] **Single window comparison:** For one 15-min window, run old code (if still available) and new pipeline with same config; compare placements and exits (order count, strategy_id, client_order_id prefix) and resolve outcomes (is_stop_loss, stop_loss_sell_price) for same market/asset.
- [ ] **DB and reports:** After a few windows, assert last_90s_placed, hourly_limit_99_placed, strategy_report_* tables have correct rows (stop_loss_triggered_at, is_stop_loss, stop_loss_sell_price, final_outcome); no missing or duplicate rows vs expected.
