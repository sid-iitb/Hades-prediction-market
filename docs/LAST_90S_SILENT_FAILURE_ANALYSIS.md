# Last-90s Silent Failure: Root-Cause Analysis

## Log status

- **`last_90s_verify.log`** was not present in the repo.
- **`console.log`** contained only a urllib3/OpenSSL warning (3 lines); no bot or last_90s output.

So the analysis below is **code-path only**. To confirm the actual failure, run the bot with logs captured and search for the phrases in [LAST_90S_VERIFY_LOGS.md](LAST_90S_VERIFY_LOGS.md).

---

## Execution path after “wake up”

“Outside trading window, sleeping…” is printed by the **main 15-min scheduler** (`bot/scheduler.run_loop` + `get_15min_schedule_state`). When it wakes, it runs `run_bot(config)` in the main thread.

The **last_90s strategy** runs in a **separate daemon thread** (`run_last_90s_loop`). It does **not** use that scheduler; it loops every `run_interval_seconds` (e.g. 1s):

```text
while True:
    run_last_90s_once(config, logger, kalshi_client, db_path, db_lock)
    time.sleep(interval_seconds)
```

So “wake up” for last_90s means: **each time `run_last_90s_once` is entered** (every 1s). Below we trace that function.

---

## Flow inside `run_last_90s_once`

1. **Config / assets**  
   If `_get_cfg(config)` is missing or `enabled` is false → **return** (no resolve, no DB writes).  
   If `_assets(...)` is empty → **return** (no resolve, no DB writes).

2. **Resolve (previous window)**  
   `_resolve_and_log_outcomes(...)` runs **first**. It writes aggregated-skip or outcome rows for the **previous** 15‑min window. So if this runs, you should see **some** DB rows for the previous window, unless every asset hits “row already exists” and no placement updates.

3. **No-trade / client**  
   If `in_no_trade_window(config)` → **return** (resolve already ran).  
   If `kalshi_client is None` → **return** (resolve already ran).

4. **Pre-fetch (ThreadPoolExecutor)**  
   - Build `items = [(asset, get_current_15min_market_id(asset))]` (swallow exceptions per asset).  
   - If `items` and `kalshi_client`: submit `_fetch_one(item)` for each item; **`wait(futures)`** (no timeout).  
   - If any `_fetch_one` (e.g. `fetch_15min_quote` → Kalshi REST) **hangs**, **`wait(futures)` blocks forever**. Then:
     - We never reach `run_asset(...)`.
     - We never call `_skip_aggregate_record` for the **current** window.
     - Next loop iteration never runs (stuck in this tick).  
   So the main thread is effectively stuck; no new rows for the **current** window, and no further resolve runs until the process is restarted.

5. **Asset loop**  
   For each asset, `_run_last_90s_for_asset` can **return before any DB write** in two places:
   - **Lines 1185–1187:** `get_current_15min_market_id(asset)` throws → **return** (no log, no skip).
   - **Lines 1207–1209:** `seconds_to_close <= 0` or `seconds_to_close > monitor_window_seconds` → **return** (no skip). So if the bot considers itself “outside” the monitor window every tick, it never records skips for the current window.

---

## Why “zero records” can happen

| Cause | Effect | Evidence you’d see |
|-------|--------|-------------------|
| **Pre-fetch blocks forever** | `wait(futures)` never returns; never reach `run_asset`; no skip rows for current window; next tick never runs. | No RUN_ERROR; process appears stuck; no new DB rows after a certain time. |
| **Return before resolve** | `run_last_90s_once` returns at step 1 (no cfg / not enabled / no assets). | No resolve, no rows. Only if config is wrong or assets list empty. |
| **Resolve throws** | Exception in `_resolve_and_log_outcomes` propagates to loop → `logger.exception("[last_90s] RUN_ERROR: %s", e)`. | **RUN_ERROR** and stack trace in logs. |
| **Every asset returns early** | For every asset: `get_current_15min_market_id` throws **or** `seconds_to_close` outside monitor window → return before `_skip_aggregate_record`. | No skip rows for current window; resolve still runs and can write previous-window or fallback rows. |
| **WebSockets down** | If `use_ws_oracles` is true and oracles are dead, we fall back to REST; we do **not** return just because WS is stale. So WS alone doesn’t explain “zero records” unless something else (e.g. REST) fails or blocks. | `[oracle_ws] Fatal WS error:` in logs if WS crashes (Task 1 patch). |

So:

- **Zero rows at all** (including previous window): resolve never ran (early return at step 1) or resolve threw (then RUN_ERROR in logs).
- **Zero rows for the current window** (no aggregated_skips, no placements): either we never reach `run_asset` (e.g. **pre-fetch blocking**) or every asset returns before `_skip_aggregate_record` (e.g. **seconds_to_close** always outside monitor window, or **get_current_15min_market_id** throwing for every asset).

---

## Most likely root cause (without log evidence)

The most plausible single cause for “wake up, then no trades and no DB writes” after adding the **ThreadPoolExecutor pre-fetch** is:

- **Pre-fetch blocks indefinitely** in `wait(futures)` because one or more of `fetch_15min_market` / `fetch_15min_quote` (Kalshi REST) hang (e.g. rate limit, slow/timeout-less HTTP, or network issue).  
- Then:
  - Resolve **has** already run at the start of that tick (so you might see previous-window rows from an earlier tick).
  - No code in the **current** tick ever writes (we never reach `run_asset` or any skip/placement path).
  - The loop never advances, so it looks like a silent stall rather than a loud exception.

So the fallback DB logging (resolve + aggregated_skips) is “bypassed” for the **current** window simply because execution never gets to the part that calls `_skip_aggregate_record` or placement; it’s stuck in the pre-fetch phase.

---

## Recommended next steps

1. **Capture logs**  
   Run with:  
   `python3 -m bot.main --config config/config.yaml 2>&1 | tee last_90s_verify.log`  
   Reproduce the “wake up then silent” behavior, then:
   - Search for `[last_90s] RUN_ERROR:` and `[oracle_ws] Fatal WS error:`.
   - Search for `[last_90s] Resolve pass:` to confirm resolve is running.
   - Check whether logs stop after a “Resolve pass” and never show pre-fetch or asset activity.

2. **Add a timeout to pre-fetch**  
   So that even if Kalshi REST hangs, we don’t block forever: use `concurrent.futures.wait(..., timeout=...)` and if timeout expires, log and proceed without `pre_fetched` (so each asset fetches inside `_run_last_90s_for_asset`). That restores the old “sync REST” behavior for that tick and keeps the loop moving.

3. **Optional: defensive logging**  
   Log right before and after the pre-fetch block (e.g. “Pre-fetch start”, “Pre-fetch done” / “Pre-fetch timeout”) so the exact stall point is visible in logs.

Implementing the pre-fetch timeout (and optional logs) in code will confirm whether the silent failure is due to pre-fetch blocking.
