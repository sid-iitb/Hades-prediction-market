# Last-90s: Why a window can have no DB row (placed=0 or placed=1)

## Most recent completed window

To follow this doc for the **window that just completed** (previous 15‑min slot in ET):

1. **Get the window suffix** (e.g. `26MAR060000`): run  
   `python3 -m tools.check_last_90s_recent_window`  
   or in Python: `from bot.market import get_previous_15min_market_id; print(get_previous_15min_market_id("btc").split("-")[-1])`
2. **Check the DB** for that window:  
   `python3 -m tools.export_last_90s_window_csv <window_suffix>`  
   If it writes **0 rows**, the window has no row → follow the debug flow below.
3. **If 0 rows:** Use the log search table and proposed fixes in the next section, substituting your window suffix (e.g. `26MAR060000`) for the example.

---

## When to debug (check DB first)

**First:** Check the DB for the window you care about (e.g. `26MAR052315`, `26MAR052345`, or the most recent from above):

- Export or query `strategy_report_last_90s` for that window (e.g. `WHERE window_id LIKE '%26MAR052315%'`).
- If you find **no row** for that window (no `placed=0` and no `placed=1`), then run the debug flow below.
- **Specifically when `use_ws_oracles: true`:** Follow the four-stage WS debug (DEBUG_WS_1 → DEBUG_WS_4) and the checklist in this doc.

**Prompt you can give to the AI / yourself:**

> First check in the DB for the window (e.g. 26MAR052345). If you find no trade — no row with placed=0 or placed=1 for that window — then do this to debug, specifically in the case where use_ws_oracles=true: check logs for Entering main while loop, Resolve pass (previous_15min_market), DEBUG_WS_1/2/3/4, and Writing aggregated skip row / placement row; identify at which stage the pipeline stopped and propose a fix.

**Debug checklist when use_ws_oracles=true and DB has 0 rows for the window:**

1. **Loop:** Logs show `Entering main while loop`? If not, look for `Aborting thread early` (config/enabled).
2. **Resolve:** Logs show `Resolve pass: ... previous_15min_market=<that window>` and `Writing aggregated skip row for window_id=...` for that window? If not, bot may not have run when previous was that window (timing).
3. **DEBUG_WS_1:** Are we getting WS price? (`is_ws_running`, `spot`, `spot_cb`).
4. **DEBUG_WS_2:** Can we compute distance? (`distance`, `official_spot`; or `No distance (no oracle data)`).
5. **DEBUG_WS_3:** Which skip reason? (`min_bid`, `distance_buffer`, `OBSERVE`, or `All gates passed, attempting placement`).
6. **DEBUG_WS_4:** Did we write a placement row? (`Writing placement row ... placed=1`). If placement failed, KALSHI API ERROR will appear.

---

## Window 26MAR052345 (0 rows): where the pipeline can stop

**DB check:** If there is **no row** for window `26MAR052345` (no `placed=0`, no `placed=1`), the pipeline stopped before any row was written for that window.

**Search logs in this order** (when `use_ws_oracles=true`):

| Stage | Log text to search | If missing → pipeline stopped at |
|-------|--------------------|-----------------------------------|
| 1 | `Entering main while loop` | Loop never started (check `Aborting thread early` → config/enabled). |
| 2 | `Resolve pass: ... previous_15min_market=...26MAR052345` | Bot did not run when "previous" was 26MAR052345 (timing: bot must run when current window is the one *after* 26MAR052345, e.g. 26MAR052350). |
| 3 | `Writing aggregated skip row for window_id=...26MAR052345` | Resolve ran but write failed or was skipped (search for `DB_WRITE_FAIL` or `DEBUG_SKIP_READ ... skipping rewrite` for that window). |
| 4 | `[DEBUG_WS_1]` for that window’s market | WS/oracle path not reached (e.g. early `continue`: no market_id, no ticker, `quote_tick` None, or outside `seconds_to_close` / outside trading window). |
| 5 | `[DEBUG_WS_2]` | Distance not computed (no oracle data → we still record skip and resolve should write). |
| 6 | `[DEBUG_WS_3]` | Skip/place evaluation not reached. |
| 7 | `[DEBUG_WS_4] ... Writing placement row` | Placement path not reached or DB write failed after place. |

**If you have no log file:** Run the bot with logs captured (e.g. `python -u bot/main.py 2>&1 | tee bot.log`) so the next run for a window produces searchable logs.

---

### For window 26MAR060000 (0 rows) — same steps, substitute suffix

**Search logs in this order:**

| Stage | Log text to search | If missing → pipeline stopped at |
|-------|--------------------|-----------------------------------|
| 1 | `Entering main while loop` | Loop never started (check `Aborting thread early` → config/enabled). |
| 2 | `Resolve pass: ... previous_15min_market=...26MAR060000` | Bot did not run when "previous" was 26MAR060000 (timing: bot must run when current window is the one *after* 26MAR060000, e.g. 26MAR060015). |
| 3 | `Writing aggregated skip row for window_id=...26MAR060000` | Resolve ran but write failed or was skipped (search for `DB_WRITE_FAIL` or `DEBUG_SKIP_READ ... skipping rewrite`). |
| 4 | `[DEBUG_WS_1]` | WS/oracle path not reached (early `continue`: no market_id, no ticker, `quote_tick` None, or outside window). |
| 5 | `[DEBUG_WS_2]` | Distance not computed (no oracle data → we still record skip and resolve should write). |
| 6 | `[DEBUG_WS_3]` | Skip/place evaluation not reached. |
| 7 | `[DEBUG_WS_4] ... Writing placement row` | Placement path not reached or DB write failed after place. |

**Log investigation result for 26MAR060015 (logs/bot.log, use_ws_oracles=true):**

- **DB:** 0 rows for 26MAR060015. **No** `Resolve pass: ... previous_15min_market=...26MAR060015` in the log.
- Last `[last_90s] Resolve pass` is at 04:43:00 with previous=26MAR052330. After that there are **no further last_90s log lines**; [atm] and [mm] continue with 26MAR060000 and 26MAR060015. So the **last_90s loop was blocked**: one or more asset tasks (with use_ws_oracles=true) did not complete, so `wait(futures)` never returned and the next loop iteration (and thus resolve for 26MAR060000 / 26MAR060015) never ran.
- **Fix applied:** A **25s timeout** on the executor wait. If asset tasks don’t complete within 25s (e.g. WS or API block), the loop continues so resolve runs on the next tick and later windows get rows. Backfill tool was used to write fallback rows for 26MAR060015.

**Log investigation result for 26MAR060000 (logs/bot.log):**

- **No** `Resolve pass: ... previous_15min_market=...26MAR060000` in the log → the bot **never ran** when "previous" was 26MAR060000.
- `Entering main while loop` appears at 04:23 and 04:39 (log time). At 04:23 ET, "previous" is 26MAR060415 (04:15), so the bot was started **after** the 00:00–00:15 ET window. Resolve only runs for a window when the bot runs in the *next* slot; since the bot wasn’t running between 00:00 and 00:15 ET, 26MAR060000 was never resolved.
- **Fix:** Multi-slot startup catch-up now backfills **all** missing windows in the last 6 hours (24 × 15‑min slots) when the bot starts. **Restart the bot once** and it will write fallback rows for 26MAR060000 and any other missed windows.

**When you have no log file:**

1. **Capture logs for the next window:**  
   `python -u bot/main.py 2>&1 | tee bot.log`  
   Then after the next 15‑min boundary, search `bot.log` for the strings above with the new window suffix.
2. **Backfill missed windows:** Restart the bot once; the startup catch-up will write fallback rows for all missed windows in the last 6 hours (including 26MAR060000 if it’s still missing).

**Proposed fixes (0 rows for 26MAR052345 or 26MAR060000):**

1. **Timing (most likely):** The row for 26MAR052345 is written at **resolve** time when `get_previous_15min_market_id()` equals that window — i.e. when the *current* 15‑min market is the slot *after* 26MAR052345 (e.g. 26MAR052350). Ensure the bot is **running across that boundary** (e.g. from ~23:44 through ~23:46 ET) so at least one tick runs with previous = 26MAR052345 and resolve writes the row.
2. **Loop not entered:** If logs show `Aborting thread early`, set `last_90s_limit_99.enabled: true` in `config/fifteen_min.yaml` and ensure the config is loaded.
3. **Resolve write failure:** If you see `Resolve pass: ... previous_15min_market=...26MAR052345` but no `Writing aggregated skip row` for that window, search for `DB_WRITE_FAIL` or DB errors; fix DB path, permissions, or schema.
4. **WS not ready:** With `use_ws_oracles=true`, ensure WebSocket oracles are started (they start when the last_90s loop starts). If every tick gets no oracle data we still record a skip and resolve should write a row; if there are 0 rows, resolve did not run for that window (see timing above).
5. **Backfill:** Use the export/dump tools to confirm the table and window_id format; if needed, run the bot once so it performs a resolve for the previous window (first tick after start resolves the previous window).

---

## Summary

If the DB has **no row** for a 15-min window (e.g. March 5 1400), the strategy did not write anything for that window. Possible causes and the fix applied are below.

---

## 1. **no_trade_window (fixed)**

**Cause:** `run_last_90s_once` used to return **before** calling `_resolve_and_log_outcomes` when `in_no_trade_window(config)` was true. So:

- No placement ran (intended).
- **Resolve also did not run**, so the previous market never got an aggregated-skip or outcome row.

If the **resolve** run for window 1400 happens to fall inside a configured no-trade window (e.g. 00:22–02:00 PT), that run was skipped entirely and the 1400 window never got a row.

**Fix (done):** Resolve is now run **before** the no_trade_window check. So we always write the previous window’s row (aggregated skip or outcome) even when the current time is in a no-trade window. Placement is still skipped when in no_trade_window.

---

## 2. **WebSocket oracles (stale / not running / both required)**

**Cause:** With `use_ws_oracles: true`, if `get_safe_spot_prices_sync` returns `None` (WS not running, data older than max_age, or previously **both** Kraken and Coinbase were required), we skip that tick and record `no_oracle_data`; resolve still writes a row. If one exchange feed was slow or disconnected, every tick could get `None` when "both required" was enforced.

**Fixes applied (WS + last_90s):** Single-oracle mode (`require_both=False`) so at least one fresh oracle (Kraken or Coinbase) is enough; warmup wait up to 5s for first WS data after start.

**Effect:** For that tick we do **not** add a skip reason to the aggregator. We **do** append to `_pre_placement_trajectory` earlier in the same tick (before the WS read). So when we later **resolve** (market_id changes), we still have pre-placement trajectory and we still write one row per (window_id, asset) with `skip_reason="aggregated_skips"` and e.g. “Still No skip history recorded” or the pre-placement payload. So a row should still appear for that window.

**If you see no row:** Then resolve did not run for that window (e.g. bot down during resolve window, or no_trade_window bug above). After the fix in §1, no_trade_window no longer blocks resolve.

**Recommendation:** Ensure WebSocket oracles are running (`pip install websockets`, `tools/check_oracle_ws`) so we don’t skip placement every tick; the row would still be written at resolve.

---

## 3. **Oracle divergence (entry-only)**

**Cause:** If `|dist_kraken - dist_cb| > MAX_DIVERGENCE[asset]`, we call `_skip_aggregate_record(..., "oracle_desync")` and return. So we **do** record a skip and at resolve we write an aggregated row. So a row **should** exist.

**If there is no row:** Divergence is not the cause; something else prevented resolve from running (e.g. §1 or §4).

---

## 4. **Bot not running during the resolve window**

**Cause:** We write the row for window 1400 when we **resolve** it, i.e. when we run with “previous market” = 1400 (e.g. right after 14:00 ET). If the bot was stopped or crashed from ~13:58 through ~14:15 ET, we might never run with `get_previous_15min_market_id() == 1400`, so we never write 1400.

**Recommendation:** Run the bot continuously across the window close, or add a separate “catch-up resolve” that iterates over recent closed markets and writes missing aggregated rows.

---

## 5. **Observe / Trade mode**

**Cause:** In `OBSERVE` we never call `place_limit_order`; we only log “would place” and return. We still call `_skip_aggregate_record` when we skip (e.g. min_bid, distance_buffer, waiting_for_dip), and we still run resolve. So we should still get a row (placed=0 with skip_reason/skip_details).

**If there is no row:** Mode is not the cause; again, resolve did not run for that window (§1 or §4).

---

## 6. **Enabled / disabled**

**Cause:** If `last_90s_limit_99.enabled` is false, `run_last_90s_once` returns at the top and we never resolve or place. So no row for any window while disabled.

**Recommendation:** Confirm `config/fifteen_min.yaml` has `last_90s_limit_99.enabled: true`.

---

## Code change (summary)

- **File:** `bot/last_90s_strategy.py`, `run_last_90s_once`.
- **Change:** Call `_resolve_and_log_outcomes(...)` **before** `if in_no_trade_window(config): return`, so resolve always runs when the strategy runs (and kalshi_client is set), and every closed window gets a row even when the current time is in a no-trade window.

This removes the main case where a window (e.g. March 5 1400) could have no row despite the strategy being enabled.
