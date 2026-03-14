# Log vs. Database Discrepancy Analysis — last_90s strategy

**Date:** 2026-03-02  
**Scope:** Why `strategy_report_last_90s` had 0 rows in the last 45 minutes despite the 15-minute last_90s bot running.

---

## 1. Is the strategy running?

**Yes.** Logs show the strategy is running at the expected cadence:

- **`[last_90s] Loop started (interval=5s)`** appears when the loop starts.
- **`[last_90s] [BTC] PLACED limit ...`** (and ETH/SOL/XRP) appear at 15-minute boundaries when the bot is in the final ~65–90 seconds of a window (e.g. 16:13, 16:28, 16:43, 16:58, 17:13, … 21:29 on 2026-03-02).
- **`[last_90s] OUTCOME`** and **`[last_90s] SUMMARY`** appear after windows close.

So the bot is evaluating, placing orders, and resolving; the issue is only that **no rows were written to `strategy_report_last_90s`**.

---

## 2. Silent exceptions preventing DB writes

Two distinct errors appear in `logs/bot.log` for 2026-03-02:

### A. "23 values for 24 columns" (earlier in the day, e.g. 16:13–18:29)

- **Message:** `[last_90s] [BTC] Report write_row_last_90s failed: 23 values for 24 columns` (and same for ETH, SOL, XRP).
- **Cause:** In `bot/strategy_report_db.py`, `write_row_last_90s` was inserting into **25 column names** (with `order_id` listed twice) and supplying **25 values**, while the table has only **24 columns**. SQLite reported this as a column/value count mismatch (e.g. "23 values for 24 columns" depending on how the driver surfaces it).
- **Fix applied:** Removed the duplicate `order_id` from both the INSERT column list and the VALUES tuple (and from the `ON CONFLICT ... DO UPDATE SET` list). Insert is now 24 columns and 24 values.

### B. "name 'min_dist_required' is not defined" (later in the day, e.g. 20:58–21:29)

- **Message:** `[last_90s] [BTC] Report write_row_last_90s failed: name 'min_dist_required' is not defined`.
- **Cause:** In `bot/last_90s_strategy.py`, inside `_run_last_90s_for_asset`, when calling `write_row_last_90s(...)` after a successful limit order placement, the code passed `min_distance_threshold=min_dist_required if min_dist_required > 0 else None`. The variable **`min_dist_required` does not exist** in that scope; the correct variable is **`decayed_min_dist_required`** (and `base_min_dist_required` for the base).
- **Fix applied:** Replaced `min_dist_required` with `decayed_min_dist_required` at that call site (line ~1039).

---

## 3. Where execution was stopping

- **Strategy execution:** Not stopping. The loop runs, assets are evaluated, orders are placed, and outcomes are logged.
- **DB reporting:** Execution reached `write_row_last_90s()` on every placement, but that call **always raised** (first the 23/24 column error, then the NameError). The exception is caught in `last_90s_strategy.py` and logged as:
  - `[last_90s] [ASSET] Report write_row_last_90s failed: <error>`
- So the flow is: **place order → set_last_90s_placed → ensure_report_db → write_row_last_90s → exception → log warning → continue**. Placements and resolve logic are unchanged; only the report table insert failed.

No other silent failures were found: no Tracebacks, KeyError, or TypeError in the last_90s path in the analyzed log window. The only DB-related errors are the two above.

---

## 4. Summary and exact fixes

| Issue | Location | Root cause | Fix |
|-------|----------|------------|-----|
| **Column count mismatch** | `bot/strategy_report_db.py` — `write_row_last_90s()` | INSERT listed `order_id` twice (25 columns) and passed 25 values; table has 24 columns. | Removed duplicate `order_id` from INSERT column list, VALUES tuple, and ON CONFLICT DO UPDATE SET. |
| **NameError on placement report** | `bot/last_90s_strategy.py` — `_run_last_90s_for_asset()` ~line 1039 | Used undefined `min_dist_required` when calling `write_row_last_90s(..., min_distance_threshold=...)`. | Use `decayed_min_dist_required` instead of `min_dist_required`. |

After these fixes, the next runs that place orders should write rows to `strategy_report_last_90s` without errors. Re-run the live DB verification script (e.g. `python3 tools/verify_last_90s_live_db.py --minutes 45`) after the next 15-minute window to confirm.

---

## 5. Note on "Evaluating" logs

The prompt asked about logs like **`[last_90s] Evaluating...`**. The current code does not log that exact phrase. It logs:

- `[last_90s] Loop started`
- `[last_90s] [ASSET] SKIP: ...` / `OBSERVE: would place ...` / `PLACED ...`
- `[last_90s] OUTCOME ...` / `SUMMARY ...`

So "Evaluating" would require a separate log line if you want it; the fact that PLACED/SKIP/OBSERVE appear at the expected times (final 65–90 seconds of each 15-minute window) is sufficient to confirm the strategy is evaluating in that window.
