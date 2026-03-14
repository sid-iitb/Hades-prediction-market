# Pre-Placement and Post-Placement Telemetry — Safety and Verification

## Why telemetry does not change core logic

### Pre-placement telemetry
- **Only adds an earlier entry point:** The bot enters the loop when `seconds_to_close <= monitor_window_seconds` (e.g. 120s) instead of `window_seconds` (e.g. 65s).
- **Action gate:** After appending one tick, we run `if seconds_to_close > window_seconds: return`. So skip reasons and placement are evaluated **only** when `seconds_to_close <= window_seconds`. The decision to skip or place uses the same inputs and logic as before; we just don’t run that logic until we’re inside the trading window.
- **Writes are append-only in memory** until we place or resolve; then we pass `pre_placement_history` into the **existing** `write_row_last_90s` call. No new branches depend on telemetry.

### Post-placement telemetry
- **Only observes:** When `placed` is true we read quote, distance, and bid; we append to an in-memory list and update `lowest_dist` / `lowest_bid`. We do not change any condition inside `_maybe_stop_loss` or placement logic.
- **Single write at exit:** We call `update_post_placement_telemetry(...)` only when the monitoring loop exits (window close or stop-loss), via a single `UPDATE ... WHERE order_id = ?`. No new rows; no duplicate writes.
- **Failure is isolated:** If the telemetry UPDATE fails, we only log a warning; `update_resolution_last_90s` and stop-loss behavior have already run.

So: **trade and stop-loss logic are unchanged**; telemetry is additive and fails safely.

---

## How to verify after one full run (and at least one trade)

After restarting the bot, running for at least one full 15‑minute cycle, and having **at least one placed trade** in that run:

### 1. Pre-placement telemetry

**Check that recent rows have pre-placement history:**

```sql
-- SQLite, run against data/bot_state.db
SELECT
  window_id,
  asset,
  placed,
  length(pre_placement_history) AS pre_len,
  json_array_length(pre_placement_history) AS pre_ticks
FROM strategy_report_last_90s
WHERE ts_utc >= date('now', '-2 hours')
ORDER BY ts_utc DESC
LIMIT 20;
```

- For rows from this run, `pre_placement_history` should be non-null for windows where the bot was running in the monitor window (e.g. 120s before close).
- `pre_ticks` should be a positive number (at least one tick per second while in monitor window, up to the moment you enter the trading window or place).

**Validate JSON shape (one row):**

```sql
SELECT pre_placement_history
FROM strategy_report_last_90s
WHERE pre_placement_history IS NOT NULL
LIMIT 1;
```

- The value should be a JSON array. Each element should look like:
  `{"sec": <float>, "dist_krak": <float or null>, "dist_cb": <float or null>, "bid": <int>, "ask": <int>}`.
- `sec` should decrease over the array (time moving toward close).

### 2. Post-placement telemetry (only when at least one trade occurred)

**Check that placed orders have post-placement fields:**

```sql
SELECT
  order_id,
  window_id,
  asset,
  placed,
  min_dist_after_placement,
  min_bid_after_placement,
  length(post_placement_history) AS post_len,
  json_array_length(post_placement_history) AS post_ticks
FROM strategy_report_last_90s
WHERE placed = 1
  AND ts_utc >= date('now', '-2 hours')
ORDER BY ts_utc DESC
LIMIT 10;
```

- For placed orders from this run, you should see:
  - `min_dist_after_placement`: real number or null (null if no distance was recorded).
  - `min_bid_after_placement`: integer (e.g. between 0 and 100).
  - `post_placement_history`: non-null JSON array (can be empty if the window closed before the next tick).

**Validate post-placement JSON shape:**

```sql
SELECT post_placement_history
FROM strategy_report_last_90s
WHERE placed = 1 AND post_placement_history IS NOT NULL
LIMIT 1;
```

- Each element should look like:
  `{"sec_to_close": <float or null>, "dist": <float or null>, "bid": <int>}`.

### 3. Sanity checks (core logic unchanged)

- **Placement:** You should still see the same kind of logs and DB rows for placed orders (same `placed=1`, `order_id`, etc.).
- **Stop-loss:** If a stop-loss triggers, the row should have `is_stop_loss=1` and resolution fields set as before; the same row should also have `min_dist_after_placement`, `min_bid_after_placement`, and `post_placement_history` set (because we flush telemetry when stop-loss triggers).
- **Aggregated skips:** Rows with `placed=0` and `skip_reason='aggregated_skips'` should have `pre_placement_history` populated for that window (we pop and pass when writing the aggregated row).

---

## Prompt you can use to verify

After one full run (and at least one trade), you can ask:

**“Verify Pre-Placement and Post-Placement telemetry using the DB:**
1. **Pre-placement:** In `strategy_report_last_90s`, for rows from the last 2 hours, check that `pre_placement_history` is a non-null JSON array and that each element has keys `sec`, `dist_krak`, `dist_cb`, `bid`, `ask`. Confirm that the number of ticks is reasonable (about one per second while in the monitor window).
2. **Post-placement:** For rows with `placed=1` from the last 2 hours, check that `min_dist_after_placement`, `min_bid_after_placement`, and `post_placement_history` are set. Confirm that `post_placement_history` is a JSON array of objects with `sec_to_close`, `dist`, and `bid`.
3. **Core logic:** Confirm that placed and stop-loss rows still have correct `placed`, `order_id`, `is_stop_loss`, and resolution fields and that no duplicate or missing rows were introduced.”  

Then run the SQL above (or equivalent) and inspect one sample of each JSON column to confirm structure and that values look correct.

---

## One-liner checks (SQLite from project root)

```bash
# Pre-placement: count recent rows with non-null pre_placement_history
sqlite3 data/bot_state.db "SELECT count(*) FROM strategy_report_last_90s WHERE ts_utc >= date('now', '-2 hours') AND pre_placement_history IS NOT NULL;"

# Post-placement: count placed rows with post_placement_history set
sqlite3 data/bot_state.db "SELECT count(*) FROM strategy_report_last_90s WHERE placed = 1 AND ts_utc >= date('now', '-2 hours') AND (post_placement_history IS NOT NULL OR min_dist_after_placement IS NOT NULL);"
```

After a full run with at least one trade, the first count should include recent windows; the second should be at least the number of placed orders in that period.
