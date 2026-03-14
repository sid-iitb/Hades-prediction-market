# V2 Dry Run: Data Captured and Pipeline Verification

When you run `python -m bot.v2_main`, the pipeline uses the **real Kalshi API** for market and orderbook data. Execution is **strictly DRY_RUN** (no real orders placed). You must set `KALSHI_API_KEY` and `KALSHI_PRIVATE_KEY` so the client can fetch markets and orderbooks; if credentials are missing, assets are skipped with a warning. This document describes what data you capture and how to verify the pipeline.

---

## 1. Data captured in dry run

### 1.1 Logs (stdout)

| What | When | Example |
|------|------|--------|
| **Startup** | Once at boot | `V2 dry_run=True (no real Kalshi orders if True)`, `V2 config loaded from config`, `V2 DB initialized at .../data/v2_state.db`, `Started fifteen_min pipeline thread` |
| **[V2 DATA]** | Every pipeline cycle, per asset | `[V2 DATA] Asset: BTC \| Quote: WS \| Spot: WS \| Distance: 42.5` — confirms quote source (WS vs REST), spot source, and distance |
| **[DRY RUN] Would execute ExitAction** | When a strategy signals an exit (e.g. stop_loss) | `[DRY RUN] Would execute ExitAction: order_id=... action=stop_loss` |
| **[DRY RUN] Would place OrderIntent** | When an intent passes aggregation and would be placed | `[DRY RUN] Would place OrderIntent: strategy_id=last_90s_limit_99 side=no price_cents=99 count=... client_order_id=...` |
| **Fetch/config errors** | If market fetch or config fails | `[fifteen_min] [BTC] Fetch error, skipping asset: ...` or `No config for interval ...` |
| **Pipeline cycle errors** | If a cycle raises | `[fifteen_min] Pipeline cycle error: ...` |

**Note:** Aggregator cap messages (e.g. “1 order per market already present”) are logged at **DEBUG**. To see them, set logging level to DEBUG or add a temporary INFO log.

### 1.2 Database: `data/v2_state.db`

| Table | Written in dry run? | Contents |
|-------|---------------------|----------|
| **v2_order_registry** | **No** | In dry run the executor never calls `register_order`, so no new rows. Table exists and may be empty or contain rows from a previous live run. |
| **v2_strategy_reports** | No (only when trades are resolved in live flow) | Unused in dry run. |
| **v2_telemetry_last_90s** | **Yes** | One row per “active window” decision for **last_90s_limit_99**: every time the strategy is in the last `window_seconds` and either **skips** (low_distance, low_bid) or **fires** (intent_fired). Columns: `window_id`, `asset`, `placed` (0=skip, 1=intent), `seconds_to_close`, `bid`, `distance`, `reason`, `pre_data` (JSON of quote + spot_kraken), `timestamp`. |

**atm_breakout_strategy** does not write to a V2 telemetry table in the current implementation; you verify it via **[DRY RUN] Would place OrderIntent** / **Would execute ExitAction** in the logs.

---

## 2. How to verify the pipeline looks good

### 2.1 Quick checklist (logs)

1. **Process starts**  
   You see `V2 dry_run=True`, config loaded, DB initialized, and “Started fifteen_min pipeline thread” (and hourly if enabled).

2. **Data layer**  
   Every few seconds you see `[V2 DATA] Asset: BTC | Quote: ... | Spot: ... | Distance: ...` for each asset (BTC, ETH, SOL, XRP).  
   - **Quote: WS** = orderbook from WebSocket; **REST** = REST fallback.  
   - **Spot: WS** = Oracle WS; **REST** = REST fallback.  
   - **Distance** should be a number (not missing) when the data layer is working.

3. **last_90s_limit_99**  
   - In the last ~90 seconds before a 15m window closes, you should see either:  
     - **[DRY RUN] Would place OrderIntent** with `strategy_id=last_90s_limit_99`, or  
     - No intent (skipped due to distance/bid), but telemetry rows still written (see DB below).

4. **atm_breakout_strategy**  
   When conditions are met you may see **[DRY RUN] Would place OrderIntent** with `strategy_id=atm_breakout_strategy`, and **[DRY RUN] Would execute ExitAction** for window-end or stop-loss/take-profit.

5. **No fatal execution errors**  
   You should **not** see `[EXECUTION FATAL]` in dry run (those paths are only used when `dry_run=False`). Any `[EXECUTION FATAL]` in dry run would indicate a bug.

### 2.2 Verify last_90s telemetry (DB)

After running for at least one 15m window (including the last ~90 seconds), check that **last_90s** is making decisions and recording them:

```bash
# From project root
sqlite3 data/v2_state.db "
SELECT reason, placed, asset, COUNT(*) as n
FROM v2_telemetry_last_90s
GROUP BY reason, placed, asset
ORDER BY reason, asset;
"
```

You should see rows for:
- **reason = low_distance**, **placed = 0** — time gate passed but distance below threshold.
- **reason = low_bid**, **placed = 0** — time gate passed but bid too low.
- **reason = intent_fired**, **placed = 1** — intent would have been placed (dry run only logs it).

Example “all good” snapshot: multiple `low_distance` / `low_bid` rows and occasionally `intent_fired` when conditions are met.

To inspect the pre_data (quote + spot at decision time):

```bash
sqlite3 data/v2_state.db "
SELECT window_id, asset, placed, reason, seconds_to_close, bid, distance, timestamp
FROM v2_telemetry_last_90s
ORDER BY timestamp DESC
LIMIT 20;
"
```

### 2.3 Summary script

From project root:

```bash
python tools/verify_v2_dry_run.py
```

This prints a short summary of **v2_telemetry_last_90s** (counts by reason/asset, recent rows) and reminds you what to check in logs. See script docstring for options.

---

## 3. Optional: increase log detail

To see aggregator cap messages and other debug lines:

- In code: set `logging.basicConfig(..., level=logging.DEBUG)` in `bot/v2_main.py`, or  
- Via env (if your code supports it): e.g. `LOG_LEVEL=DEBUG python -m bot.v2_main`.

---

## 4. Switching to live

When the pipeline behavior in dry run looks correct:

1. Set `dry_run: false` in `config/v2_common.yaml`, **or** run with `V2_DRY_RUN=false python -m bot.v2_main`.
2. Ensure only one bot (V2) is placing orders on the Kalshi account (pause V1 if it was running).
3. After that, **v2_order_registry** will be populated with real order IDs and statuses; **v2_strategy_reports** will be filled as trades resolve.
