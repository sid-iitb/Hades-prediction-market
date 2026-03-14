# Last-90s Oracle Blindness – 6h Run Report

## Summary

The bot ran for 6 hours overnight. The exported CSV had **96 rows** (4 assets × 24 windows), all **aggregated_skips** with skip_detail *"No skip history (window not entered or all ticks returned early e.g. stale oracle)"*. **pre_placement_history** was **None** for every row and **no trades were placed**. The strategy thread ran, but every tick hit an early exit and never saw orderbook (or effectively spot) data.

---

## Task 1: Log Analysis (What Actually Happened)

### Searches Performed

- **WebSocket / Oracle WS:** `[oracle_ws]`, `CERT_VERIFY`, `TimeoutError`, `SSL`, `handshake`, `WebSocket thread crashed`  
  → **No matches.** Oracle WebSocket was not the failing path (config had `use_ws_oracles: false`, so spot used REST).

- **REST errors:** `429`, `ReadTimeout`, `REST API ERROR`, `IP ban`  
  → **No real REST errors.** The only "429" matches were inside JSON timestamps (e.g. `.429737+00:00`), not HTTP 429.

- **DEBUG_WS_1 / DEBUG_WS_2 / stale oracle / no oracle data**  
  → **No matches.** With `use_ws_oracles: false`, those branches are not logged.

- **"No skip-aggregator data"**  
  → **Found.** From **2026-03-05 19:16:39** onward, every 15m window for all four assets (BTC, ETH, SOL, XRP) logs:
  - `[last_90s] No skip-aggregator data for KXBTC15M-26MAR051415 / btc (window may not have been entered or all ticks returned early e.g. stale WS); writing row with fallback details.`
  - Same pattern for ETH, SOL, XRP and for later windows (e.g. 19:56, 20:12, 21:06, 22:27, 23:20, and into 2026-03-06 01:30, 01:45, 02:00).

- **[kalshi_ws]**  
  → Only **"Kalshi orderbook WebSocket started"** appears, and the first such line on **2026-03-06** is at **06:04:25**, then again at 06:43, 06:51, 06:52, 07:23, 07:30, and 15:39, 15:49. So either:
  - Kalshi orderbook WS was not started until ~06:04 on March 6, or  
  - It was crashing and restarting (multiple “started” messages).

### Conclusion from Logs

- **Not** Oracle WebSocket (Kraken/Coinbase) – that path was off and no related errors appear.
- **Not** Kraken/Coinbase REST 429 or IP ban – no such messages.
- **Cause:** For the whole period where every window had “No skip-aggregator data”, the **Kalshi orderbook WebSocket** either never had data for the subscribed ticker or was not running. So `get_safe_orderbook(ticker)` returned **None** on every tick. That triggers the early `continue` before any pre-placement telemetry or skip aggregation, which matches: no pre_placement_history, no skip history, and the fallback message in the CSV.

---

## Task 2: Early Exit – Exact Code Path

**File:** `bot/last_90s_strategy.py`  
**Function:** `_run_last_90s_for_asset`

The **first** early exit that prevents the tick from ever appending to pre_placement history or recording skips is:

```python
# Lines 1224–1228 (before fix)
if ticker:
    start_kalshi_ws()
    subscribe_to_tickers([ticker])
    quote_tick = get_safe_orderbook(ticker)
    if quote_tick is None:
        continue   # <-- EXACT LINE: every tick hit this when Kalshi WS had no orderbook
```

- **Condition:** `quote_tick is None` (Kalshi orderbook WebSocket had no data for `ticker`).
- **Effect:** The loop `continue`s immediately. The code never reaches:
  - `get_minutes_to_close_15min` / `seconds_to_close`
  - Spot price fetch (REST or WS)
  - `_pre_placement_trajectory[key_pp].append(tick_data)`
  - Any `_skip_aggregate_record(...)`
- So at resolve time there is no in-memory skip aggregator data for that window, and when writing the aggregated row we have no `rec` → we use the fallback text *"No skip history (window not entered or all ticks returned early e.g. stale oracle)"* and leave `pre_placement_history` as None (because we never appended to `_pre_placement_trajectory`).

The “stale oracle” wording in the message is a bit misleading here: the **immediate** cause was **missing Kalshi orderbook** (WS returning None), not missing spot/oracle data. Spot is only used later, after we already have `quote_tick`.

---

## Task 3: Root Cause and Fix

### Root Cause (Stated Precisely)

- **Kalshi orderbook WebSocket** did not provide data for the subscribed ticker for the whole 6h period (either WS not running until ~06:04 March 6, or repeated restarts / no snapshot received).
- **No fallback:** The strategy uses **only** `get_safe_orderbook(ticker)` for the orderbook; there was no REST orderbook fallback. So when the WS had no data, every tick exited at `if quote_tick is None: continue`.
- Result: no pre_placement history, no skip records, and 96 CSV rows with the same aggregated_skips fallback message and no trades.

### Fix Implemented

**REST orderbook fallback** when the Kalshi WebSocket has no data:

- In `_run_last_90s_for_asset`, after `quote_tick = get_safe_orderbook(ticker)`:
  - If `quote_tick is None` and `kalshi_client is not None`, call **`kalshi_client.get_top_of_book(ticker)`** (existing Kalshi REST API) and use that as `quote_tick`.
  - If we get a non-None result, we log at DEBUG that the orderbook came from REST fallback.
  - Only if `quote_tick` is still None do we `continue`.

So:

- When the WS is down or has not yet received a snapshot, the strategy can still run using the **Kalshi REST orderbook** and will again append to pre_placement history and record skips/placements.
- No new config or env vars are required; the existing `kalshi_client` is used.

**Code change:** `bot/last_90s_strategy.py` (block that sets `quote_tick` for the current ticker).

### Optional Hardening

- **Start Kalshi orderbook WebSocket at bot boot** (e.g. from `main.py` when the fifteen_min/last_90s strategy is enabled), so the WS is already connected when the first 15m window is monitored. This reduces reliance on REST when WS is available.
- **REST rate limits:** If you run many assets/windows, add simple backoff or caching around `get_top_of_book` to avoid hitting Kalshi REST orderbook rate limits.

---

## Did atm_breakout_strategy Cause This?

**Short answer: No.** The blindness was not caused by atm_breakout_strategy running in parallel.

- **Kalshi orderbook WebSocket** is used **only** by last_90s: `start_kalshi_ws()`, `subscribe_to_tickers([ticker])`, `get_safe_orderbook(ticker)`. **atm_breakout_strategy does not use the Kalshi WS at all** — no imports from `kalshi_ws_manager`, no `subscribe_to_tickers`, no `get_safe_orderbook`.

- **atm_breakout_strategy** gets quotes via `fetch_15min_quote(market_id, client)` → `enrich_with_orderbook(client, tickers)` → **`client.get_top_of_book(ticker)`**, i.e. **Kalshi REST** only. So there is no contention on the single global Kalshi WS subscription; only last_90s subscribes to tickers and reads from the WS.

- **Possible indirect effect:** Both strategies call **Kalshi REST** (market fetch, orderbook). If both run in parallel, total REST load is higher. If Kalshi were to rate-limit (e.g. 429), both could see failures. The logs did not show 429 or REST errors, and the observed issue was WS returning None, not REST failing. So parallel ATM did not cause the 6h blindness.

- **Summary:** The failure was the Kalshi **orderbook WebSocket** having no data for the ticker. Adding a REST orderbook fallback in last_90s removes that single point of failure; running ATM in parallel does not change that conclusion.

---

## References

- Early exit: `bot/last_90s_strategy.py` → `_run_last_90s_for_asset`, `quote_tick = get_safe_orderbook(ticker)` and `if quote_tick is None: continue`.
- Fallback message: same file, resolve path → `_skip_aggregate_format_details` / fallback string when `rec` is None.
- Kalshi orderbook: `bot/kalshi_ws_manager.py` (`get_safe_orderbook`), `src/client/kalshi_client.py` (`get_top_of_book`).
- Logs: `logs/bot.log` (searched for dates 2026-03-05 and 2026-03-06, and for `[kalshi_ws]`, `[oracle_ws]`, and “No skip-aggregator data”).
