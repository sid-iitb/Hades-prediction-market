# last_90s_limit_99 and WebSocket oracles

When `use_ws_oracles: true` is set under `last_90s_limit_99` (e.g. in `config/fifteen_min.yaml`), the strategy uses WebSocket (Kraken + Coinbase) for **spot price in all trading decisions**, with REST fallback when WS is unavailable or stale.

## When WS price is used (high confidence)

1. **Loop startup**  
   If `use_ws_oracles` is true, `start_ws_oracles()` is called when the last_90s loop starts. If it fails (e.g. `websockets` not installed), a warning is logged and every tick will use REST.

2. **Entry gate (place or skip)**  
   In `_run_last_90s_for_asset`, spot for distance-to-strike and side choice:
   - If `use_ws_oracles` and `is_ws_running()`: calls `get_safe_spot_prices_sync(asset, max_age_seconds=3.0)`.
   - If that returns data: **spot and spot_cb come from WS** (Kraken + Coinbase).
   - If it returns `None` (stale or missing) or any exception: falls back to REST (`_get_spot_price` / `_get_coinbase_spot_price`).
   - When WS was used for this tick, a debug log is emitted: `Entry gate using WS spot kraken=... cb=...`.

3. **Stop-loss danger check**  
   When evaluating whether existing orders are in “danger” (distance to strike), the same pattern is used: WS when `use_ws_oracles` and running and fresh data; otherwise REST.

So for **“will last_90s_limit_99 use WS-based price?”**:  
**Yes, with high confidence**, for both “should we place?” and “should we stop-loss?” as long as:

- `use_ws_oracles: true` in config  
- WebSocket oracles started successfully at loop start  
- `get_safe_spot_prices_sync(asset, 3.0)` returns non-`None` (both Kraken and Coinbase have data &lt; 3s old)

If any of those fail, the code falls back to REST for that tick and continues running (placement/skip/DB writes still happen).

## Paths that still use REST only

These do **not** read `use_ws_oracles`; they always use REST. They are for telemetry/logging, not for the place/stop-loss decision:

- Pre-placement trajectory (distance/bid/ask tick before placement)
- Post-placement trajectory “current_distance” and lowest_dist
- Stop-loss **report** (spot logged after a stop-loss)
- Resolve/aggregate skip details (spot in skip_details)
- Post-placement log (spot in placement report)

So the **decision** price is WS when enabled and available; some **reported** spots may still be REST.

## How to confirm WS is used

- Set log level to DEBUG and look for: `[last_90s] [ASSET] Entry gate using WS spot kraken=... cb=...`
- If you see `Stale or missing WS oracle data (max_age=3s); falling back to REST this tick`, that tick used REST for the entry gate.
- At loop start, the log line `Loop started ... use_ws_oracles=True` confirms the flag; WS may still need a few seconds to receive data from both exchanges.
