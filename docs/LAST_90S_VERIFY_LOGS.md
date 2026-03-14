# Last-90s & Oracle WS: Instant Log Verification

Use this to confirm whether **RUN_ERROR** (Kalshi REST/parallel batch) or **Fatal WS error** (oracle WebSockets) are firing.

---

## Step 1: Run the bot and capture logs

From the repo root:

```bash
# Run bot and save all output to a log file (adjust config path if needed)
python3 -m bot.main --config config/config.yaml 2>&1 | tee last_90s_verify.log
```

Or run in background and tail the file:

```bash
python3 -m bot.main --config config/config.yaml >> last_90s_verify.log 2>&1 &
tail -f last_90s_verify.log
```

---

## Step 2: Search for the two key phrases

**In another terminal** (or after stopping the bot):

```bash
# Check for last_90s loop crashes (Kalshi REST / parallel batch)
grep -n "\[last_90s\] RUN_ERROR" last_90s_verify.log

# Check for oracle WebSocket crashes (Kraken/Coinbase WS)
grep -n "\[oracle_ws\] Fatal WS error" last_90s_verify.log
```

- **If you see `[last_90s] RUN_ERROR:`**  
  The exception message and stack trace right below it tell you why (e.g. Kalshi rate limit, connection error). That points to the parallel pre-fetch / REST being too aggressive.

- **If you see `[oracle_ws] Fatal WS error:`**  
  The exception and stack trace show why the Kraken or Coinbase WebSocket died (e.g. bad key, parse error). Task 1’s `logger.exception` makes this visible.

---

## Step 3: If you see rate limits (1.5s test)

1. In `config/fifteen_min.yaml` (or wherever `last_90s_limit_99` and the loop interval live), set:
   - `run_interval_seconds: 1.5` (temporarily, was 0.5).
2. Restart the bot and capture logs again.
3. If the DB starts filling and RUN_ERROR stops, the ThreadPoolExecutor pre-fetch is likely too aggressive for Kalshi’s REST; consider moving to Kalshi WebSocket for order book.

---

## Where these messages are logged in code

| Phrase | File | Meaning |
|--------|------|--------|
| `[last_90s] RUN_ERROR:` | `bot/last_90s_strategy.py` ~1970 | Exception in `run_last_90s_once()` (e.g. Kalshi API error in pre-fetch or asset run). |
| `[oracle_ws] Fatal WS error:` | `bot/oracle_ws_manager.py` ~191 (Kraken), ~258 (Coinbase) | Exception in the `async with websockets.connect(...)` / `async for raw in ws` loop; WS is reconnecting. |

Both use `logger.exception()`, so the **full stack trace** appears in the log immediately after the message.
