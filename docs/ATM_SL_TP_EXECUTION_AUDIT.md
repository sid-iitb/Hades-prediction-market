# ATM SL/TP Execution Audit

This doc answers: (1) API and JSON payload for SL/TP, (2) how we get `side`, (3) how we calculate `count`. Use it to debug when TP or SL do not execute.

---

## 1. API and JSON payload for SL/TP

Both take-profit and stop-loss are executed as a **market sell** (no limit price).

- **Endpoint:** `POST https://api.elections.kalshi.com/trade-api/v2/portfolio/orders`
- **Headers:** Auth headers from `KalshiClient._headers("POST", path)` (KALSHI-ACCESS-KEY, KALSHI-ACCESS-SIGNATURE, KALSHI-ACCESS-TIMESTAMP).
- **Body (JSON):**

```json
{
  "ticker": "<market_ticker>",
  "action": "sell",
  "side": "yes",
  "count": 1,
  "type": "market",
  "client_order_id": "<uuid>"
}
```

- **Important:** We do **not** send `yes_price`, `no_price`, or `price_cents` (Kalshi rejects market orders with a price).
- **Code:** `src/client/kalshi_client.py` → `place_market_order(ticker, action="sell", side=side, count=sell_count)`.

If TP/SL do not execute, check:

- Response status (e.g. 400/409) and body.
- That `ticker` matches the market of the entry order.
- That `side` matches the entry (we bought YES → we sell YES).
- That `count` is at least 1 and not more than the position size.

---

## 2. How we fetch `side`

- **Source:** The **entry** order (the original ATM buy), not the exit action.
- **Flow:**
  1. We have `exit_action.order_id` (the entry order id).
  2. We call `GET /trade-api/v2/portfolio/orders/{order_id}` → `kalshi_client.get_order(exit_action.order_id)`.
  3. Response is normalized with `_normalize_order_for_v2` (fill_count/remaining_count from `*_fp` if needed).
  4. We read **side** from the order: `side = (order.get("side") or "no").lower()`.

- **So:** `side` is the side we **bought** (yes or no). We sell the same side to close.

If the API returns the order without `side` or with different casing, we default to `"no"`. Check logs for `[EXECUTION FATAL] Invalid order shape` (we require `side in ("yes", "no")`).

---

## 3. How we calculate `count`

We use two values: **fill_count** (from the entry order) and **pos_count** (from current positions). The final **sell_count** is the minimum of the two when positions are available, so we never sell more than we hold.

**Step 1 – fill_count (from entry order):**

- From the same `get_order(exit_action.order_id)` response:
  - `fill_count = int(order.get("fill_count") or order.get("filled_count") or 0)`.
  - If `fill_count < 1` and `status in ("executed", "filled", "complete")`, we fall back to `fill_count = int(order.get("count") or 0)`.
- Normalization: `_normalize_order_for_v2` fills `fill_count` from `fill_count_fp` / `filled_count` / `fill_count_fp` so we always have an integer.

If **fill_count stays 0**, we never send the market sell; we log:  
`[EXECUTION] Stop-loss skipped: fill_count=0 for order_id=... ticker=... (status=...); cannot market sell.`

**Step 2 – pos_count (current position):**

- We call `_get_position_contracts(ticker_sell, side)`:
  - Uses `GET /trade-api/v2/portfolio/positions` (or equivalent) via `kalshi_client.get_positions(limit=200)`.
  - Prefers `market_positions[].position_fp` (V2): for the matching ticker, YES = positive, NO = negative; we return the absolute count for the requested side.
  - Fallback: `positions[]` with `contracts`/`quantity`/`count` and `side`/`position`.

**Step 3 – sell_count:**

- If `pos_count is not None`: `sell_count = min(fill_count, pos_count)`.
  - If `sell_count < 1`: we do **not** send the market order; we mark the entry as exited and log:  
    `[EXECUTION] Stop-loss skipped: no current position (already closed...) ... pos=%s; marking entry as exited.`
- If `pos_count is None` (positions API failed): `sell_count = fill_count`.

So **count** in the JSON payload is this **sell_count**. If TP/SL never hit the API, one of these is happening:

- **fill_count == 0** → we skip and log “fill_count=0”.
- **pos_count == 0** (or sell_count &lt; 1) → we skip and log “no current position”.
- **get_order fails** → we log `[EXECUTION FATAL] get_order failed ...` and return without sending the order.

---

## Quick checklist when TP/SL don’t execute

1. **See “Stop-loss skipped: fill_count=0”?**  
   → Entry order from Kalshi has no fill (or we’re not reading fill_count_fp correctly). Check `get_order` response and normalization.

2. **See “Stop-loss skipped: no current position”?**  
   → Positions API says we have 0 for that ticker/side (or we’re not parsing market_positions/position_fp correctly). Check `get_positions` response.

3. **No skip log but no “market sell ORDER PLACED”?**  
   → Either we never entered the SL/TP block (e.g. no exit action), or the `place_market_order` call raised (check for `[EXECUTION FATAL]` or exception logs).

4. **place_market_order returns 4xx?**  
   → Inspect response body (e.g. “insufficient position”, “invalid ticker”). The payload we send is exactly the JSON in section 1 with your `ticker`, `side`, and `count` (sell_count).

---

## Code references

| What            | File                          | Symbol / logic |
|-----------------|-------------------------------|----------------|
| API + payload   | `src/client/kalshi_client.py` | `place_market_order()` |
| get order       | `src/client/kalshi_client.py` | `get_order()` → `_normalize_order_for_v2()` |
| side            | `bot/pipeline/executor.py`    | `order.get("side")` from get_order result |
| fill_count      | `bot/pipeline/executor.py`    | `order.get("fill_count")` (normalized), fallback `order.get("count")` when status executed/filled |
| pos_count       | `bot/pipeline/executor.py`    | `_get_position_contracts(ticker_sell, side)` |
| sell_count      | `bot/pipeline/executor.py`    | `min(fill_count, pos_count)` if pos_count not None else fill_count |
