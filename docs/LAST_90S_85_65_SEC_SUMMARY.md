# Last-90s Limit-99: 85–65 Second Window — Summary

## Window and order type

- **Placement window:** `seconds_to_close <= window_seconds` (config: **85** sec for btc, eth, sol, xrp).
- **85–65 sec band:** We are allowed to place in this band. We always use the **limit order** API; the *effective* behavior is:
  - **Limit at 99¢** when bid ≥ 99 (resting limit, wait for fill).
  - **Limit at ask** (market-style) when bid < 99 and bid ≥ dynamic floor (immediate fill).

---

## Dynamic floor (85–65 vs after 65)

| Band                    | Condition              | Required bid (dynamic floor) |
|-------------------------|------------------------|-------------------------------|
| **85–65 sec** (early)   | `seconds_to_close > 65`| **≥ 98¢**                     |
| **65–0 sec** (late)     | `seconds_to_close ≤ 65`| **≥ min_bid_cents** (e.g. 90¢)|

So **between 85 and 65 sec we only place if bid ≥ 98¢** (and other gates pass). After 65 sec we use the config floor (e.g. 90¢).

---

## What can block placement (in evaluation order)

| # | Blocker               | skip_reason             | Meaning |
|---|------------------------|-------------------------|--------|
| 1 | Outside window        | (no placement path)     | `seconds_to_close > 85` → we don’t try to place. |
| 2 | Already placed        | `order_already_placed`  | Limit or market already placed this window for this asset. |
| 3 | Stale / no WS data    | (log only)              | When `use_ws_oracles`: no valid WS data this tick → skip entry. |
| 4 | Oracle desync         | `oracle_desync`         | \|dist_kraken − dist_cb\| > MAX_DIVERGENCE → skip. |
| 5 | Patience (wait for dip)| `waiting_for_dip`      | Bid = 99¢ and `seconds_to_close > patience_seconds` (60) → wait for dip. |
| 6 | Dynamic floor          | `min_bid`               | Bid < 98 (in 85–65) or bid < min_bid_cents (after 65). |
| 7 | Distance buffer        | `distance_buffer`       | distance ≤ decayed_min_distance_at_placement. |
| 8 | Market-style only     | `market_already_placed` | Already placed market-style this window. |
| 9 | Invalid ask           | `invalid_ask`            | Bid < 99 but ask invalid for market-style. |
|10 | Limit already placed  | `limit_already_placed`  | Limit order already placed this window. |

---

## How to check “last window” in your data

**DB/CSV:** `strategy_report_last_90s` (or export `last_90s_limit_99_last7h.csv`).

### Did we place in the 85–65 sec band?

- Filter: **placed = 1** and **65 < seconds_to_close ≤ 85**.
- **When:** use `ts_utc` and `seconds_to_close` for that row.
- **Type:** `limit_or_market` = `"limit"` (limit at 99 or at ask for market-style).

### Why didn’t we place?

- For **no placement** in a window, use the row with **skip_reason** / **skip_details** (or the aggregated row at resolve).
- **Primary blocker** is in `skip_details` (e.g. `Primary blocker: min_bid` or `distance_buffer`, `waiting_for_dip`, etc.).

---

## Config (relevant)

```yaml
# config/fifteen_min.yaml — last_90s_limit_99
window_seconds: { btc: 85, eth: 85, sol: 85, xrp: 85 }
min_bid_cents: 90   # late (sec ≤ 65); early (85–65) uses 98
# patience_seconds (default 60): when bid=99 and sec_to_close > 60 → skip waiting_for_dip
```

---

## Quick reference

- **85–65 sec:** Limit order allowed; **bid must be ≥ 98¢**; other gates (distance, patience, oracle, one order per window) must pass.
- **Placement time:** Stored in `seconds_to_close` and `ts_utc` when `placed = 1`.
- **Blocker when no place:** See `skip_reason` and `skip_details` (“Primary blocker: …”).
