# Last-90s 85–65 sec placement — verification result (from DB)

**Date:** 2026-03-05  
**DB:** `data/bot_state.db`  
**Tool:** `python -m tools.verify_last_90s_85_65_placement --db data/bot_state.db --windows 15`

---

## Query

- Table: `strategy_report_last_90s`
- For each (window_id, asset): use the row with `placed = 1` and non-aggregate `order_id` to get `seconds_to_close`.
- **Placed in 85–65 sec** ⟺ `65 < seconds_to_close <= 85`.

---

## Result

**No order in the queried data was placed in the 85–65 sec window.**

- All **placed** orders in the DB have `seconds_to_close` in the range **~32–64** (i.e. at or after the 65s mark, in the “late” band where the dynamic floor is `min_bid_cents` e.g. 90¢).
- Examples from DB:
  - **KXSOL15M-26MAR051245 (SOL):** placed at **52.8** sec → NO (85–65).
  - **KXBTC15M-26MAR051245 (BTC):** placed at **63.9** sec → NO (85–65).
- Windows with **no placement** (aggregated skips) report blockers such as `aggregated_skips` or `distance_buffer` in `skip_reason` / `skip_details`.

---

## How to re-run verification

```bash
# Default DB path: data/bot_state.db; last 5 windows
python -m tools.verify_last_90s_85_65_placement

# Custom DB and last 15 windows
python -m tools.verify_last_90s_85_65_placement --db data/bot_state.db --windows 15
```

---

## Summary table (from last run)

| Window (sample) | Asset | Placed? | In 85–65? | seconds_to_close / Blocker |
|-----------------|-------|---------|------------|----------------------------|
| KXXRP15M-26MAR051330 | XRP | No | — | aggregated_skips |
| KXSOL15M-26MAR051330 | SOL | No | — | aggregated_skips |
| KXETH15M-26MAR051330 | ETH | No | — | aggregated_skips |
| KXBTC15M-26MAR051330 | BTC | No | — | aggregated_skips |
| … (1315, 1300) | … | No | — | aggregated_skips |
| KXXRP15M-26MAR051245 | XRP | No | — | distance_buffer |
| KXSOL15M-26MAR051245 | SOL | Yes | **No** | 52.8 sec (market) |
| KXBTC15M-26MAR051245 | BTC | Yes | **No** | 63.9 sec (market) |

**Conclusion:** In this DB snapshot, we did **not** place any bid in the 85–65 sec window; all placements occurred when `seconds_to_close ≤ 65` (or no placement).
