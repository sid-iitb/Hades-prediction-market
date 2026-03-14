# Coin Toss Market Maker — Logging & Visibility

## Runtime logs (entry, exit, risk guards)

### Risk guards (why we didn’t enter)
- **`[mm] [ASSET] Skip gates: ...`** (DEBUG)  
  When any of the 5 gates fail: which gate(s) failed and the values used.
  - **proximity**: `dist_min` vs `max_mm_distance`
  - **parity**: `yes_ask`, `no_ask` vs `[min_price_parity, max_price_parity]`
  - **spread**: yes/no spread vs `min_spread_cents`
  - **velocity**: `|spot_delta|` vs `safe_velocity_threshold_5s`  
  Also logs: `sec_to_close`, `dist_min`, `yes_ask`, `no_ask`.

- **`[mm] [ASSET] Skip: budget ...`** (DEBUG)  
  When `max_cost_cents` is too low for one contract each side: `max_cost_cents`, `price_yes`, `price_no`, required sum.

### Entry (we placed both legs)
- **`[mm] [ASSET] ENTRY placed ...`** (INFO)  
  `ticker`, `YES@price NO@price`, `count`, `order_id_yes`, `order_id_no`, `sec_to_close`, `dist_min`, `spot`, `max_cost_cents`.

### Leg 1 fill
- **`[mm] [ASSET] LEG1_FILL side=yes|no price=Xc order_id=...`** (INFO)  
  Which leg filled first and at what price.

### Exit — happy path
- **`[mm] [ASSET] EXIT happy leg2 filled side=... price=Xc time_to_leg=Xs`** (INFO)  
  Leg 2 filled naturally; side, price, and time from leg1 to leg2.

### Exit — ripcords (scratch)
- **`[mm] [ASSET] RIPCORD Timeout stopwatch=Xs >= Ys`** (INFO)  
  TTL ripcord: time since leg1 fill exceeded `scratch_timeout_seconds`.

- **`[mm] [ASSET] RIPCORD Adverse_Price unfilled_ask=X >= cap=Y (leg1=Zc max_loss=Wc)`** (INFO)  
  Price-cap ripcord: unfilled ask above acceptable loss.

- **`[mm] [ASSET] RIPCORD Spot_Velocity |spot_delta|=X > threshold=Y (entry=Z now=W)`** (INFO)  
  Spot-escape ripcord: spot moved beyond `scratch_spot_movement_threshold`.

- **`[mm] [ASSET] EXIT scratch reason=... cancel order_id=... then market_buy side=...`** (INFO)  
  Scratch execution: cancel unfilled order, then market buy for unfilled side.

### Summary (every round)
- **`[mm] [ASSET] Done report_id=... scratch_triggered=0|1 reason=... leg1=... leg1_price=... leg2_price=... time_to_leg=Xs`** (INFO)  
  One-line summary for the round: report row id, scratch flag, reason, leg prices, time to leg.

### Failures
- **Place failed**, **get_order failed**, **Cancel unfilled ... failed**, **Scratch market buy ... failed**, **One or both orders failed to return order_id** (WARNING).

---

## Database: `strategy_report_market_maker`

Each round writes one row (synthetic `order_id` = `mm:{market_id}:{asset}:{uuid}`).

| Column | Purpose |
|--------|--------|
| `order_id` | Synthetic id for this round |
| `market_id`, `asset`, `ts_utc` | When and which market |
| `spot_kraken`, `spot_coinbase` | Spot at entry (dual oracle) |
| `distance_kraken`, `distance_coinbase` | Distance at entry (coin-toss proximity) |
| `leg1_side` | Which leg filled first: `yes` or `no` |
| `leg1_fill_price`, `leg2_fill_price` | Fill prices (cents) |
| `time_to_leg_seconds` | Seconds from leg1 fill to leg2 fill (or to scratch) |
| `scratch_triggered` | 1 = ripcord fired, 0 = happy path |
| `scratch_reason` | `Timeout` \| `Adverse_Price` \| `Spot_Velocity` \| `None` |
| `pre_placement_history` | JSON array: pre-entry ticks with `dist_kraken`, `dist_cb`, `yes_bid`, `yes_ask`, `no_bid`, `no_ask` |
| `legging_telemetry_history` | JSON array: after leg1 fill, every 0.5s: `elapsed`, `yes_bid`, `yes_ask`, `no_bid`, `no_ask`, `spot` |

Query examples:
- All scratches: `SELECT * FROM strategy_report_market_maker WHERE scratch_triggered = 1;`
- By reason: `SELECT scratch_reason, count(*) FROM strategy_report_market_maker GROUP BY scratch_reason;`
- Recent rounds: `SELECT * FROM strategy_report_market_maker ORDER BY ts_utc DESC LIMIT 20;`

---

## Visibility checklist

| What | Where |
|------|--------|
| Why we didn’t enter (gates) | DEBUG log `Skip gates: ...` |
| Budget skip | DEBUG log `Skip: budget ...` |
| Entry (prices, order ids, spot, dist) | INFO `ENTRY placed ...` + DB row |
| Leg 1 fill | INFO `LEG1_FILL ...` |
| Ripcord that fired | INFO `RIPCORD Timeout|Adverse_Price|Spot_Velocity ...` |
| Scratch execution | INFO `EXIT scratch ...` |
| Happy exit | INFO `EXIT happy ...` |
| Per-round summary | INFO `Done report_id=...` + full row in DB |
| Pre-entry context | DB `pre_placement_history` (JSON) |
| Post–leg1 context | DB `legging_telemetry_history` (JSON) |

For full visibility, set log level to **DEBUG** so gate and budget skips are visible; **INFO** gives entry, leg1, ripcords, exit, and done.
