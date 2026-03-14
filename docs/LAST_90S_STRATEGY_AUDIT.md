# Comprehensive Audit: last_90s_limit_99 Strategy

This document is a technical breakdown of how the **last_90s_limit_99** strategy (V2) handles entry and exit. The implementation lives in **`bot/pipeline/strategies/last_90s.py`** (not `bot/strategies/last_90s.py`). Distance is computed in **`bot/pipeline/data_layer.py`** (Smart Average).

---

## 1. The Entry Window (`window_seconds: 85`)

### How time-to-expiration is calculated

The pipeline does **not** compute time in the strategy. It is computed once per asset in **`bot/pipeline/run_unified.py`** and passed into **`build_context`** as `seconds_to_close`:

```144:156:bot/pipeline/run_unified.py
            close_ts = _close_ts_from_market(market)
            if close_ts is not None:
                now_ts = int(datetime.now(timezone.utc).timestamp())
                seconds_to_close = max(0.0, float(close_ts - now_ts))
            else:
                if interval == "fifteen_min":
                    mins = get_minutes_to_close_15min(market_id)
                else:
                    mins = get_minutes_to_close(market_id)
                seconds_to_close = max(0.0, mins * 60.0) if mins is not None else None
```

So **time-to-expiration** is either:

- `close_ts - now_ts` (seconds until market `close_time` / `expected_expiration_time`), or  
- Fallback: `mins * 60` from `get_minutes_to_close_15min(market_id)` (or hourly equivalent).

### Why hunting only starts when &lt; 85 seconds left

The strategy gates entry on `window_seconds` (config: **85** for all assets). Until then, it does not consider placing an order:

```134:138:bot/pipeline/strategies/last_90s.py
        asset = (ctx.asset or "").strip().lower()
        window_seconds = float(_get_asset_config(cfg.get("window_seconds"), asset, 90))
        if ctx.seconds_to_close is None or ctx.seconds_to_close > window_seconds:
            return None
```

- If `seconds_to_close > 85` (or the per-asset value), **evaluate_entry** returns `None` → no intent, no telemetry for “hunting” yet.
- Only when **`seconds_to_close <= 85`** does the strategy enter the “active hunting phase” and then apply distance + bid checks and possibly emit an intent.

So “&lt; 85 seconds” is the **entry window**: the bot only tries to place the limit order in that final slice of the market’s life.

---

## 2. Distance Calculation (Smart Average vs `min_distance_at_placement`)

### Where distance comes from (data layer)

Distance is **not** computed in the strategy. It is computed in **`bot/pipeline/data_layer.py`** inside **`build_context`**, using the **Smart Average** logic:

```339:361:bot/pipeline/data_layer.py
        # Distance from strike (absolute). Smart average: use average when oracles agree; MIN when divergence is high.
        distance_kraken: Optional[float] = None
        distance_coinbase: Optional[float] = None
        if strike is not None:
            if kraken_price is not None:
                distance_kraken = abs(kraken_price - strike)
            if coinbase_price is not None:
                distance_coinbase = abs(coinbase_price - strike)
        if distance_kraken is not None and distance_coinbase is not None:
            divergence = abs((kraken_price or 0.0) - (coinbase_price or 0.0))
            asset_lower = (asset or "").strip().lower()
            zone = SANITY_ZONES.get(asset_lower, 0.0)
            if divergence <= zone:
                distance = (distance_kraken + distance_coinbase) / 2.0
            else:
                distance = min(distance_kraken, distance_coinbase)
                logger.debug(
                    "[%s] High Divergence (%s) detected. Falling back to MIN distance.",
                    asset_lower,
                    divergence,
                )
        else:
            distance = distance_kraken if distance_coinbase is None else distance_coinbase
```

So **`ctx.distance`** is always either:

- **Average:** `(distance_kraken + distance_coinbase) / 2` when `|spot_kraken - spot_coinbase| <= SANITY_ZONES[asset]`, or  
- **Min (safety):** `min(distance_kraken, distance_coinbase)` when divergence is above the zone.

### How the strategy uses it (min_distance_at_placement)

The strategy compares this single **`ctx.distance`** (already Smart Average or MIN from the data layer) to the configured **`min_distance_at_placement`** (e.g. **40 for BTC**):

```140:150:bot/pipeline/strategies/last_90s.py
        # From here on we are in the active hunting phase; record telemetry on any skip or intent.
        min_dist = _get_asset_config(cfg.get("min_distance_at_placement"), asset, 0.0)
        if min_dist is not None and ctx.distance is not None:
            try:
                min_d = float(min_dist)
                if min_d > 0 and ctx.distance < min_d:
                    self._record_telemetry(ctx, 0, "low_distance")
                    return None
            except (TypeError, ValueError):
                pass
```

- So: **if `ctx.distance < min_distance_at_placement`** (e.g. &lt; 40 for BTC), the strategy **skips** entry (telemetry reason `"low_distance"`) and returns `None`.
- There is no second, separate “min” distance in the strategy; the only comparison is **Smart Average (or MIN) distance vs `min_distance_at_placement`**.

---

## 3. Order Book Criteria

### limit_price_cents: 99

The strategy places a **limit** order at **99 cents** (config: `limit_price_cents: 99`):

```162:174:bot/pipeline/strategies/last_90s.py
        limit_price_cents = int(cfg.get("limit_price_cents", 99))
        max_cost_cents = int(cfg.get("max_cost_cents", 10000))
        count = max(1, min(10, max_cost_cents // limit_price_cents)) if limit_price_cents else 10

        self._record_telemetry(ctx, 1, "intent_fired")
        client_order_id = f"last90s:{uuid.uuid4().hex[:12]}"
        return OrderIntent(
            side="no",
            price_cents=limit_price_cents,
            count=count,
            order_type="limit",
            client_order_id=client_order_id,
        )
```

So the **condition for the 99¢ limit** is: the strategy only fires an intent when **all** of the following are already satisfied (earlier in `evaluate_entry`):

- Within the entry window (`seconds_to_close <= window_seconds`).
- `ctx.distance >= min_distance_at_placement`.
- Bid floor check (see below).

There is no separate “order book condition” for 99¢; 99 is simply the **limit price** at which we are willing to buy. The **safety** is enforced by the **bid floor** (min_bid_cents), not by a separate 99¢ check.

### min_bid_cents: 96 as safety floor

To avoid buying a “dead” contract (no bid support), the strategy requires a **minimum current bid** before firing an intent. The logic differs **before vs after 65 seconds**:

```151:161:bot/pipeline/strategies/last_90s.py
        min_bid_cents = int(cfg.get("min_bid_cents", 90))
        no_bid = ctx.quote.get("no_bid", 0) or 0
        if ctx.seconds_to_close > 65:
            if no_bid < 98:
                self._record_telemetry(ctx, 0, "low_bid")
                return None
        else:
            if no_bid < min_bid_cents:
                self._record_telemetry(ctx, 0, "low_bid")
                return None
```

- **When `seconds_to_close > 65`:** require **`no_bid >= 98`**. So in the first ~20 seconds of the 85s window, the bar is very high (98¢).
- **When `seconds_to_close <= 65`:** require **`no_bid >= min_bid_cents`** (e.g. **96**). So in the final 65 seconds, a slightly lower floor (96¢) is enough.

So **min_bid_cents: 96** is the **safety floor** in the second half of the window: we only consider placing the 99¢ limit if the current “no” bid is at least 96¢, reducing the chance we are buying into a contract that has already collapsed.

---

## 4. Order Limit / Max Frequency (max_cost_cents: 200, “double-dip” prevention)

### How many orders per window?

Two layers matter:

**A) Strategy: contract count per intent**

```162:164:bot/pipeline/strategies/last_90s.py
        limit_price_cents = int(cfg.get("limit_price_cents", 99))
        max_cost_cents = int(cfg.get("max_cost_cents", 10000))
        count = max(1, min(10, max_cost_cents // limit_price_cents)) if limit_price_cents else 10
```

With **max_cost_cents: 200** and **limit_price_cents: 99**:

- `count = max(1, min(10, 200 // 99)) = max(1, min(10, 2)) = 2`.
- So each **intent** is for at most **2 contracts** at 99¢ (≤ $2 notional).

**B) Aggregator: at most one order per market per window**

The strategy can emit an intent on **every tick** while in the window. The **aggregator** enforces that we do **not** place a second order if there is already a **resting** order for that market:

```51:57:bot/pipeline/aggregator.py
        # Cap: for now, hardcoded 1-order-per-market — if any active order exists, allow none
        if len(active_orders) >= 1:
            logger.debug("Cap: 1 order per market already present (%d active), skipping new intents", len(active_orders))
            return []

        # Allow the highest-priority intent only
        return [sorted_intents[0]] if sorted_intents else []
```

`active_orders` comes from **`registry.get_all_active_orders_for_cap_check(interval, market_id=market_id)`**, which returns orders with **`status = 'resting'`** for that interval and market. So:

- **First tick** that passes all checks → one intent → one order (e.g. 2 contracts at 99¢).
- **Later ticks** in the same window → strategy may keep emitting intents, but aggregator returns **[]** because there is already one resting order → **no further orders** until that order is filled, cancelled, or the window ends.

So **total orders per window** = **at most 1**, and that order’s size is capped by **max_cost_cents** (e.g. 2 contracts for $2). That is how “double-dipping” is prevented once max_cost (200¢) is effectively “used” by having one resting order.

---

## 5. Exit / Stop Loss Logic

### stop_loss_pct: 30 (how a 30% loss is triggered mid-window)

Contracts settle at **100 or 0** at expiry. **Mid-window**, the strategy infers **unrealized loss** from the **current “no” bid** vs our **entry price (limit_price_cents)**:

```206:212:bot/pipeline/strategies/last_90s.py
            entry_cents = order.limit_price_cents or 99
            if entry_cents <= 0:
                continue
            loss_pct = (entry_cents - no_bid) / float(entry_cents) if no_bid is not None else 0.0
            if loss_pct < sl_pct:
                continue
```

- **entry_cents:** price we paid (or are bidding), e.g. 99.  
- **no_bid:** current best bid for the “no” side (what we could sell at now).  
- **loss_pct = (entry_cents - no_bid) / entry_cents**.  
  - Example: entry 99¢, no_bid 69¢ → loss_pct = 30/99 ≈ **30.3%**.

So **stop_loss_pct: 30** means: we consider triggering stop-loss when **loss_pct >= 0.30** (i.e. current bid has dropped enough that mark-to-market loss is ≥ 30% of our entry price). So the 30% is **not** “price moved to 30¢”; it’s “(99 - no_bid) / 99 ≥ 0.30”, i.e. no_bid ≤ ~69¢.

### stop_loss_distance_factor: 0.6 (danger zone)

We only **actually exit** (emit stop_loss) if, in addition to the loss threshold, we are in the **“danger zone”** (price has moved **closer** to the strike):

```191:215:bot/pipeline/strategies/last_90s.py
        min_dist_placement = _get_asset_config(cfg.get("min_distance_at_placement"), asset, 0.0)
        try:
            min_d = float(min_dist_placement) if min_dist_placement is not None else 0.0
        except (TypeError, ValueError):
            min_d = 0.0
        factor = _get_asset_config(cfg.get("stop_loss_distance_factor"), asset, 0.8)
        try:
            factor_f = float(factor)
        except (TypeError, ValueError):
            factor_f = 0.8
        danger_threshold = min_d * factor_f if min_d else None
        ...
            if danger_threshold is not None and ctx.distance is not None:
                if ctx.distance > danger_threshold:
                    continue
            actions.append(ExitAction(order_id=order.order_id, action="stop_loss"))
```

- **danger_threshold = min_distance_at_placement × factor** (e.g. 40 × 0.6 = **24** for BTC).  
- We **do not** exit when **`ctx.distance > danger_threshold`** (we `continue`).  
- We **do** exit when **`ctx.distance <= danger_threshold`** (and loss_pct >= sl_pct).

So: **we exit only if** (1) loss ≥ 30% **and** (2) **current distance is at or below 60% of the entry min_distance** (e.g. spot has moved within 24 of strike for BTC). So yes: the bot exits when price has moved **closer** than 60% of our entry distance (i.e. we are in the “danger zone”), not when we are still far from the strike.

---

## 6. The “65-Second” Threshold

There is a single logic gate that **changes behavior at 65 seconds remaining**:

```152:161:bot/pipeline/strategies/last_90s.py
        if ctx.seconds_to_close > 65:
            if no_bid < 98:
                self._record_telemetry(ctx, 0, "low_bid")
                return None
        else:
            if no_bid < min_bid_cents:
                self._record_telemetry(ctx, 0, "low_bid")
                return None
```

- **When `seconds_to_close > 65`** (first ~20 seconds of the 85s window): require **no_bid ≥ 98**. Stricter.
- **When `seconds_to_close <= 65`**: require **no_bid ≥ min_bid_cents** (e.g. 96). Slightly looser.

So the **65-second** mark only affects the **bid floor**: before 65s we demand 98¢; from 65s down we allow 96¢. There is no other behavior change at 65 seconds (distance, window_seconds, or stop-loss logic do not depend on 65).

---

## Summary Table

| Topic | Location | Behavior |
|-------|----------|----------|
| Entry window | `last_90s.py` | Hunting only when `seconds_to_close <= window_seconds` (85). |
| Time-to-expiration | `run_unified.py` | `seconds_to_close = close_ts - now_ts` (or mins×60 fallback). |
| Distance | `data_layer.py` | Smart Average or MIN; strategy uses `ctx.distance` vs `min_distance_at_placement`. |
| Limit price | `last_90s.py` | 99¢ limit; no separate “99¢ condition”, just the limit at which we post. |
| Bid floor | `last_90s.py` | &gt;65s: no_bid≥98; ≤65s: no_bid≥min_bid_cents (96). |
| Max orders/window | `aggregator.py` + registry | At most 1 resting order per (interval, market_id); count ≤ max_cost_cents/99 (e.g. 2). |
| Stop loss % | `last_90s.py` | loss_pct = (entry_cents - no_bid)/entry_cents; exit when ≥ 30%. |
| Danger zone | `last_90s.py` | Exit only if ctx.distance ≤ min_distance_at_placement × 0.6. |
| 65s gate | `last_90s.py` | Only switches bid floor from 98¢ to min_bid_cents (96). |
