# Guardrails Verification: last_90s vs ATM

Verification of temporal isolation, momentum logic, trailing stop, and aggregator behavior.

---

## Step 1: Structural Guardrails

### 1. Temporal Isolation (Time Barrier)

**last_90s_limit_99**
- Hunts only when `seconds_to_close <= window_seconds`. Config: `window_seconds: 75` (all assets).
- Code: `bot/pipeline/strategies/last_90s.py`

```136:137:bot/pipeline/strategies/last_90s.py
        if ctx.seconds_to_close is None or ctx.seconds_to_close > window_seconds:
            return None
```

So last_90s is active only when **seconds_to_close ≤ 75**.

**atm_breakout_strategy**
- Config: `min_seconds_to_close: 120`, `max_seconds_to_close: 840`.
- Entry is gated so ATM does **not** hunt in the last 2 minutes:

```50:56:bot/pipeline/strategies/atm_breakout.py
        secs = ctx.seconds_to_close
        if secs is None:
            return None
        min_sec = float(cfg.get("min_seconds_to_close", 120))
        max_sec = float(cfg.get("max_seconds_to_close", 840))
        if secs < min_sec or secs > max_sec:
            return None
```

So ATM returns `None` when `seconds_to_close < 120` or `> 840`. **Result:** In the final 2 minutes (secs < 120) only last_90s can fire; ATM does not compete for capital.

**Gap:** Between 75s and 120s neither strategy enters (last_90s requires secs ≤ 75; ATM requires secs ≥ 120).

---

### 2. Momentum Calculation (ATM Entry Trigger)

**Config:** `momentum_trigger_3s: { btc: 30.0, eth: 2, sol: 0.1, xrp: 0.002 }`

**Snippet:** `bot/pipeline/strategies/atm_breakout.py`

```64:91:bot/pipeline/strategies/atm_breakout.py
        wid = _window_id(ctx)
        now = time.time()
        momentum_trigger = _get_asset_config(cfg.get("momentum_trigger_3s"), ctx.asset, 1.0)
        try:
            trigger_val = float(momentum_trigger)
        except (TypeError, ValueError):
            trigger_val = 1.0

        prev = self._history.get(wid)
        self._history[wid] = {"ts": now, "yes_bid": yes_bid, "no_bid": no_bid}

        if prev is None:
            return None
        elapsed = now - prev.get("ts", 0)
        if elapsed <= 0:
            return None
        scale = min(1.0, elapsed / 3.0)
        threshold = trigger_val * scale
        delta_yes = yes_bid - (prev.get("yes_bid") or 0)
        delta_no = no_bid - (prev.get("no_bid") or 0)
        if delta_yes >= threshold and delta_yes >= delta_no:
            side = "yes"
            price_cents = yes_bid or 50
        elif delta_no >= threshold and delta_no >= delta_yes:
            side = "no"
            price_cents = no_bid or 50
        else:
            return None
```

**Behavior:**
- Uses **previous tick** for this `window_id` from `_history` (no separate 3s buffer).
- `elapsed` = time since that tick (typically ~1s with `run_interval_seconds: 1`).
- `scale = min(1.0, elapsed / 3.0)` so over 3s the full `trigger_val` is required.
- `delta_yes` / `delta_no` are **quote changes in cents** (yes_bid, no_bid are cents).
- Trigger: `delta_yes >= threshold` or `delta_no >= threshold` (and the larger delta wins).

**Units:** Config values (e.g. BTC 30.0) are compared to **cent** deltas. So 30.0 means a 30‑cent move in the quote over the scaled window, not $30 unless the config were 3000. For a $30 move you’d need a trigger of 3000 (cents).

---

### 3. Active Management / Trailing Stop (ATM Exit)

**Config:** `trailing_stop_cents: 10`, `take_profit_cents: 75`

**Snippet:** `bot/pipeline/strategies/atm_breakout.py`

```105:145:bot/pipeline/strategies/atm_breakout.py
    def evaluate_exit(self, ctx: WindowContext, my_orders: List[OrderRecord]) -> List[ExitAction]:
        ...
        for order in my_orders:
            if order.status != "filled":
                continue
            side = (order.side or "yes").lower()
            current_bid = (ctx.quote.get("yes_bid") or 0) if side == "yes" else (ctx.quote.get("no_bid") or 0)
            prev_high = self._highest_bids.get(order.order_id, 0)
            self._highest_bids[order.order_id] = max(current_bid, prev_high)
            high = self._highest_bids[order.order_id]

            if ctx.seconds_to_close is not None and ctx.seconds_to_close < min_sec:
                actions.append(ExitAction(order_id=order.order_id, action="market_sell"))
                ...
                continue

            dynamic_floor = max(take_profit_cents, high - trailing_stop_cents)
            if current_bid <= dynamic_floor and high >= take_profit_cents:
                actions.append(ExitAction(order_id=order.order_id, action="take_profit"))
                ...
```

**Verification:**
- There is no separate “OrderExecutor” or “PositionManager” polling; the **pipeline** runs every cycle (`run_interval_seconds: 1`).
- Each cycle calls `evaluate_exit(ctx, my_orders)` with the **current** quote in `ctx`.
- `_highest_bids[order_id]` is updated every cycle: `max(current_bid, prev_high)` → trailing high.
- `dynamic_floor = max(take_profit_cents, high - trailing_stop_cents)` → stop moves up as `high` increases (trailing by 10¢).
- So the trailing stop is **updated every pipeline tick** (every second) via the normal cycle; no extra polling.

---

## Step 2: No Conflicts

### 4. Priority Queue

**Config:** `config/v2_fifteen_min.yaml`

```yaml
  pipeline:
    strategy_priority: [last_90s_limit_99, atm_breakout_strategy]
```

**run_unified.py:** Strategies are evaluated in **list order**; intents are then aggregated:

```231:242:bot/pipeline/run_unified.py
        for strat in strategies:
            ...
            intent = strat.evaluate_entry(ctx)
            if intent is not None:
                asset_intents.append((intent, strat.strategy_id))
        ...
        final_intents = aggregator.resolve_intents(
            asset_intents, interval_slice, active_orders, current_cost_by_strategy
        )
```

**aggregator:** Sorts by `strategy_priority` and keeps **only the highest-priority intent**:

```49:56:bot/pipeline/aggregator.py
        sorted_intents = sorted(intents_with_ids, key=sort_key)
        intent, strategy_id = sorted_intents[0]
        ...
        return [sorted_intents[0]]
```

So when both strategies emit in the same cycle, **last_90s_limit_99** wins. In practice they don’t both emit: when `seconds_to_close ≤ 75` only last_90s can enter; when `120 ≤ seconds_to_close ≤ 840` only ATM can enter (and ATM is currently disabled). So priority is correct and last_90s is not starved by ATM near the 75s window.

---

### 5. Budget Separation

**Current behavior:**
- `current_cost_by_strategy` is keyed by **strategy_id**: `{ "last_90s_limit_99": cents, "atm_breakout_strategy": cents }`.
- Each strategy’s cost is summed from **that strategy’s** orders only (same interval, market_id, asset).
- The aggregator allows **one intent per cycle** and checks only **that** strategy’s cap:  
  `current_cost[strategy_id] + new_cost <= max_cost_cents[strategy_id]`.

So:
- **last_90s:** `max_cost_cents: 6000` (config) → cap per (window, asset) for last_90s only.
- **ATM:** no `max_cost_cents` in config → aggregator default 600; cap is per-strategy.

There is **no** single “total_spent_this_window across both strategies” or account-level limit. Each strategy has its own cap; they do not share one pool. So:
- Risk is bounded **per strategy** (e.g. last_90s up to 6000¢, ATM up to 600¢ per window/asset).
- If you want a **global** account risk limit across strategies, that would require an additional check (e.g. sum of all strategies’ current_cost vs a global cap).

---

## Summary

| Check | Status |
|-------|--------|
| last_90s only when secs ≤ window_seconds (75) | Yes |
| ATM only when 120 ≤ secs ≤ 840 (hard secs < 120 → None) | Yes |
| Momentum uses previous tick + scale elapsed/3; trigger in cents | Documented (config in cents, not $) |
| Trailing stop updated every cycle via evaluate_exit | Yes |
| last_90s first in strategy_priority | Yes |
| Budget per strategy (no shared total) | Yes; global cap not implemented |
