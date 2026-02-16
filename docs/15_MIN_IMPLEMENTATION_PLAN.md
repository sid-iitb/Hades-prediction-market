# 15-Minute Crypto Trading – Implementation Plan

## 1. Kalshi 15-Min Market Structure

| Asset | Series Ticker | Event Ticker Format    | Example                |
|-------|---------------|------------------------|------------------------|
| BTC   | KXBTC15M      | KXBTC15M-YYmmmDDHHMM  | KXBTC15M-26FEB141800   |
| ETH   | KXETH15M      | KXETH15M-YYmmmDDHHMM  | KXETH15M-26FEB142330   |

- **Slug format**: `YY` + `mmm` (jan,feb,...) + `DD` + `HH` + `MM` (minutes: 00, 15, 30, 45)
- **Resolution**: 15-min window (e.g. 18:00 = 17:45–18:00 ET)
- **Markets per event**: One binary market (“BTC/ETH price up in next 15 mins?”)
- **Market ticker**: `{event_ticker}-00` (e.g. KXBTC15M-26FEB141800-00)
- **Prices**: yes_ask, no_ask in cents (1–99)
- **Price source**: CF Benchmarks BRTI (not Kraken)

---

## 2. Market ID Discovery (15-Min)

```
Current time (ET) → round down to previous 15-min boundary
→ Next 15-min slot = current slot (we trade the *current* open market)
```

Example: 14:37 ET → slot 14:30 → event `KXBTC15M-26FEB141430`  
Slots: :00, :15, :30, :45

```
get_current_15min_market_id(asset) 
  → now_et, round to 15-min (floor)
  → slug = kxbtc15m-26feb141430 (or kxeth15m-...)
  → return slug.upper()
```

**Minutes to close**: Time until the 15-min window ends (0–15).

---

## 3. Run Mode Configuration

```yaml
run_mode: both          # hourly | fifteen_min | both
assets: [btc, eth]

# Per-interval enable/disable (when run_mode=both)
intervals:
  hourly:
    enabled: true
    assets: [btc, eth]
  fifteen_min:
    enabled: true
    assets: [btc, eth]
```

- `run_mode=hourly`: Only hourly (current behavior)
- `run_mode=fifteen_min`: Only 15-min
- `run_mode=both`: Both, with per-interval toggles

---

## 4. 15-Min Strategy: “2-Min Window Lopsided”

**Idea**: In the last 2 minutes, if one side is very cheap (1–11¢) and the other is expensive (90–99¢), buy the cheap side.

```yaml
fifteen_min:
  strategy: lopsided_2min
  late_window_minutes: 2        # Only trade when minutes_to_close <= 2
  cheap_side_min_cents: 1       # Min price for "cheap" side
  cheap_side_max_cents: 11      # Max price for "cheap" side
  expensive_side_min_cents: 90  # Min price for "expensive" side (opposite)
  expensive_side_max_cents: 99  # Max price for "expensive" side
```

**Logic**:
- If `minutes_to_close > 2` → no trade
- If `yes_ask` in [1,11] and `no_ask` in [90,99] → BUY YES
- If `no_ask` in [1,11] and `yes_ask` in [90,99] → BUY NO
- Else → no trade

---

## 5. Caps: Per Ticker, Per Interval

```yaml
caps:
  max_orders_per_ticker: 2
  max_total_orders_per_hour: 30
  cap_scope: per_side

  # 15-min specific (optional overrides)
  fifteen_min:
    max_orders_per_ticker: 1
    max_total_orders_per_15min: 10
```

- `hour_market_id` for hourly: `KXBTCD-26FEB1418`
- `hour_market_id` for 15-min: `KXBTC15M-26FEB141430` (treat as “market_id” for caps)
- Caps are per `market_id` (hourly or 15-min event)

---

## 6. Implementation Tasks

### 6.1 Slug / Market ID

- [x] `generate_15min_slug(target_time, asset)` in `generate_all_kalshi_urls.py`
  - Prefixes: btc → kxbtc15m, eth → kxeth15m
  - Format: `YYmmmDDHHMM` with MM in {00,15,30,45}
- [x] `get_current_15min_market_id(asset)` in `bot/market.py`
- [x] `get_minutes_to_close_15min(market_id)` (0–15)

### 6.2 Market Fetch

- [x] `fetch_15min_market(event_ticker)` – one market per event
- [x] `fetch_15min_quote()` – returns TickerQuote with orderbook enrichment

### 6.3 15-Min Strategy

- [x] `bot/strategy_15min.py` – `generate_signals_15min(quote, minutes_to_close, config)`
  - Only when `minutes_to_close <= late_window_minutes`
  - Lopsided logic: cheap side [1,11], expensive [90,99]

### 6.4 Main Loop

- [x] In `run_bot`:
  - If `run_mode` in (hourly, both) → run hourly cycle per asset
  - If `run_mode` in (fifteen_min, both) → run 15-min cycle per asset
- [x] 15-min loop:
  - `get_current_15min_market_id(asset)`
  - `get_minutes_to_close_15min`
  - Fetch market(s) for event
  - Run strategy
  - Execute (reuse `execute_signals` with `interval="15min"`)

### 6.5 Config

- [x] Add `run_mode`, `intervals`, `fifteen_min` (strategy + caps)
- [ ] Add `spot_window` for 15-min if needed (15-min uses single market, no strike filter)

### 6.6 State / Summary

- [x] State uses same `hour_market_id` key (15-min event ticker as market_id for caps)
- [ ] Hour summary – keep for hourly only
- [x] `log_run_15min`, `print_console_summary_15min` for 15-min runs

---

## 7. Schedule for 15-Min

- 15-min loop: run every 1–2 min (or every 15 min at slot start)
- Suggested: every 1 min so we catch the 2-min window reliably
- `interval_minutes: 1` when `run_mode=fifteen_min` or `both`

---

## 8. File Changes

| File | Changes |
|------|---------|
| `src/offline_processing/generate_all_kalshi_urls.py` | Add 15-min prefixes, `generate_15min_slug` |
| `bot/market.py` | `get_current_15min_market_id`, `get_minutes_to_close_15min`, `fetch_15min_market` |
| `bot/strategy_15min.py` | New: `generate_signals_15min` |
| `bot/main.py` | Add 15-min run path, `run_mode` / interval branching |
| `config.yaml` | `run_mode`, `intervals`, `fifteen_min` |
| `bot/state.py` | Optional: per-interval last-run tracking |
