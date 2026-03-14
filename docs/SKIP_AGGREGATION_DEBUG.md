# Skip Aggregation for placed=0: Store, Fetch, Update & Fixes

This doc describes how skip history is stored, fetched, and used when we write a **placed=0** row (no order placed in that window), and what fixes have been tried so you can debug "Still No skip history recorded".

---

## 1. Key identities

- **In-memory store**: `_last_90s_skip_aggregator` (dict, key = `(market_id, asset.lower())`).
- **DB table**: `last_90s_skip_aggregator` in `data/bot_state.db`, columns: `(market_id, asset, payload_json)`.
- **market_id** when **recording skips** = **current** 15-min market (e.g. `get_current_15min_market_id(asset)`).
- **market_id** when **resolving** = **previous** 15-min market (e.g. `get_previous_15min_market_id(asset)`).

For the same logical window (e.g. “15:30 close”), the key must match: during the 90s we use **current** = 15:30; at resolution we use **previous** = 15:30. So the key is the same only if the process that ran during the 90s is the same one that runs resolution, or the DB was persisted and read back.

---

## 2. STORE: When and how we record skips

Every time the bot **skips** placing an order in the last-90s window (waiting_for_dip, distance_buffer, min_bid, order_already_placed, etc.), it calls **`_skip_aggregate_record`** with the **current** `market_id` and `asset`.

**Location**: `bot/last_90s_strategy.py`

```python
# Key: (market_id, asset.lower()) — market_id here is CURRENT window (e.g. KXETH15M-26MAR031530)
def _skip_aggregate_record(
    market_id: str,
    asset: str,
    bid: int,
    distance: Optional[float],
    skip_reason: str,
    *,
    ticker=..., side=..., spot=..., strike=..., seconds_to_close=..., min_distance_threshold=...,
    db_path: Optional[str] = None,
) -> None:
    key = (market_id, asset.lower())
    with _last_90s_skip_aggregator_lock:
        # 1) If key missing: optionally hydrate from DB (fix: merge after restart)
        if key not in _last_90s_skip_aggregator:
            seed = None
            if db_path:
                seed = get_last_90s_skip_aggregator(db_path, market_id, asset)  # load from DB
            if seed:
                # copy seed into in-memory rec
                _last_90s_skip_aggregator[key] = { "check_count": ..., "bid_history": ..., ... }
            else:
                _last_90s_skip_aggregator[key] = { "check_count": 0, "bid_history": [], ... }
        rec = _last_90s_skip_aggregator[key]
        # 2) Append this skip
        rec["check_count"] += 1
        rec["bid_history"].append(bid)
        rec["distance_history"].append(distance or 0.0)
        rec["skip_reasons"][skip_reason] = rec["skip_reasons"].get(skip_reason, 0) + 1
        # ... update last_* fields ...
        # 3) Persist to DB (so resolution can read after restart)
        if db_path:
            persist_last_90s_skip_aggregator(db_path, market_id, asset, _copy_aggregator_rec(rec))
```

**Persistence** (`bot/state.py`):

```python
def persist_last_90s_skip_aggregator(db_path: str, market_id: str, asset: str, rec: Dict) -> None:
    ensure_state_db(db_path)  # fix: ensure table exists
    conn = sqlite3.connect(db_path)
    cur.execute(
        "INSERT OR REPLACE INTO last_90s_skip_aggregator (market_id, asset, payload_json) VALUES (?, ?, ?)",
        (market_id, str(asset).lower(), json.dumps(rec)),
    )
    conn.commit()
```

So: **store** = in-memory update by `(market_id, asset)` + **INSERT OR REPLACE** into `last_90s_skip_aggregator`. The **market_id** used here is the **current** 15-min market (the window we’re in when we skip).

---

## 3. FETCH: When we need the data at resolution (placed=0)

When the window has **closed** and we find **no order was placed** for that window, we write a single **placed=0** row and set **skip_details** from the aggregator.

**Location**: `bot/last_90s_strategy.py` inside **`_resolve_and_log_outcomes`**.

**Order of operations each run** (important for debugging):

1. **Resolve first**: `_resolve_and_log_outcomes(...)` runs for the **previous** 15-min market.
2. **Then** run last-90s for the **current** market: `_run_last_90s_for_asset(...)` per asset.

So when we resolve, we need the aggregator for the **previous** market (the one that just closed). That data was recorded in **earlier** run cycles when that market was still “current”.

**Resolution block (placed=0 branch)**:

```python
for asset in assets:
    prev_market_id = get_previous_15min_market_id(asset=asset)   # e.g. KXETH15M-26MAR031530
    placed_prev = get_last_90s_placed(db_path, prev_market_id, asset)
    if not placed_prev:
        # --- FETCH: get aggregator for (prev_market_id, asset) ---
        rec = _skip_aggregate_get_without_clear(prev_market_id, asset, db_path)
        if rec is not None:
            rec = _skip_aggregate_normalize_rec(rec) or rec
        details = _skip_aggregate_format_details(rec, prefix="Aggregated skips") if rec else "Still No skip history recorded and need to fix this bug"
        # ... last_bid, last_distance, ticker_out, spot_out, ... from rec ...
        with _db_section(db_lock):
            try:
                write_row_last_90s(db_path, prev_market_id, asset, placed=0, skip_reason="aggregated_skips", skip_details=details, ...)
                _skip_aggregate_clear_only(prev_market_id, asset, db_path)   # clear ONLY after write succeeds
            except Exception as e:
                logger.warning("[last_90s] [%s] Write aggregated skip row failed: %s", asset.upper(), e)
        continue
```

**Fetch implementation**:

```python
def _skip_aggregate_get_without_clear(market_id: str, asset: str, db_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    key = (market_id, asset.lower())
    with _last_90s_skip_aggregator_lock:
        rec = _last_90s_skip_aggregator.get(key)
    if rec is not None:
        return _copy_aggregator_rec(rec)
    if db_path:
        return get_last_90s_skip_aggregator(db_path, market_id, asset)   # load from DB
    return None
```

**DB read** (`bot/state.py`):

```python
def get_last_90s_skip_aggregator(db_path: str, market_id: str, asset: str) -> Optional[Dict]:
    cur.execute(
        "SELECT payload_json FROM last_90s_skip_aggregator WHERE market_id = ? AND asset = ?",
        (market_id, str(asset).lower()),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return None
    return json.loads(row[0])
```

So: **fetch** = read from **in-memory** by `(prev_market_id, asset)`; if missing, read from **`last_90s_skip_aggregator`** by the same key. **prev_market_id** must be the same string as the **market_id** that was used when we called **`_skip_aggregate_record`** during the 90s (e.g. both `KXETH15M-26MAR031530` for the 15:30 window).

---

## 4. UPDATE / CLEAR (no separate “update” of payload)

We do **not** update a row in place. We:

- **Append** to the in-memory record on each skip and then **INSERT OR REPLACE** the full record (overwrites one row per `(market_id, asset)`).
- **Clear** only **after** we successfully write the strategy_report_last_90s row:
  - **Resolution (placed=0)**: after `write_row_last_90s(..., placed=0, ...)` succeeds → `_skip_aggregate_clear_only(prev_market_id, asset, db_path)`.
  - **Placement**: after we place an order → `_skip_aggregate_clear_only(market_id, asset, db_path)`.

**Clear** removes the key from the in-memory dict and deletes the row from `last_90s_skip_aggregator`:

```python
def _skip_aggregate_clear_only(market_id: str, asset: str, db_path: Optional[str] = None) -> None:
    key = (market_id, asset.lower())
    with _last_90s_skip_aggregator_lock:
        _last_90s_skip_aggregator.pop(key, None)
    if db_path:
        clear_last_90s_skip_aggregator(db_path, market_id, asset)   # DELETE FROM last_90s_skip_aggregator WHERE ...
```

---

## 5. Where we actually call _skip_aggregate_record (all skip paths)

Every skip path in **`_run_last_90s_for_asset`** must call **`_skip_aggregate_record`** with the **same** `market_id` (and `db_path`) so that resolution can find the data. The **market_id** there is **current** = `get_current_15min_market_id(asset)`.

Examples (all use `market_id` and `db_path` from the same function):

- **waiting_for_dip**: `_skip_aggregate_record(market_id, asset, current_bid, distance, "waiting_for_dip", ..., db_path=db_path)`
- **order_already_placed**: `_skip_aggregate_record(market_id, asset, ..., "order_already_placed", ..., db_path=db_path)`
- **min_bid**: `_skip_aggregate_record(market_id, asset, ..., "min_bid", ..., db_path=db_path)`
- **distance_buffer**: `_skip_aggregate_record(market_id, asset, ..., "distance_buffer", ..., db_path=db_path)`
- **invalid_ask**, **market_already_placed**, **limit_already_placed**: same pattern.

So: **store** always uses **current** `market_id`; **fetch** at resolution uses **prev_market_id**. For the same 15-min window, **current** (during the 90s) and **previous** (at resolution) must be the same ID (e.g. 15:30 close → same slug).

---

## 6. Fixes tried so far

### Fix 1: Don’t clear before write (get_without_clear → write → clear_only)

- **Problem**: Clearing the aggregator before writing the placed=0 row could lose the data we need for **skip_details**.
- **Change**: At resolution we no longer call “get and clear” before write. We:
  1. **Get** with `_skip_aggregate_get_without_clear(prev_market_id, asset, db_path)` (read-only).
  2. **Write** `write_row_last_90s(..., skip_details=details, ...)`.
  3. **Then** `_skip_aggregate_clear_only(prev_market_id, asset, db_path)`.
- So clear happens **only after** a successful write.

### Fix 2: Hydrate from DB when in-memory is empty (merge after restart)

- **Problem**: After a process restart, in-memory is empty. If the bot had run during the 90s in a **previous** process and persisted, the **new** process would never see that data when resolving.
- **Change**: In **`_skip_aggregate_record`**, when the key is **not** in `_last_90s_skip_aggregator`, we try to **load** from DB first and seed the in-memory record:
  - `seed = get_last_90s_skip_aggregator(db_path, market_id, asset)` if `db_path` is set.
  - If `seed` exists, we copy it into `_last_90s_skip_aggregator[key]`, then append the current skip and persist again.
- So a new process that runs in the same window can “continue” the same aggregator row after a restart.

### Fix 3: Ensure table exists before persist

- **Problem**: If **`last_90s_skip_aggregator`** was never created (e.g. different code path or DB), **persist** could fail and we’d never save.
- **Change**: At the start of **`persist_last_90s_skip_aggregator`** we call **`ensure_state_db(db_path)`** so the table (and any other state tables) exist before **INSERT OR REPLACE**.

### Fallback message

- When **rec** is None at resolution (no in-memory and no DB row), we set:
  - **skip_details** = `"Still No skip history recorded and need to fix this bug"` so you can distinguish “aggregator empty” from older rows.

---

## 7. How to debug “Still No skip history” at resolution

1. **Confirm the key at resolution**
   - Log **prev_market_id** and **asset** in the placed=0 branch (e.g. `logger.info("Resolving placed=0 prev_market_id=%s asset=%s", prev_market_id, asset)`).
   - Check that **get_previous_15min_market_id(asset)** returns the same format as **get_current_15min_market_id(asset)** for the same close time (e.g. both `KXETH15M-26MAR031530` for 15:30).

2. **Confirm the key when recording**
   - In **`_skip_aggregate_record`**, log **market_id** and **asset** (and optionally that you’re persisting). Ensure **market_id** comes from **get_current_15min_market_id(asset)** in the same run where we’re in the 90s for that window.

3. **Check DB contents**
   - Right before resolution, query:  
     `SELECT market_id, asset, length(payload_json) FROM last_90s_skip_aggregator;`  
   - See if there is a row for **(prev_market_id, asset)**. If there is but **get_last_90s_skip_aggregator** returns None, check **asset** normalization (e.g. `.lower()` in both persist and get).

4. **Single process vs multiple**
   - If the **same process** runs both the 90s loop and resolution (e.g. **run_last_90s_once** → resolve then ** _run_last_90s_for_asset**), in-memory should have the key from the **previous** run cycles. If the bot is **multi-process** or **restarted** between the 90s and resolution, then we **must** have a row in **last_90s_skip_aggregator** and **get_last_90s_skip_aggregator** must return it.

5. **Timing**
   - If the bot **never runs** during the last 90 seconds of a window (e.g. run_interval too large or process down), there will be **no** record for that window, so **rec** will be None and you’ll see the fallback. Check that at least one run happens while **seconds_to_close** is in (0, window_seconds] for that market.

6. **Optional debug logging**
   - In **`_skip_aggregate_get_without_clear`**: log when you hit in-memory vs DB and the key.
   - In **`_resolve_and_log_outcomes`** (placed=0 branch): log **rec is None** vs **rec is not None** and **prev_market_id, asset**.

---

## 8. Code locations quick reference

| What | File | Symbol / place |
|------|------|------------------|
| In-memory aggregator | last_90s_strategy.py | `_last_90s_skip_aggregator`, key `(market_id, asset.lower())` |
| Record a skip | last_90s_strategy.py | `_skip_aggregate_record(...)` |
| Hydrate from DB on first record | last_90s_strategy.py | `_skip_aggregate_record` (block `if key not in _last_90s_skip_aggregator`) |
| Persist to DB | state.py | `persist_last_90s_skip_aggregator`; table `last_90s_skip_aggregator` |
| Ensure table | state.py | `ensure_state_db(db_path)` called inside `persist_last_90s_skip_aggregator` |
| Fetch at resolution | last_90s_strategy.py | `_skip_aggregate_get_without_clear(prev_market_id, asset, db_path)` in `_resolve_and_log_outcomes` |
| Read from DB | state.py | `get_last_90s_skip_aggregator(db_path, market_id, asset)` |
| Format skip_details | last_90s_strategy.py | `_skip_aggregate_format_details(rec, prefix="Aggregated skips")` |
| Fallback string | last_90s_strategy.py | `if rec else "Still No skip history recorded and need to fix this bug"` |
| Clear after write | last_90s_strategy.py | `_skip_aggregate_clear_only(prev_market_id, asset, db_path)` after `write_row_last_90s` |
| Current vs previous market | market.py | `get_current_15min_market_id(asset)`, `get_previous_15min_market_id(asset)` |

Using this, you can add logs and DB checks to see why **rec** is None for a given **(prev_market_id, asset)** at resolution.
