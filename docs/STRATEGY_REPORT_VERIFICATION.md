# Strategy Report — Verification Summary

## What the report shows

The hourly strategy report (email and TSVs) is built from the `strategy_report` table for **last_90s_limit_99** and **hourly_last_90s_limit_99**. Each row is one **candidate event** (one order placed or one skip decision).

---

## Metrics (verified)

| Metric | Definition | Unique? |
|--------|------------|--------|
| **Total Signals** | Number of rows (candidate events) in the time window. Each row = (asset, ticker, side, window). | **No** — same (asset, window) can have multiple rows (different tickers, yes/no, or multiple orders). |
| **unique asset×15min** | (last_90s_limit_99 only) Distinct (asset, 15‑minute bucket of `ts_utc`). | **Yes** — sanity check: 4 assets × 4 windows/hr × 10h ⇒ max **160**. |
| **unique asset×hour** | (hourly_last_90s_limit_99 only) Distinct (asset, hour bucket of `ts_utc`). | **Yes** — sanity check: 4 assets × 10h ⇒ max **40**. |
| **Skipped** | Rows with `skip_reason` set (signal was not placed). | Count of skipped events. |
| **Placed** | Rows with `placed = 1` (order was placed). | Count of placed events. |
| **Filled** | Rows with `filled = 1` (order filled). | Always ≤ Placed. |
| **Win / Loss / Stop loss** | Rows with `final_outcome` = WIN, LOSS, or STOP_LOSS. | Sum ≤ Filled. |

Invariants (enforced by tests in `tests/test_strategy_report_summary.py`):

- **Filled ≤ Placed**
- **Wins + Losses + Stop losses ≤ Filled**
- **Skipped + Placed = Total Signals** (each row is either skipped or placed)

---

## STOP_LOSS and resolution (yes/no)

Kalshi only records buy/sell; it does not label “stop loss”. The bot sets **final_outcome = STOP_LOSS** when it sells to close at a loss (last_90s or hourly_last_90s). The hourly report **refresh-from-Kalshi** step **preserves** STOP_LOSS (it no longer overwrites it with WIN/LOSS) and still fetches **resolution_price** (100 = market resolved YES, 0 = NO). So for each STOP_LOSS row you get:

- **final_outcome**: STOP_LOSS  
- **resolution_price**: 100 or 0 (how the market actually resolved)  
- **pnl_cents**: actual PnL from the stop-loss sell  

Use this to tell: “If we hadn’t stopped out, would we have lost everything?” (resolution against our bet) vs “Was it temporary volatility?” (resolution would have been in our favor) so you can tune **stop_loss_pct**.

---

## Why Total Signals can be 445 while unique asset×15min is ~124

- **Total Signals (445)** = every row in the window (each order or each skip counts as one).
- **unique asset×15min (124)** = distinct (asset, 15‑minute window). For 4 assets and ~7.75 hours of 15min windows, 4 × 31 ≈ 124.
- So 445 events across 124 (asset, 15min) slots ⇒ ~3.6 events per slot on average (multiple tickers, yes/no, or multiple orders per window).

For **last_90s_limit_99**: in 10 hours with 4 assets you’d expect at most **4 × 4 × 10 = 160** unique (asset, 15min). The report’s **unique asset×15min** should be in that ballpark.

For **hourly_last_90s_limit_99**: windows are **hourly** (not 15‑min). In 10 hours with 4 assets you’d expect at most **4 × 10 = 40** unique (asset, hour). The report’s **unique asset×hour** should be in that ballpark (e.g. 16 in the sample = 4 assets × 4 hours of data).

---

## Data source

- **Live:** Main bot writes to `strategy_report` when it runs with `last_90s_limit_99` and `hourly_last_90s_limit_99` (one row per candidate via `upsert_candidate` / `record_skip` / `record_place`).
- **Backfill:**  
  `python tools/backfill_strategy_report_from_placements.py`  
  imports from `reports/last_90s_placements.tsv`, `last_90s_skips.tsv`, `hourly_last_90s_placements.tsv`, `hourly_last_90s_skips.tsv`, and sets `filled=1` when placement TSV has `status=executed` or `executed=1`.

---

## How to run

- **One-off report (no email):**  
  `python tools/strategy_report_hourly_email.py --since-hours 1`
- **One-off report + email:**  
  `python tools/strategy_report_hourly_email.py --since-hours 1 --send-email`
- **Hourly daemon (report + email every hour):**  
  `python -m bot.main --config config/config.yaml --strategy-report-hourly`  
  By default the daemon refreshes fill/resolution from Kalshi before each report. To skip Kalshi: add `--no-refresh-from-kalshi`.

---

## Hourly filled report (recommended setup)

To get an **email every hour** with **filled** and resolved data (Win/Loss/PnL) from Kalshi:

**1. Prerequisites**

- In project root, a `.env` file with:
  - **Email:** `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `SMTP_FROM`, `SMTP_TO`
  - **Kalshi (for fill/resolution):** `KALSHI_API_KEY`, `KALSHI_PRIVATE_KEY`, `KALSHI_BASE_URL`  
  (See `.env.example`.)

**2. Run two processes** (same config → same DB)

- **Terminal 1 – Trading bot**  
  ```bash
  cd /path/to/Hades-prediction-market
  .venv/bin/python -m bot.main --config config/config.yaml
  ```

- **Terminal 2 – Hourly report daemon** (fetches latest fill/resolution from Kalshi, then sends email)  
  ```bash
  cd /path/to/Hades-prediction-market
  .venv/bin/python -m bot.main --config config/config.yaml --strategy-report-hourly
  ```

**3. Optional – Single terminal (report in background)**

  ```bash
  cd /path/to/Hades-prediction-market
  .venv/bin/python -m bot.main --config config/config.yaml --strategy-report-hourly &
  .venv/bin/python -m bot.main --config config/config.yaml
  ```

**4. Optional – Survive disconnect (e.g. tmux)**

  ```bash
  tmux new -s bot
  .venv/bin/python -m bot.main --config config/config.yaml --strategy-report-hourly &
  .venv/bin/python -m bot.main --config config/config.yaml
  # Detach: Ctrl+B, then D
  ```

To send the report **without** calling Kalshi (e.g. no API keys): use `--strategy-report-hourly --no-refresh-from-kalshi`. Filled/Win/Loss/PnL may then be missing or stale.

---

## Tests

**Summary invariants:**
```bash
python3 -m unittest tests.test_strategy_report_summary -v
```
Checks: `filled ≤ placed`, `wins+losses+stop_losses ≤ filled`, `skipped + placed = total signals`, and synthetic consistency.

**Report columns and Kalshi refresh:**
```bash
python -m pytest tests/test_strategy_report_hourly_columns.py -v
```
Checks: DB columns include all report TSV columns; `get_all_rows` returns every report key; `run()` produces TSVs with the full column set; refresh path has required DB fields (`order_id`, `market`, `ticker`) and Kalshi env is documented (see `.env.example`: `KALSHI_API_KEY`, `KALSHI_PRIVATE_KEY`, `KALSHI_BASE_URL`).

---

## Sample output (conceptually)

```
Strategy Report — Last 1 hour
Window: 2026-02-22T04:13 → 2026-03-01T04:13 UTC
Total Signals = candidate events (one per order or skip); same asset/window can have multiple (ticker/side).

--- last_90s_limit_99 ---
  Total Signals: 445  (unique asset×15min: 124)
  Skipped: 153  Placed: 292  Filled: 84
  ...

--- hourly_last_90s_limit_99 ---
  Total Signals: 513  (unique asset×hour: 16)
  Skipped: 34  Placed: 479  Filled: 14
  ...
```

- **last_90s_limit_99:** use **unique asset×15min** to sanity-check (4 × 4 × N hours).
- **hourly_last_90s_limit_99:** use **unique asset×hour** to sanity-check (4 × N hours).
