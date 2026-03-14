# Project Hermes

A cross-domain, low-latency quantitative trading bot for **Kalshi** prediction markets. It runs statistical arbitrage and event-driven strategies: limit orders in the final seconds of 15‑minute windows (**last_90s_limit_99**), and directional breakout entries with take-profit/stop-loss (**atm_breakout_strategy**). Hermes uses live spot data (Kraken, Coinbase) and Kalshi order books to place and manage positions with configurable risk (stop-loss, panic close, distance filters).

---

## Features

- **V2 unified pipeline** — Single process, config-driven; runs 15‑min (and optionally hourly) intervals in separate threads. Order registry and strategy reports in SQLite (`data/v2_state.db`).
- **last_90s_limit_99** — Places limit-at-99¢ orders in the last N seconds of each 15‑min window when distance and bid thresholds pass; per-asset `min_bid_cents`, `min_distance_at_placement`, `stop_loss_distance_factor`. Exits via stop-loss (including panic close when spot crosses strike) using min(Kraken, Coinbase) distance.
- **atm_breakout_strategy** — Momentum-based ITM entries when spot crosses strike and momentum triggers; take-profit and stop-loss executed as aggressive limit sells (IoC + reduce_only) on Kalshi.
- **Data layer** — Spot from Kraken + Coinbase; entry uses average distance (non‑BTC) or min distance (BTC); exit/stop-loss uses min distance for all assets.
- **Execution** — TP/SL and last_90s stop-loss go through a single executor path: aggressive limit sell (1¢) with `reduce_only` and `time_in_force: immediate_or_cancel`. SL execution attempts are audited to `v2_sl_execution_audit` for analysis.

---

## Requirements

- **Python 3.11+**
- **Kalshi API** — API key and RSA private key (PEM) for signed requests.
- **Kraken** (and optionally Coinbase) — Used for spot prices and distance-to-strike.

---

## Quick start

### 1. Clone and virtual environment

```bash
git clone https://github.com/aajsearch/Project-Hermes.git
cd Project-Hermes
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Environment and config

- Copy `.env.example` to `.env` and set:
  - **KALSHI_API_KEY** — Your Kalshi API key ID.
  - **KALSHI_PRIVATE_KEY** — Path to your RSA private key PEM file, or inline PEM content (e.g. for CI).
  - **KALSHI_BASE_URL** — Kalshi API base (no path). Production: `https://api.elections.kalshi.com`. Demo: use Kalshi’s demo base URL if applicable.
- V2 config lives under `config/`:
  - **v2_common.yaml** — Intervals (fifteen_min, hourly), assets, caps, no_trade_windows, `dry_run`.
  - **v2_fifteen_min.yaml** — Pipeline `run_interval_seconds`, `strategy_priority`; strategy blocks for `last_90s_limit_99` and `atm_breakout_strategy` (per-asset knobs where noted).

Do not commit `.env` or PEM files; they are in `.gitignore`.

### 4. Run the V2 bot (recommended)

```bash
# Dry run (default): no real orders, strategies and executor logic run
python -m bot.v2_main

# Live trading: set in v2_common.yaml (dry_run: false) or env
V2_DRY_RUN=false python -m bot.v2_main
```

- The bot starts the **fifteen_min** pipeline (and hourly if enabled). Each cycle: resolve markets and spot, build context, evaluate entry/exit per strategy, then execute (place/cancel/sell) when not in dry run.
- Logs go to stdout. SL/TP and execution audit rows go to `data/v2_state.db` (e.g. `v2_sl_execution_audit`).

### 5. Legacy bot (optional)

The legacy single-loop bot and dashboard are still available:

```bash
# OBSERVE mode (no orders)
python -m bot.main --config config/config.yaml

# TRADE mode (after setting mode in config or MODE=TRADE)
python -m bot.main --config config/config.yaml
```

Config is merged from `config/common.yaml`, `config/fifteen_min.yaml`, `config/hourly.yaml`, etc., with `config/config.yaml` as the main entry.

---

## Config overview (V2)

| File | Purpose |
|------|--------|
| **config/v2_common.yaml** | Intervals (fifteen_min, hourly), assets, caps, no_trade_windows, `dry_run`. |
| **config/v2_fifteen_min.yaml** | Pipeline `run_interval_seconds`, `strategy_priority`; strategy blocks for `last_90s_limit_99` and `atm_breakout_strategy`. |
| **config/v2_hourly.yaml** | Hourly pipeline and strategies (if used). |

Key strategy knobs (see YAML comments):

- **last_90s_limit_99:** `window_seconds`, `min_bid_cents` (per-asset), `min_distance_at_placement`, `stop_loss_pct`, `stop_loss_distance_factor` (per-asset), `order_count`, `max_cost_cents`, `side`.
- **atm_breakout_strategy:** `min_distance_at_placement`, `min_entry_price`, `max_entry_price`, `momentum_window_seconds`, `momentum_trigger_3s`, `take_profit_cents`, `stop_loss_cents`, `order_count`, `contracts`, `max_cost_cents`.

---

## Tools (scripts)

Run from project root. Most read from `data/v2_state.db` or `data/bot_state.db`.

| Script | Purpose |
|--------|--------|
| `tools/export_last_90s_telemetry_csv.py` | Export last_90s telemetry to CSV (e.g. `--hours 12`, `--out data/last_90s_telemetry_12h.csv`). |
| `tools/export_atm_breakout_trades_7h_csv.py` | Export ATM breakout trades (TP/SL/WindowEnd) to CSV; `--hours 7 --trades-only` or `--all --trades-only`. |
| `tools/export_atm_breakout_csv.py` | Export full `strategy_report_atm_breakout` table to CSV. |
| `tools/verify_v2_dry_run.py` | Verify V2 pipeline in dry run (no orders). |
| `tools/verify_config.py` | Check config files and strategy enablement. |
| `tools/strategy_report_generator.py` | Generate strategy reports (last_90s, hourly) from DB. |

Example:

```bash
python -m tools.export_last_90s_telemetry_csv --hours 4 --out data/last_90s_4h.csv
python -m tools.export_atm_breakout_trades_7h_csv --hours 7 --trades-only
```

---

## Repository layout

```
config/
  v2_common.yaml          # V2 intervals, caps, dry_run
  v2_fifteen_min.yaml     # 15-min pipeline + last_90s + ATM breakout
  v2_hourly.yaml          # Hourly pipeline (optional)
  common.yaml             # Legacy shared config
  fifteen_min.yaml        # Legacy 15-min
  config.yaml             # Legacy merged entry

bot/
  v2_main.py              # V2 entry: load config, start pipeline threads
  pipeline/
    run_unified.py        # Single pipeline cycle (context → intents → exits → executor)
    data_layer.py         # Spot (Kraken/Coinbase), strike, distance (min/avg)
    executor.py           # Place/cancel orders; TP/SL market-style sells; SL audit
    registry.py           # Order registry + strategy reports (v2_state.db)
    strategies/
      last_90s.py         # last_90s_limit_99
      atm_breakout.py     # atm_breakout_strategy
  v2_config_loader.py    # Load and validate V2 YAML
  main.py                 # Legacy bot entry

src/
  client/
    kalshi_client.py      # Kalshi REST client (orders, positions, place_market_order, etc.)
    kraken_client.py      # Kraken spot prices

data/                     # Created at runtime; gitignored
  v2_state.db             # V2 registry, telemetry, SL audit
  bot_state.db            # Legacy state
  *.csv                   # Exports (e.g. telemetry, ATM trades)

tools/                    # Scripts for telemetry export, ATM trade export, reports
docs/                     # Audit and verification notes
```

---

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `KALSHI_API_KEY` | Yes (for live) | Kalshi API key ID. |
| `KALSHI_PRIVATE_KEY` | Yes (for live) | Path to PEM file or inline PEM content for request signing. |
| `KALSHI_BASE_URL` | Yes (for live) | Kalshi API base URL with no path (e.g. `https://api.elections.kalshi.com` for production). |
| `V2_DRY_RUN` | No | Override config: `true` or `false` to force dry run or live. |
| `SMTP_*` | No | Optional; used for legacy hourly report email. |

---

## Safety and compliance

- **Dry run by default** — V2 starts with `dry_run: true` unless overridden in config or `V2_DRY_RUN=false`.
- Prediction markets involve real financial risk. Understand Kalshi’s rules and your local regulations before trading.
- This software is provided “as is” without warranty. Use at your own risk.

---

## License

Apache-2.0. See [LICENSE](LICENSE) in the repository.
