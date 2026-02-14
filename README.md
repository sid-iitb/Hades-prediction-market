# Hades Prediction Market

A research-grade trading workstation for Kalshi's Bitcoin hourly markets. It ingests live BTC price data, maps it to the currently active Kalshi market, stores snapshots in SQLite, and serves a real-time dashboard plus API endpoints for downstream strategy work.

The core objective is to evolve from signal collection into automated, risk-aware execution.

## Dashboard
![img.png](Dashboard.png)

The dashboard shows:
- Live BTC price and timestamp
- Last hour price curve
- High/low stats
- Latest Kalshi markets for the current event
- One-click YES/NO buttons per market with a configurable max cost (cents)
- Current portfolio panel with balance and open positions (cost, P/L, max payout) and refresh button
- Strategy panel for farthest-band execution:
  - `paper` or `live` mode toggle
  - Configurable side, ask band (`95-99c` default), max cost, and auto interval
  - Active position tracking with stop-loss rotation: if mark drops below `-20%`, strategy exits and attempts to rebuy a farther strike
  - Live auto-schedule countdown with progress bar (time remaining until next cycle)
  - 1-second preview refresh showing the next probable order continuously
  - Preview of planned order (ticker, strike, ask, count, planned cost, expected return)
  - Nearest candidate list when no exact ask-band match exists
  - Run once + auto start/status/stop controls


## Highlights
- Live BTC price from Kraken.
- Kalshi market discovery for the current hour.
- Continuous ingest pipeline with SQLite storage.
- FastAPI service with JSON endpoints and a built-in dashboard.

## Requirements
- Python 3.11+

## Architecture
1. **Market discovery**: Identify the current Kalshi event ticker based on the next hour (ET).
2. **Price fetch**: Pull the latest BTC price from Kraken.
3. **Market snapshot**: Fetch Kalshi YES/NO quotes for the event.
4. **Ingest**: Store snapshots in SQLite and prune old data.
5. **Serve**: FastAPI provides API routes and a real-time dashboard.

## Quickstart
### 1) Create a virtual environment
```bash
python -m venv .venv
source .venv/bin/activate
```

### 2) Install dependencies
```bash
pip install fastapi uvicorn requests cryptography python-dotenv pytz
```

### 3) Configure environment
Copy your `.env` file and set:
- `KALSHI_API_KEY`
- `KALSHI_PRIVATE_KEY`
- `KALSHI_BASE_URL`
- `KALSHI_DB_PATH`
- `OPENAI_API_KEY` (optional, future use)
- `XAI_API_KEY` (optional, future use)

Notes:
- `KALSHI_PRIVATE_KEY` should point to your private key PEM.
- set `KALSHI_DB_PATH` to override the default SQLite path.
- Do not commit secrets. `.env` is already ignored by git.



### 4) Run the API server + dashboard
```bash
python -m src.api
```
- This continuously writes market snapshots to `data/kalshi_ingest.db` (once per second) and prunes data older than 24 hours.
- Then open:- `http://localhost:8090/dashboard`

## API Endpoints
- `GET /get_price_ticker`
  - Returns the latest BTC price from Kraken.
- `GET /kalshi_ingest/latest`
  - Returns the most recent Kalshi ingest snapshots (last 2 hours).
- `GET /kalshi_ingest/last_hour`
  - Returns BTC price samples for the last hour.
- `GET /kalshi/place_best_ask_order?side=yes|no&ticker=...&max_cost_cents=...`
  - Places a best-ask limit order for YES/NO based on the current order book.
- `GET /kalshi/portfolio/balance`
  - Returns current portfolio balance.
- `GET /kalshi/portfolio/orders`
  - Returns current orders (filter with `status`, `ticker`, `limit`).
- `GET /kalshi/portfolio/current`
  - Returns portfolio balance and current positions/orders with estimated cost, mark-based P/L, and max payout.
- `GET /kalshi/portfolio/positions_debug`
  - Returns raw positions payload plus a small sample for debugging field mappings.
- `GET /strategy/farthest_band/preview?side=yes|no&ask_min_cents=95&ask_max_cents=99&max_cost_cents=500`
  - Uses latest spot + latest ingest snapshot to show the planned order (lower-direction strategy only).
  - If no exact band match exists, returns nearest candidates.
- `GET /strategy/farthest_band/run?side=yes|no&ask_min_cents=95&ask_max_cents=99&max_cost_cents=500&mode=paper|live`
  - Executes one strategy cycle immediately.
  - `mode=paper` returns planned action only; `mode=live` places a real order.
  - If an active tracked position is down `20%` or more, it exits and attempts to rebuy a farther strike.
- `GET /strategy/farthest_band/run?side=yes|no&ask_min_cents=95&ask_max_cents=99&max_cost_cents=500&mode=paper|live&force_new_order=1`
  - Forces a fresh entry on run-once (ignores currently tracked active position state).
- `GET /strategy/farthest_band/auto/start?side=yes|no&ask_min_cents=95&ask_max_cents=99&max_cost_cents=500&mode=paper|live&interval_minutes=15`
  - Starts background auto-execution on interval (default 15 minutes).
  - Auto cycles apply the same `-20%` stop-loss exit-and-rotate behavior.
- `GET /strategy/farthest_band/auto/status`
  - Returns running state, active config, last run time, and last result.
- `GET /strategy/farthest_band/auto/stop`
  - Stops background auto-execution.


## Repository Layout
```
src/
  api.py                        # FastAPI app and dashboard UI
  client/
    kalshi_client.py            # Signed API client for Kalshi
    kraken_client.py            # Kraken BTC price client
  offline_processing/
    ingest_kalshi.py            # Ingest loop + SQLite storage
    generate_all_kalshi_urls.py # Utility for batch Kalshi URLs
    kalshi_urls_2026.txt        # Precomputed URLs
  utils/
    fetch_current_predictions_kalshi.py  # Pull markets + normalize
    get_current_trading_markets.py       # Resolve current market URL

data/
  kalshi_ingest.db              # SQLite ingest store (generated)
```

## Strategy Roadmap (High Level)
- Integrate LLM-assisted market selection (OpenAI/xAI).
- Add risk controls (exposure caps, kill-switches, dry-run mode).
- Enhance execution logic (slippage tolerance, order retries).
- Expand market coverage beyond BTC hourly.

## Safety & Compliance
This repository is for research and experimentation. Prediction markets involve real financial risk. Ensure you understand Kalshi's rules and applicable regulations before trading.

## Disclaimer
This software is provided "as is" with no warranties. Use at your own risk.
