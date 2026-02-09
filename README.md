# Hades Prediction Market

A research-grade trading workstation for Kalshi's Bitcoin hourly markets. It ingests live BTC price data, maps it to the currently active Kalshi market, stores snapshots in SQLite, and serves a real-time dashboard plus API endpoints for downstream strategy work.

The core objective is to evolve from signal collection into automated, risk-aware execution.

## Highlights
- Live BTC price from Kraken.
- Kalshi market discovery for the current hour.
- Continuous ingest pipeline with SQLite storage.
- FastAPI service with JSON endpoints and a built-in dashboard.
- Clean, modular Python layout (clients, utils, offline processing).

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
- `OPENAI_API_KEY` (optional, future use)
- `XAI_API_KEY` (optional, future use)

Notes:
- `KALSHI_PRIVATE_KEY` should point to your private key PEM.
- Do not commit secrets. `.env` is already ignored by git.
- Optional: set `KALSHI_DB_PATH` to override the default SQLite path.
- Optional: set `KALSHI_AUTO_INGEST=false` to disable auto-starting the ingest loop when the API boots.

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
- `GET /kalshi/place_yes_ask_order`
  - Places a sample YES order at the ask (real order if keys are live). Use with caution.
- `GET /kalshi/place_best_ask_order?side=yes|no&ticker=...&max_cost_cents=...`
  - Places a best-ask limit order for YES/NO based on the current order book.

## Dashboard
The dashboard shows:
- Live BTC price and timestamp
- Last hour price curve
- High/low stats
- Latest Kalshi markets for the current event
- One-click YES/NO buttons per market with a configurable max cost (cents)

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
