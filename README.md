# Hades-prediction-market

An intelligent bot for the Kalshi prediction market platform.  
The long‑term goal is to auto‑trade based on real‑time signals and AI‑assisted
market analysis. The core trading algorithm is still under development.

Current capability:
- Fetch the latest Bitcoin price from Kraken.

Planned flow:
- Fetch the latest price (e.g., BTC).
- Use LLM analysis (OpenAI/Grok) to choose the right ticker and market.
- Use the Kalshi API to buy the ticket that maximizes expected value.
- Improve win rate over time as the strategy evolves.

## Requirements
- Python 3.11+

## Disclaimer
This project is for research and experimentation. Trading involves risk.
