import re

import requests

from src.utils.get_current_trading_markets import get_current_market_urls
from src.client.kraken_client import KrakenClient

# Configuration
KALSHI_API_URL = "https://api.elections.kalshi.com/trade-api/v2/markets"
client = KrakenClient()

def get_kalshi_markets(event_ticker):
    try:
        params = {"limit": 100, "event_ticker": event_ticker}
        response = requests.get(KALSHI_API_URL, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get('markets', []), None
    except Exception as e:
        return None, str(e)

def parse_strike(subtitle):
    # Format: "$96,250 or above"
    # Extract number, remove commas
    match = re.search(r'\$([\d,]+)', subtitle)
    if match:
        return float(match.group(1).replace(',', ''))
    return 0.0

def fetch_kalshi_data_struct():
    """
    Fetches current Kalshi markets and returns a list of market dictionaries.
    """
    try:
        # Get current market info
        market_info = get_current_market_urls()
        kalshi_url = market_info["kalshi"]

        # Extract event ticker from URL
        event_ticker = kalshi_url.split("/")[-1].upper()

        # Fetch Current BTC Price
        current_price = client.latest_btc_price().price

        # Fetch Kalshi Markets
        markets, err = get_kalshi_markets(event_ticker)
        if err:
            return None, f"Kalshi Error: {err}"

        if not markets:
            return [], None

        # Parse strikes and sort
        market_data = []
        for m in markets:
            strike = parse_strike(m.get('subtitle', ''))
            if strike > 0:
                market_data.append({
                    'strike': strike,
                    'ticker': m.get('ticker'),
                    'yes_bid': m.get('yes_bid', 0),
                    'yes_ask': m.get('yes_ask', 0),
                    'no_bid': m.get('no_bid', 0),
                    'no_ask': m.get('no_ask', 0),
                    'subtitle': m.get('subtitle')
                })

        # Sort by strike price
        market_data.sort(key=lambda x: x['strike'])

        return {
            "event_ticker": event_ticker,
            "current_price": current_price,
            "markets": market_data
        }, None

    except Exception as e:
        return None, str(e)


def select_markets_within_range(market_data, current_price, window=1000):
    if current_price is None:
        return []
    return [
        m for m in market_data
        if m.get('strike', 0) > 0 and abs(m.get('strike', 0) - current_price) <= window
    ]

def main():
    data, err = fetch_kalshi_data_struct()

    if err:
        print(f"Error: {err}")
        return

    print(f"Fetching data for Event: {data['event_ticker']}")
    if data['current_price']:
        print(f"CURRENT PRICE: ${data['current_price']:,.2f}")

    market_data = data['markets']
    if not market_data:
        print("No markets found.")
        return

    # Find the market closest to current price for display
    current_price = data['current_price'] or 0
    # Keep only strikes within +/- 1000 of current price and positive strikes
    market_data = select_markets_within_range(market_data, current_price, window=1000)
    if not market_data:
        print("No markets within $1,000 of current price.")
        return
    # Show all markets within range after filtering
    selected_markets = market_data

    # Print Data
    print("-" * 30)
    for i, m in enumerate(selected_markets):
        print(f"PRICE TO BEAT {i+1}: {m['subtitle']} & Ticker: {m['ticker']}")
        print(f"BUY YES PRICE {i+1}: {m['yes_ask']}c, BUY NO PRICE {i+1}: {m['no_ask']}c")
        print()

if __name__ == "__main__":
    main()
