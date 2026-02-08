# Hades-prediction-market

Kraken client to fetch the latest BTC price.

```python
from kraken_client import KrakenClient

client = KrakenClient()
quote = client.latest_btc_price()
print(quote)
```
