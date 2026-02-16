from __future__ import annotations

import json
import ssl
from dataclasses import dataclass
from typing import Any, Dict
from urllib.error import URLError
from urllib.request import urlopen
import sys


@dataclass(frozen=True)
class PriceQuote:
    symbol: str
    price: float
    currency: str
    source: str


class KrakenClient:
    def __init__(self, base_url: str = "https://api.kraken.com") -> None:
        self._base_url = base_url.rstrip("/")

    def latest_btc_price(self) -> PriceQuote:
        return self._latest_price("XBTUSD", "BTC")

    def latest_eth_price(self) -> PriceQuote:
        return self._latest_price("ETHUSD", "ETH")

    def latest_sol_price(self) -> PriceQuote:
        return self._latest_price("SOLUSD", "SOL")

    def latest_xrp_price(self) -> PriceQuote:
        return self._latest_price("XRPUSD", "XRP")

    def _latest_price(self, pair: str, symbol: str) -> PriceQuote:
        payload = self._get_json(f"{self._base_url}/0/public/Ticker?pair={pair}")
        if payload.get("error"):
            raise RuntimeError(f"Kraken API error: {payload['error']}")

        result = payload.get("result", {})
        pair_data = result.get(pair) or next(iter(result.values()), None)
        if not pair_data:
            raise RuntimeError("Kraken API response missing result")

        last_trade = pair_data.get("c")
        if not last_trade or not isinstance(last_trade, list):
            raise RuntimeError("Kraken API response missing last trade price")

        price = float(last_trade[0])
        return PriceQuote(symbol=symbol, price=price, currency="USD", source="kraken")

    @staticmethod
    def _get_json(url: str) -> Dict[str, Any]:
        try:
            with urlopen(url, timeout=10) as response:
                data = response.read().decode("utf-8")
            return json.loads(data)
        except (ssl.SSLError, URLError) as exc:
            if isinstance(exc, URLError) and not isinstance(exc.reason, ssl.SSLError):
                raise
            try:
                import certifi
            except Exception as exc:
                raise RuntimeError(
                    "SSL verification failed and certifi is not available. "
                    "Install certifi or configure system CA certificates."
                ) from exc

            context = ssl.create_default_context(cafile=certifi.where())
            with urlopen(url, timeout=10, context=context) as response:
                data = response.read().decode("utf-8")
            return json.loads(data)


def _main(argv: list[str]) -> int:
    client = KrakenClient()
    quote = client.latest_btc_price()
    print(quote)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv))
