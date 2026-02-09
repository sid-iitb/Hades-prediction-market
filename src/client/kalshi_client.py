import base64
import datetime
import os
import uuid
import argparse
from dataclasses import field
from pathlib import Path

import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

class KalshiClient:
    def __init__(
        self
    ):
        self.api_key_id = os.getenv("KALSHI_API_KEY")
        self.private_key_path = os.getenv("KALSHI_PRIVATE_KEY")
        self.base_url = os.getenv("KALSHI_BASE_URL")
        self._private_key = None
        print(self.api_key_id)
        print(self.private_key_path)

    def _load_private_key(self):
        if self._private_key is not None:
            return self._private_key
        if not self.private_key_path:
            raise ValueError("KALSHI_PRIVATE_KEY_PATH is not set")
        with open(self.private_key_path, "rb") as f:
            self._private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
        return self._private_key

    def _sign_request(self, timestamp_ms, method, path):
        private_key = self._load_private_key()
        path_without_query = path.split("?")[0]
        message = f"{timestamp_ms}{method}{path_without_query}".encode("utf-8")
        signature = private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _headers(self, method, path):
        if not self.api_key_id:
            raise ValueError("KALSHI_API_KEY_ID is not set")
        timestamp_ms = str(int(datetime.datetime.now().timestamp() * 1000))
        signature = self._sign_request(timestamp_ms, method, path)
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "Content-Type": "application/json",
        }

    @staticmethod
    def _extract_price(level):
        if level is None:
            return None
        if isinstance(level, (int, float)):
            return int(level)
        if isinstance(level, (list, tuple)) and level:
            return int(level[0])
        return None

    def get_market_orderbook(self, ticker, depth=1):
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        headers = self._headers("GET", path)
        params = {"depth": depth} if depth is not None else None
        resp = requests.get(self.base_url + path, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json()

    def get_top_of_book(self, ticker):
        data = self.get_market_orderbook(ticker, depth=1)
        orderbook = data.get("orderbook", {})
        yes = orderbook.get("yes", [])
        no = orderbook.get("no", [])

        best_yes_bid = self._extract_price(yes[0]) if yes else None
        best_no_bid = self._extract_price(no[0]) if no else None

        yes_ask = 100 - best_no_bid if best_no_bid is not None else None
        no_ask = 100 - best_yes_bid if best_yes_bid is not None else None

        return {
            "yes_bid": best_yes_bid,
            "no_bid": best_no_bid,
            "yes_ask": yes_ask,
            "no_ask": no_ask,
        }

    def place_yes_limit_at_ask(self, ticker, yes_ask_cents, max_cost_cents=500):
        """
        Place a demo buy order for YES at the current ask price.
        NOTE: Kalshi's API now supports limit orders only; this emulates a market
        buy by placing a limit at the ask price with a max cost budget.
        """
        if yes_ask_cents is None or yes_ask_cents <= 0:
            raise ValueError("yes_ask_cents must be > 0")
        count = int(max_cost_cents // yes_ask_cents)
        if count < 1:
            raise ValueError("Budget too small for 1 contract at this yes_ask price")

        path = "/trade-api/v2/portfolio/orders"
        order = {
            "ticker": ticker,
            "action": "buy",
            "side": "yes",
            "count": count,
            "type": "limit",
            "yes_price": int(yes_ask_cents),
            "client_order_id": str(uuid.uuid4()),
        }
        headers = self._headers("POST", path)
        return requests.post(self.base_url + path, headers=headers, json=order)

    def place_limit_at_best_ask(self, ticker, side, max_cost_cents=500):
        side = str(side).lower().strip()
        if side not in {"yes", "no"}:
            raise ValueError("side must be 'yes' or 'no'")

        top = self.get_top_of_book(ticker)
        ask_key = f"{side}_ask"
        ask_cents = top.get(ask_key)
        if ask_cents is None:
            raise RuntimeError(f"No top-of-book data available to infer {side.upper()} ask")

        count = int(max_cost_cents // int(ask_cents))
        if count < 1:
            raise ValueError("Budget too small for 1 contract at this ask price")

        path = "/trade-api/v2/portfolio/orders"
        order = {
            "ticker": ticker,
            "action": "buy",
            "side": side,
            "count": count,
            "type": "limit",
            f"{side}_price": int(ask_cents),
            "client_order_id": str(uuid.uuid4()),
        }
        headers = self._headers("POST", path)
        return requests.post(self.base_url + path, headers=headers, json=order)

    def place_yes_limit_at_best_ask(self, ticker, max_cost_cents=500):
        return self.place_limit_at_best_ask(
            ticker=ticker,
            side="yes",
            max_cost_cents=max_cost_cents,
        )

    def place_no_limit_at_best_ask(self, ticker, max_cost_cents=500):
        return self.place_limit_at_best_ask(
            ticker=ticker,
            side="no",
            max_cost_cents=max_cost_cents,
        )


def main():
    from dotenv import load_dotenv

    env_file = Path("../../.env")

    # Load environment variables
    load_dotenv()
    client = KalshiClient()
    resp = client.place_limit_at_best_ask(
        ticker="KXBTCD-26FEB0904-T69749.99",
        side='yes',
        max_cost_cents=100,
    )
    try:
        body = resp.json()
    except Exception:
        body = resp.text
    print(f"Status: {resp.status_code}")
    print(body)


if __name__ == "__main__":
    main()
