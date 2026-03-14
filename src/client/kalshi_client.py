import base64
import datetime
import os
import uuid
import argparse
from dataclasses import field
from pathlib import Path
from typing import Optional

import logging
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger(__name__)

class KalshiClient:
    def __init__(
        self
    ):
        self.api_key_id = os.getenv("KALSHI_API_KEY")
        self.private_key_path = os.getenv("KALSHI_PRIVATE_KEY")
        self.base_url = os.getenv("KALSHI_BASE_URL")
        self._private_key = None

    def _load_private_key(self):
        if self._private_key is not None:
            return self._private_key
        if not self.private_key_path:
            raise ValueError("KALSHI_PRIVATE_KEY is not set")
        # Support inline PEM (value starts with -----BEGIN) or file path
        key_content = self.private_key_path.strip()
        if key_content.startswith("-----BEGIN"):
            pem_bytes = key_content.encode("utf-8")
        else:
            with open(self.private_key_path, "rb") as f:
                pem_bytes = f.read()
        self._private_key = serialization.load_pem_private_key(
            pem_bytes, password=None, backend=default_backend()
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
            raise ValueError("KALSHI_API_KEY is not set")
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
        """
        Extract best-bid price in CENTS from an orderbook level.
        Supports legacy integer cents (e.g. 23) and V2 fixed-point dollars arrays
        like ["0.2300", "100.00"] from orderbook_fp.yes_dollars/no_dollars.
        """
        if level is None:
            return None
        # V2: [dollars_string, count_fp] → take dollars_string
        if isinstance(level, (list, tuple)) and level:
            level = level[0]
        # Legacy: integer cents
        if isinstance(level, int):
            return level
        # Tolerate float (should not normally occur)
        if isinstance(level, float):
            return int(round(level))
        # V2: dollars string "0.2300" → 23 cents
        if isinstance(level, str):
            s = level.strip()
            if not s:
                return None
            try:
                return int(round(float(s) * 100))
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _parse_count_fp(val) -> int:
        """
        Parse contract count from V2 API: int, float, or fixed-point string (e.g. '10.00').
        Legacy fill_count/remaining_count may be removed; fill_count_fp/remaining_count_fp are strings.
        """
        if val is None:
            return 0
        if isinstance(val, int):
            return val
        if isinstance(val, float):
            return int(val)
        if isinstance(val, str):
            val = val.strip()
            if not val:
                return 0
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return 0
        return 0

    @staticmethod
    def _normalize_order_for_v2(order: dict) -> dict:
        """
        Ensure order has integer fill_count and remaining_count for callers.
        Kalshi V2 fixed-point migration: legacy fill_count/remaining_count removed;
        use fill_count_fp / remaining_count_fp (fixed-point strings) when present.
        """
        if not order or not isinstance(order, dict):
            return order
        out = dict(order)
        out["fill_count"] = KalshiClient._parse_count_fp(
            order.get("fill_count") or order.get("filled_count") or order.get("fill_count_fp")
        )
        out["remaining_count"] = KalshiClient._parse_count_fp(
            order.get("remaining_count") or order.get("remaining_count_fp")
        )
        if out.get("count") is None and order.get("count_fp") is not None:
            out["count"] = KalshiClient._parse_count_fp(order.get("count_fp"))
        if out.get("initial_count") is None and order.get("initial_count_fp") is not None:
            out["initial_count"] = KalshiClient._parse_count_fp(order.get("initial_count_fp"))
        return out

    def get_market(self, ticker: str) -> Optional[dict]:
        """
        Fetch market by ticker. Returns market dict with status, result, etc.
        status: initialized|inactive|active|closed|determined|disputed|amended|finalized.
        result: 'yes'|'no'|'scalar'|'' (filled when settled).
        Returns None on 404 or request error.
        """
        path = f"/trade-api/v2/markets/{ticker}"
        try:
            headers = self._headers("GET", path)
            resp = requests.get(self.base_url + path, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            return data.get("market", data)
        except Exception:
            return None

    def get_market_orderbook(self, ticker, depth=1):
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        headers = self._headers("GET", path)
        params = {"depth": depth} if depth is not None else None
        resp = requests.get(self.base_url + path, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json()

    def get_top_of_book(self, ticker):
        """
        Return top-of-book snapshot as integer cents:
        - yes_bid / no_bid from best price levels
        - yes_ask / no_ask derived as 100 - opposite bid (binary symmetry)

        Supports both legacy integer orderbook (orderbook.yes/no) and
        V2 fixed-point orderbook_fp.yes_dollars/no_dollars.
        """
        data = self.get_market_orderbook(ticker, depth=10)

        # V2: orderbook_fp with yes_dollars / no_dollars.
        orderbook_fp = data.get("orderbook_fp") or {}
        yes_levels = orderbook_fp.get("yes_dollars") or []
        no_levels = orderbook_fp.get("no_dollars") or []

        if yes_levels or no_levels:
            # Arrays are sorted by price; best bid is the highest price → last element.
            best_yes_bid = self._extract_price(yes_levels[-1]) if yes_levels else None
            best_no_bid = self._extract_price(no_levels[-1]) if no_levels else None
        else:
            # Legacy fallback: orderbook.yes/no with integer cents, ascending prices.
            orderbook = data.get("orderbook", {}) or {}
            yes = orderbook.get("yes") or []
            no = orderbook.get("no") or []
            best_yes_bid = self._extract_price(yes[-1]) if yes else None
            best_no_bid = self._extract_price(no[-1]) if no else None

        yes_ask = 100 - best_no_bid if best_no_bid is not None else None
        no_ask = 100 - best_yes_bid if best_yes_bid is not None else None

        return {
            "yes_bid": best_yes_bid,
            "no_bid": best_no_bid,
            "yes_ask": yes_ask,
            "no_ask": no_ask,
        }

    def get_order(self, order_id: str):
        """
        Fetch a single order by ID.
        Returns order dict with status (resting|canceled|executed), fill_count, remaining_count, etc.
        Normalized for V2 API: fill_count/remaining_count derived from _fp fields when legacy fields removed.
        """
        path = f"/trade-api/v2/portfolio/orders/{order_id}"
        headers = self._headers("GET", path)
        resp = requests.get(self.base_url + path, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        order = data.get("order", data)
        return self._normalize_order_for_v2(order) if isinstance(order, dict) else order

    def cancel_order(self, order_id: str):
        """
        Cancel an order (reduces remaining contracts to zero).
        Returns response with order and reduced_by info.
        """
        path = f"/trade-api/v2/portfolio/orders/{order_id}"
        headers = self._headers("DELETE", path)
        resp = requests.delete(self.base_url + path, headers=headers)
        resp.raise_for_status()
        return resp.json() if resp.text else {}

    def get_orders(self, status=None, ticker=None, cursor=None, limit=100):
        """
        Fetch orders from the portfolio.
        Optional filters: status, ticker, cursor, limit.
        Order entries are normalized for V2 (fill_count/remaining_count from _fp when needed).
        """
        path = "/trade-api/v2/portfolio/orders"
        headers = self._headers("GET", path)
        params = {}
        if status is not None:
            params["status"] = status
        if ticker is not None:
            params["ticker"] = ticker
        if cursor is not None:
            params["cursor"] = cursor
        if limit is not None:
            params["limit"] = limit
        resp = requests.get(self.base_url + path, headers=headers, params=params or None)
        resp.raise_for_status()
        data = resp.json()
        orders = data.get("orders")
        if isinstance(orders, list):
            data = dict(data)
            data["orders"] = [self._normalize_order_for_v2(o) if isinstance(o, dict) else o for o in orders]
        return data

    def get_all_orders(self, status=None, ticker=None, limit=100, max_pages=20):
        """
        Fetch all orders with pagination.
        """
        all_orders = []
        cursor = None
        pages = 0
        while pages < max_pages:
            data = self.get_orders(
                status=status,
                ticker=ticker,
                cursor=cursor,
                limit=limit,
            )
            orders = data.get("orders", data if isinstance(data, list) else [])
            if not orders:
                break
            all_orders.extend(orders)
            cursor = data.get("next_cursor") or data.get("cursor")
            if not cursor:
                break
            pages += 1
        return all_orders

    def get_fills(self, ticker=None, order_id=None, min_ts=None, max_ts=None, cursor=None, limit=100):
        """
        Fetch fills (executed trades) from the portfolio.
        Optional: ticker, order_id, min_ts, max_ts (Unix seconds), cursor, limit (max 200).
        """
        path = "/trade-api/v2/portfolio/fills"
        headers = self._headers("GET", path)
        params = {}
        if ticker is not None:
            params["ticker"] = ticker
        if order_id is not None:
            params["order_id"] = order_id
        if min_ts is not None:
            params["min_ts"] = int(min_ts)
        if max_ts is not None:
            params["max_ts"] = int(max_ts)
        if cursor is not None:
            params["cursor"] = cursor
        if limit is not None:
            params["limit"] = min(int(limit), 200)
        resp = requests.get(self.base_url + path, headers=headers, params=params or None)
        resp.raise_for_status()
        return resp.json()

    def get_all_fills(self, ticker=None, order_id=None, min_ts=None, max_ts=None, limit=200, max_pages=50):
        """
        Fetch all fills with pagination.
        """
        all_fills = []
        cursor = None
        pages = 0
        while pages < max_pages:
            data = self.get_fills(
                ticker=ticker,
                order_id=order_id,
                min_ts=min_ts,
                max_ts=max_ts,
                cursor=cursor,
                limit=limit,
            )
            fills = data.get("fills", data if isinstance(data, list) else [])
            if not fills:
                break
            all_fills.extend(fills)
            cursor = data.get("cursor") or data.get("next_cursor")
            if not cursor:
                break
            pages += 1
        return all_fills

    def get_settlements(self, cursor=None, limit=100):
        """
        Fetch settlements from the portfolio.
        """
        path = "/trade-api/v2/portfolio/settlements"
        headers = self._headers("GET", path)
        params = {}
        if cursor is not None:
            params["cursor"] = cursor
        if limit is not None:
            params["limit"] = limit
        resp = requests.get(self.base_url + path, headers=headers, params=params or None)
        resp.raise_for_status()
        return resp.json()

    def get_all_settlements(self, limit=200, max_pages=20):
        """
        Fetch all settlements with pagination.
        """
        all_settlements = []
        cursor = None
        pages = 0
        while pages < max_pages:
            data = self.get_settlements(cursor=cursor, limit=limit)
            settlements = data.get("settlements", data if isinstance(data, list) else [])
            if not settlements:
                break
            all_settlements.extend(settlements)
            cursor = data.get("cursor") or data.get("next_cursor")
            if not cursor:
                break
            pages += 1
        return all_settlements

    def get_balance(self, timeout: Optional[int] = 10):
        """
        Fetch portfolio balance. timeout: seconds for the request (default 10; avoids hang).
        """
        path = "/trade-api/v2/portfolio/balance"
        headers = self._headers("GET", path)
        resp = requests.get(self.base_url + path, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def get_positions(self, cursor=None, limit=100):
        """
        Fetch current positions.
        """
        path = "/trade-api/v2/portfolio/positions"
        headers = self._headers("GET", path)
        params = {}
        if cursor is not None:
            params["cursor"] = cursor
        if limit is not None:
            params["limit"] = limit
        resp = requests.get(self.base_url + path, headers=headers, params=params or None)
        resp.raise_for_status()
        return resp.json()

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

    def place_limit_at_best_ask(self, ticker, side, max_cost_cents=500, client_order_id=None):
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
            "client_order_id": client_order_id or str(uuid.uuid4()),
        }
        headers = self._headers("POST", path)
        return requests.post(self.base_url + path, headers=headers, json=order)

    def place_limit_order(
        self,
        ticker,
        action,
        side,
        price_cents,
        count,
        client_order_id=None,
        time_in_force=None,
    ):
        """
        Place a limit order. time_in_force: None (default/resting), 'immediate_or_cancel', 'fill_or_kill', 'good_till_canceled'.
        """
        side = str(side).lower().strip()
        action = str(action).lower().strip()
        if side not in {"yes", "no"}:
            raise ValueError("side must be 'yes' or 'no'")
        if action not in {"buy", "sell"}:
            raise ValueError("action must be 'buy' or 'sell'")
        if int(count) < 1:
            raise ValueError("count must be >= 1")
        if int(price_cents) < 1 or int(price_cents) > 99:
            raise ValueError("price_cents must be between 1 and 99")

        path = "/trade-api/v2/portfolio/orders"
        order = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "count": int(count),
            "type": "limit",
            f"{side}_price": int(price_cents),
            "client_order_id": client_order_id or str(uuid.uuid4()),
        }
        if time_in_force is not None:
            order["time_in_force"] = str(time_in_force).strip().lower()
        headers = self._headers("POST", path)
        return requests.post(self.base_url + path, headers=headers, json=order)

    def place_market_order(
        self,
        ticker,
        action,
        side,
        count,
        client_order_id=None,
    ):
        """
        Place an aggressively priced limit order that behaves like a market order.
        Kalshi's v2 API currently requires exactly one of yes_price / no_price /
        yes_price_dollars / no_price_dollars on CreateOrder and rejects payloads
        without a price. There is no documented "true" market order on this endpoint,
        so we emulate a market order with a very aggressive limit:
          - For buys, we send side_price=99 (willing to pay up to 99c)
          - For sells, we send side_price=1  (willing to sell down to 1c)
        The matching engine still gives best available price; the aggressive limit
        just guarantees crossing the book.
        """
        side = str(side).lower().strip()
        action = str(action).lower().strip()
        if side not in {"yes", "no"}:
            raise ValueError("side must be 'yes' or 'no'")
        if action not in {"buy", "sell"}:
            raise ValueError("action must be 'buy' or 'sell'")
        if int(count) < 1:
            raise ValueError("count must be >= 1")

        path = "/trade-api/v2/portfolio/orders"
        # Emulated market order payload: aggressive limit on the given side.
        price_key = f"{side}_price"
        if action == "sell":
            price_cents = 1
        else:
            price_cents = 99
        order = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "count": int(count),
            "type": "limit",
            price_key: int(price_cents),
            "client_order_id": client_order_id or str(uuid.uuid4()),
        }
        # Sell orders: reduce_only IoC so we only close the position and do not rest on the book.
        if action == "sell":
            order["reduce_only"] = True
            order["time_in_force"] = "immediate_or_cancel"
        # Exactly one price field (yes_price or no_price) should be present.
        assert ("yes_price" in order) ^ ("no_price" in order)
        headers = self._headers("POST", path)
        resp = requests.post(self.base_url + path, headers=headers, json=order)
        if not resp.ok:
            try:
                err_body = resp.json()
            except Exception:
                err_body = resp.text[:1000] if resp.text else "(empty)"
            logger.error(
                "[Kalshi] place_market_order failed status=%s url=%s body=%s",
                resp.status_code,
                path,
                err_body,
            )
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            return {"order_id": None, "raw": resp.text[:500]}

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
