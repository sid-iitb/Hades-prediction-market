import json
import os
import sqlite3
import sys
import time
import uuid
import logging
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple, Any

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore

# Load .env so COINBASE_* are available
from dotenv import load_dotenv
_script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv()  # cwd (e.g. when run from repo root)
load_dotenv(os.path.join(_script_dir, "..", ".env"))  # repo root when run from coinbase_grid_bot/

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

try:
    from coinbase.rest import RESTClient
    from requests.exceptions import HTTPError
except ImportError:
    RESTClient = None  # type: ignore
    HTTPError = Exception  # type: ignore


# --- Custom exceptions for order API errors ---
class CoinbaseAPIError(Exception):
    """Base for Coinbase API errors (rate limit, insufficient funds, min size, etc.)."""
    pass


class RateLimitError(CoinbaseAPIError):
    """API rate limit exceeded (429)."""
    pass


class InsufficientFundsError(CoinbaseAPIError):
    """Insufficient funds for the order."""
    pass


class MinimumOrderSizeError(CoinbaseAPIError):
    """Order size below exchange minimum."""
    pass

@dataclass
class GridConfig:
    product_id: str          # e.g., 'SOL-USD'
    lower_price: float       # Bottom of your grid
    upper_price: float       # Top of your grid
    num_grids: int           # How many slices to divide the range into
    order_size_usd: float    # How much $ to spend per grid level
    is_dry_run: bool = True  # If True, no live orders (mock only)


def load_grid_config(config_path: Optional[str] = None) -> Tuple[Dict[str, Any], List[Tuple[str, Dict[str, Any]]]]:
    """
    Load YAML config. Returns (common, enabled_assets) where enabled_assets is
    [(product_id, asset_config), ...] for assets with enabled: true.
    Config path: config_path argument, or COINBASE_GRID_CONFIG env, or coinbase_grid_bot/config.yaml.
    """
    if yaml is None:
        raise ImportError("PyYAML is required for config. Install with: pip install PyYAML")
    path = config_path or os.environ.get("COINBASE_GRID_CONFIG") or os.path.join(_script_dir, "config.yaml")
    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}
    common = data.get("common") or {}
    assets = data.get("assets") or {}
    enabled = [
        (product_id, cfg)
        for product_id, cfg in assets.items()
        if isinstance(cfg, dict) and cfg.get("enabled") is True
    ]
    return common, enabled

@dataclass
class GridOrder:
    price: float
    side: str                # 'BUY' or 'SELL'
    size: float
    status: str              # 'OPEN', 'FILLED'
    order_id: str


class GridStateManager:
    """
    Persists active grid orders (level -> order_id) to sqlite so the bot can resume
    after restart. On startup, load from DB and reconcile with Coinbase API before
    placing new orders.
    """
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.path.join(_script_dir, "grid_state.db")
        self._conn: Optional[sqlite3.Connection] = None
        self._ensure_connection()
        self._create_tables()

    def _ensure_connection(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _create_tables(self) -> None:
        conn = self._ensure_connection()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS grid_orders (
                product_id TEXT NOT NULL,
                grid_level REAL NOT NULL,
                order_id TEXT NOT NULL UNIQUE,
                side TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                updated_at TEXT,
                PRIMARY KEY (order_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_grid_orders_product_status ON grid_orders(product_id, status)"
        )
        conn.commit()

    def add_order(self, product_id: str, grid_level: float, order_id: str, side: str) -> None:
        """Record a newly placed order."""
        conn = self._ensure_connection()
        conn.execute(
            "INSERT OR REPLACE INTO grid_orders (product_id, grid_level, order_id, side, status, updated_at) VALUES (?, ?, ?, ?, 'OPEN', datetime('now'))",
            (product_id, grid_level, order_id, side),
        )
        conn.commit()

    def mark_filled(self, product_id: str, order_id: str) -> None:
        """Mark an order as FILLED (removes from active set)."""
        conn = self._ensure_connection()
        conn.execute(
            "UPDATE grid_orders SET status = 'FILLED', updated_at = datetime('now') WHERE product_id = ? AND order_id = ?",
            (product_id, order_id),
        )
        conn.commit()

    def get_active_orders(self, product_id: str) -> List[Tuple[float, str, str]]:
        """Return (grid_level, order_id, side) for all OPEN orders for this product."""
        conn = self._ensure_connection()
        cur = conn.execute(
            "SELECT grid_level, order_id, side FROM grid_orders WHERE product_id = ? AND status = 'OPEN'",
            (product_id,),
        )
        return [(float(r[0]), r[1], r[2]) for r in cur.fetchall()]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def _get_price_from_product_response(resp) -> float:
    """Extract spot price from SDK get_product response (handles wrapped or flat)."""
    if hasattr(resp, "price") and resp.price is not None:
        return float(resp.price)
    if isinstance(resp, dict):
        p = resp.get("product") or resp
        price = p.get("price") if isinstance(p, dict) else None
        if price is not None:
            return float(price)
    raise ValueError("Could not get price from product response")


class MockCoinbaseClient:
    """A mock client to simulate API calls for testing grid logic."""
    def __init__(self, current_price: float):
        self.current_price = current_price
        self.mock_orders: Dict[str, GridOrder] = {}
        self.order_counter = 0

    def get_spot_price(self, product_id: str) -> float:
        # In real life, this fetches from Coinbase Advanced API
        return self.current_price

    def place_limit_order(self, product_id: str, side: str, price: float, size: float) -> str:
        self.order_counter += 1
        order_id = f"mock_{self.order_counter}"
        self.mock_orders[order_id] = GridOrder(price, side, size, 'OPEN', order_id)
        logger.info(f"Placed {side} order at ${price:.2f} for {size:.4f} {product_id.split('-')[0]}")
        return order_id

    def check_fills(self, current_price: float) -> List[GridOrder]:
        """Simulate order fills based on price movement."""
        filled = []
        for o_id, order in self.mock_orders.items():
            if order.status == 'OPEN':
                if (order.side == 'BUY' and current_price <= order.price) or \
                   (order.side == 'SELL' and current_price >= order.price):
                    order.status = 'FILLED'
                    filled.append(order)
        return filled

    def check_balances(self, product_id: str) -> Tuple[float, float]:
        """Mock: no balance check; return infinite so grid always proceeds."""
        return (float("inf"), float("inf"))


def _validate_cdp_key_file(key_file: str) -> None:
    """
    Validate that the JSON key file has CDP format (name + PEM privateKey).
    Raises ValueError if it looks like legacy format (id + base64).
    """
    try:
        with open(key_file, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        raise ValueError(f"Cannot read key file {key_file}: {e}") from e
    name = data.get("name")
    private_key = data.get("privateKey") or ""
    if not name or not private_key.strip():
        if data.get("id") and data.get("privateKey"):
            raise ValueError(
                f"The file {key_file} uses the legacy format (id + base64 secret). "
                "Coinbase Advanced Trade requires CDP keys from https://cloud.coinbase.com/access/api "
                "with 'name' (e.g. organizations/.../apiKeys/...) and PEM 'privateKey'. "
                "Create a new API key there and download the JSON, or use Option B in .env: "
                "COINBASE_API_KEY=<key name> and COINBASE_API_SECRET=\"-----BEGIN EC PRIVATE KEY-----\\n...\""
            ) from None
        raise ValueError(
            f"Key file {key_file} must contain 'name' and 'privateKey' (PEM). "
            "Get CDP keys from https://cloud.coinbase.com/access/api"
        ) from None
    if not _is_cdp_pem(private_key):
        if "BEGIN" not in private_key and len(private_key) > 80:
            raise ValueError(
                f"The file {key_file} has a base64 'privateKey', not a PEM. "
                "CDP requires a PEM private key. Create a new API key at "
                "https://cloud.coinbase.com/access/api and download the JSON, or use Option B in .env with the PEM."
            ) from None
        raise ValueError(
            f"Key file {key_file} 'privateKey' must be a PEM string (-----BEGIN EC PRIVATE KEY-----...)."
        ) from None


def _is_cdp_pem(secret: str) -> bool:
    """Return True if secret looks like a PEM private key (EC or RSA)."""
    s = (secret or "").strip()
    return s.startswith("-----BEGIN ") and "PRIVATE KEY-----" in s


def _is_cdp_key_name(key: str) -> bool:
    """Return True if key looks like CDP API key name (organizations/.../apiKeys/...)."""
    k = (key or "").strip()
    return "organizations" in k and "apiKeys" in k


def _validate_cdp_credentials(api_key: str, api_secret: str) -> None:
    """
    Validate that credentials look like CDP (key name + PEM), not legacy Exchange (UUID + base64).
    Raises ValueError with clear instructions if format is wrong.
    """
    if not api_key or not api_secret:
        return
    key, secret = api_key.strip(), api_secret.strip()
    if _is_cdp_key_name(key) and _is_cdp_pem(secret):
        return
    if _is_cdp_pem(secret):
        raise ValueError(
            "COINBASE_API_KEY should be the full CDP key name (e.g. organizations/.../apiKeys/...), "
            "not a UUID. Get it from https://cloud.coinbase.com/access/api"
        )
    if key and len(key) == 36 and key.count("-") == 4 and not secret.startswith("-----BEGIN"):
        raise ValueError(
            "You are using the legacy Coinbase Exchange API key/secret format. "
            "This bot requires Coinbase Developer Platform (CDP) credentials:\n"
            "  1. Go to https://cloud.coinbase.com/access/api\n"
            "  2. Create an API key and download the JSON key file, OR copy the key name + PEM private key\n"
            "  3. In .env set either COINBASE_KEY_FILE=/path/to/coinbase_key.json "
            "or COINBASE_API_KEY=<key name> and COINBASE_API_SECRET=\"-----BEGIN EC PRIVATE KEY-----\\n...\\n-----END EC PRIVATE KEY-----\"\n"
            "See coinbase_grid_bot/README.md for details."
        )
    if not _is_cdp_pem(secret):
        raise ValueError(
            "COINBASE_API_SECRET must be the PEM private key from CDP (starts with -----BEGIN EC PRIVATE KEY-----). "
            "Get it from https://cloud.coinbase.com/access/api — use the downloaded JSON key file or paste the PEM with \\n for newlines."
        )


def _parse_order_error(response, default_msg: str) -> str:
    """Extract error message from CreateOrderResponse or raw response."""
    err = getattr(response, "error_response", None)
    if err is not None:
        return getattr(err, "message", None) or getattr(err, "error", None) or default_msg
    if isinstance(response, dict):
        er = response.get("error_response") or {}
        return er.get("message") or er.get("error") or default_msg
    return default_msg


class LiveCoinbaseGridClient:
    """
    Grid client: live spot price (public API) and optional live order execution (CDP auth).
    If CDP credentials are provided, place_limit_order and check_fills use the real API.
    Otherwise orders are mock-only (log only).
    """
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        key_file: Optional[str] = None,
        timeout: Optional[int] = None,
    ):
        if RESTClient is None:
            raise ImportError("coinbase-advanced-py is required. Install with: pip install coinbase-advanced-py")

        key_file = key_file or os.environ.get("COINBASE_KEY_FILE") or None
        key_file = key_file.strip() if isinstance(key_file, str) and key_file.strip() else None
        api_key = api_key or os.environ.get("COINBASE_API_KEY") or None
        api_secret = api_secret or os.environ.get("COINBASE_API_SECRET") or None

        self._live_orders = False
        if key_file and os.path.isfile(key_file):
            try:
                _validate_cdp_key_file(key_file)
                self._rest_client = RESTClient(key_file=key_file, timeout=timeout)
                self._live_orders = True
                logger.info("Using CDP credentials from key file for live orders.")
            except ValueError as e:
                logger.warning("Key file invalid: %s. Trying Option B (env vars).", e)
                key_file = None
        if not self._live_orders and (api_key and api_secret):
            try:
                _validate_cdp_credentials(api_key, api_secret)
                self._rest_client = RESTClient(api_key=api_key, api_secret=api_secret, timeout=timeout)
                self._live_orders = True
                logger.info("Using CDP credentials from env/args for live orders.")
            except ValueError as e:
                logger.warning("Option B credentials invalid: %s", e)
        if not self._live_orders:
            self._rest_client = RESTClient(api_key=None, api_secret=None, timeout=timeout)
            logger.info("No valid CDP credentials: orders are mock-only (log only).")

        self.mock_orders: Dict[str, GridOrder] = {}
        self._placed_orders: Dict[str, GridOrder] = {}  # order_id -> GridOrder (for live fills)
        self.order_counter = 0

    def get_spot_price(self, product_id: str) -> float:
        """Fetch live spot price from Coinbase Advanced Trade API (public endpoint)."""
        resp = self._rest_client.get_public_product(product_id)
        return _get_price_from_product_response(resp)

    def check_balances(self, product_id: str) -> Tuple[float, float]:
        """
        Fetch available balances for base and quote currency of product_id (e.g. SOL-USD).
        Returns (base_available, quote_available). Requires auth; if not live orders, returns (inf, inf).
        """
        if not self._live_orders:
            return (float("inf"), float("inf"))
        parts = product_id.split("-")
        if len(parts) != 2:
            return (0.0, 0.0)
        base_currency, quote_currency = parts[0].strip(), parts[1].strip()
        base_available, quote_available = 0.0, 0.0
        try:
            resp = self._rest_client.get_accounts(limit=250)
            accounts = getattr(resp, "accounts", None) or []
            for acc in accounts:
                currency = (getattr(acc, "currency", None) or "").upper()
                if not currency:
                    continue
                ab = getattr(acc, "available_balance", None)
                if ab is None:
                    continue
                val_str = getattr(ab, "value", None)
                if val_str is None:
                    continue
                try:
                    val = float(val_str)
                except (TypeError, ValueError):
                    continue
                if currency == base_currency:
                    base_available += val
                elif currency == quote_currency:
                    quote_available += val
                elif quote_currency == "USD" and currency == "USDC":
                    quote_available += val
            return (base_available, quote_available)
        except HTTPError as e:
            if e.response is not None and getattr(e.response, "status_code", None) == 429:
                raise RateLimitError("API rate limit exceeded when fetching balances.") from e
            raise CoinbaseAPIError(f"Failed to fetch balances: {e}") from e

    def place_limit_order(self, product_id: str, side: str, price: float, size: float) -> str:
        """Place a GTC limit order (live if CDP auth set, else mock)."""
        base_size = str(size)
        limit_price = str(price)
        client_order_id = uuid.uuid4().hex

        if not self._live_orders:
            self.order_counter += 1
            order_id = f"mock_{self.order_counter}"
            self.mock_orders[order_id] = GridOrder(price, side, size, "OPEN", order_id)
            base = product_id.split("-")[0]
            logger.info(
                "[MOCK] Would place %s limit order: %s @ $%.4f (size %.6f %s)",
                side, product_id, price, size, base,
            )
            return order_id

        try:
            if side.upper() == "BUY":
                resp = self._rest_client.limit_order_gtc_buy(
                    client_order_id=client_order_id,
                    product_id=product_id,
                    base_size=base_size,
                    limit_price=limit_price,
                )
            else:
                resp = self._rest_client.limit_order_gtc_sell(
                    client_order_id=client_order_id,
                    product_id=product_id,
                    base_size=base_size,
                    limit_price=limit_price,
                )

            success = getattr(resp, "success", False)
            order_id = getattr(resp, "order_id", None) or (
                getattr(getattr(resp, "success_response", None), "order_id", None)
            )
            if success and order_id:
                self._placed_orders[order_id] = GridOrder(price, side, size, "OPEN", order_id)
                base = product_id.split("-")[0]
                logger.info(
                    "Placed %s limit order: %s @ $%.4f (size %.6f %s) -> order_id=%s",
                    side, product_id, price, size, base, order_id,
                )
                return order_id

            msg = _parse_order_error(resp, "Order creation failed")
            err_lower = msg.lower()
            if "insufficient" in err_lower or "funds" in err_lower or "balance" in err_lower:
                raise InsufficientFundsError(msg)
            if "minimum" in err_lower or "size" in err_lower or "below" in err_lower:
                raise MinimumOrderSizeError(msg)
            raise CoinbaseAPIError(msg)

        except HTTPError as e:
            if e.response is not None and getattr(e.response, "status_code", None) == 429:
                raise RateLimitError("API rate limit exceeded; retry later.") from e
            msg = str(getattr(e, "response", e))
            if "429" in msg:
                raise RateLimitError("API rate limit exceeded; retry later.") from e
            if "insufficient" in msg.lower() or "funds" in msg.lower():
                raise InsufficientFundsError(msg) from e
            if "minimum" in msg.lower() or "size" in msg.lower():
                raise MinimumOrderSizeError(msg) from e
            raise CoinbaseAPIError(msg) from e

    def check_fills(self, current_price: float) -> List[GridOrder]:
        """Poll API for order status and return newly filled orders; or simulate for mock."""
        if not self._live_orders:
            filled = []
            for o_id, order in self.mock_orders.items():
                if order.status == "OPEN":
                    if (order.side == "BUY" and current_price <= order.price) or (
                        order.side == "SELL" and current_price >= order.price
                    ):
                        order.status = "FILLED"
                        filled.append(order)
            return filled

        open_order_ids = [oid for oid, o in self._placed_orders.items() if o.status == "OPEN"]
        if not open_order_ids:
            return []

        filled: List[GridOrder] = []
        try:
            resp = self._rest_client.list_orders(order_ids=open_order_ids, limit=len(open_order_ids))
        except HTTPError as e:
            if e.response is not None and getattr(e.response, "status_code", None) == 429:
                raise RateLimitError("API rate limit exceeded; retry later.") from e
            raise

        orders = getattr(resp, "orders", None) or []
        for o in orders:
            oid = getattr(o, "order_id", None)
            status = (getattr(o, "status", None) or "").upper()
            if oid is None or status != "FILLED":
                continue
            if oid not in self._placed_orders:
                continue
            grid_order = self._placed_orders[oid]
            grid_order.status = "FILLED"
            filled_size = getattr(o, "filled_size", None)
            if filled_size is not None:
                try:
                    grid_order.size = float(filled_size)
                except (TypeError, ValueError):
                    pass
            filled.append(grid_order)
        return filled

class SpotGridBot:
    def __init__(self, config: GridConfig, client: MockCoinbaseClient, state: Optional["GridStateManager"] = None):
        self.config = config
        self.client = client
        self.state = state or GridStateManager()
        self.grid_levels: List[float] = []
        self.active_orders: Dict[float, str] = {}  # Map price level to order_id
        self._accumulation_mode: bool = False  # True = only BUY orders until we have enough base

    def calculate_grids(self):
        """Calculates evenly spaced grid lines between lower and upper price."""
        step = (self.config.upper_price - self.config.lower_price) / (self.config.num_grids - 1)
        self.grid_levels = [self.config.lower_price + (i * step) for i in range(self.config.num_grids)]
        logger.info("Calculated %d grid levels. Step size: $%.2f", len(self.grid_levels), step)

    def _grid_levels_by_side(self, current_price: float):
        """Return (buy_levels, sell_levels) excluding level at current price."""
        buy, sell = [], []
        for level in self.grid_levels:
            if abs(level - current_price) / current_price < 0.001:
                continue
            if level < current_price:
                buy.append(level)
            else:
                sell.append(level)
        return (buy, sell)

    def load_and_reconcile_state(self) -> None:
        """
        Load persisted active orders from DB. If any exist, query Coinbase for their status.
        Update DB and bot state (active_orders, client._placed_orders). Filled orders trigger
        replacement orders. Only place new orders in initialize_grid for levels not already active.
        """
        product_id = self.config.product_id
        rows = self.state.get_active_orders(product_id)
        if not rows:
            return
        order_id_to_level_side = {order_id: (level, side) for level, order_id, side in rows}
        active_orders: Dict[float, str] = {level: order_id for level, order_id, _ in rows}
        order_ids = list(order_id_to_level_side.keys())
        if not getattr(self.client, "_live_orders", False) or not getattr(self.client, "_rest_client", None):
            logger.info("Loaded %d orders from state (mock or no API); skipping reconcile.", len(rows))
            self.active_orders = active_orders
            if hasattr(self.client, "_placed_orders"):
                for order_id, (level, side) in order_id_to_level_side.items():
                    size = self.config.order_size_usd / level
                    self.client._placed_orders[order_id] = GridOrder(level, side, size, "OPEN", order_id)
            return
        try:
            resp = self.client._rest_client.list_orders(order_ids=order_ids, limit=len(order_ids))
        except Exception as e:
            logger.warning("Could not reconcile with API: %s. Proceeding with DB state.", e)
            self.active_orders = active_orders
            for order_id, (level, side) in order_id_to_level_side.items():
                size = self.config.order_size_usd / level
                self.client._placed_orders[order_id] = GridOrder(level, side, size, "OPEN", order_id)
            return
        orders = getattr(resp, "orders", None) or []
        for o in orders:
            oid = getattr(o, "order_id", None)
            status = (getattr(o, "status", None) or "").upper()
            if oid not in order_id_to_level_side:
                continue
            level, side = order_id_to_level_side[oid]
            if status == "FILLED":
                self.state.mark_filled(product_id, oid)
                active_orders.pop(level, None)
                try:
                    level_idx = self.grid_levels.index(min(self.grid_levels, key=lambda x: abs(x - level)))
                except ValueError:
                    continue
                if side == "BUY" and level_idx < len(self.grid_levels) - 1:
                    next_level = self.grid_levels[level_idx + 1]
                    size = self.config.order_size_usd / next_level
                    try:
                        new_id = self.client.place_limit_order(product_id, "SELL", next_level, size)
                        active_orders[next_level] = new_id
                        self.state.add_order(product_id, next_level, new_id, "SELL")
                    except (RateLimitError, InsufficientFundsError, MinimumOrderSizeError, CoinbaseAPIError) as err:
                        logger.warning("Replacement SELL after restore: %s", err)
                elif side == "SELL" and level_idx > 0:
                    next_level = self.grid_levels[level_idx - 1]
                    size = self.config.order_size_usd / next_level
                    try:
                        new_id = self.client.place_limit_order(product_id, "BUY", next_level, size)
                        active_orders[next_level] = new_id
                        self.state.add_order(product_id, next_level, new_id, "BUY")
                    except (RateLimitError, InsufficientFundsError, MinimumOrderSizeError, CoinbaseAPIError) as err:
                        logger.warning("Replacement BUY after restore: %s", err)
            else:
                size = self.config.order_size_usd / level
                self.client._placed_orders[oid] = GridOrder(level, side, size, "OPEN", oid)
        self.active_orders = active_orders
        logger.info("Reconciled state: %d active orders restored.", len(self.active_orders))

    def ensure_sufficient_balances(self) -> None:
        """
        Check wallet balances for base/quote. If insufficient for initial grid,
        print a clear WARNING and exit. Call before initialize_grid when using live orders.
        """
        if not hasattr(self.client, "check_balances"):
            return
        current_price = self.client.get_spot_price(self.config.product_id)
        buy_levels, sell_levels = self._grid_levels_by_side(current_price)
        required_quote = len(buy_levels) * self.config.order_size_usd
        required_base = sum(self.config.order_size_usd / level for level in sell_levels)
        base_available, quote_available = self.client.check_balances(self.config.product_id)
        if base_available == float("inf") and quote_available == float("inf"):
            return
        base_currency = self.config.product_id.split("-")[0]
        quote_currency = self.config.product_id.split("-")[1] if "-" in self.config.product_id else "USD"
        shortfall_quote = required_quote - quote_available
        shortfall_base = required_base - base_available
        if shortfall_quote > 0.01:
            logger.warning(
                "INSUFFICIENT QUOTE CURRENCY (%s). You have %.4f %s but need %.4f %s to place all %d BUY orders (%.2f %s per level). You are short by %.4f %s. Exiting to avoid 400 errors.",
                quote_currency, quote_available, quote_currency, required_quote, quote_currency,
                len(buy_levels), self.config.order_size_usd, quote_currency, shortfall_quote, quote_currency,
            )
            sys.exit(1)
        if shortfall_base > 1e-9:
            self._accumulation_mode = True
            logger.warning(
                "INSUFFICIENT BASE CURRENCY (%s) for SELL orders. You have %.6f %s but need %.6f %s. "
                "Running in ACCUMULATION MODE: placing BUY orders only. When BUYs fill, you will acquire %s and the bot will place SELLs automatically.",
                base_currency, base_available, base_currency, required_base, base_currency, base_currency,
            )
        else:
            self._accumulation_mode = False
            logger.info(
                "Balances OK: %.4f %s (need %.4f for BUYs), %.6f %s (need %.6f for SELLs).",
                quote_available, quote_currency, required_quote, base_available, base_currency, required_base,
            )

    def initialize_grid(self):
        """Places initial buy/sell orders based on current price."""
        current_price = self.client.get_spot_price(self.config.product_id)
        logger.info(
            "Initializing grid. Current %s price: $%.2f",
            self.config.product_id, current_price,
        )
        buy_levels, sell_levels = self._grid_levels_by_side(current_price)
        for level in buy_levels:
            if level in self.active_orders:
                continue
            size = self.config.order_size_usd / level
            try:
                order_id = self.client.place_limit_order(
                    self.config.product_id, "BUY", level, size
                )
                self.active_orders[level] = order_id
                self.state.add_order(self.config.product_id, level, order_id, "BUY")
            except RateLimitError:
                logger.warning("Rate limited at level %.2f; skipping.", level)
            except (InsufficientFundsError, MinimumOrderSizeError) as e:
                logger.warning("Skipping level %.2f: %s", level, e)
            except CoinbaseAPIError as e:
                logger.warning("Skipping level %.2f: %s", level, e)
        if getattr(self, "_accumulation_mode", False):
            logger.info("Accumulation mode: skipping SELL orders until BUY orders fill and base is available.")
            return
        for level in sell_levels:
            if level in self.active_orders:
                continue
            size = self.config.order_size_usd / level
            try:
                order_id = self.client.place_limit_order(
                    self.config.product_id, "SELL", level, size
                )
                self.active_orders[level] = order_id
                self.state.add_order(self.config.product_id, level, order_id, "SELL")
            except RateLimitError:
                logger.warning("Rate limited at level %.2f; skipping.", level)
            except (InsufficientFundsError, MinimumOrderSizeError) as e:
                logger.warning("Skipping level %.2f: %s", level, e)
            except CoinbaseAPIError as e:
                logger.warning("Skipping level %.2f: %s", level, e)

    def tick(self, simulated_new_price: Optional[float] = None):
        """The main loop that checks for fills and replaces orders."""
        if simulated_new_price and hasattr(self.client, "current_price"):
            self.client.current_price = simulated_new_price

        current_price = self.client.get_spot_price(self.config.product_id)
        logger.debug("Tick - Current Price: $%.2f", current_price)

        try:
            filled_orders = self.client.check_fills(current_price)
        except RateLimitError:
            logger.warning("Rate limited when checking fills; skipping this tick.")
            return
        except CoinbaseAPIError as e:
            logger.warning("Error checking fills: %s", e)
            return
        
        for filled in filled_orders:
            logger.info("*** ORDER FILLED: %s at $%.2f ***", filled.side, filled.price)

            try:
                filled_level = min(self.grid_levels, key=lambda x: abs(x - filled.price))
                level_idx = self.grid_levels.index(filled_level)
            except ValueError:
                continue

            self.active_orders.pop(filled_level, None)
            self.state.mark_filled(self.config.product_id, filled.order_id)

            try:
                if filled.side == "BUY" and level_idx < len(self.grid_levels) - 1:
                    next_level_up = self.grid_levels[level_idx + 1]
                    size = self.config.order_size_usd / next_level_up
                    new_id = self.client.place_limit_order(
                        self.config.product_id, "SELL", next_level_up, size
                    )
                    self.active_orders[next_level_up] = new_id
                    self.state.add_order(self.config.product_id, next_level_up, new_id, "SELL")
                elif filled.side == "SELL" and level_idx > 0:
                    next_level_down = self.grid_levels[level_idx - 1]
                    size = self.config.order_size_usd / next_level_down
                    new_id = self.client.place_limit_order(
                        self.config.product_id, "BUY", next_level_down, size
                    )
                    self.active_orders[next_level_down] = new_id
                    self.state.add_order(self.config.product_id, next_level_down, new_id, "BUY")
            except RateLimitError:
                logger.warning("Rate limited; skipping replacement order this tick.")
            except (InsufficientFundsError, MinimumOrderSizeError) as e:
                logger.warning("Order placement skipped: %s", e)
            except CoinbaseAPIError as e:
                logger.warning("Order placement failed: %s", e)

if __name__ == "__main__":
    # 1. Load config from config.yaml (or COINBASE_GRID_CONFIG env)
    common, enabled_assets = load_grid_config()
    poll_interval = int(common.get("poll_interval_seconds", 10))
    is_dry_run = common.get("is_dry_run", False)

    if not enabled_assets:
        logger.error("No enabled assets in config. Set enabled: true for at least one asset in config.yaml.")
        sys.exit(1)

    configs: List[GridConfig] = []
    for product_id, ac in enabled_assets:
        configs.append(GridConfig(
            product_id=product_id,
            lower_price=float(ac.get("lower_price", 0)),
            upper_price=float(ac.get("upper_price", 0)),
            num_grids=int(ac.get("num_grids", 5)),
            order_size_usd=float(ac.get("order_size_usd", 2.0)),
            is_dry_run=is_dry_run,
        ))

    # 2. Single client (shared across all assets)
    try:
        client = LiveCoinbaseGridClient()
        if getattr(client, "_live_orders", False):
            logger.info("Live price + live orders (CDP credentials set).")
        else:
            logger.info("Live price; orders are mock-only (set CDP credentials for live orders).")
    except ImportError:
        client = MockCoinbaseClient(current_price=150.0)
        logger.info("Using mock client (install coinbase-advanced-py for live price).")

    # 3. One bot per enabled asset
    bots: List[SpotGridBot] = [SpotGridBot(c, client) for c in configs]
    for bot in bots:
        bot.calculate_grids()
        bot.ensure_sufficient_balances()
        bot.load_and_reconcile_state()
        bot.initialize_grid()

    # 4. Run continuously: poll each bot, then sleep
    logger.info("Grid bot running for %s. Polling every %ds. Press Ctrl+C to stop.", [c.product_id for c in configs], poll_interval)
    try:
        while True:
            for bot in bots:
                bot.tick()
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        print("\nStopping grid bot (Ctrl+C).")
        logger.info("Grid bot stopped by user.")
