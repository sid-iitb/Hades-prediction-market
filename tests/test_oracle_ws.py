"""
Unit and integration tests for WebSocket oracle implementation.

Verifies:
- Subscription targets for all assets [btc, eth, sol, xrp] (Kraken pairs + Coinbase products).
- get_ws_status() / get_safe_spot_prices_sync() contract and received prices (integration).

Run live WS tests only when network is available: RUN_ORACLE_WS_LIVE=1 python3 -m unittest tests.test_oracle_ws -v
"""
from __future__ import annotations

import os
import time
import unittest

# Expected asset keys (strategy uses lowercase; manager normalizes to uppercase)
EXPECTED_ASSETS = ["BTC", "ETH", "SOL", "XRP"]


class TestOracleWsSubscriptionTargets(unittest.TestCase):
    """Verify implementation subscribes for the correct assets and exchange symbols."""

    def test_implementation_subscribes_all_four_assets(self):
        from bot.oracle_ws_manager import (
            COINBASE_TICKER_PRODUCTS,
            KRAKEN_TICKER_PAIRS,
            WS_ASSET_KEYS,
        )
        self.assertEqual(WS_ASSET_KEYS, EXPECTED_ASSETS)
        self.assertEqual(KRAKEN_TICKER_PAIRS, ["XBT/USD", "ETH/USD", "SOL/USD", "XRP/USD"])
        self.assertEqual(
            COINBASE_TICKER_PRODUCTS,
            ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"],
        )

    def test_kraken_pair_to_asset_mapping(self):
        from bot.oracle_ws_manager import KRAKEN_PAIR_TO_ASSET
        self.assertEqual(KRAKEN_PAIR_TO_ASSET["XBT/USD"], "BTC")
        self.assertEqual(KRAKEN_PAIR_TO_ASSET["ETH/USD"], "ETH")
        self.assertEqual(KRAKEN_PAIR_TO_ASSET["SOL/USD"], "SOL")
        self.assertEqual(KRAKEN_PAIR_TO_ASSET["XRP/USD"], "XRP")

    def test_coinbase_product_to_asset_mapping(self):
        from bot.oracle_ws_manager import COINBASE_PRODUCT_TO_ASSET
        self.assertEqual(COINBASE_PRODUCT_TO_ASSET["BTC-USD"], "BTC")
        self.assertEqual(COINBASE_PRODUCT_TO_ASSET["ETH-USD"], "ETH")
        self.assertEqual(COINBASE_PRODUCT_TO_ASSET["SOL-USD"], "SOL")
        self.assertEqual(COINBASE_PRODUCT_TO_ASSET["XRP-USD"], "XRP")

    def test_coinbase_feed_default_is_exchange(self):
        from bot.oracle_ws_manager import _coinbase_ws_url_and_channel_key
        with self.subTest("default"):
            url, key = _coinbase_ws_url_and_channel_key()
            self.assertIn("ws-feed.exchange.coinbase.com", url)
            self.assertEqual(key, "type")
        with self.subTest("advanced_trade when env set"):
            import os
            prev = os.environ.pop("COINBASE_WS_FEED", None)
            try:
                os.environ["COINBASE_WS_FEED"] = "advanced_trade"
                url, key = _coinbase_ws_url_and_channel_key()
                self.assertIn("advanced-trade-ws.coinbase.com", url)
                self.assertEqual(key, "channel")
            finally:
                if prev is not None:
                    os.environ["COINBASE_WS_FEED"] = prev
                else:
                    os.environ.pop("COINBASE_WS_FEED", None)


class TestOracleWsApi(unittest.TestCase):
    """Test WS manager API contract and behavior without network."""

    def setUp(self):
        from bot.oracle_ws_manager import get_ws_status, is_ws_running, stop_ws_oracles
        self.get_ws_status = get_ws_status
        self.is_ws_running = is_ws_running
        self.stop_ws_oracles = stop_ws_oracles
        stop_ws_oracles()

    def test_get_ws_status_structure(self):
        status = self.get_ws_status()
        self.assertIn("running", status)
        self.assertIn("assets", status)
        self.assertIsInstance(status["running"], bool)
        self.assertIsInstance(status["assets"], dict)

    def test_get_safe_spot_prices_returns_none_when_no_data(self):
        from bot.oracle_ws_manager import get_safe_spot_prices_sync, stop_ws_oracles
        stop_ws_oracles()
        for asset in ["btc", "eth", "sol", "xrp"]:
            out = get_safe_spot_prices_sync(asset, max_age_seconds=3.0)
            self.assertIsNone(out, f"Expected None for {asset} when no WS data")

    def test_get_safe_spot_prices_normalizes_asset_key(self):
        from bot.oracle_ws_manager import get_safe_spot_prices_sync, stop_ws_oracles
        stop_ws_oracles()
        self.assertIsNone(get_safe_spot_prices_sync("BTC"))
        self.assertIsNone(get_safe_spot_prices_sync("  eth  "))


def _should_skip_live_ws():
    if os.environ.get("RUN_ORACLE_WS_LIVE") != "1":
        return "Set RUN_ORACLE_WS_LIVE=1 to run live WebSocket tests (requires network)"
    try:
        import websockets  # noqa: F401
    except ImportError:
        return "websockets not installed"
    return None


_SKIP_LIVE_MSG = _should_skip_live_ws()


@unittest.skipIf(_SKIP_LIVE_MSG is not None, _SKIP_LIVE_MSG or "Live WS disabled")
class TestOracleWsLive(unittest.TestCase):
    """Integration test: start WS oracles and check we receive prices for all assets. Requires network."""

    def setUp(self):
        from bot.oracle_ws_manager import stop_ws_oracles
        stop_ws_oracles()
        time.sleep(0.5)

    def tearDown(self):
        from bot.oracle_ws_manager import stop_ws_oracles
        stop_ws_oracles()

    def test_ws_receives_prices_for_all_assets(self):
        from bot.oracle_ws_manager import (
            get_ws_status,
            get_safe_spot_prices_sync,
            is_ws_running,
            start_ws_oracles,
        )
        start_ws_oracles()
        self.assertTrue(is_ws_running(), "WS oracles should be running")
        # Wait for at least one tick from both exchanges (often 1–3 s)
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            status = get_ws_status()
            assets = status.get("assets", {})
            if not assets:
                time.sleep(0.5)
                continue
            have_all = all(
                assets.get(a) and (assets[a].get("kraken") is not None or assets[a].get("cb") is not None)
                for a in EXPECTED_ASSETS
            )
            if have_all:
                break
            time.sleep(0.5)
        status = get_ws_status()
        assets = status.get("assets", {})
        for asset in EXPECTED_ASSETS:
            self.assertIn(asset, assets, f"Asset {asset} should appear in WS status")
            entry = assets[asset]
            kraken_price = entry.get("kraken")
            cb_price = entry.get("cb")
            self.assertTrue(
                kraken_price is not None or cb_price is not None,
                f"Asset {asset} should have at least one price (kraken={kraken_price}, cb={cb_price})",
            )
            if kraken_price is not None:
                self.assertIsInstance(kraken_price, (int, float))
                self.assertGreater(kraken_price, 0, f"Kraken price for {asset} should be positive")
            if cb_price is not None:
                self.assertIsInstance(cb_price, (int, float))
                self.assertGreater(cb_price, 0, f"Coinbase price for {asset} should be positive")

    def test_get_safe_spot_prices_after_ws_has_data(self):
        from bot.oracle_ws_manager import (
            get_safe_spot_prices_sync,
            get_ws_status,
            start_ws_oracles,
        )
        start_ws_oracles()
        time.sleep(6.0)
        for asset in ["btc", "eth", "sol", "xrp"]:
            out = get_safe_spot_prices_sync(asset, max_age_seconds=5.0)
            if out is not None:
                self.assertIn("kraken", out)
                self.assertIn("cb", out)
                self.assertIsInstance(out["kraken"], (int, float))
                self.assertIsInstance(out["cb"], (int, float))
                self.assertGreater(out["kraken"], 0)
                self.assertGreater(out["cb"], 0)
            else:
                status = get_ws_status()
                assets = status.get("assets", {})
                self.fail(
                    f"get_safe_spot_prices_sync({asset!r}) returned None; status assets: {list(assets.keys())}"
                )


if __name__ == "__main__":
    unittest.main()
