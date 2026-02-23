"""
Test KalshiClient.get_top_of_book - verifies we use last element (best bid) per Kalshi API.
Kalshi orderbook returns bids only; arrays are sorted ascending, so best bid = last element.
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.client.kalshi_client import KalshiClient


class TestGetTopOfBook(unittest.TestCase):
    """Ensure get_top_of_book uses best (highest) bid = last element in ascending orderbook."""

    def setUp(self):
        self.client = KalshiClient()

    @patch.object(KalshiClient, "get_market_orderbook")
    def test_best_bid_is_last_element_ascending(self, mock_orderbook):
        # Kalshi orderbook: ascending by price, so last = best
        # yes: [10, 20, 30, 42] -> best_yes_bid = 42
        # no:  [1, 2, 3, 4]    -> best_no_bid = 4 -> yes_ask = 100 - 4 = 96
        mock_orderbook.return_value = {
            "orderbook": {
                "yes": [[10, 100], [20, 50], [30, 20], [42, 13]],
                "no": [[1, 200], [2, 50], [3, 30], [4, 10]],
            }
        }
        top = self.client.get_top_of_book("KXBTC-26FEB2112-T1")
        self.assertEqual(top["yes_bid"], 42, "best_yes_bid should be last element (42)")
        self.assertEqual(top["no_bid"], 4, "best_no_bid should be last element (4)")
        self.assertEqual(top["yes_ask"], 96, "yes_ask = 100 - best_no_bid = 96")
        self.assertEqual(top["no_ask"], 58, "no_ask = 100 - best_yes_bid = 58")

    @patch.object(KalshiClient, "get_market_orderbook")
    def test_single_level_still_works(self, mock_orderbook):
        mock_orderbook.return_value = {
            "orderbook": {"yes": [[42, 10]], "no": [[4, 20]]},
        }
        top = self.client.get_top_of_book("KXBTC-X")
        self.assertEqual(top["yes_bid"], 42)
        self.assertEqual(top["no_bid"], 4)
        self.assertEqual(top["yes_ask"], 96)
        self.assertEqual(top["no_ask"], 58)

    @patch.object(KalshiClient, "get_market_orderbook")
    def test_empty_orderbook_returns_none(self, mock_orderbook):
        mock_orderbook.return_value = {"orderbook": {"yes": [], "no": []}}
        top = self.client.get_top_of_book("KXBTC-X")
        self.assertIsNone(top["yes_bid"])
        self.assertIsNone(top["no_bid"])
        self.assertIsNone(top["yes_ask"])
        self.assertIsNone(top["no_ask"])


if __name__ == "__main__":
    unittest.main()
