"""
Verify order size (max_cost_cents) for hourly and 15-min BTC/ETH.
Config: $10 (1000 cents). Roll re-entry uses 5x = $50 (5000 cents) for hourly only.
"""
import os
import sys
import tempfile
from pathlib import Path

# Project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bot.config_loader import load_config, resolve_config_path
from bot.execution import execute_signals
from bot.state import ensure_state_db
from bot.strategy import Signal


def _resolve_max_cost(config: dict, interval: str, asset: str, max_cost_override: int = None) -> int:
    """Mirror logic from execute_signals to resolve max_cost_cents."""
    order_config = config.get("order", {})
    if interval == "15min":
        order_cfg = order_config.get("fifteen_min") or order_config
    else:
        order_cfg = order_config.get("hourly") or order_config
    base_max = order_cfg.get("max_cost_cents", order_config.get("max_cost_cents", 100))
    if max_cost_override is not None:
        return max_cost_override
    if asset:
        by_asset = order_cfg.get("max_cost_cents_by_asset") or {}
        return by_asset.get(str(asset).lower(), base_max)
    return base_max


def test_config_order_size():
    """Verify split config yields $10 for hourly/15min BTC/ETH; no 5x unless roll override."""
    config_path = resolve_config_path()
    assert config_path.parent / "common.yaml" in list(config_path.parent.iterdir()) or (
        config_path.parent / "common.yaml"
    ).exists(), "Expected split config (config/common.yaml)"
    config = load_config(str(config_path))

    # Normal hourly/15min: expect 1000 cents ($10) for btc, eth
    for interval in ("hourly", "15min"):
        order_cfg = (
            config.get("order", {}).get("hourly" if interval == "hourly" else "fifteen_min")
            or config.get("order", {})
        )
        base = order_cfg.get("max_cost_cents", 0)
        by_asset = order_cfg.get("max_cost_cents_by_asset") or {}
        btc_max = by_asset.get("btc", base)
        eth_max = by_asset.get("eth", base)
        assert btc_max == 1000, f"hourly/fifteen_min btc max_cost_cents expected 1000, got {btc_max}"
        assert eth_max == 1000, f"hourly/fifteen_min eth max_cost_cents expected 1000, got {eth_max}"

    # Roll multiplier: 5x only for STOPLOSS_ROLL_REENTRY; base 1000 -> 5000
    roll_cfg = (
        (config.get("schedule") or {}).get("hourly_risk_guards") or {}
    ).get("stoploss_roll_reentry") or {}
    mult = roll_cfg.get("order_size_multiplier")
    assert mult == 5, f"stoploss_roll_reentry order_size_multiplier expected 5, got {mult}"
    # Simulated roll override: 1000 * 5 = 5000
    roll_override = int(1000 * float(mult))
    assert roll_override == 5000, f"roll override expected 5000, got {roll_override}"

    # Resolve via helper (mirrors execution)
    assert _resolve_max_cost(config, "hourly", "btc") == 1000
    assert _resolve_max_cost(config, "hourly", "eth") == 1000
    assert _resolve_max_cost(config, "15min", "btc") == 1000
    assert _resolve_max_cost(config, "15min", "eth") == 1000
    assert _resolve_max_cost(config, "hourly", "btc", max_cost_override=5000) == 5000

    print("OK: max_cost_cents = 1000 ($10) for hourly/15min BTC/ETH")
    print("OK: roll re-entry uses 5x = 5000 ($50) when order_size_multiplier=5")


def test_execution_contracts_mock():
    """With mocked top-of-book (ask=99), verify contracts = max_cost_cents // 99."""
    config_path = resolve_config_path()
    config = load_config(str(config_path))
    # Use a fake client that returns ask=99
    class MockClient:
        def get_top_of_book(self, ticker):
            return {"yes_bid": 98, "yes_ask": 99, "no_bid": 1, "no_ask": 2}

        def place_limit_order(self, *a, **kw):
            from unittest.mock import MagicMock
            r = MagicMock()
            r.status_code = 200
            r.json = lambda: {"order": {"order_id": "mock-id"}}
            r.text = "{}"
            return r

        def get_order(self, order_id):
            return {"status": "resting", "fill_count": 0}

        def cancel_order(self, order_id):
            pass

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        ensure_state_db(db_path)
    finally:
        pass  # cleanup after both runs

    sig = Signal(ticker="KXBTCD-TEST-T67749.99", side="yes", price=99, reason="YES_BUY_LATE", late_window=True)
    # Normal: max_cost_cents=1000 -> contracts = 1000//99 = 10
    results = execute_signals(
        [sig], "KXBTCD-TEST", db_path, MockClient(), config, "TRADE",
        max_cost_cents_override=None, asset="btc", interval="hourly",
    )
    assert len(results) == 1
    r = results[0]
    assert r.status == "PLACED"
    assert r.contracts == 10, f"Expected 10 contracts ($10 at 99c), got {r.contracts}"

    # Roll override: max_cost_cents_override=5000 -> contracts = 5000//99 = 50
    results2 = execute_signals(
        [sig], "KXBTCD-TEST2", db_path, MockClient(), config, "TRADE",
        max_cost_cents_override=5000, asset="btc", interval="hourly",
    )
    assert len(results2) == 1
    r2 = results2[0]
    assert r2.status == "PLACED"
    assert r2.contracts == 50, f"Expected 50 contracts ($50 at 99c), got {r2.contracts}"

    print("OK: normal order -> 10 contracts ($10)")
    print("OK: roll override (5000) -> 50 contracts ($50)")
    try:
        os.unlink(db_path)
    except Exception:
        pass


if __name__ == "__main__":
    test_config_order_size()
    test_execution_contracts_mock()
    print("\nAll order size checks passed.")
