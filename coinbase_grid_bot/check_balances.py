#!/usr/bin/env python3
"""
Diagnostic: print all Coinbase account balances (API view) with balance > 0.
Uses same CDP auth as the grid bot (COINBASE_KEY_FILE or COINBASE_API_KEY + COINBASE_API_SECRET).
Run from repo root: python coinbase_grid_bot/check_balances.py
"""
import os
import sys

_script_dir = os.path.dirname(os.path.abspath(__file__))

from dotenv import load_dotenv
load_dotenv()
load_dotenv(os.path.join(_script_dir, "..", ".env"))

try:
    from coinbase.rest import RESTClient
except ImportError:
    print("Install coinbase-advanced-py: pip install coinbase-advanced-py")
    sys.exit(1)


def make_client():
    key_file = os.environ.get("COINBASE_KEY_FILE") or None
    if key_file:
        key_file = key_file.strip()
    api_key = os.environ.get("COINBASE_API_KEY") or None
    api_secret = os.environ.get("COINBASE_API_SECRET") or None
    if key_file and os.path.isfile(key_file):
        return RESTClient(key_file=key_file)
    if api_key and api_secret:
        return RESTClient(api_key=api_key, api_secret=api_secret)
    print("Set COINBASE_KEY_FILE or COINBASE_API_KEY + COINBASE_API_SECRET in .env")
    sys.exit(1)


def main():
    client = make_client()
    rows = []
    cursor = None
    while True:
        resp = client.get_accounts(limit=250, cursor=cursor)
        accounts = getattr(resp, "accounts", None) or []
        for acc in accounts:
            currency = getattr(acc, "currency", None) or "?"
            ab = getattr(acc, "available_balance", None)
            try:
                val = float(getattr(ab, "value", 0) or 0)
            except (TypeError, ValueError):
                val = 0.0
            if val <= 0:
                continue
            account_uuid = getattr(acc, "uuid", None) or ""
            portfolio_id = getattr(acc, "retail_portfolio_id", None) or ""
            name = getattr(acc, "name", None) or ""
            rows.append((currency, val, account_uuid, portfolio_id, name))
        if not getattr(resp, "has_next", False):
            break
        cursor = getattr(resp, "cursor", None)
        if not cursor:
            break

    if not rows:
        print("No accounts with balance > 0 returned by the API.")
        print("(If the app shows balance, it may be in a different portfolio or product.)")
        return

    print("Accounts with balance > 0 (as seen by the API):\n")
    print(f"{'Currency':<12} {'Available':>14} {'Account UUID':<38} {'Portfolio ID':<38} Name")
    print("-" * 12 + " " + "-" * 14 + " " + "-" * 38 + " " + "-" * 38 + " " + "-" * 24)
    for currency, val, account_uuid, portfolio_id, name in sorted(rows, key=lambda r: (-r[1], r[0])):
        print(f"{currency:<12} {val:>14.8} {account_uuid:<38} {portfolio_id or '(none)':<38} {name[:24]}")
    print()
    print(f"Total rows: {len(rows)}")


if __name__ == "__main__":
    main()
