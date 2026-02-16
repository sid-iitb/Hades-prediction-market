"""
Hourly summary - trades, YES/NO counts, P&L, cash balance.
Emitted when the bot transitions to a new hour (previous hour just ended).
"""
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from bot.state import (
    get_last_run_hour,
    get_side_order_count,
    get_total_order_count,
    set_last_run_hour,
)


@dataclass
class HourSummary:
    """Summary for one completed hour."""
    hour_market_id: str
    total_trades: int
    yes_trades: int
    no_trades: int
    cash_balance_cents: Optional[int] = None
    portfolio_value_cents: Optional[int] = None
    realized_pnl_cents: Optional[int] = None
    error: Optional[str] = None


def _parse_balance(balance: dict) -> tuple:
    """Extract cash and portfolio from Kalshi balance response."""
    cash = None
    portfolio = None
    if not balance:
        return cash, portfolio
    cash = balance.get("cash_balance_cents") or balance.get("cash_balance")
    portfolio = balance.get("portfolio_value_cents") or balance.get("portfolio_value")
    if cash is not None:
        try:
            cash = int(cash)
        except (TypeError, ValueError):
            cash = None
    if portfolio is not None:
        try:
            portfolio = int(portfolio)
        except (TypeError, ValueError):
            portfolio = None
    return cash, portfolio


def _parse_settlements_pnl(settlements: list, event_prefix: str) -> Optional[int]:
    """Sum realized PnL from settlements matching the hour's event."""
    event_prefix = (event_prefix or "").upper()
    total = 0
    for s in settlements or []:
        ticker = (s.get("ticker") or s.get("market_ticker") or "").upper()
        if not ticker.startswith(event_prefix):
            continue
        revenue = s.get("revenue") or s.get("revenue_cents") or 0
        yes_cost = s.get("yes_total_cost") or s.get("yes_total_cost_cents") or 0
        no_cost = s.get("no_total_cost") or s.get("no_total_cost_cents") or 0
        try:
            total += int(revenue) - int(yes_cost) - int(no_cost)
        except (TypeError, ValueError):
            pass
    return total if total != 0 else None


def build_hour_summary(
    hour_market_id: str,
    db_path: str,
    kalshi_client: Optional[Any] = None,
) -> HourSummary:
    """
    Build summary for a completed hour.
    Uses bot_state for trade counts; Kalshi API for balance and settlements.
    """
    total_trades = get_total_order_count(db_path, hour_market_id)
    yes_trades = get_side_order_count(db_path, hour_market_id, "yes")
    no_trades = get_side_order_count(db_path, hour_market_id, "no")

    cash_cents = None
    portfolio_cents = None
    realized_pnl = None
    error = None

    if kalshi_client is not None:
        try:
            balance = kalshi_client.get_balance()
            cash_cents, portfolio_cents = _parse_balance(balance)
        except Exception as e:
            error = str(e)
        try:
            settlements = kalshi_client.get_all_settlements(limit=200)
            raw = settlements if isinstance(settlements, list) else settlements.get("settlements", [])
            realized_pnl = _parse_settlements_pnl(raw, hour_market_id)
        except Exception:
            pass

    return HourSummary(
        hour_market_id=hour_market_id,
        total_trades=total_trades,
        yes_trades=yes_trades,
        no_trades=no_trades,
        cash_balance_cents=cash_cents,
        portfolio_value_cents=portfolio_cents,
        realized_pnl_cents=realized_pnl,
        error=error,
    )


def format_cents(cents: Optional[int]) -> str:
    """Format cents as dollars for display."""
    if cents is None:
        return "N/A"
    return f"${cents / 100:.2f}"


def log_hour_summary(logger: logging.Logger, summary: HourSummary) -> None:
    """Write hour summary to log file."""
    entry = {
        "type": "hour_summary",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "hour_market_id": summary.hour_market_id,
        "total_trades": summary.total_trades,
        "yes_trades": summary.yes_trades,
        "no_trades": summary.no_trades,
        "cash_balance_cents": summary.cash_balance_cents,
        "portfolio_value_cents": summary.portfolio_value_cents,
        "realized_pnl_cents": summary.realized_pnl_cents,
        "error": summary.error,
    }
    logger.info(json.dumps(entry, default=str))


def _asset_from_market_id(hour_market_id: str) -> str:
    """Infer asset from hour market id (KXBTCD vs KXETHD)."""
    mid = (hour_market_id or "").upper()
    return "ETH" if "KXETHD" in mid or mid.startswith("KXETHD") else "BTC"


def print_hour_summary(summary: HourSummary) -> None:
    """Print hour summary to console."""
    asset = _asset_from_market_id(summary.hour_market_id)
    print("\n" + "=" * 70)
    print(f"  HOUR SUMMARY [{asset}] (Previous Hour Ended)")
    print("=" * 70)
    print(f"  Hour Market:     {summary.hour_market_id}")
    print(f"  Total Trades:    {summary.total_trades}")
    print(f"  YES Trades:      {summary.yes_trades}")
    print(f"  NO Trades:       {summary.no_trades}")
    print("-" * 70)
    print(f"  Cash Balance:    {format_cents(summary.cash_balance_cents)}")
    print(f"  Portfolio Value: {format_cents(summary.portfolio_value_cents)}")
    print(f"  Realized P&L:    {format_cents(summary.realized_pnl_cents)}")
    if summary.error:
        print(f"  (Balance error: {summary.error})")
    print("=" * 70 + "\n")


def check_and_emit_hour_summary(
    current_hour_market_id: str,
    db_path: str,
    kalshi_client: Optional[Any],
    logger: Optional[logging.Logger] = None,
    asset: Optional[str] = None,
) -> Optional[HourSummary]:
    """
    If we just transitioned to a new hour, build and emit summary for the previous hour.
    asset: for per-asset last-hour tracking (btc, eth).
    Returns the summary if emitted, else None.
    """
    last = get_last_run_hour(db_path, asset=asset)
    if not last or last == current_hour_market_id:
        return None

    summary = build_hour_summary(last, db_path, kalshi_client)
    if logger:
        log_hour_summary(logger, summary)
    print_hour_summary(summary)
    return summary
