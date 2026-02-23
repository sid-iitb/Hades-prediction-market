"""
Structured logging and console summary for the bot.
"""
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from bot.execution import ExecutionResult
from bot.state import get_per_ticker_counts, get_total_order_count
from bot.strategy import Signal

# Console tee: writes to both stdout and console log file (append)
_console_tee: Optional["_TeeWriter"] = None


class _TeeWriter:
    """Write to both stdout and a file (append mode)."""
    def __init__(self, file_path: str):
        self._stdout = sys.__stdout__
        self._path = Path(file_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self._path, "a", encoding="utf-8")

    def write(self, data: str):
        self._stdout.write(data)
        self._file.write(data)
        self._file.flush()

    def flush(self):
        self._stdout.flush()
        self._file.flush()

    def close(self):
        self._file.close()


def setup_console_log(console_log_file: Optional[str], project_root: Optional[Path] = None) -> None:
    """
    Redirect stdout so console output is appended to console_log_file.
    Call early in main(). Survives Ctrl+C and restart.
    """
    global _console_tee
    if not console_log_file or not console_log_file.strip():
        return
    path = Path(console_log_file)
    if not path.is_absolute() and project_root:
        path = project_root / path
    path = path.resolve()
    _console_tee = _TeeWriter(str(path))
    sys.stdout = _console_tee


def setup_logging(log_file: str, level: str = "INFO") -> logging.Logger:
    """
    Configure logging. JSON entries go to file only.
    Console output is via print_console_summary() only (avoids duplicate output).
    """
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("hades_bot")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(fh)
    # No StreamHandler - log_run JSON goes to file only; console uses print_console_summary
    return logger


def log_run(
    logger: logging.Logger,
    ctx: Any,
    tickers_checked: int,
    signals: List[Signal],
    execution_results: List[ExecutionResult],
    db_path: str,
    hour_market_id: str,
    exit_out: Dict[str, Any] = None,
    asset: str = None,
) -> None:
    """Write structured log entry for a run."""
    sig_yes = sum(1 for s in signals if s.side == "yes")
    sig_no = sum(1 for s in signals if s.side == "no")
    would_trade = sum(1 for r in execution_results if r.status in ("PLACED", "WOULD_TRADE"))
    total_orders = get_total_order_count(db_path, hour_market_id)
    per_ticker = get_per_ticker_counts(db_path, hour_market_id, limit=10)

    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "asset": asset or "btc",
        "market_id": hour_market_id,
        "market_hour_window": ctx.market_hour_la if hasattr(ctx, "market_hour_la") else hour_market_id,
        "minutes_to_close": getattr(ctx, "minutes_to_close", None),
        "tickers_checked": tickers_checked,
        "signals": [
            {"ticker": s.ticker, "side": s.side, "price": s.price, "reason": s.reason, "late_window": s.late_window}
            for s in signals
        ],
        "summary": {
            "signals_yes": sig_yes,
            "signals_no": sig_no,
            "would_trade_count": would_trade,
        },
        "cap_usage": {
            "total_orders_this_hour": total_orders,
            "per_ticker_top10": [{"ticker": t, "count": c} for t, c in per_ticker],
        },
        "execution": [
            {"ticker": r.signal.ticker, "side": r.signal.side, "status": r.status, "order_id": r.order_id, "error": r.error}
            for r in execution_results
        ],
    }
    if exit_out:
        entry["exit_criteria"] = {
            "positions_checked": exit_out.get("positions_checked", 0),
            "exits": [r for r in exit_out.get("exit_results", []) if r.get("action") in ("STOP_LOSS", "TAKE_PROFIT")],
        }
    logger.info(json.dumps(entry, default=str))


def print_console_summary(
    ctx: Any,
    tickers_checked: int,
    signals: List[Signal],
    execution_results: List[ExecutionResult],
    db_path: str,
    hour_market_id: str,
    mode: str,
    exit_out: Dict[str, Any] = None,
    asset: str = None,
) -> None:
    """Print human-readable console summary."""
    total_orders = get_total_order_count(db_path, hour_market_id)
    per_ticker = get_per_ticker_counts(db_path, hour_market_id, limit=10)
    sig_yes = sum(1 for s in signals if s.side == "yes")
    sig_no = sum(1 for s in signals if s.side == "no")

    asset_label = (asset or "btc").upper()
    print("\n" + "=" * 60)
    print(f"HADES BOT RUN SUMMARY [{asset_label}]")
    print("=" * 60)
    print(f"  Timestamp:        {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Asset:            {asset_label}")
    print(f"  Market ID:        {hour_market_id}")
    print(f"  Market Hour:      {getattr(ctx, 'market_hour_la', hour_market_id)}")
    print(f"  Minutes to close: {getattr(ctx, 'minutes_to_close', 'N/A')}")
    print(f"  Mode:             {mode}")
    print("-" * 60)
    print(f"  Tickers checked:  {tickers_checked}")
    print(f"  Signals YES:      {sig_yes}")
    print(f"  Signals NO:       {sig_no}")
    print(f"  Would trade:      {len([r for r in execution_results if r.status in ('PLACED', 'WOULD_TRADE')])}")
    print("-" * 60)
    print("  SIGNALS:")
    for s in signals:
        print(f"    - {s.ticker} | {s.side.upper()} @ {s.price}c | {s.reason} | late={s.late_window}")
    if not signals:
        print("    (none)")
    print("-" * 60)
    print("  CAP USAGE:")
    print(f"    Total orders (this hour): {total_orders}")
    print("    Per ticker (top 10):")
    for ticker, count in per_ticker:
        print(f"      {ticker}: {count}")
    if not per_ticker:
        print("      (none)")
    if exit_out:
        print("-" * 60)
        print("  EXIT CRITERIA:")
        n = exit_out.get("positions_checked", 0)
        exits = [r for r in exit_out.get("exit_results", []) if r.get("action") in ("STOP_LOSS", "TAKE_PROFIT")]
        print(f"    Positions checked: {n}")
        for r in exits:
            pos = r.get("position", {})
            print(f"    EXIT {r.get('action')}: {pos.get('ticker')} {pos.get('side', '').upper()} PnL={((r.get('pnl_pct') or 0)*100):.2f}%")
        if not exits and n > 0:
            print("    All positions held (no stop-loss/take-profit triggered)")
        if n == 0:
            print("    No positions in current hour market")
    print("-" * 60)
    print("  EXECUTION:")
    for r in execution_results:
        extra = f" | order_id={r.order_id}" if r.order_id else ""
        extra = extra + f" | error={r.error}" if r.error else extra
        print(f"    {r.signal.ticker} {r.signal.side.upper()}: {r.status}{extra}")
    if not execution_results:
        print("    (none)")
    print("=" * 60 + "\n")


def log_run_15min(
    logger: logging.Logger,
    market_id: str,
    minutes_to_close: float,
    quote_ticker: str,
    signals: List[Any],
    execution_results: List[ExecutionResult],
    db_path: str,
    asset: str = "btc",
    yes_ask: Optional[int] = None,
    no_ask: Optional[int] = None,
    no_signal_reason: Optional[str] = None,
    exit_out: Optional[Dict[str, Any]] = None,
) -> None:
    """Write structured log entry for a 15-min run."""
    sig_yes = sum(1 for s in signals if s.side == "yes")
    sig_no = sum(1 for s in signals if s.side == "no")
    total_orders = get_total_order_count(db_path, market_id)
    per_ticker = get_per_ticker_counts(db_path, market_id, limit=10)
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "interval": "15min",
        "asset": asset,
        "market_id": market_id,
        "minutes_to_close": minutes_to_close,
        "quote_ticker": quote_ticker,
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "no_signal_reason": no_signal_reason,
        "signals": [
            {"ticker": s.ticker, "side": s.side, "price": s.price, "reason": s.reason}
            for s in signals
        ],
        "summary": {
            "signals_yes": sig_yes,
            "signals_no": sig_no,
            "would_trade_count": sum(1 for r in execution_results if r.status in ("PLACED", "WOULD_TRADE")),
        },
        "cap_usage": {"total_orders_this_15min": total_orders},
        "execution": [
            {"ticker": r.signal.ticker, "side": r.signal.side, "status": r.status, "order_id": r.order_id, "error": r.error}
            for r in execution_results
        ],
    }
    if exit_out:
        entry["exit_criteria"] = {
            "positions_checked": exit_out.get("positions_checked", 0),
            "exits": [r for r in exit_out.get("exit_results", []) if r.get("action") in ("STOP_LOSS", "TAKE_PROFIT")],
        }
    logger.info(json.dumps(entry, default=str))


def print_console_summary_15min(
    market_id: str,
    minutes_to_close: float,
    signals: List[Any],
    execution_results: List[ExecutionResult],
    db_path: str,
    mode: str,
    asset: str = "btc",
    yes_ask: Optional[int] = None,
    no_ask: Optional[int] = None,
    no_signal_reason: Optional[str] = None,
    exit_out: Optional[Dict[str, Any]] = None,
) -> None:
    """Print human-readable console summary for 15-min run."""
    total_orders = get_total_order_count(db_path, market_id)
    asset_label = (asset or "btc").upper()
    print("\n" + "=" * 60)
    print(f"HADES BOT 15-MIN RUN [{asset_label}]")
    print("=" * 60)
    print(f"  Timestamp:        {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Asset:            {asset_label}")
    print(f"  Market ID:        {market_id}")
    print(f"  Minutes to close: {minutes_to_close:.1f}")
    print(f"  Mode:             {mode}")
    print("-" * 60)
    print(f"  Signals:          {len(signals)}")
    for s in signals:
        print(f"    - {s.ticker} | {s.side.upper()} @ {s.price}c | {s.reason}")
    if not signals:
        print(f"    (none) {no_signal_reason or 'No market data'}")
    print("-" * 60)
    print(f"  Cap usage (this 15min): {total_orders}")
    if exit_out:
        print("-" * 60)
        print("  EXIT CRITERIA:")
        n = exit_out.get("positions_checked", 0)
        exits = [r for r in exit_out.get("exit_results", []) if r.get("action") in ("STOP_LOSS", "TAKE_PROFIT")]
        print(f"    Positions checked: {n}")
        for r in exits:
            pos = r.get("position", {})
            print(f"    EXIT {r.get('action')}: {pos.get('ticker')} {pos.get('side', '').upper()} PnL={((r.get('pnl_pct') or 0)*100):.2f}%")
        if not exits and n > 0:
            print("    All positions held (no stop-loss/take-profit triggered)")
        if n == 0:
            print("    No positions in current 15min market")
    print("-" * 60)
    print("  EXECUTION:")
    for r in execution_results:
        extra = f" | order_id={r.order_id}" if r.order_id else ""
        if r.error:
            extra = extra + f" | error={r.error}"
        print(f"    {r.signal.ticker} {r.signal.side.upper()}: {r.status}{extra}")
    if not execution_results:
        print("    (none)")
    print("=" * 60 + "\n")
