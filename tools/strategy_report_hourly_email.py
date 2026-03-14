#!/usr/bin/env python3
"""
Hourly strategy report job: generate canonical TSV ledger, compute summary for last 1 hour,
send email with summary in body and the two strategy report TSV files as attachments.

Keeps the consolidated ledger on disk: reports/strategy_report_last_90s_limit_99.tsv and
reports/strategy_report_hourly_last_90s_limit_99.tsv.

If the report shows all zeros and TSVs are empty, the strategy_report table has no rows. Either
run the main bot with last_90s_limit_99 and hourly_last_90s_limit_99 enabled so it records
signals, or backfill from existing placement TSVs once:
  python tools/backfill_strategy_report_from_placements.py

Run every hour (e.g. cron or a separate daemon):
  python tools/strategy_report_hourly_email.py --send-email
  python -m bot.main --config config/config.yaml --strategy-report-hourly   # daemon loop

Usage:
  python tools/strategy_report_hourly_email.py [--config config/config.yaml] [--send-email]
  python tools/strategy_report_hourly_email.py --since-hours 1 --send-email
  python tools/strategy_report_hourly_email.py --since-hours 1 --refresh-from-kalshi --send-email   # fetch order/fill and market result from Kalshi API before report
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load .env for SMTP_*
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass


def _db_path(project_root: Path, config_path: str) -> str:
    try:
        from bot.config_loader import load_config, resolve_config_path
        cfg_path = Path(config_path) if config_path and Path(config_path).exists() else resolve_config_path(project_root)
        if not cfg_path.exists():
            return str(project_root / "data" / "bot_state.db")
        config = load_config(str(cfg_path))
        return (config.get("state") or {}).get("db_path", str(project_root / "data" / "bot_state.db"))
    except Exception:
        return str(project_root / "data" / "bot_state.db")


def refresh_report_from_kalshi(db_path: str, since_ts_utc: str) -> int:
    """
    For report rows in the time window that have order_id, fetch order status and market
    result from Kalshi API and update strategy_report (filled, final_outcome, pnl_cents).
    Returns number of rows updated.
    """
    from bot.strategy_report_db import get_all_rows, update_resolution, update_resolution_last_90s, update_resolution_hourly_last_90s
    rows = get_all_rows(db_path, strategy_name=None, since_ts_utc=since_ts_utc)
    with_order = [r for r in rows if (r.get("order_id") or "").strip()]
    if not with_order:
        return 0
    try:
        from src.client.kalshi_client import KalshiClient
        client = KalshiClient()
    except Exception as e:
        print(f"[strategy_report_hourly] Kalshi client init failed (skip refresh): {e}", file=sys.stderr)
        return 0
    try:
        from bot.market import fetch_15min_market_result, fetch_ticker_outcome
    except ImportError:
        print("[strategy_report_hourly] bot.market not available (skip refresh)", file=sys.stderr)
        return 0
    updated = 0
    for r in with_order:
        order_id = (r.get("order_id") or "").strip()
        strategy = (r.get("strategy_name") or "").strip()
        side = ((r.get("side") or "yes").lower())[:3]
        price_cents = r.get("price_cents")
        try:
            order = client.get_order(order_id)
        except Exception as e:
            print(f"[strategy_report_hourly] get_order {order_id} failed: {e}", file=sys.stderr)
            continue
        fill_count = int(order.get("fill_count") or order.get("filled_count") or 0)
        if strategy == "last_90s_limit_99":
            market_id = (r.get("market") or r.get("window_id") or "").strip()
            market_result = fetch_15min_market_result(market_id) if market_id else None
        else:
            ticker = (r.get("ticker") or "").strip()
            market_result = fetch_ticker_outcome(ticker) if ticker else None
        if fill_count > 0 and market_result in ("yes", "no"):
            outcome = "positive" if (side == "yes" and market_result == "yes") or (side == "no" and market_result == "no") else "negative"
            res_price = 100 if outcome == "positive" else 0
            # Preserve STOP_LOSS: Kalshi only has buy/sell; we set STOP_LOSS when we sold to close.
            # Keep it so we can analyze: "stopped out; market resolved yes/no" (tight SL vs bet was wrong).
            existing_outcome = (r.get("final_outcome") or "").strip().upper()
            if existing_outcome == "STOP_LOSS":
                final = "STOP_LOSS"
                pnl = r.get("pnl_cents")  # keep actual stop-loss PnL
            else:
                final = "WIN" if outcome == "positive" else "LOSS"
                pnl = None
                if price_cents is not None:
                    pnl = (100 - int(price_cents)) * fill_count if outcome == "positive" else -int(price_cents) * fill_count
        else:
            final = r.get("final_outcome")  # preserve existing (e.g. STOP_LOSS) if no resolution yet
            res_price = None
            pnl = r.get("pnl_cents")  # preserve so we don't overwrite STOP_LOSS pnl or other outcome
        try:
            if strategy == "last_90s_limit_99":
                update_resolution_last_90s(
                    db_path,
                    order_id=order_id,
                    window_id=r.get("window_id"),
                    asset=r.get("asset"),
                    filled=1 if fill_count > 0 else 0,
                    fill_price=int(price_cents) if price_cents is not None else None,
                    resolution_price=res_price,
                    final_outcome=final,
                    pnl_cents=pnl,
                    respect_stop_loss=True,
                )
            elif strategy == "hourly_last_90s_limit_99":
                update_resolution_hourly_last_90s(
                    db_path,
                    order_id=order_id,
                    window_id=r.get("window_id"),
                    asset=r.get("asset"),
                    ticker=r.get("ticker"),
                    side=r.get("side"),
                    filled=1 if fill_count > 0 else 0,
                    fill_price=int(price_cents) if price_cents is not None else None,
                    resolution_price=res_price,
                    final_outcome=final,
                    pnl_cents=pnl,
                )
            else:
                update_resolution(
                    db_path,
                    order_id,
                    filled=1 if fill_count > 0 else 0,
                    fill_price=int(price_cents) if price_cents is not None else None,
                    resolution_price=res_price,
                    final_outcome=final,
                    pnl_cents=pnl,
                )
            updated += 1
        except Exception as e:
            print(f"[strategy_report_hourly] update_resolution {order_id} failed: {e}", file=sys.stderr)
    return updated


def _bucket_15min(ts_utc: str) -> str:
    """Round ts_utc down to 15-min boundary (e.g. 2026-02-27T21:28:57 -> 2026-02-27T21:15)."""
    if not ts_utc or len(ts_utc) < 16:
        return ts_utc or ""
    try:
        minute = int(ts_utc[14:16])
        slot = (minute // 15) * 15
        return ts_utc[:14] + f"{slot:02d}"
    except (ValueError, IndexError):
        return ts_utc[:16] if len(ts_utc) >= 16 else ts_utc


def _bucket_hour(ts_utc: str) -> str:
    """Truncate ts_utc to hour (e.g. 2026-02-27T21:28:57 -> 2026-02-27T21). For hourly_last_90s_limit_99 sanity check."""
    if not ts_utc or len(ts_utc) < 13:
        return ts_utc or ""
    return ts_utc[:13]  # YYYY-MM-DDTHH


def _summary_for_rows(rows: list[dict]) -> dict[str, Any]:
    """Compute counts for lean report: skipped, placed_limit, placed_market, filled, resolved, stop_losses.
    Each row is one (window, asset, ticker, side). limit_or_market is 'limit' or 'market' for placed rows.
    """
    n = len(rows)
    if n == 0:
        return {
            "signals": 0,
            "unique_asset_15min": 0,
            "unique_asset_hour": 0,
            "skipped": 0,
            "placed": 0,
            "placed_limit": 0,
            "placed_market": 0,
            "placed_other": 0,
            "filled": 0,
            "wins": 0,
            "losses": 0,
            "stop_losses": 0,
            "resolved": 0,
            "skip_pct": 0.0,
            "placement_pct": 0.0,
            "fill_pct": 0.0,
            "win_rate": 0.0,
            "pnl_cents_sum": 0,
            "ev_cents": 0.0,
        }
    skipped = sum(1 for r in rows if (r.get("skip_reason") or "").strip())
    placed = sum(1 for r in rows if r.get("placed"))
    placed_limit = sum(1 for r in rows if r.get("placed") and (r.get("limit_or_market") or "").strip().lower() == "limit")
    placed_market = sum(1 for r in rows if r.get("placed") and (r.get("limit_or_market") or "").strip().lower() == "market")
    # So that Skipped + Placed (limit) + Placed (market) + Placed (other) = total signals
    placed_other = max(0, placed - placed_limit - placed_market)
    filled = sum(1 for r in rows if r.get("filled"))
    wins = sum(1 for r in rows if (r.get("final_outcome") or "").strip().upper() == "WIN")
    losses = sum(1 for r in rows if (r.get("final_outcome") or "").strip().upper() == "LOSS")
    stop_losses = sum(1 for r in rows if (r.get("final_outcome") or "").strip().upper() == "STOP_LOSS")
    resolved = wins + losses + stop_losses
    pnl_sum = sum(int(r.get("pnl_cents") or 0) for r in rows if r.get("pnl_cents") is not None)
    win_rate = (wins / resolved * 100.0) if resolved else 0.0
    ev_cents = (pnl_sum / n) if n else 0.0
    unique_15min = set()
    unique_hour = set()
    for r in rows:
        ts = (r.get("ts_utc") or "").strip()
        asset = (r.get("asset") or "").strip()
        if ts and asset:
            unique_15min.add(asset + "\t" + _bucket_15min(ts)[:16])
            unique_hour.add(asset + "\t" + _bucket_hour(ts))
    return {
        "signals": n,
        "unique_asset_15min": len(unique_15min),
        "unique_asset_hour": len(unique_hour),
        "skipped": skipped,
        "placed": placed,
        "placed_limit": placed_limit,
        "placed_market": placed_market,
        "placed_other": placed_other,
        "filled": filled,
        "wins": wins,
        "losses": losses,
        "stop_losses": stop_losses,
        "resolved": resolved,
        "skip_pct": (skipped / n * 100.0) if n else 0.0,
        "placement_pct": (placed / n * 100.0) if n else 0.0,
        "fill_pct": (filled / n * 100.0) if n else 0.0,
        "win_rate": win_rate,
        "pnl_cents_sum": pnl_sum,
        "ev_cents": ev_cents,
    }


def _load_config_snippets_for_email(project_root: Path) -> str:
    """Load strategy config snippets from hourly.yaml and fifteen_min.yaml for email verification."""
    lines_out = [
        "",
        "--- Config (for verification) ---",
    ]
    # hourly_last_90s_limit_99: config/hourly.yaml lines 7-34 (1-based)
    hourly_path = project_root / "config" / "hourly.yaml"
    if hourly_path.exists():
        try:
            with open(hourly_path, encoding="utf-8") as f:
                file_lines = f.readlines()
            snippet = "".join(file_lines[6:34])  # 0-based: 6..33
            lines_out.append("--- hourly_last_90s_limit_99 (config/hourly.yaml) ---")
            lines_out.append(snippet.rstrip())
        except Exception:
            lines_out.append("--- hourly_last_90s_limit_99 (config/hourly.yaml) --- (read error)")
    else:
        lines_out.append("--- hourly_last_90s_limit_99 (config/hourly.yaml) --- (file not found)")
    lines_out.append("")
    # last_90s_limit_99: config/fifteen_min.yaml lines 9-31 (1-based; under fifteen_min: key)
    fifteen_path = project_root / "config" / "fifteen_min.yaml"
    if fifteen_path.exists():
        try:
            with open(fifteen_path, encoding="utf-8") as f:
                file_lines = f.readlines()
            snippet = "".join(file_lines[8:31])  # 0-based: 8..30
            lines_out.append("--- last_90s_limit_99 (config/fifteen_min.yaml) ---")
            lines_out.append(snippet.rstrip())
        except Exception:
            lines_out.append("--- last_90s_limit_99 (config/fifteen_min.yaml) --- (read error)")
    else:
        lines_out.append("--- last_90s_limit_99 (config/fifteen_min.yaml) --- (file not found)")
    return "\n".join(lines_out)


def _format_cents(cents: int | None) -> str:
    """Format cents as dollars for display."""
    if cents is None:
        return "—"
    try:
        return "$%.2f" % (int(cents) / 100.0)
    except (TypeError, ValueError):
        return "—"


def _format_summary_text(
    summary_last_1h: dict[str, dict],
    since_ts: str,
    to_ts: str,
    portfolio_snapshot: dict | None = None,
) -> str:
    """Lean email body: portfolio balance + per-strategy counts. Readable, minimal."""
    lines = [
        "Strategy Report — Last 1 hour",
        f"Window: {since_ts} → {to_ts} UTC",
        "",
        "  --- Portfolio summary ---",
    ]
    if portfolio_snapshot:
        cash = portfolio_snapshot.get("cash_available")
        pv = portfolio_snapshot.get("total_position_cost_cents")
        lines.append(f"    Balance (cash):      {_format_cents(cash)}")
        lines.append(f"    Portfolio value:     {_format_cents(pv)}")
    else:
        lines.append("    Balance (cash):      — (Kalshi API unavailable)")
        lines.append("    Portfolio value:     —")
    lines.append("")
    for strategy_label, key in [
        ("last_90s_limit_99", "last_90s_limit_99"),
        ("hourly_last_90s_limit_99", "hourly_last_90s_limit_99"),
    ]:
        s = summary_last_1h.get(key) or {}
        n_orders = s.get("signals", 0)
        lines.append(f"  --- {strategy_label} ({n_orders} rows) ---")
        lines.append(f"    Skipped:             {s.get('skipped', 0)}")
        lines.append(f"    Placed (limit):      {s.get('placed_limit', 0)}")
        lines.append(f"    Placed (market):     {s.get('placed_market', 0)}")
        placed_other = s.get("placed_other", 0)
        if placed_other:
            lines.append(f"    Placed (other):      {placed_other}")
        lines.append(f"    Filled:              {s.get('filled', 0)}")
        lines.append(f"    Resolved:            {s.get('resolved', 0)}")
        lines.append(f"    Stop loss:           {s.get('stop_losses', 0)}")
        # Sanity: Skipped + Placed (limit) + Placed (market) + Placed (other) = total
        lines.append(f"    ─────────────────────────────────")
        lines.append(f"    Total (Skipped+Placed): {s.get('skipped', 0) + s.get('placed', 0)}  (= {n_orders} signals)")
        lines.append("")
    lines.append("  Attached TSVs: one row per market per asset per ticker (lean ledger).")
    return "\n".join(lines)


def _format_stop_loss_details(rows_in_window: list) -> str:
    """Format stop-loss rows for email body (strategy, ticker, side, entry, resolution, pnl)."""
    stop_rows = [r for r in rows_in_window if (r.get("final_outcome") or "").strip().upper() == "STOP_LOSS"]
    if not stop_rows:
        return "\n--- Stop losses (in this window) ---\n  None.\n"
    lines = ["", "--- Stop losses (in this window) ---"]
    by_strategy = {}
    for r in stop_rows:
        s = (r.get("strategy_name") or "").strip() or "?"
        by_strategy.setdefault(s, []).append(r)
    for strategy in ("last_90s_limit_99", "hourly_last_90s_limit_99"):
        if strategy not in by_strategy:
            continue
        lines.append(f"  [{strategy}]")
        for r in by_strategy[strategy]:
            ticker = (r.get("ticker") or "").strip() or "—"
            side = (r.get("side") or "yes").strip().lower()
            entry = r.get("price_cents") or r.get("fill_price") or "—"
            res = r.get("resolution_price")
            res_str = "YES" if res == 100 else ("NO" if res == 0 else str(res) if res is not None else "—")
            pnl = r.get("pnl_cents")
            pnl_str = f"{pnl}c" if pnl is not None else "—"
            ts = (r.get("ts_utc") or "")[:19]
            lines.append(f"    {ticker}  side={side}  entry={entry}c  resolution={res_str}  pnl={pnl_str}  {ts}")
        lines.append("")
    return "\n".join(lines)


def _last_4_window_suffixes_last_90s(rows: list[dict]) -> list[str]:
    """From last_90s rows, return the last 4 distinct window time suffixes (e.g. 26MAR011630)."""
    def suffix(w: str) -> str:
        if not w or "-" not in w:
            return ""
        return w.split("-")[-1]

    suffixes = sorted(set(suffix(r.get("window_id") or "") for r in rows if suffix(r.get("window_id") or "")))
    return suffixes[-4:] if len(suffixes) >= 4 else suffixes


def _hourly_run_key(window_id: str) -> str:
    """Extract run key DDMONHH from hourly window_id (e.g. KXBTC-26MAR0117_... -> 26MAR0117)."""
    if not window_id:
        return ""
    prefix = window_id.split("_")[0] if "_" in window_id else window_id
    parts = prefix.split("-")
    if len(parts) >= 2 and len(parts[1]) >= 7:
        return parts[1]
    return ""


def _last_1_run_key_hourly(rows: list[dict]) -> str:
    """From hourly_last_90s rows, return the last run key (DDMONHH, e.g. 26MAR0117)."""
    keys = sorted(set(_hourly_run_key(r.get("window_id") or "") for r in rows if _hourly_run_key(r.get("window_id") or "")))
    return keys[-1] if keys else ""


def run(
    config_path: str = "",
    send_email: bool = False,
    since_hours: float = 1.0,
    project_root: Path | None = None,
    refresh_from_kalshi: bool = False,
) -> tuple[Path, Path]:
    """
    Generate ledger TSVs, optionally send email. Returns (path_last_90s, path_hourly).
    When send_email is True, we default to refresh_from_kalshi (order resolution from Kalshi API)
    and fetch portfolio balance for the email body.
    """
    root = project_root or PROJECT_ROOT
    reports_dir = root / "reports"
    db_path = _db_path(root, config_path or str(root / "config" / "config.yaml"))

    since_dt = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    since_ts = since_dt.strftime("%Y-%m-%dT%H:%M:%S")
    to_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # When sending email, default to refreshing order resolution from Kalshi so report is up to date.
    do_refresh = refresh_from_kalshi or send_email
    if do_refresh:
        n = refresh_report_from_kalshi(db_path, since_ts)
        print(f"[strategy_report_hourly] Refreshed {n} rows from Kalshi API")

    from bot.strategy_report_db import get_all_rows, LAST_90S_COLUMNS, HOURLY_LAST_90S_COLUMNS
    all_rows = get_all_rows(db_path, strategy_name=None, since_ts_utc=since_ts)
    last_90s = [r for r in all_rows if (r.get("strategy_name") or "").strip() == "last_90s_limit_99"]
    hourly_rows = [r for r in all_rows if (r.get("strategy_name") or "").strip() == "hourly_last_90s_limit_99"]

    summary_last_1h = {
        "last_90s_limit_99": _summary_for_rows(last_90s),
        "hourly_last_90s_limit_99": _summary_for_rows(hourly_rows),
    }

    # TSV attachments: when since_hours <= 1 use last 4 windows (last_90s) + last 1 run (hourly); else include all in window
    all_rows_full = get_all_rows(db_path, strategy_name=None, since_ts_utc=since_ts)
    total_signals_1h = (summary_last_1h["last_90s_limit_99"].get("signals") or 0) + (summary_last_1h["hourly_last_90s_limit_99"].get("signals") or 0)
    last_90s_all = [r for r in all_rows_full if (r.get("strategy_name") or "").strip() == "last_90s_limit_99"]
    hourly_all = [r for r in all_rows_full if (r.get("strategy_name") or "").strip() == "hourly_last_90s_limit_99"]
    if total_signals_1h == 0 and len(all_rows_full) == 0:
        print("[strategy_report_hourly] No data in strategy_report table. Run the main bot with last_90s_limit_99 and hourly_last_90s_limit_99 enabled, or backfill once: python tools/backfill_strategy_report_from_placements.py")

    if since_hours <= 1:
        last_4_suffixes = _last_4_window_suffixes_last_90s(last_90s_all)
        last_90s_tsv = [r for r in last_90s_all if (r.get("window_id") or "").split("-")[-1] in last_4_suffixes]
        last_1_run_key = _last_1_run_key_hourly(hourly_all)
        hourly_tsv = [r for r in hourly_all if _hourly_run_key(r.get("window_id")) == last_1_run_key]
    else:
        last_90s_tsv = last_90s_all
        hourly_tsv = hourly_all
    last_90s_tsv.sort(key=lambda r: (r.get("window_id") or "", r.get("asset") or ""))
    hourly_tsv.sort(key=lambda r: (r.get("window_id") or "", r.get("asset") or "", r.get("ticker") or "", r.get("side") or ""))

    def _fmt(v):
        if v is None or v == "": return ""
        return str(v)

    p1 = reports_dir / "strategy_report_last_90s_limit_99.tsv"
    p2 = reports_dir / "strategy_report_hourly_last_90s_limit_99.tsv"
    reports_dir.mkdir(parents=True, exist_ok=True)
    # Lean schema: last 4 runs (1h) for last_90s; last 1 run for hourly_last_90s
    with open(p1, "w", encoding="utf-8") as f:
        f.write("\t".join(LAST_90S_COLUMNS) + "\n")
        for r in last_90s_tsv:
            f.write("\t".join(_fmt(r.get(c)) for c in LAST_90S_COLUMNS) + "\n")
    with open(p2, "w", encoding="utf-8") as f:
        f.write("\t".join(HOURLY_LAST_90S_COLUMNS) + "\n")
        for r in hourly_tsv:
            f.write("\t".join(_fmt(r.get(c)) for c in HOURLY_LAST_90S_COLUMNS) + "\n")

    if send_email:
        to_email = __import__("os").environ.get("SMTP_TO", "").strip()
        if not to_email:
            print("[strategy_report_hourly] SMTP_TO not set; skipping email")
        else:
            from bot.email_reporter import send_ledger_email, fetch_portfolio_snapshot
            subject = f"[Kalshi Bot] Strategy Report — Last {since_hours:.0f}h ({since_dt.strftime('%Y-%m-%d %H:%M')} UTC)"
            portfolio_snapshot = fetch_portfolio_snapshot()
            body = _format_summary_text(summary_last_1h, since_ts, to_ts, portfolio_snapshot=portfolio_snapshot)
            attachments = [p1, p2]
            send_ledger_email(to_email=to_email, subject=subject, body_text=body, attachments=attachments)
            print(f"Email sent to {to_email}")

    return (p1, p2)


def main() -> int:
    ap = argparse.ArgumentParser(description="Hourly strategy report: ledger TSVs + optional email")
    ap.add_argument("--config", default="", help="Config path")
    ap.add_argument("--send-email", action="store_true", help="Send email with summary and TSV attachments")
    ap.add_argument("--since-hours", type=float, default=1.0, help="Summary window in hours (default 1)")
    ap.add_argument("--refresh-from-kalshi", action="store_true", help="Fetch order/market data from Kalshi API and update report before generating")
    args = ap.parse_args()
    run(
        config_path=args.config,
        send_email=args.send_email,
        since_hours=args.since_hours,
        refresh_from_kalshi=getattr(args, "refresh_from_kalshi", False),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
