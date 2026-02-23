"""
Email reporter for ledger summaries and hourly strategy reports. Uses SMTP env vars.
"""
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _env(key: str, default: Optional[str] = None) -> str:
    v = os.environ.get(key, default)
    return (v or "").strip()


def send_ledger_email(
    to_email: str,
    subject: str,
    body_text: str,
    attachments: Optional[List[Path]] = None,
) -> None:
    """
    Send email with optional file attachments via SMTP.
    Env: SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM, (SMTP_TO used if to_email empty).
    """
    host = _env("SMTP_HOST")
    port = int(_env("SMTP_PORT", "587"))
    user = _env("SMTP_USER")
    password = _env("SMTP_PASS")
    from_addr = _env("SMTP_FROM") or user
    to_addr = to_email or _env("SMTP_TO") or ""
    if not to_addr:
        raise ValueError("to_email or SMTP_TO must be set")
    if not host:
        raise ValueError("SMTP_HOST must be set")

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.attach(MIMEText(body_text, "plain", "utf-8"))

    if attachments:
        for path in attachments:
            path = Path(path)
            if not path.exists():
                continue
            name = path.name
            with open(path, "rb") as f:
                part = MIMEApplication(f.read(), _subtype="octet-stream")
            part.add_header("Content-Disposition", "attachment", filename=name)
            msg.attach(part)

    with smtplib.SMTP(host, port) as server:
        if user and password:
            server.starttls()
            server.login(user, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())


def _balance_cents(balance: dict, cents_keys: List[str], dollar_keys: List[str]) -> Optional[int]:
    """Extract balance in cents from API response. Tries cents keys first, then dollar keys (×100)."""
    if not balance:
        return None
    for k in cents_keys:
        v = balance.get(k)
        if v is not None and v != "":
            try:
                return int(v)
            except (TypeError, ValueError):
                pass
    for k in dollar_keys:
        v = balance.get(k)
        if v is not None and v != "":
            try:
                return int(float(v) * 100)
            except (TypeError, ValueError):
                pass
    return None


def fetch_portfolio_snapshot() -> Dict[str, Any]:
    """
    Fetch cash balance and open positions from Kalshi API if available.
    Kalshi Get Balance API returns: balance (cents), portfolio_value (cents), updated_ts.
    Returns dict: cash_available, total_position_cost_cents, unrealized_pnl_cents,
    open_positions_count, open_positions.
    On API error or no client, returns same keys with None/empty values.
    """
    out = {
        "cash_available": None,
        "total_position_cost_cents": None,
        "unrealized_pnl_cents": None,
        "open_positions_count": 0,
        "open_positions": [],
    }
    try:
        from src.client.kalshi_client import KalshiClient
        client = KalshiClient()
        balance_raw = client.get_balance()
        # API may return {balance, portfolio_value, updated_ts} at top level or wrapped
        balance = balance_raw
        if isinstance(balance_raw, dict) and "balance" not in balance_raw and len(balance_raw) == 1:
            # e.g. {"balance_response": {...}}
            for v in balance_raw.values():
                if isinstance(v, dict) and ("balance" in v or "portfolio_value" in v):
                    balance = v
                    break
        if balance:
            # Kalshi docs: balance and portfolio_value are int64 cents — try exact keys first
            out["cash_available"] = _balance_cents(
                balance,
                ["balance", "cash_balance_cents", "available_cash_cents", "free_balance_cents", "spendable_balance_cents"],
                ["cash_balance", "available_cash", "free_balance", "spendable_balance"],
            )
            pv = _balance_cents(
                balance,
                ["portfolio_value", "portfolio_value_cents", "account_value_cents", "net_liquidation_value_cents"],
                ["portfolio_value", "account_value", "net_liquidation_value"],
            )
            if pv is not None and out["total_position_cost_cents"] is None:
                out["total_position_cost_cents"] = pv
        positions_resp = client.get_positions(limit=200)
        positions_list = positions_resp.get("market_positions") or positions_resp.get("positions") or []
        total_cost = 0
        rows = []
        for p in positions_list:
            ticker = p.get("ticker") or p.get("market_ticker") or p.get("event_ticker")
            side = (p.get("side") or p.get("position") or "yes").lower()
            pos = p.get("position") or p.get("count") or 0
            try:
                count = abs(int(pos))
            except (TypeError, ValueError):
                count = 0
            if count <= 0:
                continue
            # API may return avg_price in cents or as decimal 0-1
            avg_price = p.get("average_price_cents") or p.get("avg_price_cents")
            if avg_price is None:
                raw = p.get("average_price") or p.get("avg_price") or p.get("yes_price") or p.get("no_price")
                if raw is not None:
                    try:
                        avg_price = int(float(raw) * 100) if float(raw) <= 1.5 else int(float(raw))
                    except (TypeError, ValueError):
                        avg_price = None
            if avg_price is not None:
                avg_price = int(avg_price)
            cost = (avg_price or 0) * count
            total_cost += cost
            mark = p.get("mark_price_cents") or p.get("mark_price")
            current_price = None
            if mark is not None:
                try:
                    current_price = int(float(mark) * 100) if float(mark) <= 1.5 else int(float(mark))
                except (TypeError, ValueError):
                    current_price = int(mark) if isinstance(mark, (int, float)) else None
            pnl_pct = p.get("pnl_pct")
            if pnl_pct is None and current_price is not None and avg_price and avg_price > 0:
                pnl_pct = (current_price - avg_price) / avg_price
            rows.append({
                "interval": "",
                "asset": "",
                "ticker": ticker or "",
                "side": side.upper(),
                "entry_price": avg_price,
                "contracts": count,
                "current_price": current_price,
                "pnl_pct": pnl_pct,
                "seconds_to_close": None,
            })
        out["open_positions"] = rows
        out["open_positions_count"] = len(rows)
        if total_cost:
            out["total_position_cost_cents"] = total_cost
        elif out["total_position_cost_cents"] is None and balance:
            pv = _balance_cents(balance, ["portfolio_value", "portfolio_value_cents"], ["portfolio_value"])
            if pv is not None:
                out["total_position_cost_cents"] = pv
        out["unrealized_pnl_cents"] = positions_resp.get("unrealized_pnl_cents") if isinstance(positions_resp, dict) else None
    except Exception as e:
        import sys
        print("[email_reporter] Portfolio snapshot failed: %s" % e, file=sys.stderr)
    return out


def format_portfolio_block(snapshot: Dict[str, Any], as_of_ts: str) -> str:
    """Format portfolio snapshot as plain text for email body."""
    lines = [
        "PORTFOLIO SNAPSHOT (as of %s)" % as_of_ts,
        "",
        "  Cash Balance: %s" % (_format_cents(snapshot.get("cash_available"))),
        "  Total Cost (Open Positions): %s" % (_format_cents(snapshot.get("total_position_cost_cents"))),
        "  Open Positions Count: %s" % (snapshot.get("open_positions_count") or 0),
        "  Realized PnL (Last Hour): %s" % (_format_pnl_pct(snapshot.get("realized_pnl_last_hour"))),
        "  Stop-Loss Count (Last Hour): %s" % (snapshot.get("stoploss_count_last_hour", 0)),
        "  Unrealized PnL: %s" % (_format_cents(snapshot.get("unrealized_pnl_cents"))),
        "",
    ]
    positions = snapshot.get("open_positions") or []
    if not positions:
        lines.append("  (No open positions)")
        return "\n".join(lines)
    lines.append("  Open Positions:")
    lines.append("  " + " | ".join(["interval", "asset", "ticker", "side", "entry_price", "contracts", "current_price", "pnl_pct", "seconds_to_close"]))
    for p in positions[:30]:
        entry = p.get("entry_price") or ""
        cur = p.get("current_price") or ""
        pnl = p.get("pnl_pct")
        pnl_s = "%.2f%%" % (pnl * 100) if pnl is not None else ""
        sec = p.get("seconds_to_close") or ""
        lines.append("  " + " | ".join([
            str(p.get("interval", "")),
            str(p.get("asset", "")),
            str(p.get("ticker", "")),
            str(p.get("side", "")),
            str(entry),
            str(p.get("contracts", "")),
            str(cur),
            pnl_s,
            str(sec),
        ]))
    if len(positions) > 30:
        lines.append("  ... and %d more" % (len(positions) - 30))
    return "\n".join(lines)


def _format_cents(cents: Optional[int]) -> str:
    if cents is None:
        return "N/A"
    return "$%.2f" % (cents / 100.0)


def _format_pnl_pct(pnl: Optional[float]) -> str:
    if pnl is None:
        return "N/A"
    return "%.2f%%" % (float(pnl) * 100)


def format_strategy_summary_table(summary_rows: List[Dict]) -> str:
    """Table with header and separator line for strategy summary (email body)."""
    cols = ["interval", "asset", "attempts", "submitted", "failed", "skipped", "exited", "stoploss",
            "dist_fail", "persist_fail", "cutoff_block", "cap_block", "top_1_error"]
    widths = [10, 6, 8, 9, 6, 7, 6, 8, 9, 10, 11, 9, 22]
    lines = []
    header = " | ".join(c.ljust(w)[:w] for c, w in zip(cols, widths))
    sep = "-+-".join("-" * w for w in widths)
    lines.append(header)
    lines.append(sep)
    for r in summary_rows:
        submitted = r.get("submitted_count", 0)
        failed = r.get("failed_count", 0)
        skipped = r.get("skipped_count", 0)
        exited = r.get("exited_count", 0)
        stoploss = r.get("stoploss_count", 0)
        row = (
            str(r.get("interval", ""))[:10].ljust(10),
            str(r.get("asset", ""))[:6].ljust(6),
            str(r.get("trade_attempts", ""))[:8].ljust(8),
            str(submitted)[:9].ljust(9),
            str(failed)[:6].ljust(6),
            str(skipped)[:7].ljust(7),
            str(exited)[:6].ljust(6),
            str(stoploss)[:8].ljust(8),
            str(r.get("dist_fail_rate", ""))[:9].ljust(9),
            str(r.get("persist_fail_rate", ""))[:10].ljust(10),
            str(r.get("cutoff_block_rate", ""))[:11].ljust(11),
            str(r.get("cap_block_rate", ""))[:9].ljust(9),
            (str(r.get("top_1_error", "")) or "")[:22].ljust(22),
        )
        lines.append(" | ".join(row))
    return "\n".join(lines)


def _ledger_sort_priority(r: Dict) -> tuple:
    """Sort STOPLOSS/FAILED first for tuning focus."""
    status = str(r.get("exec_status") or "")
    reason = str(r.get("exec_reason") or "")
    if status == "EXITED" and "STOPLOSS" in reason:
        return (0, r.get("ts_first_seen", ""))
    if status == "FAILED":
        return (1, r.get("ts_first_seen", ""))
    if status in ("SUBMITTED", "OPEN"):
        return (3, r.get("ts_first_seen", ""))
    return (2, r.get("ts_first_seen", ""))


INTERVALS_ORDER = ("fifteen_min", "hourly", "daily", "weekly")


def _partition_by_interval(
    summary_rows: List[Dict], ledger_rows: List[Dict]
) -> Dict[str, Tuple[List[Dict], List[Dict]]]:
    """Partition summary and ledger rows by interval. Returns {interval: (summary_rows, ledger_rows)}."""
    by_interval: Dict[str, Tuple[List[Dict], List[Dict]]] = {}
    for r in summary_rows:
        iv = str(r.get("interval", "hourly") or "hourly")
        if iv == "15min":
            iv = "fifteen_min"
        if iv not in by_interval:
            by_interval[iv] = ([], [])
        by_interval[iv][0].append(r)
    for r in ledger_rows:
        iv = str(r.get("interval", "hourly") or "hourly")
        if iv == "15min":
            iv = "fifteen_min"
        if iv not in by_interval:
            by_interval[iv] = ([], [])
        by_interval[iv][1].append(r)
    return by_interval


def format_daily_weekly_ledger_table(ledger_rows: List[Dict], interval_label: str, max_rows: int = 30) -> str:
    """Table for daily/weekly: strike, entry price, exit reason, P&L. Emphasizes profit/loss and exit type."""
    cols = ["ts_first_seen", "asset", "ticker", "strike", "side", "entry_c", "contracts", "exec_status", "exit_reason", "exit_c", "pnl_pct"]
    widths = [19, 6, 26, 10, 4, 6, 4, 10, 16, 6, 8]
    lines = [f"--- {interval_label.upper()} LEDGER ---", ""]
    header = " | ".join(c.ljust(w)[:w] for c, w in zip(cols, widths))
    sep = "-+-".join("-" * w for w in widths)
    lines.append(header)
    lines.append(sep)
    sorted_rows = sorted(ledger_rows, key=_ledger_sort_priority)
    for r in sorted_rows[:max_rows]:
        ts = (r.get("ts_first_seen") or "")[:19].ljust(19)
        strike = str(r.get("strike") or "")[:10].ljust(10)
        entry_c = str(r.get("entry_price_cents") or "")[:6].ljust(6)
        contracts = str(r.get("contracts") or 1)[:4].ljust(4)
        pnl = r.get("pnl_pct")
        pnl_s = ("%.2f%%" % (float(pnl) * 100) if pnl is not None and pnl != "" else "")[:8].ljust(8)
        exit_reason = (str(r.get("exit_reason") or r.get("exec_reason") or "")[:16]).ljust(16)
        exit_c = str(r.get("exit_price_cents") or "")[:6].ljust(6)
        lines.append(" | ".join([
            ts,
            str(r.get("asset", ""))[:6].ljust(6),
            (str(r.get("ticker", "")) or "")[:26].ljust(26),
            strike,
            str(r.get("side", ""))[:4].ljust(4),
            entry_c,
            contracts,
            str(r.get("exec_status", ""))[:10].ljust(10),
            exit_reason,
            exit_c,
            pnl_s,
        ]))
    if len(ledger_rows) > max_rows:
        lines.append("... (%d more in attachment)" % (len(ledger_rows) - max_rows))
    lines.append("")
    return "\n".join(lines)


def format_strategy_ledger_table(ledger_rows: List[Dict], max_rows: int = 50) -> str:
    """Table with header and separator for trade attempts (email body). STOPLOSS/FAILED first.
    Includes guard_tune_knob, guard_checks_summary, and resolved_as (Kalshi outcome yes/no)."""
    cols = ["ts_first_seen", "interval", "asset", "ticker", "side", "outcome", "entry_c", "exec_status", "exec_reason", "tune_knob", "guard_summary", "pnl_pct", "dist_margin", "persist_margin"]
    widths = [19, 10, 6, 22, 4, 6, 6, 9, 18, 28, 30, 8, 10, 12]
    lines = []
    header = " | ".join(c.ljust(w)[:w] for c, w in zip(cols, widths))
    sep = "-+-".join("-" * w for w in widths)
    lines.append(header)
    lines.append(sep)
    sorted_rows = sorted(ledger_rows, key=_ledger_sort_priority)
    for r in sorted_rows[:max_rows]:
        ts = (r.get("ts_first_seen") or "")[:19].ljust(19)
        entry_c = str(r.get("entry_price_cents") or "")[:6].ljust(6)
        pnl = r.get("pnl_pct")
        pnl_s = ("%.2f%%" % (float(pnl) * 100) if pnl is not None and pnl != "" else "")[:8].ljust(8)
        exec_reason = (str(r.get("exec_reason", "")) or "")[:18].ljust(18)
        tune_knob = (str(r.get("guard_tune_knob", "")) or "")[:28].ljust(28)
        guard_summary = (str(r.get("guard_checks_summary", "")) or "")[:30].ljust(30)
        outcome = (str(r.get("resolved_as", "")) or "")[:6].ljust(6)
        lines.append(" | ".join([
            ts,
            str(r.get("interval", ""))[:10].ljust(10),
            str(r.get("asset", ""))[:6].ljust(6),
            str(r.get("ticker", ""))[:22].ljust(22),
            str(r.get("side", ""))[:4].ljust(4),
            outcome,
            entry_c,
            str(r.get("exec_status", ""))[:9].ljust(9),
            exec_reason,
            tune_knob,
            guard_summary,
            pnl_s,
            str(r.get("dist_margin", ""))[:10].ljust(10),
            str(r.get("persist_margin", ""))[:12].ljust(12),
        ]))
    if len(ledger_rows) > max_rows:
        lines.append("... (%d more rows in attachment)" % (len(ledger_rows) - max_rows))
    return "\n".join(lines)


def format_tuning_summary_block(
    tuning_summary: Optional[Dict] = None, sub_header: bool = False, content_only: bool = False
) -> str:
    """Format tuning summary as plain text. sub_header=True uses '--- TUNING SUMMARY ---'; content_only omits header."""
    if not tuning_summary:
        return ""
    lines = []
    if not content_only:
        header = "--- TUNING SUMMARY ---" if sub_header else "=== TUNING SUMMARY ==="
        lines = [header, ""]
    counts = tuning_summary.get("counts") or {}
    lines.append("  Counts: eval=%d submitted=%d failed=%d skipped=%d exited=%d stoploss=%d" % (
        counts.get("eval_count", 0), counts.get("submitted", 0), counts.get("failed", 0),
        counts.get("skipped", 0), counts.get("exited", 0), counts.get("stoploss", 0),
    ))
    top_err = tuning_summary.get("top_errors") or {}
    if top_err:
        lines.append("  Top errors: %s" % ", ".join("%s:%d" % (k, v) for k, v in list(top_err.items())[:3]))
    top_skips = tuning_summary.get("top_guard_skips") or {}
    if top_skips:
        lines.append("  Top guard skips: %s" % ", ".join("%s:%d" % (k, v) for k, v in list(top_skips.items())[:5]))
    guard_knobs = tuning_summary.get("guard_tune_knob_by_reason") or {}
    if guard_knobs:
        lines.append("  Tune knobs (skip_reason -> config):")
        for reason, knob in list(guard_knobs.items())[:5]:
            lines.append("    %s -> %s" % (reason, knob))
    nm = tuning_summary.get("near_miss") or {}
    if nm:
        lines.append("  Near-miss: dist_fail=%d persist_fail=%d cutoff_block=%d" % (
            nm.get("distance_fail_count", 0), nm.get("persistence_fail_count", 0), nm.get("cutoff_block_count", 0),
        ))
        dm = nm.get("dist_margin_distribution") or {}
        if dm:
            lines.append("    dist_margin: min=%s p50=%s p90=%s" % (dm.get("min"), dm.get("median"), dm.get("p90")))
    whatif = tuning_summary.get("whatif_entry_band") or {}
    if whatif:
        lines.append("  What-if: stoploss with entry>=99: %d (would filter if max=98)" % whatif.get("would_filter_if_max_98", 0))
    whatif_floor = tuning_summary.get("whatif_floor_usd") or {}
    for asset, v in whatif_floor.items():
        lines.append("  What-if %s: flip_if_floor+10=%d +25=%d" % (asset, v.get("flip_if_plus_10", 0), v.get("flip_if_plus_25", 0)))
    sl = tuning_summary.get("stoploss_diagnostics") or []
    if sl:
        lines.append("  Stop-loss trades (first 5): entry_price dist_margin sec_to_close mae_pct mfe_pct")
        for r in sl[:5]:
            lines.append("    %s %s %s %s %s" % (
                r.get("entry_price_cents"), r.get("dist_margin"), r.get("seconds_to_close_at_entry"),
                r.get("mae_pct"), r.get("mfe_pct"),
            ))
    lines.append("")
    return "\n".join(lines)


def build_hourly_strategy_email_body(
    time_start_pt: str,
    time_end_pt: str,
    portfolio_snapshot: Dict[str, Any],
    summary_rows: List[Dict],
    ledger_rows: List[Dict],
    hour_end_ts_iso: str,
    tuning_summary: Optional[Dict] = None,
    stoploss_report_text: Optional[str] = None,
    stoploss_by_interval: Optional[Dict[str, str]] = None,
    tuning_by_interval: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """
    Build email body: PORTFOLIO SNAPSHOT, then per-interval sections (fifteen_min, hourly, daily, weekly).
    Each interval: STRATEGY SUMMARY, TRADE LEDGER (STOPLOSS/FAILED first), STOP LOSS, TUNING SUMMARY.
    When stoploss_by_interval/tuning_by_interval are provided, use per-interval; else fall back to combined.
    """
    by_iv = _partition_by_interval(summary_rows, ledger_rows)
    attachments_note = []

    sections = [
        "------------------------------------------------------------",
        "KALSHI BOT REPORT",
        "Time Range: %s -> %s (PT)" % (time_start_pt, time_end_pt),
        "------------------------------------------------------------",
        "",
        "=== PORTFOLIO SNAPSHOT ===",
        format_portfolio_block(portfolio_snapshot, hour_end_ts_iso),
        "",
    ]

    for iv in INTERVALS_ORDER:
        sum_rows, led_rows = by_iv.get(iv, ([], []))
        has_activity = bool(sum_rows) or bool(led_rows)

        interval_label = iv.replace("_", " ").replace("fifteen min", "15min")
        sections.append("")
        sections.append("=== %s ===" % interval_label.upper())
        sections.append("")

        # STRATEGY SUMMARY
        sections.append("--- STRATEGY SUMMARY ---")
        if sum_rows:
            sections.append(format_strategy_summary_table(sum_rows))
        else:
            sections.append("  (no activity)")
        sections.append("")

        # TRADE LEDGER (STOPLOSS/FAILED first)
        sections.append("--- TRADE LEDGER (STOPLOSS/FAILED first) ---")
        if led_rows:
            if iv in ("daily", "weekly"):
                sections.append(format_daily_weekly_ledger_table(led_rows, iv, max_rows=30))
            else:
                sections.append(format_strategy_ledger_table(led_rows, max_rows=50))
        else:
            sections.append("  (no activity)")
        sections.append("")

        # STOP LOSS (text already includes --- STOP LOSS --- header)
        if stoploss_by_interval and iv in stoploss_by_interval:
            txt = (stoploss_by_interval.get(iv) or "").strip()
            sections.append(txt if txt else "--- STOP LOSS ---\n  No stop-loss exits in this interval.")
        elif stoploss_report_text and not stoploss_by_interval:
            sections.append(stoploss_report_text.strip() or "--- STOP LOSS ---\n  (no stop-loss data)")
        else:
            sections.append("--- STOP LOSS ---\n  No stop-loss exits in this interval.")
        sections.append("")

        # TUNING SUMMARY
        sections.append("--- TUNING SUMMARY ---")
        if tuning_by_interval and iv in tuning_by_interval:
            t = tuning_by_interval.get(iv)
            sections.append(format_tuning_summary_block(t, content_only=True) if t else "  (no tuning data)")
        elif tuning_summary and not tuning_by_interval and iv == "hourly":
            sections.append(format_tuning_summary_block(tuning_summary, content_only=True))
        else:
            sections.append("  (no tuning data)")
        sections.append("")

        if has_activity:
            attachments_note.append(f"strategy_ledger_{iv}.tsv, strategy_summary_{iv}.tsv, tuning_summary_{iv}.json, stop_loss_{iv}.txt")

    sections.append("Attachments: " + "; ".join(attachments_note) if attachments_note else "Attachments: (per-interval files)")
    return "\n".join(sections)
