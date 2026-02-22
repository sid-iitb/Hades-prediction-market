import datetime
import html
import json
import os
import sqlite3
import smtplib
import threading
import traceback
from datetime import timedelta, timezone
from email.message import EmailMessage
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from src.client.kalshi_client import KalshiClient
from src.client.kraken_client import KrakenClient
from src.offline_processing.ingest_kalshi import ingest_loop
from src.strategy.farthest_band import (
    FarthestBandConfig,
    run_farthest_band_cycle,
    select_farthest_band_market,
)
from dotenv import load_dotenv

env_file = Path("../.env")

# Load environment variables
load_dotenv()

app = FastAPI()

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all for dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "data",
    "kalshi_ingest.db",
)


def get_db_path():
    return os.getenv("KALSHI_DB_PATH") or DEFAULT_DB_PATH


def _open_ledger_db():
    db_path = get_db_path()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_trade_ledger(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            source TEXT,
            run_mode TEXT,
            action TEXT,
            side TEXT,
            ticker TEXT,
            price_cents INTEGER,
            count INTEGER,
            cost_cents INTEGER,
            status_code INTEGER,
            success INTEGER,
            note TEXT,
            payload_json TEXT
        )
        """
    )
    conn.commit()


def _ensure_stop_loss_triggers(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stop_loss_triggers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            sold_ticker TEXT,
            sold_price_cents INTEGER,
            sold_count INTEGER,
            bought_ticker TEXT,
            bought_price_cents INTEGER,
            bought_count INTEGER,
            payload_json TEXT
        )
        """
    )
    conn.commit()


def _log_stop_loss_trigger(snapshot: dict):
    if not isinstance(snapshot, dict):
        return
    sold = snapshot.get("sold") or {}
    bought = snapshot.get("bought") or {}
    ts = snapshot.get("triggered_at") or datetime.datetime.now(timezone.utc).isoformat()
    payload_json = None
    try:
        payload_json = json.dumps(snapshot)
    except Exception:
        payload_json = str(snapshot)

    conn = _open_ledger_db()
    try:
        _ensure_stop_loss_triggers(conn)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO stop_loss_triggers (
                ts, sold_ticker, sold_price_cents, sold_count,
                bought_ticker, bought_price_cents, bought_count, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                sold.get("ticker"),
                _maybe_int(sold.get("price_cents")),
                _maybe_int(sold.get("count")),
                bought.get("ticker"),
                _maybe_int(bought.get("price_cents")),
                _maybe_int(bought.get("count")),
                payload_json,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _get_stop_loss_trigger_history(limit: int = 5000) -> list[dict]:
    limit = max(1, min(int(limit), 5000))
    conn = _open_ledger_db()
    try:
        _ensure_stop_loss_triggers(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ts, sold_ticker, sold_price_cents, sold_count,
                   bought_ticker, bought_price_cents, bought_count, payload_json
            FROM stop_loss_triggers
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        # Return oldest->newest so UI can slice/reverse consistently.
        out = []
        for r in reversed(rows):
            payload = {}
            try:
                payload = json.loads(r["payload_json"]) if r["payload_json"] else {}
            except Exception:
                payload = {}
            out.append(
                {
                    "triggered_at": r["ts"],
                    "sold": {
                        "ticker": r["sold_ticker"],
                        "price_cents": r["sold_price_cents"],
                        "count": r["sold_count"],
                    },
                    "bought": {
                        "ticker": r["bought_ticker"],
                        "price_cents": r["bought_price_cents"],
                        "count": r["bought_count"],
                    },
                    "entry": payload.get("entry"),
                    "sold_value_cents": payload.get("sold_value_cents"),
                    "realized_pnl_cents": payload.get("realized_pnl_cents"),
                    "realized_pnl_pct": payload.get("realized_pnl_pct"),
                    "pre_exit_pnl_pct": payload.get("pre_exit_pnl_pct"),
                    "buy_selection_reason": payload.get("buy_selection_reason"),
                    "buy_fallback_used": payload.get("buy_fallback_used"),
                    "buy_fallback_reason": payload.get("buy_fallback_reason"),
                    "sell_error": payload.get("sell_error"),
                    "buy_error": payload.get("buy_error"),
                    "sell_status": payload.get("sell_status"),
                    "buy_status": payload.get("buy_status"),
                    "event_action": payload.get("event_action"),
                }
            )
        return out
    finally:
        conn.close()


def _log_trade_ledger(
    source: str,
    run_mode: str | None,
    action: str | None,
    side: str | None,
    ticker: str | None,
    price_cents: int | None,
    count: int | None,
    status_code: int | None = None,
    success: bool | None = None,
    note: str | None = None,
    payload: dict | list | str | None = None,
):
    should_trigger_email = _parse_bool_env("LEDGER_EMAIL_TRIGGER_ENABLED", True)
    action_lower = str(action or "").strip().lower()
    note_lower = str(note or "").strip().lower()
    is_trade_action = action_lower in {"buy", "sell"}
    is_stoploss_note = ("stop_loss" in note_lower) or ("stop-loss" in note_lower)
    is_live_mode = str(run_mode or "").strip().lower() == "live"
    has_ticker = bool(str(ticker or "").strip()) and str(ticker).strip() != "--"
    is_selection_noise = "no market matched ask band filters" in note_lower
    if is_selection_noise:
        return
    is_trigger_event = (is_trade_action or is_stoploss_note) and is_live_mode and has_ticker and not is_selection_noise
    cost_cents = int(price_cents * count) if isinstance(price_cents, int) and isinstance(count, int) else None
    payload_json = None
    if payload is not None:
        try:
            payload_json = json.dumps(payload)
        except Exception:
            payload_json = str(payload)
    ts_iso = datetime.datetime.now(timezone.utc).isoformat()
    inserted_id = None
    conn = _open_ledger_db()
    try:
        _ensure_trade_ledger(conn)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO trade_ledger (
                ts, source, run_mode, action, side, ticker, price_cents, count, cost_cents,
                status_code, success, note, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts_iso,
                source,
                run_mode,
                action,
                side,
                ticker,
                price_cents,
                count,
                cost_cents,
                status_code,
                1 if success is True else (0 if success is False else None),
                note,
                payload_json,
            ),
        )
        inserted_id = cur.lastrowid
        conn.commit()
    finally:
        conn.close()

    if _parse_bool_env("LEDGER_EMAIL_ENABLED", False) and should_trigger_email and is_trigger_event:
        record = {
            "id": inserted_id,
            "ts": ts_iso,
            "source": source,
            "run_mode": run_mode,
            "action": action,
            "side": side,
            "ticker": ticker,
            "price_cents": price_cents,
            "count": count,
            "cost_cents": cost_cents,
            "status_code": status_code,
            "success": 1 if success is True else (0 if success is False else None),
            "note": note,
            "payload_json": payload_json,
        }
        threading.Thread(
            target=_safe_send_ledger_trigger_email,
            args=(record,),
            name="ledger-trigger-email",
            daemon=True,
        ).start()


def _maybe_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _coerce_contract_price_cents(value):
    """
    Normalize various price encodings into per-contract cents in [1, 100].
    Handles probability (0..1), cents (1..100), and over-scaled values.
    """
    if value is None:
        return None
    try:
        v = float(value)
    except Exception:
        return None
    if v <= 0:
        return None
    if v <= 1.0:
        cents = v * 100.0
        return int(round(cents)) if 0 < cents <= 100 else None
    if v <= 100:
        return int(round(v))

    # Some upstream fields can be over-scaled (e.g., 705 instead of 70.5).
    scaled = float(v)
    for _ in range(4):
        if 0 < scaled <= 100:
            return int(round(scaled))
        scaled /= 10.0
    return None


def _record_strategy_ledger(out: dict, source: str):
    cycle = (out or {}).get("result") or {}
    spot = (out or {}).get("spot")
    action_name = cycle.get("action")
    mode_hint = (
        cycle.get("entry", {}).get("mode")
        or cycle.get("reentry", {}).get("mode")
        or cycle.get("exit", {}).get("mode")
        or None
    )

    entry = cycle.get("entry")
    multi_leg_actions = {"stop_loss_rotate", "rollover_reenter"}
    if isinstance(entry, dict) and entry.get("active_position") and action_name not in multi_leg_actions:
        ap = entry.get("active_position") or {}
        _log_trade_ledger(
            source=source,
            run_mode=entry.get("mode") or mode_hint,
            action="buy",
            side=ap.get("side"),
            ticker=ap.get("ticker"),
            price_cents=_maybe_int(ap.get("entry_price_cents")),
            count=_maybe_int(ap.get("count")),
            status_code=_maybe_int(entry.get("status_code")),
            success=(entry.get("mode") != "live") or (_maybe_int(entry.get("status_code")) in {200, 201}),
            note=f"strategy_action={action_name} spot={spot}",
            payload=entry,
        )
    elif isinstance(entry, dict) and action_name not in multi_leg_actions:
        # Capture failed/held entry attempts too (especially live buy failures).
        sel = (entry.get("selection") or {}).get("selected") or {}
        active_side = ((cycle.get("active_position") or {}) if isinstance(cycle, dict) else {}).get("side") or "yes"
        _log_trade_ledger(
            source=source,
            run_mode=entry.get("mode") or mode_hint,
            action="buy",
            side=(sel.get("ask_key") or f"{active_side}_ask").replace("_ask", ""),
            ticker=sel.get("ticker"),
            price_cents=_maybe_int(sel.get("ask_cents")),
            count=_maybe_int((entry.get("selection") or {}).get("count")),
            status_code=_maybe_int(entry.get("status_code")),
            success=False if str(entry.get("mode") or mode_hint).lower() == "live" else None,
            note=f"strategy_action={action_name} failed_entry reason={entry.get('reason')}",
            payload=entry,
        )

    # Capture stop-loss-triggered exit failures that return hold_active.
    if action_name == "hold_active" and "stop-loss triggered" in str(cycle.get("reason") or "").lower():
        ap = cycle.get("active_position") or {}
        exit_leg = cycle.get("exit") or {}
        _log_trade_ledger(
            source=source,
            run_mode=exit_leg.get("mode") or mode_hint,
            action="sell",
            side=ap.get("side"),
            ticker=ap.get("ticker"),
            price_cents=_maybe_int(exit_leg.get("exit_price_cents")),
            count=_maybe_int(ap.get("count")),
            status_code=_maybe_int(exit_leg.get("status_code")),
            success=False,
            note=f"stop_loss_exit_failed reason={exit_leg.get('reason') or cycle.get('reason')}",
            payload=exit_leg or cycle,
        )

    if action_name in multi_leg_actions:
        exited = cycle.get("exited_position") or {}
        exit_leg = cycle.get("exit") or {}
        if action_name == "stop_loss_rotate":
            exit_note = f"stop_loss pnl_pct={cycle.get('pnl_pct')}"
            reentry_note = "reentry_after_stop_loss"
        else:
            exit_note = "rollover_exit_stale_ticker"
            reentry_note = "rollover_reentry_latest_event"
        _log_trade_ledger(
            source=source,
            run_mode=exit_leg.get("mode") or mode_hint,
            action="sell",
            side=exited.get("side"),
            ticker=exited.get("ticker"),
            price_cents=_maybe_int(exit_leg.get("exit_price_cents")),
            count=_maybe_int(exited.get("count")),
            status_code=_maybe_int(exit_leg.get("status_code")),
            success=(exit_leg.get("mode") != "live") or (_maybe_int(exit_leg.get("status_code")) in {200, 201}),
            note=exit_note,
            payload=exit_leg,
        )
        reentry = cycle.get("reentry") or cycle.get("entry") or {}
        ap = reentry.get("active_position") or {}
        if ap:
            _log_trade_ledger(
                source=source,
                run_mode=reentry.get("mode") or mode_hint,
                action="buy",
                side=ap.get("side"),
                ticker=ap.get("ticker"),
                price_cents=_maybe_int(ap.get("entry_price_cents")),
                count=_maybe_int(ap.get("count")),
                status_code=_maybe_int(reentry.get("status_code")),
                success=(reentry.get("mode") != "live") or (_maybe_int(reentry.get("status_code")) in {200, 201}),
                note=reentry_note,
                payload=reentry,
            )
        else:
            # Capture failed/held re-entry attempts with planned selection details.
            sel = (reentry.get("selection") or {}).get("selected") or {}
            _log_trade_ledger(
                source=source,
                run_mode=reentry.get("mode") or mode_hint,
                action="buy",
                side=(sel.get("ask_key") or f"{(exited.get('side') or 'yes')}_ask").replace("_ask", ""),
                ticker=sel.get("ticker"),
                price_cents=_maybe_int(sel.get("ask_cents")),
                count=_maybe_int((reentry.get("selection") or {}).get("count")),
                status_code=_maybe_int(reentry.get("status_code")),
                success=False if str(reentry.get("mode") or mode_hint).lower() == "live" else None,
                note=f"{reentry_note}_failed reason={reentry.get('reason')}",
                payload=reentry,
            )


_ingest_thread: threading.Thread | None = None
_ledger_email_thread: threading.Thread | None = None
_ledger_email_stop = threading.Event()
_farthest_auto_thread: threading.Thread | None = None
_farthest_auto_stop = threading.Event()
_farthest_auto_lock = threading.Lock()
_farthest_auto_state = {
    "running": False,
    "config": None,
    "last_run_at": None,
    "last_scheduled_run_at": None,
    "last_risk_check_at": None,
    "next_scheduled_run_at": None,
    "next_risk_check_at": None,
    "last_result": None,
    "active_position": None,
    "active_positions": [],
    "active_position_mode": None,
    "risk_snapshot": [],
    "last_stop_loss_trigger": None,
    "stop_loss_trigger_history": [],
}


def _start_ingest_loop():
    global _ingest_thread
    if _ingest_thread and _ingest_thread.is_alive():
        return
    db_path = get_db_path()
    _ingest_thread = threading.Thread(
        target=ingest_loop,
        kwargs={"db_path": db_path},
        name="kalshi-ingest-loop",
        daemon=True,
    )
    _ingest_thread.start()


def _parse_bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _format_local_ts(iso_ts: str | None) -> str:
    if not iso_ts:
        return "--"
    try:
        dt = datetime.datetime.fromisoformat(str(iso_ts).replace("Z", "+00:00"))
        return dt.astimezone().strftime("%Y-%m-%d %I:%M:%S %p %Z")
    except Exception:
        return str(iso_ts)


def _read_ledger_window(start_iso: str, end_iso: str) -> list[dict]:
    conn = _open_ledger_db()
    try:
        _ensure_trade_ledger(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id, ts, source, run_mode, action, side, ticker, price_cents, count, cost_cents,
                status_code, success, note, payload_json
            FROM trade_ledger
            WHERE ts > ? AND ts <= ?
            ORDER BY id ASC
            """,
            (start_iso, end_iso),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _build_ledger_record_lines(record: dict) -> list[str]:
    lines = []
    ordered_keys = [
        "id",
        "ts",
        "source",
        "run_mode",
        "action",
        "side",
        "ticker",
        "price_cents",
        "count",
        "cost_cents",
        "status_code",
        "success",
        "note",
        "payload_json",
    ]
    for key in ordered_keys:
        val = record.get(key)
        lines.append(f"{key}: {val if val is not None else '--'}")
    return lines


def _to_pretty_json(value) -> str:
    try:
        return json.dumps(value, indent=2, sort_keys=True, default=str)
    except Exception:
        return str(value)


def _format_cents_usd(value) -> str:
    try:
        if value is None:
            return "--"
        return f"${(float(value) / 100.0):,.2f}"
    except Exception:
        return "--"


def _extract_failure_reason(status_code, payload_json, note_text) -> str:
    s = _maybe_int(status_code)
    parsed = None
    if payload_json:
        try:
            parsed = json.loads(payload_json) if isinstance(payload_json, str) else payload_json
        except Exception:
            parsed = None

    def pull_msg(obj):
        if not isinstance(obj, dict):
            return ""
        keys = ["error", "message", "reason", "detail", "msg", "code"]
        for k in keys:
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for k in keys:
            v = obj.get(k)
            if isinstance(v, dict):
                nested = pull_msg(v)
                if nested:
                    return nested
        return ""

    extracted = pull_msg(parsed)
    if extracted:
        return extracted
    note = str(note_text or "")
    idx = note.lower().find("reason=")
    if idx >= 0:
        return note[idx + 7 :].strip()
    if note.lower().startswith("exception="):
        return note.replace("exception=", "", 1).strip()
    if s == 401:
        return "Unauthorized (401): check API key/signature/permissions"
    if isinstance(s, int) and s >= 400:
        return f"Request failed with status {s}"
    return ""


def _render_ledger_html_table(records: list[dict]) -> str:
    rows_html = []
    for r in records:
        ts = _format_local_ts(r.get("ts"))
        source = r.get("source") or "--"
        action = str(r.get("action") or "--").upper()
        side = str(r.get("side") or "--").upper()
        ticker = r.get("ticker") or "--"
        price = f"{r.get('price_cents')}c" if r.get("price_cents") is not None else "--"
        count = r.get("count") if r.get("count") is not None else "--"
        cost = _format_cents_usd(r.get("cost_cents")) if r.get("cost_cents") is not None else "--"
        status = r.get("status_code") if r.get("status_code") is not None else "--"
        reason = _extract_failure_reason(r.get("status_code"), r.get("payload_json"), r.get("note")) or "--"
        note = r.get("note") or "--"
        cells = [ts, source, action, side, ticker, price, str(count), str(cost), str(status), reason, note]
        row = "<tr>" + "".join(f"<td>{html.escape(c)}</td>" for c in cells) + "</tr>"
        rows_html.append(row)
    if not rows_html:
        rows_html.append("<tr><td colspan='11'>No ledger records in this window.</td></tr>")
    return (
        "<table style='border-collapse:collapse;width:100%;font-family:Arial,sans-serif;font-size:12px;'>"
        "<thead><tr>"
        "<th style='border:1px solid #2d3748;padding:6px;background:#111827;color:#f9fafb;'>TIME</th>"
        "<th style='border:1px solid #2d3748;padding:6px;background:#111827;color:#f9fafb;'>SOURCE</th>"
        "<th style='border:1px solid #2d3748;padding:6px;background:#111827;color:#f9fafb;'>ACTION</th>"
        "<th style='border:1px solid #2d3748;padding:6px;background:#111827;color:#f9fafb;'>SIDE</th>"
        "<th style='border:1px solid #2d3748;padding:6px;background:#111827;color:#f9fafb;'>TICKER</th>"
        "<th style='border:1px solid #2d3748;padding:6px;background:#111827;color:#f9fafb;'>PRICE</th>"
        "<th style='border:1px solid #2d3748;padding:6px;background:#111827;color:#f9fafb;'>COUNT</th>"
        "<th style='border:1px solid #2d3748;padding:6px;background:#111827;color:#f9fafb;'>COST</th>"
        "<th style='border:1px solid #2d3748;padding:6px;background:#111827;color:#f9fafb;'>STATUS</th>"
        "<th style='border:1px solid #2d3748;padding:6px;background:#111827;color:#f9fafb;'>REASON</th>"
        "<th style='border:1px solid #2d3748;padding:6px;background:#111827;color:#f9fafb;'>NOTE</th>"
        "</tr></thead><tbody>"
        + "".join(rows_html)
        + "</tbody></table>"
    )


def _send_email_message(subject: str, body_lines: list[str], html_body: str | None = None):
    smtp_host = os.getenv("LEDGER_EMAIL_SMTP_HOST")
    smtp_port = int(os.getenv("LEDGER_EMAIL_SMTP_PORT", "587"))
    smtp_user = os.getenv("LEDGER_EMAIL_SMTP_USER")
    smtp_pass = os.getenv("LEDGER_EMAIL_SMTP_PASS")
    smtp_tls = _parse_bool_env("LEDGER_EMAIL_SMTP_TLS", True)
    to_addr = os.getenv("LEDGER_EMAIL_TO")
    from_addr = os.getenv("LEDGER_EMAIL_FROM") or smtp_user

    if not smtp_host or not to_addr or not from_addr:
        raise ValueError("Missing email config (LEDGER_EMAIL_SMTP_HOST, LEDGER_EMAIL_TO, LEDGER_EMAIL_FROM)")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content("\n".join(body_lines))
    if html_body:
        msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
        if smtp_tls:
            server.starttls()
        if smtp_user and smtp_pass:
            server.login(smtp_user, smtp_pass)
        server.send_message(msg)


def _send_ledger_trigger_email(record: dict):
    now_utc = datetime.datetime.now(timezone.utc)
    local_now = now_utc.astimezone().strftime("%Y-%m-%d %I:%M:%S %p %Z")
    utc_now = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    action = str(record.get("action") or "").strip().lower()
    note = str(record.get("note") or "").strip().lower()
    if "stop_loss" in note or "stop-loss" in note:
        trigger_type = "STOPLOSS"
    elif action == "buy":
        trigger_type = "BUY"
    elif action == "sell":
        trigger_type = "SELL"
    else:
        trigger_type = (action or "UNKNOWN").upper()

    body_lines = [
        "Buy/Sell/StopLoss Order Triggered",
        f"Generated at: {local_now} ({utc_now})",
        f"Trigger type: {trigger_type}",
        "",
    ]
    html_table = _render_ledger_html_table([record])
    html_body = (
        "<html><body>"
        f"<h3 style='font-family:Arial,sans-serif;margin:0 0 8px;'>Buy/Sell/StopLoss Order Triggered</h3>"
        f"<p style='font-family:Arial,sans-serif;margin:0 0 4px;'>Generated at: {html.escape(local_now)} ({html.escape(utc_now)})</p>"
        f"<p style='font-family:Arial,sans-serif;margin:0 0 12px;'>Trigger type: {html.escape(trigger_type)}</p>"
        f"{html_table}"
        "</body></html>"
    )
    _send_email_message("Buy/Sell/StopLoss Order Triggered", body_lines, html_body=html_body)


def _safe_send_ledger_trigger_email(record: dict):
    try:
        _send_ledger_trigger_email(record)
    except Exception as exc:
        print(f"Ledger trigger email error: {exc}")


def _send_ledger_snapshot_email(records: list[dict], window_start_iso: str, window_end_iso: str, portfolio_snapshot: dict | None = None):
    now_utc = datetime.datetime.now(timezone.utc)
    local_now = now_utc.astimezone().strftime("%Y-%m-%d %I:%M:%S %p %Z")
    utc_now = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    window_start_local = _format_local_ts(window_start_iso)
    window_end_local = _format_local_ts(window_end_iso)

    lines = [
        "Ledge Snapshot",
        f"Generated at: {local_now} ({utc_now})",
        f"Window covered: {window_start_local} -> {window_end_local}",
        f"Total records: {len(records)}",
        "",
    ]
    for r in records:
        ts = _format_local_ts(r.get("ts"))
        source = r.get("source") or "--"
        action = str(r.get("action") or "--").upper()
        side = str(r.get("side") or "--").upper()
        ticker = r.get("ticker") or "--"
        price = f"{r.get('price_cents')}c" if r.get("price_cents") is not None else "--"
        count = r.get("count") if r.get("count") is not None else "--"
        cost = _format_cents_usd(r.get("cost_cents")) if r.get("cost_cents") is not None else "--"
        status = r.get("status_code") if r.get("status_code") is not None else "--"
        reason = _extract_failure_reason(r.get("status_code"), r.get("payload_json"), r.get("note")) or "--"
        note = r.get("note") or "--"
        lines.append(
            f"{ts} | {source} | {action} | {side} | {ticker} | {price} | {count} | {cost} | {status} | {reason} | {note}"
        )

    lines.append("Current Portfolio (full):")
    lines.append(_to_pretty_json(portfolio_snapshot if portfolio_snapshot is not None else {"error": "Portfolio snapshot unavailable"}))

    html_table = _render_ledger_html_table(records)
    portfolio_json = _to_pretty_json(portfolio_snapshot if portfolio_snapshot is not None else {"error": "Portfolio snapshot unavailable"})
    html_body = (
        "<html><body>"
        "<h3 style='font-family:Arial,sans-serif;margin:0 0 8px;'>Ledge Snapshot</h3>"
        f"<p style='font-family:Arial,sans-serif;margin:0 0 4px;'>Generated at: {html.escape(local_now)} ({html.escape(utc_now)})</p>"
        f"<p style='font-family:Arial,sans-serif;margin:0 0 4px;'>Window covered: {html.escape(window_start_local)} -> {html.escape(window_end_local)}</p>"
        f"<p style='font-family:Arial,sans-serif;margin:0 0 12px;'>Total records: {len(records)}</p>"
        f"{html_table}"
        "<h4 style='font-family:Arial,sans-serif;margin:16px 0 8px;'>Current Portfolio (full)</h4>"
        f"<pre style='white-space:pre-wrap;background:#f8fafc;border:1px solid #e5e7eb;padding:10px;font-size:12px;'>{html.escape(portfolio_json)}</pre>"
        "</body></html>"
    )

    _send_email_message("Ledge Snapshot", lines, html_body=html_body)


def _send_ledger_startup_email():
    if not _parse_bool_env("LEDGER_EMAIL_SEND_ON_STARTUP", True):
        return
    smtp_host = os.getenv("LEDGER_EMAIL_SMTP_HOST")
    smtp_port = int(os.getenv("LEDGER_EMAIL_SMTP_PORT", "587"))
    smtp_user = os.getenv("LEDGER_EMAIL_SMTP_USER")
    smtp_pass = os.getenv("LEDGER_EMAIL_SMTP_PASS")
    smtp_tls = _parse_bool_env("LEDGER_EMAIL_SMTP_TLS", True)
    to_addr = os.getenv("LEDGER_EMAIL_TO")
    from_addr = os.getenv("LEDGER_EMAIL_FROM") or smtp_user
    if not smtp_host or not to_addr or not from_addr:
        raise ValueError("Missing email config (LEDGER_EMAIL_SMTP_HOST, LEDGER_EMAIL_TO, LEDGER_EMAIL_FROM)")

    now_utc = datetime.datetime.now(timezone.utc)
    local_now = now_utc.astimezone().strftime("%Y-%m-%d %I:%M:%S %p %Z")
    utc_now = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    _send_email_message(
        "Ledger startup check",
        [
            "Ledger email startup check",
            f"Generated at: {local_now} ({utc_now})",
            "Status: SMTP configuration loaded and startup send attempted.",
        ],
    )


def _ledger_email_worker():
    interval_min = max(1, int(os.getenv("LEDGER_EMAIL_SNAPSHOT_INTERVAL_MINUTES", os.getenv("LEDGER_EMAIL_INTERVAL_MINUTES", "15"))))
    window_min = max(interval_min, int(os.getenv("LEDGER_EMAIL_SNAPSHOT_WINDOW_MINUTES", "30")))
    while not _ledger_email_stop.is_set():
        if _ledger_email_stop.wait(timeout=interval_min * 60):
            break
        try:
            window_end_iso = datetime.datetime.now(timezone.utc).isoformat()
            window_start_iso = (datetime.datetime.now(timezone.utc) - timedelta(minutes=window_min)).isoformat()
            rows = _read_ledger_window(window_start_iso, window_end_iso)
            try:
                portfolio_snapshot = get_portfolio_current_orders()
            except Exception as portfolio_exc:
                portfolio_snapshot = {
                    "error": f"portfolio_fetch_failed: {type(portfolio_exc).__name__}: {portfolio_exc}",
                    "ts": datetime.datetime.now(timezone.utc).isoformat(),
                }
            _send_ledger_snapshot_email(
                rows,
                window_start_iso=window_start_iso,
                window_end_iso=window_end_iso,
                portfolio_snapshot=portfolio_snapshot,
            )
        except Exception as exc:
            print(f"Ledger email worker error: {exc}")


def _start_ledger_email_loop():
    global _ledger_email_thread
    if _ledger_email_thread and _ledger_email_thread.is_alive():
        return
    if not _parse_bool_env("LEDGER_EMAIL_ENABLED", False):
        return
    _ledger_email_stop.clear()
    _ledger_email_thread = threading.Thread(
        target=_ledger_email_worker,
        name="ledger-email-loop",
        daemon=True,
    )
    _ledger_email_thread.start()


@app.on_event("startup")
def startup_ingest():
    auto_ingest = os.getenv("KALSHI_AUTO_INGEST", "true").lower()
    if auto_ingest in {"1", "true", "yes", "on"}:
        _start_ingest_loop()
    if _parse_bool_env("LEDGER_EMAIL_ENABLED", False):
        try:
            _send_ledger_startup_email()
        except Exception as exc:
            print(f"Ledger startup email error: {exc}")
    _start_ledger_email_loop()


@app.get("/get_price_ticker")
def get_arbitrage_data():
    # Fetch Data
    client = KrakenClient()
    bitcoin_price = client.latest_btc_price().price

    response = {
        "Timestamp": datetime.datetime.now().isoformat(),
        "BtcPrice": bitcoin_price,
    }
    return response


class KalshiOrderRequest(BaseModel):
    ticker: str
    yes_ask_cents: int
    max_cost_cents: int = 500


@app.get("/kalshi_ingest/latest")
def get_latest_ingest():
    db_path = get_db_path()
    if not os.path.exists(db_path):
        return {"error": f"DB not found at {db_path}", "records": []}

    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cutoff = datetime.datetime.now(timezone.utc) - timedelta(hours=2)
        cutoff_iso = cutoff.isoformat()
        cur.execute(
            """
            SELECT id, ts, event_ticker, current_price
            FROM ingest_runs
            WHERE ts >= ?
            ORDER BY id DESC
            LIMIT 200
            """,
            (cutoff_iso,),
        )
        runs = cur.fetchall()
        results = []
        for run in runs:
            cur.execute(
                """
                SELECT strike, yes_bid, yes_ask, no_bid, no_ask, subtitle, ticker
                FROM kalshi_markets
                WHERE run_id = ?
                ORDER BY strike ASC
                """,
                (run["id"],),
            )
            markets = [dict(row) for row in cur.fetchall()]
            results.append(
                {
                    "id": run["id"],
                    "ts": run["ts"],
                    "event_ticker": run["event_ticker"],
                    "current_price": run["current_price"],
                    "markets": markets,
                }
            )
        return {"records": results}
    finally:
        conn.close()


@app.get("/kalshi_ingest/last_hour")
def get_last_hour_ingest():
    db_path = get_db_path()
    if not os.path.exists(db_path):
        return {"error": f"DB not found at {db_path}", "records": []}

    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cutoff = datetime.datetime.now(timezone.utc) - timedelta(hours=1)
        cutoff_iso = cutoff.isoformat()
        cur.execute(
            """
            SELECT ts, current_price
            FROM ingest_runs
            WHERE ts >= ?
            ORDER BY ts ASC
            """,
            (cutoff_iso,),
        )
        records = [dict(row) for row in cur.fetchall()]
        return {"records": records}
    finally:
        conn.close()


def _latest_ingest_record():
    db_path = get_db_path()
    if not os.path.exists(db_path):
        return None

    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, ts, event_ticker, current_price
            FROM ingest_runs
            ORDER BY id DESC
            LIMIT 1
            """
        )
        run = cur.fetchone()
        if not run:
            return None

        cur.execute(
            """
            SELECT strike, yes_bid, yes_ask, no_bid, no_ask, subtitle, ticker
            FROM kalshi_markets
            WHERE run_id = ?
            ORDER BY strike ASC
            """,
            (run["id"],),
        )
        markets = [dict(row) for row in cur.fetchall()]
        return {
            "id": run["id"],
            "ts": run["ts"],
            "event_ticker": run["event_ticker"],
            "current_price": run["current_price"],
            "markets": markets,
        }
    finally:
        conn.close()


def _run_farthest_band_once(
    config: FarthestBandConfig,
    active_position: dict | None = None,
    force_rebalance: bool = False,
):
    ingest = _latest_ingest_record()
    if not ingest:
        return {"action": "hold", "reason": "No ingest record found", "active_position": active_position}

    kraken = KrakenClient()
    spot = kraken.latest_btc_price().price
    markets = ingest.get("markets", [])
    client = KalshiClient()
    cycle = run_farthest_band_cycle(
        client=client,
        spot=spot,
        markets=markets,
        config=config,
        active_position=active_position,
        force_rebalance=force_rebalance,
    )
    return {
        "spot": spot,
        "ingest_run_id": ingest.get("id"),
        "ingest_ts": ingest.get("ts"),
        "event_ticker": ingest.get("event_ticker"),
        "result": cycle,
        "active_position": cycle.get("active_position"),
    }


def _normalize_active_positions(raw_positions):
    normalized = []
    for p in raw_positions or []:
        if not isinstance(p, dict):
            continue
        ticker = str(p.get("ticker") or "").strip()
        side = str(p.get("side") or "").strip().lower()
        count = _maybe_int(p.get("count")) or 0
        entry_price = _coerce_contract_price_cents(p.get("entry_price_cents")) or 0
        if not ticker or side not in {"yes", "no"} or count < 1 or entry_price < 1:
            continue
        row = dict(p)
        row["ticker"] = ticker
        row["side"] = side
        row["count"] = int(count)
        row["entry_price_cents"] = int(entry_price)
        normalized.append(row)
    return normalized


def _build_risk_snapshot(active_positions):
    snapshot = []
    positions = _normalize_active_positions(active_positions or [])
    if not positions:
        return snapshot

    client = KalshiClient()
    for p in positions:
        ticker = str(p.get("ticker") or "")
        side = str(p.get("side") or "").lower()
        entry = _maybe_int(p.get("entry_price_cents")) or 0
        count = _maybe_int(p.get("count")) or 0
        mark_cents = None
        pnl_pct = None
        error = None
        if ticker and side in {"yes", "no"} and entry > 0:
            try:
                top = client.get_top_of_book(ticker)
                mark_cents = _maybe_int(top.get(f"{side}_bid"))
                if mark_cents is None:
                    mark_cents = 0
                pnl_pct = (float(mark_cents) - float(entry)) / float(entry)
            except Exception as exc:
                error = str(exc)
        snapshot.append(
            {
                "ticker": ticker,
                "side": side,
                "entry_price_cents": entry,
                "count": count,
                "mark_cents": mark_cents,
                "pnl_pct": pnl_pct,
                "error": error,
            }
        )
    return snapshot


def _market_event_prefix(ticker: str | None) -> str | None:
    t = str(ticker or "").strip()
    if not t:
        return None
    i = t.rfind("-T")
    if i > 0:
        return t[:i]
    return None


def _seed_active_positions_from_portfolio(config: FarthestBandConfig) -> list[dict] | None:
    """
    Recover strategy-tracked positions from live portfolio so stop-loss can
    continue protecting positions after API restarts.
    """
    if str(config.mode).lower() != "live":
        return []

    client = KalshiClient()
    try:
        positions = client.get_positions()
    except Exception:
        # Return None on fetch failure so callers can keep prior tracked state.
        return None

    ingest = _latest_ingest_record() or {}
    latest_event = str(ingest.get("event_ticker") or "").strip()
    markets = ingest.get("markets") or []
    latest_market_tickers = {str(m.get("ticker")) for m in markets if m.get("ticker")}

    side_filter = str(config.side or "").lower()
    rows = positions.get("market_positions") or positions.get("positions") or []
    recovered = []
    def _to_float(v):
        try:
            if v is None:
                return None
            return float(v)
        except Exception:
            return None

    for p in rows:
        ticker = str(p.get("ticker") or p.get("market_ticker") or p.get("event_ticker") or "").strip()
        if not ticker:
            continue

        # Restrict recovery to BTC contract family and, when available, current event set.
        if not ticker.startswith("KXBTCD-"):
            continue
        if latest_market_tickers and ticker not in latest_market_tickers:
            prefix = _market_event_prefix(ticker)
            if latest_event and prefix != latest_event:
                continue

        raw_side = p.get("side") if p.get("side") is not None else p.get("position")
        side = str(raw_side).lower() if raw_side is not None else ""
        if isinstance(raw_side, (int, float)) and raw_side != 0:
            side = "yes" if raw_side > 0 else "no"
        if side not in {"yes", "no"}:
            continue
        if side != side_filter:
            continue

        count = p.get("contracts") or p.get("quantity") or p.get("count") or 0
        if not count and isinstance(raw_side, (int, float)):
            count = int(abs(raw_side))
        count = _maybe_int(count) or 0
        if count < 1:
            continue

        avg_price = p.get("avg_price") or p.get("average_price")
        entry_price = _coerce_contract_price_cents(avg_price)
        if entry_price is None:
            traded = (
                _parse_dollar_str_to_cents(p.get("total_traded_dollars"))
                or _parse_dollar_str_to_cents(p.get("total_traded"))
            )
            if traded is not None and count > 0:
                entry_price = _coerce_contract_price_cents(float(traded) / float(count))
        if entry_price is None or entry_price < 1:
            continue

        strike = _to_float(p.get("strike"))
        if strike is None:
            strike = _to_float(p.get("floor_strike"))
        if strike is None:
            strike = _to_float(p.get("cap_strike"))

        recovered.append(
            {
                "ticker": ticker,
                "strike": float(strike) if strike is not None else None,
                "side": side,
                "entry_price_cents": int(entry_price),
                "count": int(count),
                "opened_spot": None,
            }
        )

    return _normalize_active_positions(recovered)


def _farthest_band_worker():
    next_scheduled_at = datetime.datetime.now(timezone.utc)
    next_risk_check_due_at = datetime.datetime.now(timezone.utc)
    while not _farthest_auto_stop.is_set():
        now_utc = datetime.datetime.now(timezone.utc)
        with _farthest_auto_lock:
            cfg_dict = dict(_farthest_auto_state.get("config") or {})
            active_mode = _farthest_auto_state.get("active_position_mode")
            cfg_mode = str(cfg_dict.get("mode") or "").lower()
            if active_mode == cfg_mode:
                tracked = _normalize_active_positions(_farthest_auto_state.get("active_positions") or [])
                if not tracked:
                    single = dict(_farthest_auto_state.get("active_position") or {}) or None
                    tracked = _normalize_active_positions([single] if single else [])
                active_positions = tracked
            else:
                active_positions = []
            _farthest_auto_state["next_scheduled_run_at"] = next_scheduled_at.isoformat()
            last_stop_loss_trigger = dict(_farthest_auto_state.get("last_stop_loss_trigger") or {}) or None
            stop_loss_trigger_history = list(_farthest_auto_state.get("stop_loss_trigger_history") or [])
        if not cfg_dict:
            break

        config = FarthestBandConfig(**cfg_dict)
        # Always prefer live portfolio quantities for stop-loss decisions in live mode.
        if str(config.mode).lower() == "live":
            live_positions = _seed_active_positions_from_portfolio(config)
            if live_positions is not None:
                active_positions = live_positions
        had_positions_before = bool(active_positions)
        is_scheduled_run = now_utc >= next_scheduled_at
        should_check_risk = bool(active_positions) and now_utc >= next_risk_check_due_at
        did_run = False
        run_at = None
        latest_out = None
        next_active_positions = list(active_positions or [])
        risk_snapshot = []

        if active_positions or is_scheduled_run:
            did_run = True
            run_at = datetime.datetime.now(timezone.utc).isoformat()
            if should_check_risk:
                next_active_positions = []
                for pos in active_positions:
                    try:
                        out = _run_farthest_band_once(
                            config,
                            active_position=pos,
                            force_rebalance=False,
                        )
                    except Exception as exc:
                        out = {"action": "error", "reason": str(exc), "active_position": pos}
                    try:
                        _record_strategy_ledger(out, source="/strategy/farthest_band/auto")
                    except Exception:
                        # Ledger write must never kill the strategy worker.
                        pass
                    latest_out = out
                    cycle = (out or {}).get("result") or {}
                    cycle_action = str(cycle.get("action") or "").lower()
                    cycle_reason = str(cycle.get("reason") or "")
                    stop_loss_failed_exit = (
                        cycle_action == "hold_active" and "stop-loss triggered" in cycle_reason.lower()
                    )
                    if cycle_action == "stop_loss_rotate" or stop_loss_failed_exit:
                        exit_leg = cycle.get("exit") or {}
                        reentry_leg = cycle.get("reentry") or {}
                        exited = cycle.get("exited_position") or cycle.get("active_position") or {}
                        rebought = {}
                        if cycle_action == "stop_loss_rotate":
                            rebought = (
                                reentry_leg.get("active_position")
                                or ((reentry_leg.get("selection") or {}).get("selected") or {})
                            )
                        sell_error = None
                        if str(exit_leg.get("action") or "").lower() == "hold":
                            sell_error = exit_leg.get("reason") or cycle_reason or "Unknown sell failure"
                        buy_error = None
                        if cycle_action == "stop_loss_rotate":
                            if str(reentry_leg.get("action") or "").lower() == "hold":
                                buy_error = reentry_leg.get("reason") or "Unknown buy failure"
                        else:
                            buy_error = "Not attempted because sell leg failed"
                        entry_price_cents = _maybe_int(exited.get("entry_price_cents"))
                        sold_price_cents = _maybe_int(exit_leg.get("exit_price_cents"))
                        sold_count = _maybe_int(exited.get("count"))
                        purchase_value_cents = (
                            int(entry_price_cents * sold_count)
                            if isinstance(entry_price_cents, int) and isinstance(sold_count, int)
                            else None
                        )
                        sold_value_cents = (
                            int(sold_price_cents * sold_count)
                            if isinstance(sold_price_cents, int) and isinstance(sold_count, int)
                            else None
                        )
                        realized_pnl_cents = (
                            int(sold_value_cents - purchase_value_cents)
                            if isinstance(sold_value_cents, int) and isinstance(purchase_value_cents, int)
                            else None
                        )
                        realized_pnl_pct = (
                            (float(sold_price_cents) - float(entry_price_cents)) / float(entry_price_cents)
                            if isinstance(sold_price_cents, int) and isinstance(entry_price_cents, int) and entry_price_cents > 0
                            else None
                        )
                        last_stop_loss_trigger = {
                            "triggered_at": run_at,
                            "sold": {
                                "ticker": exited.get("ticker"),
                                "price_cents": _maybe_int(exit_leg.get("exit_price_cents")),
                                "count": _maybe_int(exited.get("count")),
                            },
                            "entry": {
                                "price_cents": entry_price_cents,
                                "count": sold_count,
                                "purchase_value_cents": purchase_value_cents,
                            },
                            "bought": {
                                "ticker": rebought.get("ticker"),
                                "price_cents": _maybe_int(
                                    rebought.get("entry_price_cents")
                                    if rebought.get("entry_price_cents") is not None
                                    else rebought.get("ask_cents")
                                ),
                                "count": _maybe_int(
                                    rebought.get("count")
                                    if rebought.get("count") is not None
                                    else (reentry_leg.get("selection") or {}).get("count")
                                ),
                            },
                            "sold_value_cents": sold_value_cents,
                            "realized_pnl_cents": realized_pnl_cents,
                            "realized_pnl_pct": realized_pnl_pct,
                            "pre_exit_pnl_pct": cycle.get("pnl_pct"),
                            "buy_selection_reason": (reentry_leg.get("selection") or {}).get("reason"),
                            "buy_fallback_used": (reentry_leg.get("selection") or {}).get("fallback_used"),
                            "buy_fallback_reason": (reentry_leg.get("selection") or {}).get("fallback_reason"),
                            "sell_error": sell_error,
                            "buy_error": buy_error,
                            "sell_status": "failed" if sell_error else "ok",
                            "buy_status": (
                                "failed"
                                if buy_error and cycle_action == "stop_loss_rotate"
                                else ("skipped" if buy_error else "ok")
                            ),
                            "event_action": cycle_action,
                        }
                        stop_loss_trigger_history.append(last_stop_loss_trigger)
                        if len(stop_loss_trigger_history) > 5000:
                            stop_loss_trigger_history = stop_loss_trigger_history[-5000:]
                        _log_stop_loss_trigger(last_stop_loss_trigger)
                    mark = out.get("mark") if isinstance(out, dict) else None
                    pnl_pct = out.get("pnl_pct") if isinstance(out, dict) else None
                    if pnl_pct is None and isinstance(mark, dict):
                        pnl_pct = mark.get("pnl_pct")
                    mark_cents = mark.get("mark_cents") if isinstance(mark, dict) else None
                    risk_snapshot.append(
                        {
                            "ticker": pos.get("ticker"),
                            "side": pos.get("side"),
                            "entry_price_cents": pos.get("entry_price_cents"),
                            "count": pos.get("count"),
                            "pnl_pct": pnl_pct,
                            "mark_cents": mark_cents,
                            "action": out.get("action") if isinstance(out, dict) else "error",
                        }
                    )
                    next_pos = out.get("active_position")
                    if isinstance(next_pos, dict):
                        next_active_positions.append(next_pos)

            if is_scheduled_run:
                try:
                    scheduled_out = _run_farthest_band_once(
                        config,
                        active_position=None,
                        force_rebalance=False,
                    )
                except Exception as exc:
                    scheduled_out = {"action": "error", "reason": str(exc)}
                try:
                    _record_strategy_ledger(scheduled_out, source="/strategy/farthest_band/auto")
                except Exception:
                    # Ledger write must never kill the strategy worker.
                    pass
                latest_out = scheduled_out
                scheduled_pos = scheduled_out.get("active_position")
                if isinstance(scheduled_pos, dict):
                    next_active_positions.append(scheduled_pos)

            if is_scheduled_run:
                interval_seconds = max(int(config.interval_minutes) * 60, 60)
                next_scheduled_at = now_utc + timedelta(seconds=interval_seconds)

        if did_run:
            normalized_positions = _normalize_active_positions(next_active_positions)
            with _farthest_auto_lock:
                _farthest_auto_state["last_run_at"] = run_at
                if is_scheduled_run:
                    _farthest_auto_state["last_scheduled_run_at"] = run_at
                if should_check_risk:
                    _farthest_auto_state["last_risk_check_at"] = run_at
                    _farthest_auto_state["risk_snapshot"] = risk_snapshot
                _farthest_auto_state["next_scheduled_run_at"] = next_scheduled_at.isoformat()
                _farthest_auto_state["last_result"] = latest_out
                _farthest_auto_state["active_positions"] = normalized_positions
                _farthest_auto_state["active_position"] = normalized_positions[-1] if normalized_positions else None
                _farthest_auto_state["active_position_mode"] = cfg_mode if normalized_positions else None
                _farthest_auto_state["last_stop_loss_trigger"] = last_stop_loss_trigger
                _farthest_auto_state["stop_loss_trigger_history"] = stop_loss_trigger_history

        configured_risk_sec = max(1, int(getattr(config, "stop_loss_check_seconds", 300) or 300))
        if should_check_risk:
            next_risk_check_due_at = datetime.datetime.now(timezone.utc) + timedelta(seconds=configured_risk_sec)
        elif next_active_positions and not had_positions_before:
            # First tracked position was created on a scheduled cycle; start risk cadence from now.
            next_risk_check_due_at = datetime.datetime.now(timezone.utc) + timedelta(seconds=configured_risk_sec)

        # Wake at the earlier of:
        # - next scheduled interval execution
        # - next configured stop-loss check time
        now_after = datetime.datetime.now(timezone.utc)
        seconds_until_schedule = max(1, int((next_scheduled_at - now_after).total_seconds()))
        seconds_until_risk_check = max(1, int((next_risk_check_due_at - now_after).total_seconds()))
        wait_seconds = min(seconds_until_schedule, seconds_until_risk_check) if next_active_positions else seconds_until_schedule
        next_risk_check_at = next_risk_check_due_at if next_active_positions else now_after + timedelta(seconds=configured_risk_sec)
        with _farthest_auto_lock:
            _farthest_auto_state["next_risk_check_at"] = next_risk_check_at.isoformat()
        if _farthest_auto_stop.wait(timeout=wait_seconds):
            break

    with _farthest_auto_lock:
        _farthest_auto_state["running"] = False


def _parse_dollar_str_to_cents(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(round(float(value) * 100))
    try:
        cleaned = str(value).replace("$", "").replace(",", "").strip()
        return int(round(float(cleaned) * 100))
    except Exception:
        return None


def _pick_balance_amount_cents(balance: dict, cents_keys: list[str], dollar_keys: list[str]):
    for k in cents_keys:
        v = balance.get(k)
        if isinstance(v, (int, float)):
            return int(round(float(v))), k
    for k in dollar_keys:
        v = _parse_dollar_str_to_cents(balance.get(k))
        if v is not None:
            return int(v), k
    return None, None


def _extract_cashflow_summary(balance: dict):
    balance = balance or {}
    deposits_cents, deposits_key = _pick_balance_amount_cents(
        balance,
        [
            "deposits_cents",
            "deposit_cents",
            "total_deposits_cents",
            "lifetime_deposits_cents",
            "cash_in_cents",
        ],
        [
            "deposits",
            "deposit",
            "total_deposits",
            "lifetime_deposits",
            "cash_in",
            "deposits_dollars",
            "total_deposits_dollars",
            "lifetime_deposits_dollars",
            "cash_in_dollars",
        ],
    )
    withdrawals_cents, withdrawals_key = _pick_balance_amount_cents(
        balance,
        [
            "withdrawals_cents",
            "withdrawal_cents",
            "total_withdrawals_cents",
            "lifetime_withdrawals_cents",
            "cash_out_cents",
        ],
        [
            "withdrawals",
            "withdrawal",
            "total_withdrawals",
            "lifetime_withdrawals",
            "cash_out",
            "withdrawals_dollars",
            "total_withdrawals_dollars",
            "lifetime_withdrawals_dollars",
            "cash_out_dollars",
        ],
    )

    net_transfer_cents = None
    if deposits_cents is not None and withdrawals_cents is not None:
        net_transfer_cents = int(deposits_cents) - int(withdrawals_cents)

    available = deposits_cents is not None or withdrawals_cents is not None
    reason = None
    if not available:
        reason = (
            "Kalshi /portfolio/balance payload did not include deposit/withdrawal fields. "
            "Only cash/portfolio values are currently available for this account response."
        )

    return {
        "available": available,
        "reason": reason,
        "deposits_cents": deposits_cents,
        "withdrawals_cents": withdrawals_cents,
        "net_transfer_cents": net_transfer_cents,
        "source_keys": {
            "deposits": deposits_key,
            "withdrawals": withdrawals_key,
        },
        "balance_keys_seen": sorted(list(balance.keys())),
    }


@app.get("/kalshi/place_best_ask_order")
def place_best_ask_order(side: str, ticker: str, max_cost_cents: int = 500):
    try:
        side_norm = str(side or "").lower()
        if side_norm not in {"yes", "no"}:
            return JSONResponse(
                status_code=400,
                content={"error": "side must be 'yes' or 'no'", "stage": "validation"},
            )

        client = KalshiClient()
        if side_norm == "yes":
            resp = client.place_yes_limit_at_best_ask(
                ticker=ticker,
                max_cost_cents=max_cost_cents,
            )
        else:
            resp = client.place_no_limit_at_best_ask(
                ticker=ticker,
                max_cost_cents=max_cost_cents,
            )

        try:
            body = resp.json()
        except Exception:
            body = resp.text

        body_obj = body if isinstance(body, dict) else {}

        def _deep_find_first_number(obj, keys):
            if isinstance(obj, dict):
                for k in keys:
                    v = obj.get(k)
                    if isinstance(v, (int, float)):
                        return int(v)
                for v in obj.values():
                    found = _deep_find_first_number(v, keys)
                    if found is not None:
                        return found
            elif isinstance(obj, list):
                for item in obj:
                    found = _deep_find_first_number(item, keys)
                    if found is not None:
                        return found
            return None

        price_keys = [f"{side_norm}_price", "yes_price", "no_price", "price", "limit_price", "avg_price"]
        count_keys = ["filled_count", "matched_count", "executed_count", "count", "quantity"]
        price_cents = _deep_find_first_number(body_obj, price_keys)
        count = _deep_find_first_number(body_obj, count_keys)

        # Fallback to submitted best-ask intent when exchange response omits fill/order fields.
        if price_cents is None or count is None:
            try:
                top = client.get_top_of_book(ticker)
                ask = top.get(f"{side_norm}_ask")
                if price_cents is None and ask is not None:
                    price_cents = int(ask)
                if count is None and ask is not None and int(ask) > 0:
                    count = int(int(max_cost_cents) // int(ask))
            except Exception:
                pass
        _log_trade_ledger(
            source="/kalshi/place_best_ask_order",
            run_mode="live",
            action="buy",
            side=side_norm,
            ticker=ticker,
            price_cents=price_cents,
            count=count,
            status_code=resp.status_code if resp is not None else None,
            success=bool(resp is not None and resp.status_code in {200, 201}),
            note=f"max_cost_cents={max_cost_cents}",
            payload=body,
        )
        return {"status_code": resp.status_code, "response": body}
    except Exception as exc:
        _log_trade_ledger(
            source="/kalshi/place_best_ask_order",
            run_mode="live",
            action="buy",
            side=str(side or "").lower() if side is not None else None,
            ticker=ticker,
            price_cents=None,
            count=None,
            status_code=500,
            success=False,
            note=f"exception={type(exc).__name__}: {exc}",
            payload={"traceback": traceback.format_exc().splitlines()[-8:]},
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": str(exc),
                "error_type": type(exc).__name__,
                "stage": "place_best_ask_order",
                "side": str(side or "").lower(),
                "ticker": ticker,
                "max_cost_cents": max_cost_cents,
                "traceback": traceback.format_exc().splitlines()[-8:],
            },
        )


@app.get("/kalshi/portfolio/balance")
def get_portfolio_balance():
    client = KalshiClient()
    return client.get_balance()


@app.get("/kalshi/portfolio/cashflow")
def get_portfolio_cashflow():
    """
    Best-effort deposits/withdrawals summary from Kalshi balance payload.
    """
    client = KalshiClient()
    balance = client.get_balance() or {}
    cashflow = _extract_cashflow_summary(balance)
    return {
        "cashflow": cashflow,
        "raw_balance": balance,
    }


@app.get("/kalshi/portfolio/orders")
def get_portfolio_orders(status: str | None = None, ticker: str | None = None, limit: int = 100):
    """
    Fetch current orders (optionally filtered by status/ticker).
    """
    client = KalshiClient()
    return client.get_orders(status=status, ticker=ticker, limit=limit)


def _extract_order_count(order):
    for key in ("filled_count", "count", "quantity"):
        val = order.get(key)
        if isinstance(val, (int, float)):
            return int(val)
    return 0


def _extract_order_price_cents(order):
    for key in ("avg_price", "yes_price", "no_price", "price", "limit_price"):
        val = order.get(key)
        if isinstance(val, (int, float)):
            return int(val)
    return None


def _extract_order_price_cents_float(order):
    for key in ("avg_price", "yes_price", "no_price", "price", "limit_price"):
        val = order.get(key)
        if isinstance(val, (int, float)):
            v = float(val)
            if 0 < v <= 1.0:
                return v * 100.0
            return v
    return None


def _extract_order_fees_cents(order):
    return (
        _parse_dollar_str_to_cents(order.get("fees_paid_dollars"))
        or _parse_dollar_str_to_cents(order.get("fees_paid"))
        or _parse_dollar_str_to_cents(order.get("total_fees_dollars"))
        or _parse_dollar_str_to_cents(order.get("total_fees"))
        or _parse_dollar_str_to_cents(order.get("fee_dollars"))
        or _parse_dollar_str_to_cents(order.get("fee"))
        or 0
    )


def _order_sort_key(order):
    for k in ("created_time", "created_at", "updated_time", "updated_at", "ts", "time"):
        v = order.get(k)
        if not v:
            continue
        try:
            return datetime.datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
    return 0.0


def _midpoint(a, b):
    if a is None and b is None:
        return None
    if a is None:
        return b
    if b is None:
        return a
    return int(round((a + b) / 2))

def _price_to_cents(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        if 0 < v <= 1.0:
            return int(round(v * 100))
        if 0 < v <= 100:
            return int(round(v))
        return int(round(v))
    return None

def _price_to_cents_float(value):
    if value is None:
        return None
    try:
        v = float(value)
    except Exception:
        return None
    if 0 < v <= 1.0:
        return v * 100.0
    if 0 < v <= 100:
        return v
    return v


def _extract_money_cents(position: dict, cents_keys: list[str], dollar_keys: list[str]):
    for k in cents_keys:
        v = position.get(k)
        if isinstance(v, (int, float)):
            return int(round(float(v)))
    for k in dollar_keys:
        v = _parse_dollar_str_to_cents(position.get(k))
        if v is not None:
            return int(v)
    return None


@app.get("/kalshi/portfolio/current")
def get_portfolio_current_orders(status: str | None = "open", limit: int = 200):
    """
    Current orders/positions with estimated cost, mark-based PnL, and max payout.
    """
    client = KalshiClient()
    orders = client.get_all_orders(status=status, limit=limit, max_pages=20) if status else client.get_all_orders(limit=limit, max_pages=20)
    balance = client.get_balance()
    cashflow = _extract_cashflow_summary(balance)
    positions = client.get_positions()
    positions_list = positions.get("market_positions") or positions.get("positions") or []

    rows = []

    # Prefer positions if available (what the Kalshi UI shows under Portfolio)
    for p in positions_list:
        ticker = p.get("ticker") or p.get("market_ticker") or p.get("event_ticker")
        raw_side = p.get("side") if p.get("side") is not None else p.get("position")
        side = str(raw_side).lower() if raw_side is not None else ""
        count = p.get("contracts") or p.get("quantity") or p.get("count") or 0
        if not count and isinstance(raw_side, (int, float)):
            count = int(abs(raw_side))
        if isinstance(raw_side, (int, float)) and raw_side != 0:
            side = "yes" if raw_side > 0 else "no"
        if side not in {"yes", "no"}:
            continue

        avg_price = p.get("avg_price") or p.get("average_price")
        avg_price_cents_float = _price_to_cents_float(avg_price)
        price_cents = _price_to_cents(avg_price)
        if price_cents is None:
            price_cents = _extract_order_price_cents({"avg_price": avg_price})

        fee_cents = (
            _parse_dollar_str_to_cents(p.get("fees_paid_dollars"))
            or _parse_dollar_str_to_cents(p.get("fees_paid"))
            or _parse_dollar_str_to_cents(p.get("total_fees_dollars"))
            or _parse_dollar_str_to_cents(p.get("total_fees"))
            or 0
        )
        total_traded_cents = (
            _parse_dollar_str_to_cents(p.get("total_traded_dollars"))
            or _parse_dollar_str_to_cents(p.get("total_traded"))
        )
        total_cost_cents = (
            _parse_dollar_str_to_cents(p.get("total_cost_dollars"))
            or _parse_dollar_str_to_cents(p.get("total_cost"))
            or _parse_dollar_str_to_cents(p.get("cost"))
        )

        cost_cents = None
        # Prefer explicit fee-inclusive total cost if available.
        if total_cost_cents is not None:
            cost_cents = total_cost_cents
        # Otherwise derive fee-inclusive cost from traded + fees.
        if cost_cents is None and total_traded_cents is not None:
            cost_cents = int(total_traded_cents) + int(fee_cents or 0)
        # Fallback to avg_price * contracts.
        if cost_cents is None and avg_price_cents_float is not None and count:
            cost_cents = int(round(float(avg_price_cents_float) * int(count)))
        if cost_cents is None and price_cents is not None and count:
            cost_cents = price_cents * int(count)
        max_payout_cents = 100 * int(count) if count else None
        mark_cents = None
        market_value_cents = None
        pnl_cents = None
        if ticker and side in {"yes", "no"} and count:
            try:
                top = client.get_top_of_book(ticker)
                mark_cents = _maybe_int(top.get(f"{side}_bid"))
            except Exception:
                mark_cents = None
        # Preferred valuation for portfolio panel: executable bid mark.
        if mark_cents is not None and int(count) > 0:
            market_value_cents = int(mark_cents) * int(count)
            if cost_cents is not None:
                pnl_cents = int(market_value_cents) - int(cost_cents)
        # Fallback to Kalshi payload market value/pnl when bid mark is unavailable.
        if market_value_cents is None:
            market_value_cents = _extract_money_cents(
                p,
                cents_keys=[
                    "market_value_cents",
                    "market_exposure_cents",
                    "position_value_cents",
                    "positions_value_cents",
                ],
                dollar_keys=[
                    "market_value",
                    "market_value_dollars",
                    "market_exposure_dollars",
                    "market_exposure",
                    "position_value",
                    "position_value_dollars",
                ],
            )
        if pnl_cents is None:
            pnl_cents = _extract_money_cents(
                p,
                cents_keys=[
                    "unrealized_pnl_cents",
                    "realized_pnl_cents",
                    "pnl_cents",
                ],
                dollar_keys=[
                    "unrealized_pnl_dollars",
                    "unrealized_pnl",
                    "realized_pnl_dollars",
                    "realized_pnl",
                    "pnl",
                ],
            )
        if pnl_cents is None and market_value_cents is not None and cost_cents is not None:
            pnl_cents = market_value_cents - cost_cents
        if cost_cents is None and market_value_cents is not None and pnl_cents is not None:
            cost_cents = market_value_cents - pnl_cents
        if pnl_cents is None and ticker and count:
            if price_cents is None and total_traded_cents is not None and count:
                price_cents = int(round(total_traded_cents / int(count)))
            if mark_cents is not None and price_cents is not None:
                pnl_cents = int((mark_cents - price_cents) * int(count))

        rows.append(
            {
                "order_id": p.get("position_id") or p.get("id"),
                "ticker": ticker,
                "side": side.upper() if side else p.get("side"),
                "status": "POSITION",
                "count": int(count) if count else 0,
                "price_cents": price_cents,
                "cost_cents": cost_cents,
                "market_value_cents": market_value_cents,
                "mark_cents": mark_cents,
                "pnl_cents": pnl_cents,
                "max_payout_cents": max_payout_cents,
            }
        )

    # Fallback to orders if no positions
    if not rows:
        for order in orders:
            ticker = order.get("ticker")
            side = (order.get("side") or "").lower()
            price = _extract_order_price_cents(order)
            count = _extract_order_count(order)
            cost_cents = price * count if price is not None and count else None
            max_payout_cents = 100 * count if count else None

            mark_cents = None
            pnl_cents = None
            market_value_cents = None
            if ticker and side in {"yes", "no"} and price is not None and count:
                top = client.get_top_of_book(ticker)
                if side == "yes":
                    mark_cents = _maybe_int(top.get("yes_bid"))
                    if mark_cents is None:
                        mark_cents = _midpoint(top.get("yes_bid"), top.get("yes_ask"))
                    if mark_cents is not None:
                        market_value_cents = int(mark_cents) * int(count)
                        pnl_cents = int((mark_cents - price) * count)
                else:
                    mark_cents = _maybe_int(top.get("no_bid"))
                    if mark_cents is None:
                        mark_cents = _midpoint(top.get("no_bid"), top.get("no_ask"))
                    if mark_cents is not None:
                        market_value_cents = int(mark_cents) * int(count)
                        pnl_cents = int((mark_cents - price) * count)

            rows.append(
                {
                    "order_id": order.get("order_id") or order.get("id"),
                    "ticker": ticker,
                    "side": order.get("side"),
                    "status": order.get("status"),
                    "count": count,
                    "price_cents": price,
                    "cost_cents": cost_cents,
                    "market_value_cents": market_value_cents,
                    "mark_cents": mark_cents,
                    "pnl_cents": pnl_cents,
                    "max_payout_cents": max_payout_cents,
                }
            )

    return {"balance": balance, "cashflow": cashflow, "orders": rows}


@app.get("/kalshi/pnl/summary")
def get_pnl_summary(limit: int = 300):
    """
    Compute FIFO realized/unrealized/net PnL from filled buy/sell orders.
    """
    client = KalshiClient()
    order_sources = {}
    merged = {}
    statuses_to_try = [None, "filled", "executed", "closed"]
    for st in statuses_to_try:
        try:
            batch = client.get_all_orders(status=st, limit=max(50, int(limit)), max_pages=50)
        except Exception:
            batch = []
        order_sources[str(st)] = len(batch or [])
        for o in batch or []:
            oid = str(o.get("order_id") or o.get("id") or "")
            if oid:
                merged[oid] = o
            else:
                # fallback key when id is absent
                k = f"{o.get('ticker')}|{o.get('side')}|{o.get('action')}|{o.get('created_time') or o.get('created_at') or ''}|{o.get('client_order_id') or ''}"
                merged[k] = o
    orders_sorted = sorted(list(merged.values()), key=_order_sort_key)

    # FIFO lots keyed by (ticker, side)
    lots: dict[tuple[str, str], list[dict]] = {}
    realized_by_key: dict[tuple[str, str], int] = {}
    buys_cost_cents = 0
    sells_proceeds_cents = 0
    fees_cents_total = 0
    fill_count = 0

    for o in orders_sorted:
        action = str(o.get("action") or "").lower()
        side = str(o.get("side") or "").lower()
        ticker = str(o.get("ticker") or "")
        if action not in {"buy", "sell"} or side not in {"yes", "no"} or not ticker:
            continue

        filled_count = (
            _maybe_int(o.get("filled_count"))
            or _maybe_int(o.get("matched_count"))
            or _maybe_int(o.get("executed_count"))
            or 0
        )
        order_count = int(_extract_order_count(o) or 0)
        status_txt = str(o.get("status") or "").lower()
        count = int(filled_count if filled_count > 0 else (order_count if status_txt in {"filled", "executed", "closed"} else 0))
        if count < 1:
            continue

        px = _extract_order_price_cents_float(o)
        if px is None:
            continue

        fees = int(_extract_order_fees_cents(o) or 0)
        fees_cents_total += fees
        fill_count += 1
        key = (ticker, side)

        # Per-contract fee allocation for consistent FIFO matching.
        fee_per_contract = float(fees) / float(count) if count > 0 else 0.0

        if action == "buy":
            total_cost = int(round(px * count + fees))
            buys_cost_cents += total_cost
            lots.setdefault(key, []).append(
                {
                    "remaining": int(count),
                    "cost_per_contract": float(px) + fee_per_contract,
                }
            )
            continue

        # sell leg
        total_proceeds = int(round(px * count - fees))
        sells_proceeds_cents += total_proceeds
        sell_proceeds_per_contract = float(px) - fee_per_contract
        remaining = int(count)
        realized_this = 0.0
        fifo = lots.setdefault(key, [])
        while remaining > 0 and fifo:
            lot = fifo[0]
            take = min(remaining, int(lot["remaining"]))
            realized_this += (sell_proceeds_per_contract - float(lot["cost_per_contract"])) * float(take)
            lot["remaining"] = int(lot["remaining"]) - take
            remaining -= take
            if lot["remaining"] <= 0:
                fifo.pop(0)
        # If unmatched sells exist, treat basis as 0 so PnL remains conservative/traceable.
        if remaining > 0:
            realized_this += sell_proceeds_per_contract * float(remaining)
        realized_by_key[key] = int(realized_by_key.get(key, 0) + int(round(realized_this)))

    # Mark open lots for unrealized PnL using executable bid side.
    open_cost_basis_cents = 0
    open_market_value_cents = 0
    by_market = []
    for (ticker, side), fifo in lots.items():
        open_qty = sum(int(l.get("remaining") or 0) for l in fifo)
        if open_qty < 1:
            continue
        basis = int(round(sum(float(l["cost_per_contract"]) * int(l["remaining"]) for l in fifo)))
        try:
            top = client.get_top_of_book(ticker)
        except Exception:
            top = {}
        mark = top.get(f"{side}_bid")
        if mark is None:
            mark = 0
        mv = int(round(float(mark) * float(open_qty)))
        unrealized = mv - basis
        realized = int(realized_by_key.get((ticker, side), 0))
        open_cost_basis_cents += basis
        open_market_value_cents += mv
        by_market.append(
            {
                "ticker": ticker,
                "side": side,
                "open_count": open_qty,
                "realized_pnl_cents": realized,
                "open_cost_basis_cents": basis,
                "open_market_value_cents": mv,
                "unrealized_pnl_cents": unrealized,
                "net_pnl_cents": realized + unrealized,
            }
        )

    realized_total = int(sum(realized_by_key.values()))
    unrealized_total = int(open_market_value_cents - open_cost_basis_cents)
    net_total = int(realized_total + unrealized_total)

    return {
        "fills_count": fill_count,
        "orders_considered": len(orders_sorted),
        "order_sources": order_sources,
        "totals": {
            "buys_cost_cents": int(buys_cost_cents),
            "sells_proceeds_cents": int(sells_proceeds_cents),
            "fees_cents": int(fees_cents_total),
            "realized_pnl_cents": realized_total,
            "open_cost_basis_cents": int(open_cost_basis_cents),
            "open_market_value_cents": int(open_market_value_cents),
            "unrealized_pnl_cents": unrealized_total,
            "net_pnl_cents": net_total,
        },
        "by_market": sorted(by_market, key=lambda r: (r["ticker"], r["side"])),
    }


@app.get("/kalshi/portfolio/positions_debug")
def get_portfolio_positions_debug():
    client = KalshiClient()
    positions = client.get_positions()
    positions_list = positions.get("market_positions") or positions.get("positions") or []
    return {
        "positions_sample": positions_list[:5],
        "raw": positions,
    }


@app.get("/strategy/farthest_band/preview")
def strategy_farthest_band_preview(
    side: str = "yes",
    ask_min_cents: int = 95,
    ask_max_cents: int = 99,
    max_cost_cents: int = 500,
    stop_loss_pct: float = 0.25,
):
    config = FarthestBandConfig(
        direction="lower",
        side=side,
        ask_min_cents=ask_min_cents,
        ask_max_cents=ask_max_cents,
        max_cost_cents=max_cost_cents,
        stop_loss_pct=stop_loss_pct,
        mode="paper",
    )
    ingest = _latest_ingest_record()
    if not ingest:
        return {"error": "No ingest record found. Start ingest first."}

    spot = KrakenClient().latest_btc_price().price
    selection = select_farthest_band_market(
        spot=spot,
        markets=ingest.get("markets", []),
        config=config,
    )
    return {
        "spot": spot,
        "ingest_run_id": ingest.get("id"),
        "ingest_ts": ingest.get("ts"),
        "event_ticker": ingest.get("event_ticker"),
        "selection": selection,
    }


@app.get("/strategy/farthest_band/run")
def strategy_farthest_band_run(
    side: str = "yes",
    ask_min_cents: int = 95,
    ask_max_cents: int = 99,
    max_cost_cents: int = 500,
    mode: str = "paper",
    stop_loss_pct: float = 0.25,
    force_new_order: bool = False,
):
    config = FarthestBandConfig(
        direction="lower",
        side=side,
        ask_min_cents=ask_min_cents,
        ask_max_cents=ask_max_cents,
        max_cost_cents=max_cost_cents,
        stop_loss_pct=stop_loss_pct,
        mode=mode,
    )
    try:
        with _farthest_auto_lock:
            active_mode = _farthest_auto_state.get("active_position_mode")
            cfg_mode = str(config.mode).lower()
            if force_new_order:
                active_position = None
            elif active_mode == cfg_mode:
                active_position = dict(_farthest_auto_state.get("active_position") or {}) or None
            else:
                active_position = None
        out = _run_farthest_band_once(config, active_position=active_position)
        _record_strategy_ledger(out, source="/strategy/farthest_band/run")
        with _farthest_auto_lock:
            _farthest_auto_state["active_position"] = out.get("active_position")
            _farthest_auto_state["active_position_mode"] = cfg_mode if out.get("active_position") else None
            _farthest_auto_state["last_run_at"] = datetime.datetime.now(timezone.utc).isoformat()
            _farthest_auto_state["last_result"] = out
        return out
    except Exception as exc:
        return {"action": "error", "reason": str(exc)}


@app.get("/strategy/farthest_band/auto/start")
def strategy_farthest_band_auto_start(
    side: str = "yes",
    ask_min_cents: int = 95,
    ask_max_cents: int = 99,
    max_cost_cents: int = 500,
    mode: str = "paper",
    interval_minutes: int = 15,
    stop_loss_pct: float = 0.25,
    stop_loss_check_seconds: int = 300,
):
    global _farthest_auto_thread
    cfg_mode = str(mode or "").lower()

    config = FarthestBandConfig(
        direction="lower",
        side=side,
        ask_min_cents=ask_min_cents,
        ask_max_cents=ask_max_cents,
        max_cost_cents=max_cost_cents,
        mode=mode,
        interval_minutes=interval_minutes,
        stop_loss_pct=stop_loss_pct,
        stop_loss_check_seconds=stop_loss_check_seconds,
    )

    with _farthest_auto_lock:
        if _farthest_auto_thread and _farthest_auto_thread.is_alive():
            return {
                "running": True,
                "message": "Auto strategy already running",
                "state": _farthest_auto_state,
            }

        _farthest_auto_stop.clear()
        _farthest_auto_state["running"] = True
        _farthest_auto_state["config"] = config.__dict__.copy()
        _farthest_auto_state["last_run_at"] = None
        _farthest_auto_state["last_scheduled_run_at"] = None
        _farthest_auto_state["last_risk_check_at"] = None
        _farthest_auto_state["next_scheduled_run_at"] = datetime.datetime.now(timezone.utc).isoformat()
        _farthest_auto_state["next_risk_check_at"] = datetime.datetime.now(timezone.utc).isoformat()
        _farthest_auto_state["last_result"] = None
        _farthest_auto_state["risk_snapshot"] = []
        history = _get_stop_loss_trigger_history(limit=5000)
        _farthest_auto_state["stop_loss_trigger_history"] = history
        _farthest_auto_state["last_stop_loss_trigger"] = history[-1] if history else None
        seeded_positions = _seed_active_positions_from_portfolio(config) or []
        _farthest_auto_state["active_positions"] = seeded_positions
        _farthest_auto_state["active_position"] = seeded_positions[-1] if seeded_positions else None
        _farthest_auto_state["active_position_mode"] = cfg_mode if seeded_positions else None
        _farthest_auto_thread = threading.Thread(
            target=_farthest_band_worker,
            name="farthest-band-auto",
            daemon=True,
        )
        _farthest_auto_thread.start()

    return {
        "running": True,
        "message": "Auto strategy started",
        "seeded_active_positions": len(_farthest_auto_state.get("active_positions") or []),
        "state": _farthest_auto_state,
    }


@app.get("/strategy/farthest_band/auto/stop")
def strategy_farthest_band_auto_stop():
    _farthest_auto_stop.set()
    with _farthest_auto_lock:
        _farthest_auto_state["running"] = False
        _farthest_auto_state["risk_snapshot"] = []
        active_position = _farthest_auto_state.get("active_position")
        active_positions = list(_farthest_auto_state.get("active_positions") or [])
    return {
        "running": False,
        "message": "Auto strategy stop requested",
        "active_position": active_position,
        "active_positions": active_positions,
    }


@app.get("/strategy/farthest_band/reset")
def strategy_farthest_band_reset():
    with _farthest_auto_lock:
        _farthest_auto_state["active_position"] = None
        _farthest_auto_state["active_positions"] = []
        _farthest_auto_state["active_position_mode"] = None
        _farthest_auto_state["last_result"] = None
        _farthest_auto_state["risk_snapshot"] = []
        history = _get_stop_loss_trigger_history(limit=5000)
        _farthest_auto_state["stop_loss_trigger_history"] = history
        _farthest_auto_state["last_stop_loss_trigger"] = history[-1] if history else None
    return {"ok": True, "message": "Strategy active position reset"}


@app.get("/strategy/farthest_band/auto/status")
def strategy_farthest_band_auto_status():
    with _farthest_auto_lock:
        thread_alive = bool(_farthest_auto_thread and _farthest_auto_thread.is_alive())
        state = dict(_farthest_auto_state)
        state["running"] = thread_alive
    history = _get_stop_loss_trigger_history(limit=5000)
    state["stop_loss_trigger_history"] = history
    state["last_stop_loss_trigger"] = history[-1] if history else None
    # Refresh risk snapshot on demand so stop-loss panel can always show latest P/L%.
    if thread_alive:
        active_positions = state.get("active_positions") or []
        state["risk_snapshot"] = _build_risk_snapshot(active_positions)
    return state


@app.get("/ledger/trades")
def get_trade_ledger(limit: int = 200):
    limit = max(1, min(int(limit), 2000))
    conn = _open_ledger_db()
    try:
        _ensure_trade_ledger(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, ts, source, run_mode, action, side, ticker, price_cents, count,
                   cost_cents, status_code, success, note, payload_json
            FROM trade_ledger
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        return {"records": [dict(r) for r in cur.fetchall()]}
    finally:
        conn.close()


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>BTC Ticker Dashboard</title>
    <style>
      :root {
        --bg-1: #0b1020;
        --bg-2: #141b2f;
        --accent: #f4c430;
        --text: #eef2ff;
        --muted: #9aa4bf;
        --card: rgba(255, 255, 255, 0.06);
        --border: rgba(255, 255, 255, 0.12);
        --graph-1: #22d3ee;
        --graph-2: #f97316;
        --graph-3: #a3e635;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        min-height: 100vh;
        color: var(--text);
        background: radial-gradient(1200px 600px at 10% 10%, #1f2a4d 0%, transparent 60%),
                    radial-gradient(900px 500px at 90% 20%, #2b1a3b 0%, transparent 55%),
                    linear-gradient(160deg, var(--bg-1), var(--bg-2));
        font-family: "Space Grotesk", "IBM Plex Sans", "Segoe UI", sans-serif;
        display: grid;
        place-items: center;
        padding: 12px;
      }
      .wrap {
        width: min(2200px, 100%);
      }
      .card {
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 20px;
        padding: 34px;
        box-shadow: 0 10px 40px rgba(0, 0, 0, 0.35);
        backdrop-filter: blur(6px);
      }
      .grid {
        display: grid;
        gap: 18px;
      }
      .chart-wrap {
        height: 150px;
        background: rgba(7, 12, 24, 0.45);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 12px;
        position: relative;
      }
      canvas {
        width: 100%;
        height: 100%;
      }
      .title {
        display: flex;
        align-items: center;
        gap: 12px;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        color: var(--muted);
        font-size: 12px;
      }
      .dot {
        width: 10px;
        height: 10px;
        border-radius: 999px;
        background: var(--accent);
        box-shadow: 0 0 14px var(--accent);
      }
      .price {
        font-size: clamp(42px, 6vw, 84px);
        font-weight: 700;
        margin: 18px 0 8px 0;
      }
      .hero {
        display: grid;
        grid-template-columns: minmax(0, 1.8fr) minmax(0, 1fr) minmax(0, 1fr);
        gap: 20px;
        align-items: stretch;
      }
      .hero-balance {
        display: grid;
        align-content: center;
        justify-items: center;
        gap: 8px;
        background:
          radial-gradient(circle at 20% 15%, rgba(34, 211, 238, 0.18), transparent 50%),
          radial-gradient(circle at 80% 90%, rgba(250, 204, 21, 0.16), transparent 48%),
          rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.2);
        border-radius: 14px;
        padding: 16px 18px;
        min-height: 120px;
        box-shadow:
          inset 0 0 0 1px rgba(255, 255, 255, 0.04),
          0 0 20px rgba(34, 211, 238, 0.12),
          0 0 24px rgba(250, 204, 21, 0.1);
      }
      .hero-balance-label {
        color: #dbeafe;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-weight: 700;
      }
      .hero-balance-values {
        display: flex;
        gap: 12px;
        align-items: baseline;
        flex-wrap: wrap;
        justify-content: center;
      }
      .hero-balance-item {
        display: grid;
        gap: 2px;
        padding: 8px 10px;
        border-radius: 10px;
        border: 1px solid rgba(255, 255, 255, 0.18);
        background: rgba(8, 16, 32, 0.45);
        min-width: 130px;
      }
      .hero-balance-key {
        color: #a5b4fc;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }
      .hero-balance-val {
        color: #e2e8f0;
        font-size: 24px;
        font-weight: 700;
        line-height: 1.1;
      }
      #hero-cash {
        color: #86efac;
        text-shadow: 0 0 14px rgba(34, 197, 94, 0.32);
      }
      #hero-portfolio {
        color: #fcd34d;
        text-shadow: 0 0 16px rgba(250, 204, 21, 0.35);
      }
      .expiry-card {
        display: grid;
        justify-items: end;
        gap: 8px;
      }
      .expiry-label {
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }
      .expiry-ticker {
        color: var(--text);
        font-size: 12px;
        opacity: 0.85;
      }
      .expiry-value {
        font-size: clamp(32px, 4.6vw, 62px);
        font-weight: 700;
        color: #ffd36a;
        line-height: 1;
        text-shadow: 0 0 18px rgba(244, 196, 48, 0.28);
      }
      .chart-tooltip {
        position: absolute;
        pointer-events: none;
        transform: translate(-50%, -120%);
        background: rgba(10, 16, 32, 0.95);
        border: 1px solid rgba(255, 255, 255, 0.2);
        border-radius: 8px;
        color: var(--text);
        font-size: 11px;
        line-height: 1.25;
        padding: 6px 8px;
        white-space: nowrap;
        display: none;
        z-index: 2;
      }
      .sub {
        color: var(--muted);
        font-size: 14px;
      }
      .pill {
        padding: 6px 10px;
        border-radius: 999px;
        background: rgba(244, 196, 48, 0.12);
        color: var(--accent);
        border: 1px solid rgba(244, 196, 48, 0.35);
      }
      .pill.secondary {
        background: rgba(255, 255, 255, 0.08);
        color: var(--text);
        border: 1px solid rgba(255, 255, 255, 0.18);
      }
      .markets {
        background: rgba(10, 16, 32, 0.6);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 12px;
        align-self: start;
      }
      .top-panels {
        display: grid;
        grid-template-columns: minmax(0, 1.2fr) minmax(0, 1.9fr) minmax(0, 1.2fr);
        grid-template-areas:
          "markets auto right";
        gap: 16px;
        align-items: start;
      }
      .markets { grid-area: markets; }
      .right-stack {
        grid-area: right;
        display: grid;
        gap: 16px;
        align-content: start;
      }
      .portfolio-panel {
        background: rgba(10, 16, 32, 0.6);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 12px;
        min-height: 260px;
      }
      .auto-panel { grid-area: auto; }
      .strategy-panel {
        background: rgba(10, 16, 32, 0.6);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 12px;
      }
      .portfolio-summary {
        font-size: 12px;
        color: var(--muted);
        margin: 0 0 8px 0;
      }
      .portfolio-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
        table-layout: fixed;
      }
      .ledger-panel {
        background: rgba(10, 16, 32, 0.6);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 12px;
      }
      .pnl-panel {
        background: rgba(10, 16, 32, 0.6);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 12px;
      }
      .pnl-totals {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 8px;
        margin-bottom: 10px;
      }
      .pnl-total {
        background: rgba(255, 255, 255, 0.04);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 10px;
        padding: 8px;
      }
      .pnl-total .k {
        font-size: 11px;
        color: var(--muted);
        text-transform: uppercase;
      }
      .pnl-total .v {
        font-size: 14px;
        color: var(--text);
        margin-top: 4px;
      }
      .pnl-table-wrap {
        max-height: 220px;
        overflow-y: auto;
      }
      .pnl-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
      }
      .pnl-table th,
      .pnl-table td {
        padding: 6px 4px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        text-align: left;
        white-space: nowrap;
      }
      .pnl-table th {
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
      }
      .ledger-table-wrap {
        max-height: 260px;
        overflow-y: auto;
      }
      .ledger-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 12px;
      }
      .ledger-table th,
      .ledger-table td {
        padding: 6px 4px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        text-align: left;
        white-space: nowrap;
      }
      .ledger-table th {
        color: var(--muted);
        font-weight: 600;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }
      .portfolio-table th,
      .portfolio-table td {
        padding: 6px 3px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        text-align: left;
      }
      .portfolio-table th:nth-child(1),
      .portfolio-table td:nth-child(1) {
        width: 33%;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      .portfolio-table th:nth-child(2),
      .portfolio-table td:nth-child(2) {
        width: 7%;
      }
      .portfolio-table th:nth-child(3),
      .portfolio-table td:nth-child(3),
      .portfolio-table th:nth-child(4),
      .portfolio-table td:nth-child(4),
      .portfolio-table th:nth-child(5),
      .portfolio-table td:nth-child(5),
      .portfolio-table th:nth-child(6),
      .portfolio-table td:nth-child(6),
      .portfolio-table th:nth-child(7),
      .portfolio-table td:nth-child(7) {
        width: 9%;
        white-space: nowrap;
      }
      .portfolio-table th {
        color: var(--muted);
        font-weight: 600;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }
      .pnl-pos { color: #34d399; }
      .pnl-neg { color: #f87171; }
      .pnl-flat { color: var(--text); }
      .markets h3 {
        margin: 0;
        font-size: 13px;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }
      .markets-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin: 0 0 10px 0;
        gap: 12px;
      }
      .markets-actions {
        display: flex;
        align-items: center;
        gap: 10px;
      }
      .max-cost {
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 12px;
        color: var(--muted);
      }
      .max-cost input {
        width: 72px;
        padding: 4px 8px;
        border-radius: 8px;
        border: 1px solid var(--border);
        background: rgba(255, 255, 255, 0.06);
        color: var(--text);
      }
      .btn {
        padding: 6px 12px;
        border-radius: 999px;
        border: 1px solid var(--border);
        background: rgba(244, 196, 48, 0.18);
        color: var(--accent);
        font-size: 12px;
        cursor: pointer;
      }
      .btn:hover {
        background: rgba(244, 196, 48, 0.26);
      }
      .btn.secondary {
        background: rgba(255, 255, 255, 0.07);
        color: var(--text);
      }
      .btn.trade {
        padding: 4px 8px;
        font-size: 11px;
        margin-left: 6px;
      }
      .markets tr.row-hot td {
        background: rgba(34, 211, 238, 0.2);
        color: #d8fbff;
        font-weight: 700;
        text-shadow: 0 0 8px rgba(34, 211, 238, 0.35);
      }
      .markets tr.row-hot td:first-child {
        box-shadow: inset 4px 0 0 #22d3ee, inset 0 0 0 1px rgba(34, 211, 238, 0.25);
      }
      .markets tr.row-hot td .btn.trade {
        border-color: rgba(34, 211, 238, 0.7);
        box-shadow: 0 0 12px rgba(34, 211, 238, 0.25);
      }
      .markets tr.row-pending td {
        background: rgba(244, 196, 48, 0.12);
      }
      .markets tr.row-error td {
        background: rgba(239, 68, 68, 0.14);
      }
      .markets tr.row-success td {
        background: rgba(16, 185, 129, 0.12);
      }
      .trade-status {
        margin-top: 8px;
        font-size: 12px;
        color: var(--muted);
      }
      .trade-status.pending { color: #fde68a; }
      .trade-status.error { color: #fca5a5; }
      .trade-status.success { color: #86efac; }
      .markets table {
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
      }
      .markets th,
      .markets td {
        padding: 8px 6px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        text-align: left;
      }
      .markets th {
        color: var(--muted);
        font-weight: 600;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }
      .markets tr:last-child td {
        border-bottom: none;
      }
      .strategy-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 8px;
      }
      .field {
        display: grid;
        gap: 4px;
      }
      .field label {
        font-size: 11px;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.05em;
      }
      .field input,
      .field select {
        width: 100%;
        padding: 6px 8px;
        border-radius: 8px;
        border: 1px solid var(--border);
        background: rgba(255, 255, 255, 0.06);
        color: var(--text);
        font-size: 12px;
      }
      .strategy-actions {
        margin-top: 10px;
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }
      .strategy-status {
        margin-top: 10px;
        font-size: 12px;
        color: var(--muted);
      }
      .auto-trade-badge {
        margin-top: 8px;
        font-size: 12px;
        color: var(--text);
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.14);
        border-radius: 10px;
        padding: 8px;
      }
      .strategy-schedule {
        margin-top: 8px;
      }
      .strategy-next-run {
        font-size: 12px;
        color: var(--muted);
        margin-bottom: 6px;
      }
      .progress-track {
        width: 100%;
        height: 8px;
        background: rgba(255, 255, 255, 0.08);
        border: 1px solid rgba(255, 255, 255, 0.12);
        border-radius: 999px;
        overflow: hidden;
      }
      .progress-fill {
        height: 100%;
        width: 0%;
        background: linear-gradient(90deg, #f4c430, #f97316);
        transition: width 0.3s ease;
      }
      .stoploss-panel {
        margin-top: 10px;
        padding: 8px;
        border-radius: 10px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        background: rgba(255, 255, 255, 0.04);
      }
      .stoploss-title {
        font-size: 12px;
        color: var(--muted);
        margin-bottom: 6px;
      }
      .stoploss-next {
        font-size: 12px;
        color: var(--text);
        margin-bottom: 6px;
      }
      .stoploss-positions {
        margin-top: 8px;
        font-size: 12px;
        color: var(--muted);
        min-height: 110px;
        max-height: 110px;
        overflow-y: auto;
        white-space: pre-wrap;
      }
      .stoploss-last-run {
        margin-top: 8px;
        font-size: 12px;
        color: var(--text);
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.12);
        border-radius: 10px;
        padding: 8px;
        white-space: pre-wrap;
        min-height: 84px;
      }
      .planned-order {
        margin-top: 10px;
        font-size: 12px;
        color: var(--text);
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 10px;
        padding: 8px;
        white-space: pre-wrap;
        min-height: 112px;
        max-height: 112px;
        overflow-y: scroll;
      }
      .planned-order.execution-ok {
        border-color: rgba(16, 185, 129, 0.45);
        box-shadow: inset 0 0 0 1px rgba(16, 185, 129, 0.18);
      }
      .planned-order.execution-fail {
        border-color: rgba(239, 68, 68, 0.45);
        box-shadow: inset 0 0 0 1px rgba(239, 68, 68, 0.18);
      }
      .planned-order.execution-none {
        border-color: rgba(255, 255, 255, 0.14);
      }
      .candidate-list {
        margin-top: 10px;
        font-size: 12px;
        color: var(--muted);
        min-height: 170px;
        max-height: 170px;
        overflow-y: scroll;
      }
      .candidate-list table {
        width: 100%;
        border-collapse: collapse;
      }
      .candidate-list th,
      .candidate-list td {
        text-align: left;
        padding: 4px 2px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.08);
      }
      @media (max-width: 520px) {
        .card { padding: 22px; }
        .hero { grid-template-columns: 1fr; }
        .hero-balance { justify-items: start; }
        .expiry-card { justify-items: start; }
        .top-panels {
          grid-template-columns: 1fr;
          grid-template-areas:
            "markets"
            "auto"
            "right";
        }
      }
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="card">
        <div class="grid">
          <div class="hero">
            <div>
              <div class="title">
                <span class="dot"></span>
                Live BTC Ticker
              </div>
              <div id="price" class="price">$--</div>
              <div id="ts" class="sub">Last update: --</div>
            </div>
            <div class="hero-balance">
              <div class="hero-balance-label">Portfolio Snapshot</div>
              <div class="hero-balance-values">
                <div class="hero-balance-item">
                  <div class="hero-balance-key">Cash</div>
                  <div id="hero-cash" class="hero-balance-val">$--</div>
                </div>
                <div class="hero-balance-item">
                  <div class="hero-balance-key">Portfolio</div>
                  <div id="hero-portfolio" class="hero-balance-val">$--</div>
                </div>
              </div>
            </div>
            <div class="expiry-card">
              <div class="expiry-label">Ticker Expiry</div>
              <div id="event-ticker-name" class="expiry-ticker">--</div>
              <div id="event-expiry" class="expiry-value">--</div>
            </div>
          </div>
          <div class="chart-wrap">
            <canvas id="chart"></canvas>
            <div id="chart-tooltip" class="chart-tooltip"></div>
          </div>
          <div class="top-panels">
            <div class="markets">
              <div class="markets-header">
                <h3>Latest 10 Kalshi Markets</h3>
                <div class="markets-actions">
                  <label class="max-cost">
                    Max Cost (c)
                    <input id="max-cost" type="number" min="1" value="100" />
                  </label>
                  <button id="refresh-markets" class="btn" type="button">Refresh</button>
                </div>
              </div>
              <table>
                <thead>
                  <tr>
                    <th>Strike</th>
                    <th>Yes Ask</th>
                    <th>No Ask</th>
                    <th>Subtitle</th>
                  </tr>
                </thead>
                <tbody id="markets-body">
                  <tr><td colspan="4">Click Refresh to load markets.</td></tr>
                </tbody>
              </table>
            </div>
            <div class="strategy-panel auto-panel">
                <div class="markets-header">
                  <h3>Farthest Strategy: Auto</h3>
                </div>
                <div class="strategy-grid">
                  <div class="field">
                    <label for="strategy-mode">Mode</label>
                    <select id="strategy-mode">
                      <option value="paper" selected>paper</option>
                      <option value="live">live</option>
                    </select>
                  </div>
                  <div class="field">
                    <label for="strategy-side">Side</label>
                    <select id="strategy-side">
                      <option value="yes" selected>yes</option>
                      <option value="no">no</option>
                    </select>
                  </div>
                  <div class="field">
                    <label for="strategy-ask-min">Ask Min (c)</label>
                    <input id="strategy-ask-min" type="number" min="1" max="99" value="95" />
                  </div>
                  <div class="field">
                    <label for="strategy-ask-max">Ask Max (c)</label>
                    <input id="strategy-ask-max" type="number" min="1" max="99" value="99" />
                  </div>
                  <div class="field">
                    <label for="strategy-max-cost">Max Cost (c)</label>
                    <input id="strategy-max-cost" type="number" min="1" value="500" />
                  </div>
                  <div class="field">
                    <label for="strategy-interval">Auto Interval (min)</label>
                    <input id="strategy-interval" type="number" min="1" value="15" />
                  </div>
                  <div class="field">
                    <label for="strategy-stop-loss-pct">Stop-Loss %</label>
                    <input id="strategy-stop-loss-pct" type="number" min="0.1" step="0.1" value="25.0" />
                  </div>
                  <div class="field">
                    <label for="strategy-stop-loss-seconds">Risk Check (sec)</label>
                    <input id="strategy-stop-loss-seconds" type="number" min="1" value="300" />
                  </div>
                </div>
                <div class="strategy-actions">
                  <button id="strategy-auto-start" class="btn" type="button">Auto Start</button>
                  <button id="strategy-auto-status" class="btn secondary" type="button">Auto Status</button>
                  <button id="strategy-auto-stop" class="btn secondary" type="button">Auto Stop</button>
                </div>
                <div id="strategy-status" class="strategy-status">Strategy idle.</div>
                <div id="auto-last-trade-badge" class="auto-trade-badge">Last Auto Trade: none yet.</div>
                <div class="strategy-schedule">
                  <div id="strategy-next-run" class="strategy-next-run">Next schedule: --</div>
                  <div class="progress-track">
                    <div id="strategy-progress-fill" class="progress-fill"></div>
                  </div>
                </div>
                <div class="stoploss-panel">
                  <div class="stoploss-title">Stop-loss check cadence</div>
                  <div id="stoploss-config" class="stoploss-next">Frequency: -- | Trigger: --</div>
                  <div id="stoploss-next-run" class="stoploss-next">Next stop-loss check: --</div>
                  <div class="progress-track">
                    <div id="stoploss-progress-fill" class="progress-fill"></div>
                  </div>
                  <div id="stoploss-positions" class="stoploss-positions">Protected positions: --</div>
                  <div id="stoploss-last-run" class="stoploss-last-run">Last stop-loss run: --</div>
                </div>
                <div id="auto-strategy-planned-order" class="planned-order">Auto plan will appear after Auto Status.</div>
                <div id="auto-strategy-candidates" class="candidate-list"></div>
            </div>
            <div class="right-stack">
              <div class="portfolio-panel">
                <div class="markets-header">
                  <h3>Current Portfolio</h3>
                  <div class="markets-actions">
                    <label class="max-cost">
                      <input id="portfolio-btc-only" type="checkbox" checked />
                      BTC only
                    </label>
                    <button id="refresh-portfolio" class="btn" type="button">Refresh</button>
                  </div>
                </div>
                <div id="portfolio-summary" class="portfolio-summary">No data loaded.</div>
                <table class="portfolio-table">
                  <thead>
                    <tr>
                      <th>Ticker</th>
                      <th>Side</th>
                      <th>Cost</th>
                      <th>Value</th>
                      <th>P/L</th>
                      <th>P/L %</th>
                      <th>Max</th>
                    </tr>
                  </thead>
                  <tbody id="portfolio-body">
                    <tr><td colspan="7">Click Refresh to load orders.</td></tr>
                  </tbody>
                </table>
              </div>
              <div class="strategy-panel manual-panel">
                <div class="markets-header">
                  <h3>Farthest Strategy: Manual</h3>
                </div>
                <div class="strategy-grid">
                  <div class="field">
                    <label for="manual-strategy-mode">Mode</label>
                    <select id="manual-strategy-mode">
                      <option value="paper" selected>paper</option>
                      <option value="live">live</option>
                    </select>
                  </div>
                  <div class="field">
                    <label for="manual-strategy-side">Side</label>
                    <select id="manual-strategy-side">
                      <option value="yes" selected>yes</option>
                      <option value="no">no</option>
                    </select>
                  </div>
                  <div class="field">
                    <label for="manual-strategy-ask-min">Ask Min (c)</label>
                    <input id="manual-strategy-ask-min" type="number" min="1" max="99" value="95" />
                  </div>
                  <div class="field">
                    <label for="manual-strategy-ask-max">Ask Max (c)</label>
                    <input id="manual-strategy-ask-max" type="number" min="1" max="99" value="99" />
                  </div>
                  <div class="field">
                    <label for="manual-strategy-max-cost">Max Cost (c)</label>
                    <input id="manual-strategy-max-cost" type="number" min="1" value="500" />
                  </div>
                  <div class="field">
                    <label for="manual-strategy-interval">Auto Interval (min)</label>
                    <input id="manual-strategy-interval" type="number" min="1" value="15" />
                  </div>
                  <div class="field">
                    <label for="manual-strategy-stop-loss-pct">Stop-Loss %</label>
                    <input id="manual-strategy-stop-loss-pct" type="number" min="0.1" step="0.1" value="25.0" />
                  </div>
                  <div class="field">
                    <label for="manual-strategy-stop-loss-seconds">Risk Check (sec)</label>
                    <input id="manual-strategy-stop-loss-seconds" type="number" min="1" value="300" />
                  </div>
                </div>
                <div class="strategy-actions">
                  <button id="strategy-preview" class="btn secondary" type="button">Preview</button>
                  <button id="strategy-run" class="btn" type="button">Run Once</button>
                </div>
                <div id="manual-strategy-planned-order" class="planned-order">Manual planned order will appear after Preview.</div>
                <div id="manual-strategy-candidates" class="candidate-list"></div>
              </div>
            </div>
          </div>
          <div class="ledger-panel">
            <div class="markets-header">
              <h3>Trade Ledger</h3>
              <button id="refresh-ledger" class="btn" type="button">Refresh</button>
            </div>
            <div class="ledger-table-wrap">
              <table class="ledger-table">
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Source</th>
                    <th>Action</th>
                    <th>Side</th>
                    <th>Ticker</th>
                    <th>Price</th>
                    <th>Count</th>
                    <th>Cost</th>
                    <th>Status</th>
                    <th>Reason</th>
                    <th>Note</th>
                  </tr>
                </thead>
                <tbody id="ledger-body">
                  <tr><td colspan="11">Click Refresh to load ledger.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
          <div class="pnl-panel">
            <div class="markets-header">
              <h3>P/L Summary (All Orders)</h3>
              <button id="refresh-pnl" class="btn" type="button">Refresh</button>
            </div>
            <div id="pnl-totals" class="pnl-totals">
              <div class="pnl-total"><div class="k">Realized</div><div class="v">--</div></div>
              <div class="pnl-total"><div class="k">Unrealized</div><div class="v">--</div></div>
              <div class="pnl-total"><div class="k">Net</div><div class="v">--</div></div>
              <div class="pnl-total"><div class="k">Fees</div><div class="v">--</div></div>
            </div>
            <div class="pnl-table-wrap">
              <table class="pnl-table">
                <thead>
                  <tr>
                    <th>Ticker</th>
                    <th>Side</th>
                    <th>Open</th>
                    <th>Realized</th>
                    <th>Unrealized</th>
                    <th>Net</th>
                  </tr>
                </thead>
                <tbody id="pnl-body">
                  <tr><td colspan="6">Click Refresh to load P/L summary.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
    <script>
      const priceEl = document.getElementById("price");
      const tsEl = document.getElementById("ts");
      const eventExpiryEl = document.getElementById("event-expiry");
      const eventTickerNameEl = document.getElementById("event-ticker-name");
      const heroCashEl = document.getElementById("hero-cash");
      const heroPortfolioEl = document.getElementById("hero-portfolio");
      const canvas = document.getElementById("chart");
      const ctx = canvas.getContext("2d");
      const chartTooltipEl = document.getElementById("chart-tooltip");
      const marketsBody = document.getElementById("markets-body");
      const refreshMarketsBtn = document.getElementById("refresh-markets");
      const maxCostEl = document.getElementById("max-cost");
      const refreshPortfolioBtn = document.getElementById("refresh-portfolio");
      const portfolioBtcOnlyEl = document.getElementById("portfolio-btc-only");
      const portfolioSummaryEl = document.getElementById("portfolio-summary");
      const portfolioBody = document.getElementById("portfolio-body");
      const refreshLedgerBtn = document.getElementById("refresh-ledger");
      const ledgerBody = document.getElementById("ledger-body");
      const refreshPnlBtn = document.getElementById("refresh-pnl");
      const pnlTotalsEl = document.getElementById("pnl-totals");
      const pnlBody = document.getElementById("pnl-body");
      const strategyModeEl = document.getElementById("strategy-mode");
      const strategySideEl = document.getElementById("strategy-side");
      const strategyAskMinEl = document.getElementById("strategy-ask-min");
      const strategyAskMaxEl = document.getElementById("strategy-ask-max");
      const strategyMaxCostEl = document.getElementById("strategy-max-cost");
      const strategyIntervalEl = document.getElementById("strategy-interval");
      const strategyStopLossPctEl = document.getElementById("strategy-stop-loss-pct");
      const strategyStopLossSecondsEl = document.getElementById("strategy-stop-loss-seconds");
      const manualStrategyModeEl = document.getElementById("manual-strategy-mode");
      const manualStrategySideEl = document.getElementById("manual-strategy-side");
      const manualStrategyAskMinEl = document.getElementById("manual-strategy-ask-min");
      const manualStrategyAskMaxEl = document.getElementById("manual-strategy-ask-max");
      const manualStrategyMaxCostEl = document.getElementById("manual-strategy-max-cost");
      const manualStrategyIntervalEl = document.getElementById("manual-strategy-interval");
      const manualStrategyStopLossPctEl = document.getElementById("manual-strategy-stop-loss-pct");
      const manualStrategyStopLossSecondsEl = document.getElementById("manual-strategy-stop-loss-seconds");
      const strategyPreviewBtn = document.getElementById("strategy-preview");
      const strategyRunBtn = document.getElementById("strategy-run");
      const strategyAutoStartBtn = document.getElementById("strategy-auto-start");
      const strategyAutoStatusBtn = document.getElementById("strategy-auto-status");
      const strategyAutoStopBtn = document.getElementById("strategy-auto-stop");
      const strategyStatusEl = document.getElementById("strategy-status");
      const autoLastTradeBadgeEl = document.getElementById("auto-last-trade-badge");
      const strategyNextRunEl = document.getElementById("strategy-next-run");
      const strategyProgressFillEl = document.getElementById("strategy-progress-fill");
      const stopLossConfigEl = document.getElementById("stoploss-config");
      const stopLossNextRunEl = document.getElementById("stoploss-next-run");
      const stopLossProgressFillEl = document.getElementById("stoploss-progress-fill");
      const stopLossPositionsEl = document.getElementById("stoploss-positions");
      const stopLossLastRunEl = document.getElementById("stoploss-last-run");
      const manualStrategyPlannedOrderEl = document.getElementById("manual-strategy-planned-order");
      const manualStrategyCandidatesEl = document.getElementById("manual-strategy-candidates");
      const autoStrategyPlannedOrderEl = document.getElementById("auto-strategy-planned-order");
      const autoStrategyCandidatesEl = document.getElementById("auto-strategy-candidates");
      let strategyPreviewInflight = false;
      let strategyAutoStatusInflight = false;
      let currentEventTicker = null;
      let currentEventExpiryMs = null;
      let chartScreenPoints = [];
      let chartBounds = { left: 0, top: 0, width: 0, height: 0 };

      function syncStrategyControls(fromManual) {
        if (fromManual) {
          strategyModeEl.value = manualStrategyModeEl.value;
          strategySideEl.value = manualStrategySideEl.value;
          strategyAskMinEl.value = manualStrategyAskMinEl.value;
          strategyAskMaxEl.value = manualStrategyAskMaxEl.value;
          strategyMaxCostEl.value = manualStrategyMaxCostEl.value;
          strategyIntervalEl.value = manualStrategyIntervalEl.value;
          strategyStopLossPctEl.value = manualStrategyStopLossPctEl.value;
          strategyStopLossSecondsEl.value = manualStrategyStopLossSecondsEl.value;
          return;
        }
        manualStrategyModeEl.value = strategyModeEl.value;
        manualStrategySideEl.value = strategySideEl.value;
        manualStrategyAskMinEl.value = strategyAskMinEl.value;
        manualStrategyAskMaxEl.value = strategyAskMaxEl.value;
        manualStrategyMaxCostEl.value = strategyMaxCostEl.value;
        manualStrategyIntervalEl.value = strategyIntervalEl.value;
        manualStrategyStopLossPctEl.value = strategyStopLossPctEl.value;
        manualStrategyStopLossSecondsEl.value = strategyStopLossSecondsEl.value;
      }

      function resizeCanvas() {
        const rect = canvas.getBoundingClientRect();
        const dpr = window.devicePixelRatio || 1;
        canvas.width = Math.floor(rect.width * dpr);
        canvas.height = Math.floor(rect.height * dpr);
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      }

      function drawChart(points) {
        resizeCanvas();
        const w = canvas.getBoundingClientRect().width;
        const h = canvas.getBoundingClientRect().height;
        chartBounds = canvas.getBoundingClientRect();
        ctx.clearRect(0, 0, w, h);
        chartScreenPoints = [];

        const leftPad = 8;
        const rightPad = 8;
        const topPad = 6;
        const bottomPad = 18;
        const graphW = Math.max(1, w - leftPad - rightPad);
        const graphH = Math.max(1, h - topPad - bottomPad);

        if (!points.length) {
          ctx.fillStyle = "rgba(255,255,255,0.6)";
          ctx.font = "12px system-ui";
          ctx.fillText("No data in last hour", 12, 20);
          return;
        }

        const prices = points.map(p => p.price);
        const min = Math.min(...prices);
        const max = Math.max(...prices);
        const pad = (max - min) * 0.08 || 1;
        const minY = min - pad;
        const maxY = max + pad;

        const stepX = graphW / Math.max(points.length - 1, 1);

        const gradient = ctx.createLinearGradient(0, 0, w, 0);
        gradient.addColorStop(0, getComputedStyle(document.documentElement).getPropertyValue("--graph-1").trim());
        gradient.addColorStop(0.5, getComputedStyle(document.documentElement).getPropertyValue("--graph-2").trim());
        gradient.addColorStop(1, getComputedStyle(document.documentElement).getPropertyValue("--graph-3").trim());

        const firstTs = Number(points[0]?.tsMs || 0);
        const lastTs = Number(points[points.length - 1]?.tsMs || 0);
        if (firstTs > 0 && lastTs > firstTs) {
          const fiveMinMs = 5 * 60 * 1000;
          const firstTick = Math.ceil(firstTs / fiveMinMs) * fiveMinMs;
          ctx.strokeStyle = "rgba(255,255,255,0.08)";
          ctx.fillStyle = "rgba(255,255,255,0.45)";
          ctx.font = "10px system-ui";
          for (let t = firstTick; t <= lastTs; t += fiveMinMs) {
            const x = leftPad + ((t - firstTs) / (lastTs - firstTs)) * graphW;
            ctx.beginPath();
            ctx.moveTo(x, topPad);
            ctx.lineTo(x, topPad + graphH);
            ctx.stroke();
            const d = new Date(t);
            const hh = String(d.getHours()).padStart(2, "0");
            const mm = String(d.getMinutes()).padStart(2, "0");
            ctx.fillText(`${hh}:${mm}`, x - 14, h - 3);
          }
        }

        ctx.lineWidth = 2;
        ctx.strokeStyle = gradient;
        ctx.beginPath();
        points.forEach((p, i) => {
          const x = leftPad + i * stepX;
          const y = topPad + graphH - ((p.price - minY) / (maxY - minY)) * graphH;
          chartScreenPoints.push({ x, y, ts: p.ts, price: p.price });
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        });
        ctx.stroke();

        ctx.fillStyle = "rgba(255,255,255,0.08)";
        ctx.beginPath();
        points.forEach((p, i) => {
          const x = leftPad + i * stepX;
          const y = topPad + graphH - ((p.price - minY) / (maxY - minY)) * graphH;
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        });
        ctx.lineTo(leftPad + graphW, topPad + graphH);
        ctx.lineTo(leftPad, topPad + graphH);
        ctx.closePath();
        ctx.fill();
      }

      async function refresh() {
        try {
          const res = await fetch("/get_price_ticker");
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          const price = Number(data.BtcPrice || 0);
          priceEl.textContent = price
            ? price.toLocaleString("en-US", { style: "currency", currency: "USD" })
            : "$--";
          tsEl.textContent = `Last update: ${data.Timestamp || "n/a"}`;
        } catch (err) {}
      }

      async function refreshChart() {
        try {
          const res = await fetch("/kalshi_ingest/last_hour");
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          const points = (data.records || []).map(r => ({
            ts: r.ts,
            tsMs: Date.parse(r.ts || ""),
            price: Number(r.current_price || 0),
          })).filter(p => p.price > 0);
          drawChart(points);
        } catch (err) {
          drawChart([]);
        }
      }

      function renderMarkets(markets) {
        if (!markets || !markets.length) {
          marketsBody.innerHTML = "<tr><td colspan=\\"4\\">No markets found.</td></tr>";
          return;
        }
        const rows = markets.slice(0, 20).map(m => {
          const strike = Number(m.strike || 0);
          const ticker = m.ticker || "";
          const yesAsk = m.yes_ask ?? "--";
          const noAsk = m.no_ask ?? "--";
          const yesNum = Number(yesAsk);
          const rowHot = Number.isFinite(yesNum) && yesNum >= 90 && yesNum <= 99;
          const subtitle = m.subtitle || "";
          return `
            <tr class="${rowHot ? "row-hot" : ""}" data-market-ticker="${ticker}">
              <td>${strike ? strike.toLocaleString("en-US", { style: "currency", currency: "USD" }) : "--"}</td>
              <td>
                ${yesAsk}c
                <button class="btn trade" data-side="yes" data-ticker="${ticker}">YES</button>
              </td>
              <td>
                ${noAsk}c
                <button class="btn trade" data-side="no" data-ticker="${ticker}">NO</button>
              </td>
              <td>${subtitle}</td>
            </tr>
          `;
        }).join("");
        marketsBody.innerHTML = rows + `<tr><td colspan="4"><div id="trade-status" class="trade-status">Ready.</div></td></tr>`;
        marketsBody.querySelectorAll("button.trade").forEach(btn => {
          btn.addEventListener("click", () => {
            const side = btn.getAttribute("data-side");
            const ticker = btn.getAttribute("data-ticker");
            placeBestAskOrder(side, ticker, btn);
          });
        });
      }

      function setTradeRowState(ticker, stateClass) {
        const rows = Array.from(marketsBody.querySelectorAll("tr[data-market-ticker]"));
        rows.forEach((row) => {
          if (String(row.getAttribute("data-market-ticker")) !== String(ticker)) return;
          row.classList.remove("row-pending", "row-error", "row-success");
          if (stateClass) row.classList.add(stateClass);
        });
      }

      function extractTradeErrorText(data, fallbackStatus) {
        const statusTxt = fallbackStatus ? `HTTP ${fallbackStatus}` : "HTTP error";
        const objectToMsg = (obj) => {
          if (!obj || typeof obj !== "object") return "";
          const msg = obj.error || obj.message || obj.detail || obj.reason || obj.title;
          const code = obj.error_code || obj.code || obj.status || obj.status_code;
          if (msg && code) return `${code}: ${msg}`;
          if (msg) return String(msg);
          try { return JSON.stringify(obj); } catch (_) { return ""; }
        };
        if (!data) return statusTxt;
        if (data.error) {
          const errMsg = (typeof data.error === "object") ? objectToMsg(data.error) : String(data.error);
          return `${statusTxt}${data.stage ? ` [${data.stage}]` : ""}: ${(errMsg || "Unknown error").slice(0, 220)}`;
        }
        if (data.raw_response && String(data.raw_response).trim()) {
          return `${statusTxt}: ${String(data.raw_response).trim().slice(0, 220)}`;
        }
        const body = data.response;
        if (typeof body === "string" && body.trim()) return `${statusTxt}: ${body.trim().slice(0, 220)}`;
        if (body && typeof body === "object") {
          const nestedError = (typeof body.error === "object") ? body.error : null;
          if (nestedError) {
            const nestedMsg = objectToMsg(nestedError);
            if (nestedMsg) return `${statusTxt}: ${nestedMsg.slice(0, 220)}`;
          }
          const msg = objectToMsg(body);
          if (msg) return `${statusTxt}: ${msg.slice(0, 220)}`;
          try { return `${statusTxt}: ${JSON.stringify(body).slice(0, 220)}`; } catch (_) {}
        }
        if (data.status_code) return `HTTP ${data.status_code}`;
        return statusTxt;
      }

      async function refreshMarkets() {
        try {
          const res = await fetch("/kalshi_ingest/latest");
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          const records = data.records || [];
          const latest = records.length ? records[0] : null;
          setEventExpiryFromIngest(latest);
          updateEventExpiryCountdown();
          renderMarkets(latest ? (latest.markets || []) : []);
        } catch (err) {
          renderMarkets([]);
        }
      }

      async function refreshExpiryMeta() {
        try {
          const res = await fetch("/kalshi_ingest/latest");
          if (!res.ok) return;
          const data = await res.json();
          const latest = (data.records || [])[0] || null;
          setEventExpiryFromIngest(latest);
          updateEventExpiryCountdown();
        } catch (_) {}
      }

      async function placeBestAskOrder(side, ticker, btnEl = null) {
        const statusEl = document.getElementById("trade-status");
        const maxCost = Number(maxCostEl.value || 0);
        if (!ticker) {
          if (statusEl) statusEl.textContent = "Missing ticker.";
          return;
        }
        if (!maxCost || maxCost < 1) {
          if (statusEl) statusEl.textContent = "Invalid max cost.";
          return;
        }
        if (statusEl) {
          statusEl.className = "trade-status pending";
          statusEl.textContent = `Placing ${side.toUpperCase()} order...`;
        }
        if (btnEl) btnEl.disabled = true;
        setTradeRowState(ticker, "row-pending");
        try {
          const url = `/kalshi/place_best_ask_order?side=${encodeURIComponent(side)}&ticker=${encodeURIComponent(ticker)}&max_cost_cents=${encodeURIComponent(maxCost)}`;
          const res = await fetch(url);
          let data = null;
          const rawText = await res.text();
          try {
            data = rawText ? JSON.parse(rawText) : {};
          } catch (_) {
            data = { raw_response: rawText };
          }
          const exchangeStatus = Number(data?.status_code || 0);
          const failed = !res.ok || !!data.error || (exchangeStatus && ![200, 201].includes(exchangeStatus));
          if (failed) {
            const reason = extractTradeErrorText(data, res.status);
            if (statusEl) {
              statusEl.className = "trade-status error";
              statusEl.textContent = `Order failed (${side.toUpperCase()} ${ticker}): ${reason}`;
            }
            setTradeRowState(ticker, "row-error");
            return;
          }
          if (statusEl) {
            statusEl.className = "trade-status success";
            statusEl.textContent = `Order submitted (${side.toUpperCase()}) for ${ticker}.`;
          }
          setTradeRowState(ticker, "row-success");
        } catch (err) {
          if (statusEl) {
            statusEl.className = "trade-status error";
            statusEl.textContent = `Request failed (${side.toUpperCase()} ${ticker}): ${err?.message || "network error"}`;
          }
          setTradeRowState(ticker, "row-error");
        } finally {
          if (btnEl) btnEl.disabled = false;
        }
      }

      function formatCents(cents) {
        if (cents === null || cents === undefined) return "--";
        const dollars = cents / 100;
        const sign = dollars >= 0 ? "" : "-";
        return `${sign}$${Math.abs(dollars).toFixed(2)}`;
      }

      function shortTicker(ticker) {
        const t = String(ticker || "--");
        return t.replace(/^KXBTCD-/i, "");
      }

      function asIntOrNull(v) {
        if (v === null || v === undefined) return null;
        const n = Number(v);
        if (!Number.isFinite(n)) return null;
        return Math.round(n);
      }

      function dollarsToCentsOrNull(v) {
        if (v === null || v === undefined) return null;
        if (typeof v === "string") {
          const raw = v.trim();
          const cleaned = raw.replace(/[$,]/g, "");
          if (!cleaned) return null;
          const n = Number(cleaned);
          if (!Number.isFinite(n)) return null;
          // Heuristic: whole numbers >= 1000 are often cent-denominated even without _cents suffix.
          if (/^-?\d+$/.test(cleaned) && Math.abs(n) >= 1000) return Math.round(n);
          return Math.round(n * 100);
        }
        const n = Number(v);
        if (!Number.isFinite(n)) return null;
        // Heuristic: large whole numbers are likely cents.
        if (Number.isInteger(n) && Math.abs(n) >= 1000) return Math.round(n);
        return Math.round(n * 100);
      }

      function pickBalanceCents(balance, centsKeys, dollarKeys) {
        for (const k of centsKeys) {
          const v = asIntOrNull(balance?.[k]);
          if (v !== null) return v;
        }
        for (const k of dollarKeys) {
          const v = dollarsToCentsOrNull(balance?.[k]);
          if (v !== null) return v;
        }
        return null;
      }

      function strategyParams(includeMode = false, includeInterval = false) {
        const params = new URLSearchParams();
        params.set("side", strategySideEl.value || "yes");
        params.set("ask_min_cents", String(Number(strategyAskMinEl.value || 95)));
        params.set("ask_max_cents", String(Number(strategyAskMaxEl.value || 99)));
        params.set("max_cost_cents", String(Number(strategyMaxCostEl.value || 500)));
        params.set("stop_loss_pct", String((Number(strategyStopLossPctEl.value || 25) / 100)));
        if (includeMode) params.set("mode", strategyModeEl.value || "paper");
        if (includeInterval) params.set("interval_minutes", String(Number(strategyIntervalEl.value || 15)));
        if (includeInterval) params.set("stop_loss_check_seconds", String(Number(strategyStopLossSecondsEl.value || 300)));
        return params.toString();
      }

      function formatDuration(sec) {
        const s = Math.max(0, Math.floor(Number(sec || 0)));
        const m = Math.floor(s / 60);
        const r = s % 60;
        return `${m}m ${String(r).padStart(2, "0")}s`;
      }

      function parseEventExpiryMs(eventTicker, ingestTs) {
        const raw = String(eventTicker || "");
        const parts = raw.split("-");
        if (parts.length < 2) return null;
        const token = String(parts[1] || "").toUpperCase();
        const match = token.match(/^(\\d{2})([A-Z]{3})(\\d{2})(\\d{2})$/);
        if (!match) return null;
        // Kalshi BTC event token format: YYMONDDHH (e.g., 26FEB1505).
        const year = 2000 + Number(match[1]);
        const monTxt = match[2];
        const day = Number(match[3]);
        const hour = Number(match[4]);
        const minute = 0;
        const monthMap = { JAN: 0, FEB: 1, MAR: 2, APR: 3, MAY: 4, JUN: 5, JUL: 6, AUG: 7, SEP: 8, OCT: 9, NOV: 10, DEC: 11 };
        const month = monthMap[monTxt];
        if (month === undefined) return null;
        // Ticker time token is interpreted as EST (UTC-5), then converted to UTC.
        const expiryMs = Date.UTC(year, month, day, hour + 5, minute, 0, 0);
        return Number.isFinite(expiryMs) ? expiryMs : null;
      }

      function setEventExpiryFromIngest(latest) {
        if (!latest) {
          currentEventTicker = null;
          currentEventExpiryMs = null;
          eventTickerNameEl.textContent = "--";
          return;
        }
        currentEventTicker = latest.event_ticker || null;
        eventTickerNameEl.textContent = currentEventTicker || "--";
        currentEventExpiryMs = parseEventExpiryMs(latest.event_ticker, latest.ts);
      }

      function updateEventExpiryCountdown() {
        if (!currentEventTicker || !currentEventExpiryMs) {
          eventExpiryEl.textContent = "--";
          return;
        }
        const remSec = Math.floor((currentEventExpiryMs - Date.now()) / 1000);
        if (remSec <= 0) {
          eventExpiryEl.textContent = "Expired";
          return;
        }
        eventExpiryEl.textContent = formatDuration(remSec);
      }

      function updateScheduleProgress(statusData) {
        if (!statusData || !statusData.running) {
          strategyNextRunEl.textContent = "Next schedule: auto not running";
          strategyProgressFillEl.style.width = "0%";
          return;
        }

        const intervalMin = Number(statusData?.config?.interval_minutes || strategyIntervalEl.value || 15);
        const intervalSec = Math.max(60, Math.floor(intervalMin * 60));
        const nextRunAt = statusData?.next_scheduled_run_at ? Date.parse(statusData.next_scheduled_run_at) : NaN;
        const lastScheduledAt = statusData?.last_scheduled_run_at ? Date.parse(statusData.last_scheduled_run_at) : NaN;
        if (!Number.isFinite(nextRunAt)) {
          strategyNextRunEl.textContent = `Next schedule: waiting schedule (every ${intervalMin}m)`;
          strategyProgressFillEl.style.width = "0%";
          return;
        }

        const nowMs = Date.now();
        const remainingSec = Math.max(0, (nextRunAt - nowMs) / 1000);
        let elapsedSec = intervalSec - remainingSec;
        if (Number.isFinite(lastScheduledAt)) {
          elapsedSec = Math.max(0, Math.min(intervalSec, (nowMs - lastScheduledAt) / 1000));
        }
        const clampedElapsed = Math.max(0, Math.min(intervalSec, elapsedSec));
        const progressPct = Math.max(0, Math.min(100, (clampedElapsed / intervalSec) * 100));
        strategyNextRunEl.textContent = `Next schedule in ${formatDuration(remainingSec)} (every ${intervalMin}m)`;
        strategyProgressFillEl.style.width = `${progressPct.toFixed(1)}%`;
      }

      function updateStopLossPanel(statusData) {
        const configuredRiskSec = Math.max(1, Number(statusData?.config?.stop_loss_check_seconds || strategyStopLossSecondsEl.value || 300));
        const fmtTs = (iso) => {
          if (!iso) return "n/a";
          const d = new Date(iso);
          if (!Number.isFinite(d.getTime())) return String(iso);
          return d.toLocaleString();
        };
        const triggerHistory = (statusData && typeof statusData === "object" && Array.isArray(statusData.stop_loss_trigger_history))
          ? statusData.stop_loss_trigger_history
          : [];
        const trigger = (statusData && typeof statusData === "object")
          ? (statusData.last_stop_loss_trigger || triggerHistory[triggerHistory.length - 1] || null)
          : null;
        const recentHistory = triggerHistory
          .slice()
          .reverse()
          .map((t, i) => {
            const soldTicker = t?.sold?.ticker || "--";
            const soldPx = t?.sold?.price_cents ?? "--";
            const soldCount = t?.sold?.count ?? "--";
            const boughtTicker = t?.bought?.ticker || "--";
            const boughtPx = t?.bought?.price_cents ?? "--";
            const boughtCount = t?.bought?.count ?? "--";
            const lossPct = (t?.realized_pnl_pct !== null && t?.realized_pnl_pct !== undefined && Number.isFinite(Number(t.realized_pnl_pct)))
              ? `${(Number(t.realized_pnl_pct) * 100).toFixed(2)}%`
              : "--";
            const sellStatus = String(t?.sell_status || (t?.sell_error ? "failed" : "ok")).toUpperCase();
            const buyStatus = String(t?.buy_status || (t?.buy_error ? "failed" : "ok")).toUpperCase();
            const sellReason = t?.sell_error ? ` (${t.sell_error})` : "";
            const buyReason = t?.buy_error ? ` (${t.buy_error})` : (t?.buy_selection_reason ? ` (${t.buy_selection_reason})` : "");
            return `${i + 1}) ${fmtTs(t?.triggered_at)} | Sell: ${soldTicker} @ ${soldPx}c x ${soldCount} [${sellStatus}]${sellReason} | Loss: ${lossPct} | Buy: ${boughtTicker} @ ${boughtPx}c x ${boughtCount} [${buyStatus}]${buyReason}`;
          })
          .join("\\n");
        const triggerBlock = trigger
          ? [
              "",
              "Last stop-loss trigger snapshot",
              `Time: ${fmtTs(trigger?.triggered_at)}`,
              `Purchased: ${(trigger?.sold?.ticker || "--")} @ ${(trigger?.entry?.price_cents ?? "--")}c x ${(trigger?.entry?.count ?? "--")} (${formatCents(trigger?.entry?.purchase_value_cents)})`,
              `Sell order: ${(trigger?.sold?.ticker || "--")} @ ${(trigger?.sold?.price_cents ?? "--")}c x ${(trigger?.sold?.count ?? "--")} [${String(trigger?.sell_status || (trigger?.sell_error ? "failed" : "ok")).toUpperCase()}]`,
              `Sold value: ${formatCents(trigger?.sold_value_cents)} | Realized P/L: ${formatCents(trigger?.realized_pnl_cents)} (${(trigger?.realized_pnl_pct !== null && trigger?.realized_pnl_pct !== undefined && Number.isFinite(Number(trigger?.realized_pnl_pct))) ? `${(Number(trigger.realized_pnl_pct) * 100).toFixed(2)}%` : "--"})`,
              trigger?.sell_error ? `Sell failure reason: ${trigger.sell_error}` : "",
              `Buy order: ${(trigger?.bought?.ticker || "--")} @ ${(trigger?.bought?.price_cents ?? "--")}c x ${(trigger?.bought?.count ?? "--")} [${String(trigger?.buy_status || (trigger?.buy_error ? "failed" : "ok")).toUpperCase()}]`,
              trigger?.buy_fallback_used ? `Buy fallback: used nearest candidate (${trigger?.buy_fallback_reason || "fallback"})` : "",
              (!trigger?.buy_fallback_used && trigger?.buy_selection_reason) ? `Buy selection reason: ${trigger.buy_selection_reason}` : "",
              trigger?.buy_error ? `Buy failure reason: ${trigger.buy_error}` : "",
              recentHistory ? `\\nTrigger history (${triggerHistory.length})\\n${recentHistory}` : "",
            ].filter(Boolean).join("\\n")
          : "";
        const cadenceSec = Math.max(1, Math.floor(configuredRiskSec));
        const cadenceText = cadenceSec % 60 === 0
          ? `${Math.floor(cadenceSec / 60)}m`
          : `${cadenceSec}s`;
        const stopLossPct = Math.abs(Number(statusData?.config?.stop_loss_pct ?? 0.25)) * 100;
        if (stopLossConfigEl) {
          stopLossConfigEl.textContent = `Frequency: every ${cadenceText} | Trigger: loss > ${stopLossPct.toFixed(2)}%`;
        }
        if (!statusData || !statusData.running) {
          stopLossNextRunEl.textContent = "Next stop-loss check: auto not running";
          stopLossProgressFillEl.style.width = "0%";
          stopLossPositionsEl.textContent = "Protected positions: none";
          if (stopLossLastRunEl) {
            stopLossLastRunEl.textContent = `Last stop-loss run: auto not running${triggerBlock}`;
          }
          return;
        }

        const nextRiskAt = statusData?.next_risk_check_at ? Date.parse(statusData.next_risk_check_at) : NaN;
        const lastRiskAt = statusData?.last_risk_check_at ? Date.parse(statusData.last_risk_check_at) : NaN;
        if (!Number.isFinite(nextRiskAt)) {
          stopLossNextRunEl.textContent = "Next stop-loss check: waiting";
          stopLossProgressFillEl.style.width = "0%";
        } else {
          const nowMs = Date.now();
          const remainingSec = Math.max(0, (nextRiskAt - nowMs) / 1000);
          let elapsedSec = cadenceSec - remainingSec;
          if (Number.isFinite(lastRiskAt)) {
            elapsedSec = Math.max(0, Math.min(cadenceSec, (nowMs - lastRiskAt) / 1000));
          }
          const progressPct = Math.max(0, Math.min(100, (elapsedSec / cadenceSec) * 100));
          stopLossNextRunEl.textContent = `Next stop-loss check in ${formatDuration(remainingSec)}`;
          stopLossProgressFillEl.style.width = `${progressPct.toFixed(1)}%`;
        }

        const positions = Array.isArray(statusData?.active_positions) ? statusData.active_positions : [];
        const snapshots = Array.isArray(statusData?.risk_snapshot) ? statusData.risk_snapshot : [];
        if (!positions.length) {
          stopLossPositionsEl.textContent = "Protected positions: none (stop-loss starts after first tracked entry/seed).";
          if (stopLossLastRunEl) {
            stopLossLastRunEl.textContent = `Last stop-loss run\\nTime: ${fmtTs(statusData?.last_risk_check_at)}\\nResult: No protected positions.${triggerBlock}`;
          }
          return;
        }
        const fmtPct = (v) => {
          if (v === null || v === undefined || !Number.isFinite(Number(v))) return "--";
          const n = Number(v) * 100;
          const sign = n > 0 ? "+" : "";
          return `${sign}${n.toFixed(2)}%`;
        };
        const lines = positions.slice(0, 6).map((p, i) => {
          const side = String(p?.side || "--").toUpperCase();
          const ticker = p?.ticker || "--";
          const entry = p?.entry_price_cents ?? "--";
          const count = p?.count ?? "--";
          const snap = snapshots.find(s =>
            String(s?.ticker || "") === String(ticker || "") &&
            String((s?.side || "").toUpperCase()) === side
          ) || snapshots[i] || null;
          let pnlPctText = fmtPct(snap?.pnl_pct);
          let suffix = "";
          if (!snap) {
            pnlPctText = "n/a";
            suffix = " (no snapshot yet)";
          } else if (snap?.error) {
            suffix = " (mark err)";
          } else if ((snap?.pnl_pct === null || snap?.pnl_pct === undefined) && (snap?.mark_cents === null || snap?.mark_cents === undefined)) {
            suffix = " (mark unavailable)";
          }
          return `${i + 1}. ${ticker} (${side}) entry ${entry}c x ${count} | last P/L ${pnlPctText}${suffix}`;
        });
        const extra = positions.length > 6 ? `\\n... +${positions.length - 6} more` : "";
        stopLossPositionsEl.textContent = `Protected positions: ${positions.length}\\n${lines.join("\\n")}${extra}`;

        if (stopLossLastRunEl) {
          const lastRiskAt = statusData?.last_risk_check_at;
          const lastOut = statusData?.last_result || {};
          const cycle = (lastOut && typeof lastOut === "object" && lastOut.result && typeof lastOut.result === "object")
            ? lastOut.result
            : lastOut;
          const action = String(cycle?.action || "").toLowerCase();
          const reason = cycle?.reason || "";
          let decision = "Checked protected positions; no stop-loss trigger.";
          let detail = "";
          if (action === "stop_loss_rotate") {
            const exit = cycle?.exit || {};
            const reentry = cycle?.reentry || {};
            const soldTicker = cycle?.exited_position?.ticker || "--";
            const soldCount = cycle?.exited_position?.count ?? "--";
            const soldPx = exit?.exit_price_cents ?? "--";
            const boughtTicker = reentry?.active_position?.ticker || reentry?.selection?.selected?.ticker || "--";
            const boughtCount = reentry?.active_position?.count ?? reentry?.selection?.count ?? "--";
            const boughtPx = reentry?.active_position?.entry_price_cents ?? reentry?.selection?.selected?.ask_cents ?? "--";
            decision = "Triggered: stop-loss hit.";
            detail = `Sold: ${soldTicker} @ ${soldPx}c x ${soldCount}\\nBought: ${boughtTicker} @ ${boughtPx}c x ${boughtCount}`;
            if (exit?.action === "hold" && exit?.reason) {
              detail += `\\nSell error: ${exit.reason}`;
            }
            if (reentry?.action === "hold" && reentry?.reason) {
              detail += `\\nBuy error: ${reentry.reason}`;
            }
          } else if (action === "hold_active" && String(reason || "").toLowerCase().includes("stop-loss triggered")) {
            decision = `Triggered, but exit failed: ${reason || "unknown reason"}`;
            const exitReason = cycle?.exit?.reason || "";
            if (exitReason) detail = `Sell error: ${exitReason}`;
          } else if (action === "rollover_reenter") {
            decision = "Rollover action executed (not stop-loss).";
          } else if (action === "error") {
            decision = `Risk check error: ${reason || "unknown error"}`;
          } else {
            const checked = snapshots
              .slice(0, 4)
              .map((s) => `${s?.ticker || "--"} ${fmtPct(s?.pnl_pct)}`)
              .join(" | ");
            detail = checked ? `Checked: ${checked}` : "";
          }
          stopLossLastRunEl.textContent = `Last stop-loss run\\nTime: ${fmtTs(lastRiskAt)}\\nResult: ${decision}${detail ? `\\n${detail}` : ""}${triggerBlock}`;
        }
      }

      function renderStrategyCandidates(candidates, targetEl) {
        if (!candidates || !candidates.length) {
          targetEl.innerHTML = "";
          return;
        }
        const strikeText = (v) => {
          const n = Number(v);
          return Number.isFinite(n) ? n.toLocaleString("en-US", { style: "currency", currency: "USD" }) : "--";
        };
        const rows = candidates.map(c => `
          <tr>
            <td>${c.ticker || "--"}</td>
            <td>${strikeText(c.strike)}</td>
            <td>${c.ask_cents ?? "--"}c</td>
            <td>${c.ask_band_distance_cents ?? "--"}c</td>
          </tr>
        `).join("");
        targetEl.innerHTML = `
          <div>Nearest candidates</div>
          <table>
            <thead>
              <tr>
                <th>Ticker</th>
                <th>Strike</th>
                <th>Ask</th>
                <th>Band Gap</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        `;
      }

      function renderStrategySelection(payload, label, panel = "manual") {
        const plannedEl = panel === "auto" ? autoStrategyPlannedOrderEl : manualStrategyPlannedOrderEl;
        const candidatesEl = panel === "auto" ? autoStrategyCandidatesEl : manualStrategyCandidatesEl;
        const setExecutionStyle = (status) => {
          plannedEl.classList.remove("execution-ok", "execution-fail", "execution-none");
          plannedEl.classList.add(status === "ok" ? "execution-ok" : (status === "fail" ? "execution-fail" : "execution-none"));
        };
        const cycle = payload?.result || payload || {};
        const selection =
          payload?.selection ||
          cycle?.selection ||
          cycle?.entry?.selection ||
          cycle?.reentry?.selection ||
          null;
        const selected = selection?.selected || null;
        const count = selection?.count ?? cycle?.entry?.selection?.count ?? cycle?.reentry?.selection?.count ?? null;
        const estCost = selection?.estimated_cost_cents ?? null;
        const mode =
          cycle?.entry?.mode ||
          cycle?.reentry?.mode ||
          cycle?.mode ||
          strategyModeEl.value ||
          "paper";
        const executionLeg = cycle?.reentry || cycle?.entry || null;
        const liveExecutionLine = () => {
          if (String(mode).toLowerCase() !== "live") return null;
          if (!executionLeg || typeof executionLeg !== "object") return "Execution: no order attempted.";
          const legAction = String(executionLeg?.action || "").toLowerCase();
          const statusCode = executionLeg?.status_code;
          const reason = executionLeg?.reason || "";
          if (legAction === "hold") {
            return `Execution: Order NOT placed${statusCode ? ` (status ${statusCode})` : ""}. Reason: ${reason || "unknown failure"}`;
          }
          if (statusCode === 200 || statusCode === 201) {
            return `Execution: Order placed (status ${statusCode}).`;
          }
          if (statusCode) {
            return `Execution: Order NOT placed (status ${statusCode}). Reason: ${reason || "exchange rejected/failed"}`;
          }
          if (executionLeg?.active_position) {
            return "Execution: Order placed.";
          }
          return `Execution: Order NOT placed. Reason: ${reason || "unknown failure"}`;
        };
        const liveExecutionStatus = () => {
          if (String(mode).toLowerCase() !== "live") return "none";
          if (!executionLeg || typeof executionLeg !== "object") return "none";
          const legAction = String(executionLeg?.action || "").toLowerCase();
          const statusCode = executionLeg?.status_code;
          if (legAction === "hold") return "fail";
          if (statusCode === 200 || statusCode === 201) return "ok";
          if (statusCode) return "fail";
          if (executionLeg?.active_position) return "ok";
          return "none";
        };
        if (!selection) {
          setExecutionStyle(liveExecutionStatus());
          const active = cycle?.active_position || payload?.active_position || null;
          const executionSummary = liveExecutionLine();
          if (active) {
            const lines = [
              `${label} (${mode})`,
              `No new order selected.`,
              `Active Position: ${active.ticker || "--"} (${String(active.side || "--").toUpperCase()})`,
              `Entry: ${active.entry_price_cents ?? "--"}c`,
              `Count: ${active.count ?? "--"}`,
              `Reason: ${cycle?.reason || "holding active position"}`,
            ];
            if (executionSummary) lines.push(executionSummary);
            plannedEl.textContent = lines.join("\\n");
          } else {
            plannedEl.textContent = [`No strategy selection returned. ${cycle?.reason || ""}`.trim(), executionSummary]
              .filter(Boolean)
              .join("\\n");
          }
          candidatesEl.innerHTML = "";
          return;
        }
        if (!selected) {
          setExecutionStyle(liveExecutionStatus());
          const executionSummary = liveExecutionLine();
          plannedEl.textContent = [`${label}: no exact match. ${selection.reason || ""}`.trim(), executionSummary]
            .filter(Boolean)
            .join("\\n");
          renderStrategyCandidates(selection.nearest_candidates || [], candidatesEl);
          return;
        }
        const lines = [
          `${label} (${mode})`,
          `Ticker: ${selected.ticker}`,
          `Strike: ${Number(selected.strike || 0).toLocaleString("en-US", { style: "currency", currency: "USD" })}`,
          `Ask: ${selected.ask_cents}c (${selected.ask_key})`,
          `Planned Count: ${count ?? "--"}`,
          `Planned Cost: ${estCost !== null ? `${estCost}c / ${formatCents(estCost)}` : "--"}`,
          `Expected Return: ${selected.expected_return_pct !== null && selected.expected_return_pct !== undefined ? `${(selected.expected_return_pct * 100).toFixed(2)}%` : "--"}`,
        ];
        const executionSummary = liveExecutionLine();
        if (executionSummary) lines.push(executionSummary);
        setExecutionStyle(liveExecutionStatus());
        plannedEl.textContent = lines.join("\\n");
        renderStrategyCandidates(selection.nearest_candidates || [], candidatesEl);
      }

      async function strategyPreview(quiet = false) {
        if (strategyPreviewInflight) return;
        strategyPreviewInflight = true;
        if (!quiet) strategyStatusEl.textContent = "Previewing strategy...";
        try {
          const res = await fetch(`/strategy/farthest_band/preview?${strategyParams(false, false)}`);
          const data = await res.json();
          if (!res.ok || data.error) throw new Error(data.error || `HTTP ${res.status}`);
          if (!quiet) {
            strategyStatusEl.textContent = `Preview OK. Spot ${Number(data.spot || 0).toLocaleString("en-US", { style: "currency", currency: "USD" })}`;
          }
          renderStrategySelection(data, "Planned order", "manual");
        } catch (err) {
          if (!quiet) strategyStatusEl.textContent = `Preview failed: ${err.message || "error"}`;
        } finally {
          strategyPreviewInflight = false;
        }
      }

      async function strategyRun() {
        strategyStatusEl.textContent = `Running once (${strategyModeEl.value})...`;
        try {
          const res = await fetch(`/strategy/farthest_band/run?${strategyParams(true, false)}&force_new_order=1`);
          const data = await res.json();
          if (!res.ok || data.error || data.action === "error") throw new Error(data.error || data.reason || `HTTP ${res.status}`);
          const action = data?.result?.action || data?.action || "ok";
          strategyStatusEl.textContent = `Run complete: ${action}`;
          renderStrategySelection(data, "Order placed/plan", "manual");
          refreshPortfolio();
          refreshLedger();
        } catch (err) {
          strategyStatusEl.textContent = `Run failed: ${err.message || "error"}`;
        }
      }

      async function strategyAutoStart() {
        strategyStatusEl.textContent = `Starting auto (${strategyModeEl.value})...`;
        try {
          const res = await fetch(`/strategy/farthest_band/auto/start?${strategyParams(true, true)}`);
          const data = await res.json();
          if (!res.ok || data.error) throw new Error(data.error || `HTTP ${res.status}`);
          strategyStatusEl.textContent = data.message || "Auto started";
        } catch (err) {
          strategyStatusEl.textContent = `Auto start failed: ${err.message || "error"}`;
        }
      }

      async function strategyAutoStatus(quiet = false) {
        if (strategyAutoStatusInflight) return;
        strategyAutoStatusInflight = true;
        if (!quiet) strategyStatusEl.textContent = "Loading auto status...";
        try {
          const res = await fetch("/strategy/farthest_band/auto/status");
          const data = await res.json();
          if (!res.ok || data.error) throw new Error(data.error || `HTTP ${res.status}`);
          const mode = data?.config?.mode || strategyModeEl.value;
          const running = data.running ? "running" : "stopped";
          if (!quiet) strategyStatusEl.textContent = `Auto ${running} (${mode}). Last run: ${data.last_run_at || "n/a"}`;
          updateScheduleProgress(data);
          updateStopLossPanel(data);
          if (data.last_result) renderStrategySelection(data.last_result, "Last auto plan", "auto");
        } catch (err) {
          if (!quiet) strategyStatusEl.textContent = `Auto status failed: ${err.message || "error"}`;
          updateScheduleProgress(null);
          updateStopLossPanel(null);
        } finally {
          strategyAutoStatusInflight = false;
        }
      }

      async function strategyAutoStop() {
        strategyStatusEl.textContent = "Stopping auto...";
        try {
          const res = await fetch("/strategy/farthest_band/auto/stop");
          const data = await res.json();
          if (!res.ok || data.error) throw new Error(data.error || `HTTP ${res.status}`);
          strategyStatusEl.textContent = data.message || "Auto stopped";
        } catch (err) {
          strategyStatusEl.textContent = `Auto stop failed: ${err.message || "error"}`;
        }
      }

      async function refreshPortfolio() {
        portfolioSummaryEl.textContent = "Loading portfolio...";
        try {
          const res = await fetch("/kalshi/portfolio/current");
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          const balance = data.balance || {};
          const cashflow = data.cashflow || {};
          const cashCents = pickBalanceCents(
            balance,
            ["cash_balance_cents", "available_cash_cents", "free_balance_cents", "spendable_balance_cents", "balance_cents"],
            ["cash_balance", "available_cash", "free_balance", "spendable_balance", "balance"],
          );
          const portfolioCents = pickBalanceCents(
            balance,
            ["portfolio_value_cents", "account_value_cents", "net_liquidation_value_cents", "total_value_cents", "portfolio_balance_cents"],
            ["portfolio_value", "account_value", "net_liquidation_value", "total_value", "portfolio_balance"],
          );
          const balancePositionCents = pickBalanceCents(
            balance,
            ["market_value_cents", "position_value_cents", "positions_value_cents"],
            ["market_value", "position_value", "positions_value"],
          );

          const allOrders = data.orders || [];
          const btcOnly = Boolean(portfolioBtcOnlyEl?.checked);
          const orders = btcOnly
            ? allOrders.filter(o => String(o?.ticker || "").toUpperCase().startsWith("KXBTCD-"))
            : allOrders;
          let rowsPositionCents = null;
          if (orders.length) {
            let acc = 0;
            let seen = 0;
            for (const o of orders) {
              const cnt = asIntOrNull(o?.count) || 0;
              if (cnt <= 0) continue;
              const mv = asIntOrNull(o?.market_value_cents);
              if (mv !== null) {
                acc += mv;
                seen += 1;
                continue;
              }
              const c = asIntOrNull(o?.cost_cents);
              const p = asIntOrNull(o?.pnl_cents);
              if (c !== null && p !== null) {
                acc += (c + p);
                seen += 1;
              }
            }
            if (seen > 0) rowsPositionCents = acc;
          }

          let positionValueCents = rowsPositionCents;
          if (positionValueCents === null) positionValueCents = balancePositionCents;
          if (positionValueCents === null && portfolioCents !== null && cashCents !== null) {
            positionValueCents = portfolioCents - cashCents;
          }

          let portfolioDisplayCents = null;
          if (cashCents !== null && positionValueCents !== null) {
            portfolioDisplayCents = cashCents + positionValueCents;
          } else {
            portfolioDisplayCents = portfolioCents;
          }

          const cash = formatCents(cashCents);
          const portfolio = formatCents(portfolioDisplayCents);
          const positionValue = formatCents(positionValueCents);
          const deposits = formatCents(cashflow?.deposits_cents);
          const withdrawals = formatCents(cashflow?.withdrawals_cents);
          const netTransfer = formatCents(cashflow?.net_transfer_cents);
          const hasCashflow = cashflow && (cashflow.deposits_cents !== null || cashflow.withdrawals_cents !== null);
          if (heroCashEl) heroCashEl.textContent = cash;
          if (heroPortfolioEl) heroPortfolioEl.textContent = portfolio;
          portfolioSummaryEl.textContent = `Position Value: ${positionValue}${hasCashflow ? ` | In: ${deposits} | Out: ${withdrawals} | Net: ${netTransfer}` : ""}${btcOnly ? " | Filter: BTC only" : ""}`;

          if (!orders.length) {
            portfolioBody.innerHTML = "<tr><td colspan=\\"7\\">No orders found.</td></tr>";
            return;
          }
          const rows = orders.map(o => {
            const ticker = shortTicker(o.ticker);
            const side = (o.side || "--").toUpperCase();
            const cost = formatCents(o.cost_cents);
            const marketValue = formatCents(o.market_value_cents);
            const pnl = formatCents(o.pnl_cents);
            const c = asIntOrNull(o?.cost_cents);
            const p = asIntOrNull(o?.pnl_cents);
            const pnlPctVal = (c && c !== 0 && p !== null)
              ? (p * 100 / c)
              : null;
            const pnlPct = (pnlPctVal !== null) ? `${pnlPctVal.toFixed(2)}%` : "--";
            const pnlClass = (p === null || p === undefined) ? "pnl-flat" : (p > 0 ? "pnl-pos" : (p < 0 ? "pnl-neg" : "pnl-flat"));
            const pnlPctClass = (pnlPctVal === null) ? "pnl-flat" : (pnlPctVal > 0 ? "pnl-pos" : (pnlPctVal < 0 ? "pnl-neg" : "pnl-flat"));
            const max = formatCents(o.max_payout_cents);
            return `
              <tr>
                <td>${ticker}</td>
                <td>${side}</td>
                <td>${cost}</td>
                <td>${marketValue}</td>
                <td class="${pnlClass}">${pnl}</td>
                <td class="${pnlPctClass}">${pnlPct}</td>
                <td>${max}</td>
              </tr>
            `;
          }).join("");
          portfolioBody.innerHTML = rows;
        } catch (err) {
          if (heroCashEl) heroCashEl.textContent = "$--";
          if (heroPortfolioEl) heroPortfolioEl.textContent = "$--";
          portfolioSummaryEl.textContent = "Failed to load portfolio.";
          portfolioBody.innerHTML = "<tr><td colspan=\\"7\\">Error loading orders.</td></tr>";
        }
      }

      async function refreshLedger() {
        ledgerBody.innerHTML = "<tr><td colspan=\\"11\\">Loading ledger...</td></tr>";
        try {
          const res = await fetch("/ledger/trades?limit=50");
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          const records = data.records || [];
          const autoRecords = records.filter(r => r.source === "/strategy/farthest_band/auto");
          const isCompleteTrade = (r) => {
            const hasTicker = Boolean(String(r?.ticker || "").trim());
            const hasPrice = r?.price_cents !== null && r?.price_cents !== undefined;
            const hasCount = r?.count !== null && r?.count !== undefined;
            return hasTicker && hasPrice && hasCount;
          };
          const lastAuto = autoRecords.find(isCompleteTrade) || null;
          if (lastAuto) {
            const side = String(lastAuto.side || "--").toUpperCase();
            const action = String(lastAuto.action || "--").toUpperCase();
            const ticker = lastAuto.ticker || "--";
            const px = lastAuto.price_cents !== null && lastAuto.price_cents !== undefined ? `${lastAuto.price_cents}c` : "--";
            const cnt = lastAuto.count ?? "--";
            const tsRaw = lastAuto.ts || null;
            let at = "--";
            if (tsRaw) {
              const dt = new Date(tsRaw);
              at = Number.isFinite(dt.getTime()) ? dt.toLocaleString() : String(tsRaw);
            }
            const status = lastAuto.status_code !== null && lastAuto.status_code !== undefined ? ` status=${lastAuto.status_code}` : "";
            autoLastTradeBadgeEl.textContent = `Last Auto Trade: ${action} ${side} ${ticker} @ ${px} x ${cnt} (${at})${status}`;
          } else if (autoRecords.length) {
            const lastAttempt = autoRecords[0];
            const tsRaw = lastAttempt.ts || null;
            let at = "--";
            if (tsRaw) {
              const dt = new Date(tsRaw);
              at = Number.isFinite(dt.getTime()) ? dt.toLocaleString() : String(tsRaw);
            }
            const status = lastAttempt.status_code !== null && lastAttempt.status_code !== undefined ? ` status=${lastAttempt.status_code}` : "";
            const reason = String(lastAttempt.note || "").trim();
            autoLastTradeBadgeEl.textContent = `Last Auto Attempt: order not placed (${at})${status}${reason ? ` | ${reason}` : ""}`;
          } else {
            autoLastTradeBadgeEl.textContent = "Last Auto Trade: none yet.";
          }
          if (!records.length) {
            ledgerBody.innerHTML = "<tr><td colspan=\\"11\\">No ledger records.</td></tr>";
            return;
          }
          const extractFailureReason = (statusCode, payloadJson, noteText) => {
            const s = Number(statusCode);
            let parsed = null;
            if (payloadJson) {
              try {
                parsed = JSON.parse(payloadJson);
              } catch (err) {
                parsed = null;
              }
            }
            const pullMsg = (obj) => {
              if (!obj || typeof obj !== "object") return "";
              const keys = ["error", "message", "reason", "detail", "msg", "code"];
              for (const k of keys) {
                const v = obj[k];
                if (typeof v === "string" && v.trim()) return v.trim();
              }
              for (const k of keys) {
                const v = obj[k];
                if (v && typeof v === "object") {
                  const nested = pullMsg(v);
                  if (nested) return nested;
                }
              }
              return "";
            };
            const extracted = pullMsg(parsed);
            if (extracted) return extracted;
            const note = String(noteText || "");
            const idx = note.toLowerCase().indexOf("reason=");
            if (idx >= 0) return note.slice(idx + 7).trim();
            if (note.toLowerCase().startsWith("exception=")) return note.replace(/^exception=/i, "").trim();
            if (s === 401) {
              return "Unauthorized (401): check API key, signature, timestamp, and account permissions.";
            }
            if (s >= 400) {
              return `Request failed with status ${s}`;
            }
            return "";
          };
          const rows = records.map(r => {
            const tsRaw = r.ts || "";
            let ts = "--";
            if (tsRaw) {
              const d = new Date(tsRaw);
              ts = Number.isFinite(d.getTime()) ? d.toLocaleString() : String(tsRaw);
            }
            const source = r.source || "--";
            const action = r.action || "--";
            const side = r.side || "--";
            const ticker = r.ticker || "--";
            const price = r.price_cents !== null && r.price_cents !== undefined ? `${r.price_cents}c` : "--";
            const count = r.count ?? "--";
            const cost = r.cost_cents !== null && r.cost_cents !== undefined ? formatCents(r.cost_cents) : "--";
            const status = r.status_code ?? "--";
            const reason = extractFailureReason(r.status_code, r.payload_json, r.note) || "--";
            const note = r.note || "--";
            return `
              <tr>
                <td>${ts}</td>
                <td>${source}</td>
                <td>${action}</td>
                <td>${String(side).toUpperCase()}</td>
                <td>${ticker}</td>
                <td>${price}</td>
                <td>${count}</td>
                <td>${cost}</td>
                <td>${status}</td>
                <td>${reason}</td>
                <td>${note}</td>
              </tr>
            `;
          }).join("");
          ledgerBody.innerHTML = rows;
        } catch (err) {
          ledgerBody.innerHTML = "<tr><td colspan=\\"11\\">Error loading ledger.</td></tr>";
        }
      }

      async function refreshPnlSummary() {
        pnlBody.innerHTML = "<tr><td colspan=\\"6\\">Loading P/L summary...</td></tr>";
        try {
          const res = await fetch("/kalshi/pnl/summary");
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          const t = data.totals || {};
          pnlTotalsEl.innerHTML = `
            <div class="pnl-total"><div class="k">Realized</div><div class="v">${formatCents(t.realized_pnl_cents)}</div></div>
            <div class="pnl-total"><div class="k">Unrealized</div><div class="v">${formatCents(t.unrealized_pnl_cents)}</div></div>
            <div class="pnl-total"><div class="k">Net</div><div class="v">${formatCents(t.net_pnl_cents)}</div></div>
            <div class="pnl-total"><div class="k">Fees</div><div class="v">${formatCents(t.fees_cents)}</div></div>
          `;
          const rows = data.by_market || [];
          if (!rows.length) {
            pnlBody.innerHTML = "<tr><td colspan=\\"6\\">No P/L rows.</td></tr>";
            return;
          }
          pnlBody.innerHTML = rows.map(r => `
            <tr>
              <td>${r.ticker || "--"}</td>
              <td>${String(r.side || "--").toUpperCase()}</td>
              <td>${r.open_count ?? "--"}</td>
              <td>${formatCents(r.realized_pnl_cents)}</td>
              <td>${formatCents(r.unrealized_pnl_cents)}</td>
              <td>${formatCents(r.net_pnl_cents)}</td>
            </tr>
          `).join("");
        } catch (err) {
          pnlBody.innerHTML = "<tr><td colspan=\\"6\\">Error loading P/L summary.</td></tr>";
        }
      }

      function hideChartTooltip() {
        chartTooltipEl.style.display = "none";
      }

      function showChartTooltip(clientX) {
        if (!chartScreenPoints.length) {
          hideChartTooltip();
          return;
        }
        const x = clientX - chartBounds.left;
        let best = chartScreenPoints[0];
        let bestDist = Math.abs(best.x - x);
        for (let i = 1; i < chartScreenPoints.length; i += 1) {
          const d = Math.abs(chartScreenPoints[i].x - x);
          if (d < bestDist) {
            bestDist = d;
            best = chartScreenPoints[i];
          }
        }
        if (!best || !Number.isFinite(best.price)) {
          hideChartTooltip();
          return;
        }
        const t = new Date(best.ts || Date.now());
        const hh = String(t.getHours()).padStart(2, "0");
        const mm = String(t.getMinutes()).padStart(2, "0");
        const priceText = Number(best.price).toLocaleString("en-US", { style: "currency", currency: "USD" });
        chartTooltipEl.textContent = `${hh}:${mm}  ${priceText}`;
        chartTooltipEl.style.left = `${best.x}px`;
        chartTooltipEl.style.top = `${best.y}px`;
        chartTooltipEl.style.display = "block";
      }

      refresh();
      refreshChart();
      refreshMarketsBtn.addEventListener("click", refreshMarkets);
      refreshPortfolioBtn.addEventListener("click", refreshPortfolio);
      portfolioBtcOnlyEl.addEventListener("change", refreshPortfolio);
      refreshLedgerBtn.addEventListener("click", refreshLedger);
      refreshPnlBtn.addEventListener("click", refreshPnlSummary);
      [manualStrategyModeEl, manualStrategySideEl, manualStrategyAskMinEl, manualStrategyAskMaxEl, manualStrategyMaxCostEl, manualStrategyIntervalEl, manualStrategyStopLossPctEl, manualStrategyStopLossSecondsEl]
        .forEach(el => el.addEventListener("change", () => syncStrategyControls(true)));
      [strategyModeEl, strategySideEl, strategyAskMinEl, strategyAskMaxEl, strategyMaxCostEl, strategyIntervalEl, strategyStopLossPctEl, strategyStopLossSecondsEl]
        .forEach(el => el.addEventListener("change", () => syncStrategyControls(false)));
      strategyPreviewBtn.addEventListener("click", strategyPreview);
      strategyRunBtn.addEventListener("click", strategyRun);
      strategyAutoStartBtn.addEventListener("click", strategyAutoStart);
      strategyAutoStatusBtn.addEventListener("click", strategyAutoStatus);
      strategyAutoStopBtn.addEventListener("click", strategyAutoStop);
      canvas.addEventListener("mousemove", (e) => showChartTooltip(e.clientX));
      canvas.addEventListener("mouseleave", hideChartTooltip);
      setInterval(() => {
        refresh();
        refreshChart();
        updateEventExpiryCountdown();
      }, 1000);
      setInterval(() => {
        strategyPreview(true);
        strategyAutoStatus(true);
      }, 1000);
      setInterval(() => {
        refreshPortfolio();
      }, 3000);
      setInterval(refreshExpiryMeta, 30000);
      syncStrategyControls(false);
      refreshMarkets();
      refreshExpiryMeta();
      strategyPreview();
      refreshLedger();
      refreshPnlSummary();
      window.addEventListener("resize", refreshChart);
    </script>
  </body>
</html>
    """


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8090"))
    uvicorn.run(app, host="0.0.0.0", port=port)
