from __future__ import annotations

import datetime
import json
import os
import sqlite3
import threading
import traceback
from typing import Optional
from datetime import timedelta, timezone
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

# Load .env from project root (parent of src/)
_project_root = Path(__file__).resolve().parent.parent
load_dotenv(_project_root / ".env")

app = FastAPI()

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all for dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Project root: from src/api.py, go up one level to project root
DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
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
    cost_cents = int(price_cents * count) if isinstance(price_cents, int) and isinstance(count, int) else None
    payload_json = None
    if payload is not None:
        try:
            payload_json = json.dumps(payload)
        except Exception:
            payload_json = str(payload)
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
                datetime.datetime.now(timezone.utc).isoformat(),
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
        conn.commit()
    finally:
        conn.close()


def _maybe_int(v):
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
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

    if action_name in multi_leg_actions:
        exited = cycle.get("exited_position") or {}
        exit_leg = cycle.get("exit") or {}
        if action_name == "stop_loss_rotate":
            exit_mode = exit_leg.get("mode") or mode_hint or "paper"
            exit_note = f"stop_loss pnl_pct={cycle.get('pnl_pct')} [{exit_mode}]"
            reentry_note = "reentry_after_stop_loss"
        elif action_name == "scheduled_rebalance":
            exit_mode = exit_leg.get("mode") or mode_hint or "paper"
            exit_note = f"scheduled_rebalance_exit [{exit_mode}]"
            reentry_note = "scheduled_rebalance_reentry"
        else:
            exit_mode = exit_leg.get("mode") or mode_hint or "paper"
            exit_note = f"rollover_exit_stale_ticker [{exit_mode}]"
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
        reentry_mode = reentry.get("mode") or mode_hint or "paper"
        if ap:
            reentry_note_with_mode = f"{reentry_note} [{reentry_mode}]"
            _log_trade_ledger(
                source=source,
                run_mode=reentry_mode,
                action="buy",
                side=ap.get("side"),
                ticker=ap.get("ticker"),
                price_cents=_maybe_int(ap.get("entry_price_cents")),
                count=_maybe_int(ap.get("count")),
                status_code=_maybe_int(reentry.get("status_code")),
                success=(reentry_mode != "live") or (_maybe_int(reentry.get("status_code")) in {200, 201}),
                note=reentry_note_with_mode,
                payload=reentry,
            )


_ingest_thread: Optional[threading.Thread] = None
_farthest_auto_thread: Optional[threading.Thread] = None
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


@app.on_event("startup")
def startup_ingest():
    auto_ingest = os.getenv("KALSHI_AUTO_INGEST", "true").lower()
    if auto_ingest in {"1", "true", "yes", "on"}:
        _start_ingest_loop()


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

            def _is_100(val):
                if val is None:
                    return False
                try:
                    return round(float(val)) == 100
                except (TypeError, ValueError):
                    return False

            # Filter out markets where Yes Ask or No Ask is 100
            markets = [
                m for m in markets
                if not _is_100(m.get("yes_ask")) and not _is_100(m.get("no_ask"))
            ]
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
        entry_price = _maybe_int(p.get("entry_price_cents")) or 0
        if not ticker or side not in {"yes", "no"} or count < 1 or entry_price < 1:
            continue
        row = dict(p)
        row["ticker"] = ticker
        row["side"] = side
        row["count"] = int(count)
        row["entry_price_cents"] = int(entry_price)
        normalized.append(row)
    return normalized


def _market_event_prefix(ticker: str | None) -> str | None:
    t = str(ticker or "").strip()
    if not t:
        return None
    i = t.rfind("-T")
    if i > 0:
        return t[:i]
    return None


def _seed_active_positions_from_portfolio(config: FarthestBandConfig) -> list[dict]:
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
        return []

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
        entry_price = _price_to_cents(avg_price)
        if entry_price is None:
            traded = (
                _parse_dollar_str_to_cents(p.get("total_traded_dollars"))
                or _parse_dollar_str_to_cents(p.get("total_traded"))
            )
            if traded is not None and count > 0:
                entry_price = int(round(float(traded) / float(count)))
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
        if not cfg_dict:
            break

        config = FarthestBandConfig(**cfg_dict)
        is_scheduled_run = now_utc >= next_scheduled_at
        did_run = False
        run_at = None
        latest_out = None
        next_active_positions = []

        if active_positions or is_scheduled_run:
            did_run = True
            run_at = datetime.datetime.now(timezone.utc).isoformat()
            for pos in active_positions:
                try:
                    out = _run_farthest_band_once(
                        config,
                        active_position=pos,
                        force_rebalance=False,
                    )
                except Exception as exc:
                    out = {"action": "error", "reason": str(exc), "active_position": pos}
                _record_strategy_ledger(out, source="/strategy/farthest_band/auto")
                latest_out = out
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
                _record_strategy_ledger(scheduled_out, source="/strategy/farthest_band/auto")
                latest_out = scheduled_out
                scheduled_pos = scheduled_out.get("active_position")
                if isinstance(scheduled_pos, dict):
                    next_active_positions.append(scheduled_pos)

            # Advance schedule: when no position, check for entries every 60s;
            # when holding, use full interval for scheduled runs
            if is_scheduled_run:
                has_position = bool(next_active_positions) or (latest_out and latest_out.get("active_position"))
                if has_position:
                    interval_seconds = max(int(config.interval_minutes) * 60, 60)
                    next_scheduled_at = now_utc + timedelta(seconds=interval_seconds)
                else:
                    # No position: retry in 60s to catch entry opportunities (ingest ready, conditions met)
                    next_scheduled_at = now_utc + timedelta(seconds=60)

        if did_run:
            normalized_positions = _normalize_active_positions(next_active_positions)
            with _farthest_auto_lock:
                _farthest_auto_state["last_run_at"] = run_at
                if is_scheduled_run:
                    _farthest_auto_state["last_scheduled_run_at"] = run_at
                if active_positions:
                    _farthest_auto_state["last_risk_check_at"] = run_at
                _farthest_auto_state["next_scheduled_run_at"] = next_scheduled_at.isoformat()
                _farthest_auto_state["last_result"] = latest_out
                _farthest_auto_state["active_positions"] = normalized_positions
                _farthest_auto_state["active_position"] = normalized_positions[-1] if normalized_positions else None
                _farthest_auto_state["active_position_mode"] = cfg_mode if normalized_positions else None

        # Wake at the earlier of:
        # - next scheduled interval execution
        # - 5-minute risk-check cadence
        now_after = datetime.datetime.now(timezone.utc)
        seconds_until_schedule = max(1, int((next_scheduled_at - now_after).total_seconds()))
        wait_seconds = min(300, seconds_until_schedule)
        next_risk_check_at = now_after + timedelta(seconds=wait_seconds)
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


@app.get("/kalshi/portfolio/orders")
def get_portfolio_orders(status: Optional[str] = None, ticker: Optional[str] = None, limit: int = 100):
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


@app.get("/kalshi/portfolio/current")
def get_portfolio_current_orders(status: Optional[str] = "open", limit: int = 200):
    """
    Current orders/positions with estimated cost, mark-based PnL, and max payout.
    """
    client = KalshiClient()
    orders = client.get_all_orders(status=status, limit=limit, max_pages=20) if status else client.get_all_orders(limit=limit, max_pages=20)
    balance = client.get_balance()
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
        market_value_cents = (
            _parse_dollar_str_to_cents(p.get("market_value"))
            or _parse_dollar_str_to_cents(p.get("market_exposure_dollars"))
            or _parse_dollar_str_to_cents(p.get("market_exposure"))
        )
        pnl_cents = (
            _parse_dollar_str_to_cents(p.get("unrealized_pnl_dollars"))
            or _parse_dollar_str_to_cents(p.get("unrealized_pnl"))
            or _parse_dollar_str_to_cents(p.get("realized_pnl_dollars"))
            or _parse_dollar_str_to_cents(p.get("realized_pnl"))
            or _parse_dollar_str_to_cents(p.get("pnl"))
        )
        if pnl_cents is None and market_value_cents is not None and cost_cents is not None:
            pnl_cents = market_value_cents - cost_cents
        if cost_cents is None and market_value_cents is not None and pnl_cents is not None:
            cost_cents = market_value_cents - pnl_cents
        if pnl_cents is None and ticker and count:
            if price_cents is None and total_traded_cents is not None and count:
                price_cents = int(round(total_traded_cents / int(count)))
            try:
                top = client.get_top_of_book(ticker)
                yes_mid = _midpoint(top.get("yes_bid"), top.get("yes_ask"))
                no_mid = _midpoint(top.get("no_bid"), top.get("no_ask"))
                if no_mid is None and yes_mid is not None:
                    no_mid = 100 - yes_mid
                if side == "yes" and yes_mid is not None and price_cents is not None:
                    pnl_cents = int((yes_mid - price_cents) * int(count))
                elif side == "no" and no_mid is not None and price_cents is not None:
                    pnl_cents = int((no_mid - price_cents) * int(count))
            except Exception:
                pass

        rows.append(
            {
                "order_id": p.get("position_id") or p.get("id"),
                "ticker": ticker,
                "side": side.upper() if side else p.get("side"),
                "status": "POSITION",
                "count": int(count) if count else 0,
                "price_cents": price_cents,
                "cost_cents": cost_cents,
                "mark_cents": None,
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
            if ticker and side in {"yes", "no"} and price is not None and count:
                top = client.get_top_of_book(ticker)
                yes_mid = _midpoint(top.get("yes_bid"), top.get("yes_ask"))
                no_mid = _midpoint(top.get("no_bid"), top.get("no_ask"))
                if no_mid is None and yes_mid is not None:
                    no_mid = 100 - yes_mid
                if side == "yes":
                    mark_cents = yes_mid
                    if mark_cents is not None:
                        pnl_cents = int((mark_cents - price) * count)
                else:
                    mark_cents = no_mid
                    if mark_cents is not None:
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
                    "mark_cents": mark_cents,
                    "pnl_cents": pnl_cents,
                    "max_payout_cents": max_payout_cents,
                }
            )

    return {"balance": balance, "orders": rows}


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
):
    config = FarthestBandConfig(
        direction="lower",
        side=side,
        ask_min_cents=ask_min_cents,
        ask_max_cents=ask_max_cents,
        max_cost_cents=max_cost_cents,
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
    force_new_order: bool = False,
    stop_loss_pct: float = 0.20,
    profit_target_pct: Optional[float] = None,
):
    config = FarthestBandConfig(
        direction="lower",
        side=side,
        ask_min_cents=ask_min_cents,
        ask_max_cents=ask_max_cents,
        max_cost_cents=max_cost_cents,
        mode=mode,
        stop_loss_pct=stop_loss_pct,
        profit_target_pct=profit_target_pct,
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
    stop_loss_pct: float = 0.20,
    profit_target_pct: Optional[float] = None,
    rebalance_each_interval: bool = False,
):
    """rebalance_each_interval: If True, exits+reenters every interval. If False (default), only exits on stop-loss/take-profit/rollover."""
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
        profit_target_pct=profit_target_pct,
        rebalance_each_interval=rebalance_each_interval,
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
        seeded_positions = _seed_active_positions_from_portfolio(config)
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
    return {"ok": True, "message": "Strategy active position reset"}


@app.get("/strategy/farthest_band/auto/status")
def strategy_farthest_band_auto_status():
    with _farthest_auto_lock:
        thread_alive = bool(_farthest_auto_thread and _farthest_auto_thread.is_alive())
        state = dict(_farthest_auto_state)
        state["running"] = thread_alive
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
        grid-template-columns: minmax(0, 1.35fr) minmax(0, 1.55fr) minmax(0, 1.55fr) minmax(0, 1.35fr);
        grid-template-areas:
          "markets manual auto portfolio";
        gap: 16px;
        align-items: start;
      }
      .markets { grid-area: markets; }
      .portfolio-panel {
        background: rgba(10, 16, 32, 0.6);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 12px;
        min-height: 260px;
        grid-area: portfolio;
      }
      .manual-panel {
        grid-area: manual;
      }
      .auto-panel {
        grid-area: auto;
      }
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
      .ledger-table .text-muted { color: var(--muted); }
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
        padding: 6px 4px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        text-align: left;
      }
      .portfolio-table th {
        color: var(--muted);
        font-weight: 600;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }
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
      .markets tr.row-highlight-yes td {
        background: rgba(244, 196, 48, 0.35) !important;
        border-left: 4px solid #f4c430;
      }
      .markets tr.row-highlight-no td {
        background: rgba(34, 211, 238, 0.35) !important;
        border-left: 4px solid #22d3ee;
      }
      .markets tr.row-highlight-yes.row-highlight-no td {
        background: linear-gradient(90deg, rgba(244, 196, 48, 0.3) 0%, rgba(34, 211, 238, 0.3) 100%) !important;
        border-left: 4px solid #a3e635;
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
        min-height: 68px;
        max-height: 68px;
        overflow-y: auto;
        white-space: pre-wrap;
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
            "manual"
            "auto"
            "portfolio";
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
                </div>
                <div class="strategy-actions">
                  <button id="strategy-preview" class="btn secondary" type="button">Preview</button>
                  <button id="strategy-run" class="btn" type="button">Run Once</button>
                </div>
                <div id="manual-strategy-planned-order" class="planned-order">Manual planned order will appear after Preview.</div>
                <div id="manual-strategy-candidates" class="candidate-list"></div>
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
                  <div class="stoploss-title">Stop-loss check (5m cadence)</div>
                  <div id="stoploss-next-run" class="stoploss-next">Next stop-loss check: --</div>
                  <div class="progress-track">
                    <div id="stoploss-progress-fill" class="progress-fill"></div>
                  </div>
                  <div id="stoploss-positions" class="stoploss-positions">Protected positions: --</div>
                </div>
                <div class="field">
                  <label for="strategy-stop-loss">Stop Loss (%)</label>
                  <input id="strategy-stop-loss" type="number" min="0" max="100" step="1" value="20" title="Exit when PnL <= -this %" />
                </div>
                <div class="field">
                  <label for="strategy-profit-target">Profit Target (%)</label>
                  <input id="strategy-profit-target" type="number" min="0" max="100" step="0.5" value="" placeholder="disabled" title="Exit when PnL >= this %. Empty = disabled." />
                </div>
                <div id="auto-strategy-planned-order" class="planned-order">Auto plan will appear after Auto Status.</div>
                <div id="auto-strategy-candidates" class="candidate-list"></div>
              </div>
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
                      <th>P/L</th>
                      <th>Max</th>
                    </tr>
                  </thead>
                  <tbody id="portfolio-body">
                    <tr><td colspan="5">Click Refresh to load orders.</td></tr>
                  </tbody>
                </table>
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
                    <th>Mode</th>
                    <th>Action</th>
                    <th>Side</th>
                    <th>Ticker</th>
                    <th>Price</th>
                    <th>Count</th>
                    <th>Cost</th>
                    <th>Status</th>
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
      const manualStrategyModeEl = document.getElementById("manual-strategy-mode");
      const manualStrategySideEl = document.getElementById("manual-strategy-side");
      const manualStrategyAskMinEl = document.getElementById("manual-strategy-ask-min");
      const manualStrategyAskMaxEl = document.getElementById("manual-strategy-ask-max");
      const manualStrategyMaxCostEl = document.getElementById("manual-strategy-max-cost");
      const manualStrategyIntervalEl = document.getElementById("manual-strategy-interval");
      const strategyPreviewBtn = document.getElementById("strategy-preview");
      const strategyRunBtn = document.getElementById("strategy-run");
      const strategyAutoStartBtn = document.getElementById("strategy-auto-start");
      const strategyAutoStatusBtn = document.getElementById("strategy-auto-status");
      const strategyAutoStopBtn = document.getElementById("strategy-auto-stop");
      const strategyStatusEl = document.getElementById("strategy-status");
      const autoLastTradeBadgeEl = document.getElementById("auto-last-trade-badge");
      const strategyNextRunEl = document.getElementById("strategy-next-run");
      const strategyProgressFillEl = document.getElementById("strategy-progress-fill");
      const stopLossNextRunEl = document.getElementById("stoploss-next-run");
      const stopLossProgressFillEl = document.getElementById("stoploss-progress-fill");
      const stopLossPositionsEl = document.getElementById("stoploss-positions");
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
          return;
        }
        manualStrategyModeEl.value = strategyModeEl.value;
        manualStrategySideEl.value = strategySideEl.value;
        manualStrategyAskMinEl.value = strategyAskMinEl.value;
        manualStrategyAskMaxEl.value = strategyAskMaxEl.value;
        manualStrategyMaxCostEl.value = strategyMaxCostEl.value;
        manualStrategyIntervalEl.value = strategyIntervalEl.value;
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
        const parseNum = (v) => {
          if (v == null || v === "") return NaN;
          const s = String(v).replace(/c$/i, "").replace(/\s/g, "").trim();
          return Number(s);
        };
        const filteredMarkets = markets.filter(m => {
          const y = Math.round(parseNum(m.yes_ask ?? m.yesAsk));
          const n = Math.round(parseNum(m.no_ask ?? m.noAsk));
          return !(y === 100 || n === 100);
        });
        const rows = filteredMarkets.slice(0, 20).map(m => {
          const strike = Number(m.strike || 0);
          const ticker = m.ticker || "";
          const yesAskRaw = m.yes_ask ?? m.yesAsk;
          const noAskRaw = m.no_ask ?? m.noAsk;
          const yesAsk = yesAskRaw ?? "--";
          const noAsk = noAskRaw ?? "--";
          const yesNum = parseNum(yesAskRaw);
          const rowHot = Number.isFinite(yesNum) && yesNum >= 90 && yesNum <= 99;
          const subtitle = m.subtitle || "";
          const inRange = v => { const n = Math.round(v); return !isNaN(v) && n >= 85 && n <= 99; };
          const yesInRange = inRange(yesAskVal);
          const noInRange = inRange(noAskVal);
          const classes = [];
          if (yesInRange) classes.push('row-highlight-yes');
          if (noInRange) classes.push('row-highlight-no');
          const rowClass = classes.length ? ` class="${classes.join(' ')}"` : '';
          const inRange = v => { const n = Math.round(v); return !isNaN(v) && n >= 85 && n <= 99; };
          const yesInRange = inRange(parseNum(yesAskRaw));
          const noInRange = inRange(parseNum(noAskRaw));
          const classes = [];
          if (yesInRange) classes.push('row-highlight-yes');
          if (noInRange) classes.push('row-highlight-no');
          const rowClass = classes.length ? ` class="${classes.join(' ') + (rowHot ? ' row-hot' : '')}"` : (rowHot ? ' class="row-hot"' : '');
          return `
            <tr${rowClass} data-market-ticker="${ticker}">
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

      const strategyStopLossEl = document.getElementById("strategy-stop-loss");
      const strategyProfitTargetEl = document.getElementById("strategy-profit-target");
      function strategyParams(includeMode = false, includeInterval = false) {
        const params = new URLSearchParams();
        params.set("side", strategySideEl.value || "yes");
        params.set("ask_min_cents", String(Number(strategyAskMinEl.value || 95)));
        params.set("ask_max_cents", String(Number(strategyAskMaxEl.value || 99)));
        params.set("max_cost_cents", String(Number(strategyMaxCostEl.value || 500)));
        if (includeMode) params.set("mode", strategyModeEl.value || "paper");
        if (includeInterval) params.set("interval_minutes", String(Number(strategyIntervalEl.value || 15)));
        params.set("stop_loss_pct", String(Number(strategyStopLossEl?.value || 20) / 100));
        const pt = strategyProfitTargetEl?.value?.trim();
        if (pt && !isNaN(Number(pt))) params.set("profit_target_pct", String(Number(pt) / 100));
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
        const riskSec = 300;
        if (!statusData || !statusData.running) {
          stopLossNextRunEl.textContent = "Next stop-loss check: auto not running";
          stopLossProgressFillEl.style.width = "0%";
          stopLossPositionsEl.textContent = "Protected positions: none";
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
          let elapsedSec = riskSec - remainingSec;
          if (Number.isFinite(lastRiskAt)) {
            elapsedSec = Math.max(0, Math.min(riskSec, (nowMs - lastRiskAt) / 1000));
          }
          const progressPct = Math.max(0, Math.min(100, (elapsedSec / riskSec) * 100));
          stopLossNextRunEl.textContent = `Next stop-loss check in ${formatDuration(remainingSec)}`;
          stopLossProgressFillEl.style.width = `${progressPct.toFixed(1)}%`;
        }

        const positions = Array.isArray(statusData?.active_positions) ? statusData.active_positions : [];
        if (!positions.length) {
          stopLossPositionsEl.textContent = "Protected positions: none (stop-loss starts after first tracked entry/seed).";
          return;
        }
        const lines = positions.slice(0, 6).map((p, i) => {
          const side = String(p?.side || "--").toUpperCase();
          const ticker = p?.ticker || "--";
          const entry = p?.entry_price_cents ?? "--";
          const count = p?.count ?? "--";
          return `${i + 1}. ${ticker} (${side}) entry ${entry}c x ${count}`;
        });
        const extra = positions.length > 6 ? `\\n... +${positions.length - 6} more` : "";
        stopLossPositionsEl.textContent = `Protected positions: ${positions.length}\\n${lines.join("\\n")}${extra}`;
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
        if (!selection) {
          const active = cycle?.active_position || payload?.active_position || null;
          const exitEval = cycle?.exit_criteria_evaluated || payload?.exit_criteria_evaluated || null;
          if (active) {
            const lines = [
              `${label} (${mode})`,
              `No new order selected.`,
              `Active Position: ${active.ticker || "--"} (${String(active.side || "--").toUpperCase()})`,
              `Entry: ${active.entry_price_cents ?? "--"}c`,
              `Count: ${active.count ?? "--"}`,
              `Reason: ${cycle?.reason || "holding active position"}`,
            ];
            if (exitEval?.evaluated) {
              lines.push("--- Exit criteria (every run) ---");
              lines.push(`Side: ${exitEval.side ?? "--"}`);
              lines.push(`PnL: ${exitEval.pnl_pct != null ? (exitEval.pnl_pct * 100).toFixed(2) + "%" : "--"}`);
              lines.push(`Stop-loss threshold: ${exitEval.stop_loss_threshold_pct != null ? (exitEval.stop_loss_threshold_pct * 100).toFixed(0) + "%" : "--"}`);
              lines.push(`Stop-loss triggered: ${exitEval.stop_loss_triggered ? "YES" : "no"}`);
              if (exitEval.profit_target_threshold_pct != null) {
                lines.push(`Take-profit threshold: ${(exitEval.profit_target_threshold_pct * 100).toFixed(2)}%`);
                lines.push(`Take-profit triggered: ${exitEval.take_profit_triggered ? "YES" : "no"}`);
              }
              lines.push(`What happened: ${exitEval.what_happened ?? "--"}`);
            }
            plannedEl.textContent = lines.join("\\n");
          } else {
            plannedEl.textContent = `No strategy selection returned. ${cycle?.reason || ""}`.trim();
          }
          candidatesEl.innerHTML = "";
          return;
        }
        if (!selected) {
          plannedEl.textContent = `${label}: no exact match. ${selection.reason || ""}`;
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
        const exitEval2 = cycle?.exit_criteria_evaluated || payload?.exit_criteria_evaluated || null;
        if (exitEval2?.evaluated !== undefined) {
          lines.push("--- Exit criteria ---");
          lines.push(exitEval2.what_happened ?? (exitEval2.evaluated ? "Evaluated (no active position)" : "Not evaluated"));
        }
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
        const mode = strategyModeEl.value || "paper";
        if (mode === "paper") {
          if (!confirm("Auto will run in PAPER mode — no real orders will be placed. Select LIVE in the Mode dropdown to trade for real. Continue?")) return;
        }
        strategyStatusEl.textContent = `Starting auto (${mode})...`;
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
          const mode = (data?.config?.mode || strategyModeEl.value || "paper").toLowerCase();
          const running = data.running ? "running" : "stopped";
          const modeLabel = mode === "live" ? "LIVE (real orders)" : "PAPER (no real orders)";
          if (!quiet) strategyStatusEl.textContent = `Auto ${running} — ${modeLabel}. Last run: ${data.last_run_at || "n/a"}`;
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
          if (heroCashEl) heroCashEl.textContent = cash;
          if (heroPortfolioEl) heroPortfolioEl.textContent = portfolio;
          portfolioSummaryEl.textContent = `Cash: ${cash} | Portfolio: ${portfolio} | Position Value: ${positionValue}${btcOnly ? " | Filter: BTC only" : ""}`;

          if (!orders.length) {
            portfolioBody.innerHTML = "<tr><td colspan=\\"5\\">No orders found.</td></tr>";
            return;
          }
          const rows = orders.map(o => {
            const ticker = o.ticker || "--";
            const side = (o.side || "--").toUpperCase();
            const cost = formatCents(o.cost_cents);
            const pnl = formatCents(o.pnl_cents);
            const max = formatCents(o.max_payout_cents);
            return `
              <tr>
                <td>${ticker}</td>
                <td>${side}</td>
                <td>${cost}</td>
                <td>${pnl}</td>
                <td>${max}</td>
              </tr>
            `;
          }).join("");
          portfolioBody.innerHTML = rows;
        } catch (err) {
          if (heroCashEl) heroCashEl.textContent = "$--";
          if (heroPortfolioEl) heroPortfolioEl.textContent = "$--";
          portfolioSummaryEl.textContent = "Failed to load portfolio.";
          portfolioBody.innerHTML = "<tr><td colspan=\\"5\\">Error loading orders.</td></tr>";
        }
      }

      async function refreshLedger() {
        ledgerBody.innerHTML = "<tr><td colspan=\\"11\\">Loading ledger...</td></tr>";
        try {
          const res = await fetch("/ledger/trades?limit=50");
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          const records = data.records || [];
          const lastAuto = records.find(r => r.source === "/strategy/farthest_band/auto");
          if (lastAuto) {
            const side = String(lastAuto.side || "--").toUpperCase();
            const action = String(lastAuto.action || "--").toUpperCase();
            const ticker = lastAuto.ticker || "--";
            const px = lastAuto.price_cents !== null && lastAuto.price_cents !== undefined ? `${lastAuto.price_cents}c` : "--";
            const cnt = lastAuto.count ?? "--";
            const at = lastAuto.ts || "--";
            autoLastTradeBadgeEl.textContent = `Last Auto Trade: ${action} ${side} ${ticker} @ ${px} x ${cnt} (${at})`;
          } else {
            autoLastTradeBadgeEl.textContent = "Last Auto Trade: none yet.";
          }
          if (!records.length) {
            ledgerBody.innerHTML = "<tr><td colspan=\\"11\\">No ledger records.</td></tr>";
            return;
          }
          const rows = records.map(r => {
            const ts = r.ts || "--";
            const source = r.source || "--";
            const mode = r.run_mode || "--";
            const action = r.action || "--";
            const side = r.side || "--";
            const ticker = r.ticker || "--";
            const price = r.price_cents !== null && r.price_cents !== undefined ? `${r.price_cents}c` : "--";
            const count = r.count ?? "--";
            const cost = r.cost_cents !== null && r.cost_cents !== undefined ? formatCents(r.cost_cents) : "--";
            const status = r.status_code ?? "--";
            const note = r.note || "--";
            const modeClass = mode === "paper" ? "text-muted" : "";
            return `
              <tr>
                <td>${ts}</td>
                <td>${source}</td>
                <td class="${modeClass}" title="${mode === "paper" ? "No real orders placed" : "Real exchange orders"}">${mode}</td>
                <td>${action}</td>
                <td>${String(side).toUpperCase()}</td>
                <td>${ticker}</td>
                <td>${price}</td>
                <td>${count}</td>
                <td>${cost}</td>
                <td>${status}</td>
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
      [manualStrategyModeEl, manualStrategySideEl, manualStrategyAskMinEl, manualStrategyAskMaxEl, manualStrategyMaxCostEl, manualStrategyIntervalEl]
        .forEach(el => el.addEventListener("change", () => syncStrategyControls(true)));
      [strategyModeEl, strategySideEl, strategyAskMinEl, strategyAskMaxEl, strategyMaxCostEl, strategyIntervalEl]
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

    uvicorn.run(app, host="0.0.0.0", port=8090)
