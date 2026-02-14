import datetime
import json
import os
import sqlite3
import threading
from datetime import timedelta, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
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
    mode_hint = (
        cycle.get("entry", {}).get("mode")
        or cycle.get("reentry", {}).get("mode")
        or cycle.get("exit", {}).get("mode")
        or None
    )

    entry = cycle.get("entry")
    if isinstance(entry, dict) and entry.get("active_position"):
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
            note=f"strategy_action={cycle.get('action')} spot={spot}",
            payload=entry,
        )

    if cycle.get("action") == "stop_loss_rotate":
        exited = cycle.get("exited_position") or {}
        exit_leg = cycle.get("exit") or {}
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
            note=f"stop_loss pnl_pct={cycle.get('pnl_pct')}",
            payload=exit_leg,
        )
        reentry = cycle.get("reentry") or {}
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
                note="stop_loss_reentry_further",
                payload=reentry,
            )


_ingest_thread: threading.Thread | None = None
_farthest_auto_thread: threading.Thread | None = None
_farthest_auto_stop = threading.Event()
_farthest_auto_lock = threading.Lock()
_farthest_auto_state = {
    "running": False,
    "config": None,
    "last_run_at": None,
    "last_scheduled_run_at": None,
    "next_scheduled_run_at": None,
    "last_result": None,
    "active_position": None,
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


def _run_farthest_band_once(config: FarthestBandConfig, active_position: dict | None = None):
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
    )
    return {
        "spot": spot,
        "ingest_run_id": ingest.get("id"),
        "ingest_ts": ingest.get("ts"),
        "event_ticker": ingest.get("event_ticker"),
        "result": cycle,
        "active_position": cycle.get("active_position"),
    }


def _farthest_band_worker():
    next_scheduled_at = datetime.datetime.now(timezone.utc)
    while not _farthest_auto_stop.is_set():
        now_utc = datetime.datetime.now(timezone.utc)
        with _farthest_auto_lock:
            cfg_dict = dict(_farthest_auto_state.get("config") or {})
            active_mode = _farthest_auto_state.get("active_position_mode")
            cfg_mode = str(cfg_dict.get("mode") or "").lower()
            if active_mode == cfg_mode:
                active_position = dict(_farthest_auto_state.get("active_position") or {}) or None
            else:
                active_position = None
            _farthest_auto_state["next_scheduled_run_at"] = next_scheduled_at.isoformat()
        if not cfg_dict:
            break

        config = FarthestBandConfig(**cfg_dict)
        should_run = bool(active_position) or (now_utc >= next_scheduled_at)
        is_scheduled_run = now_utc >= next_scheduled_at

        if should_run:
            run_at = datetime.datetime.now(timezone.utc).isoformat()
            try:
                out = _run_farthest_band_once(config, active_position=active_position)
            except Exception as exc:
                out = {"action": "error", "reason": str(exc)}
            _record_strategy_ledger(out, source="/strategy/farthest_band/auto")

            if is_scheduled_run:
                interval_seconds = max(int(config.interval_minutes) * 60, 60)
                next_scheduled_at = now_utc + timedelta(seconds=interval_seconds)

            with _farthest_auto_lock:
                _farthest_auto_state["last_run_at"] = run_at
                if is_scheduled_run:
                    _farthest_auto_state["last_scheduled_run_at"] = run_at
                _farthest_auto_state["next_scheduled_run_at"] = next_scheduled_at.isoformat()
                _farthest_auto_state["last_result"] = out
                _farthest_auto_state["active_position"] = out.get("active_position")
                _farthest_auto_state["active_position_mode"] = cfg_mode if out.get("active_position") else None

        # Risk checks run every 5 minutes while auto mode is running.
        if _farthest_auto_stop.wait(timeout=300):
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
    client = KalshiClient()
    resp = None
    if side.lower() == "yes":
        resp = client.place_yes_limit_at_best_ask(
            ticker=ticker,
            max_cost_cents=max_cost_cents,
        )
    elif side.lower() == "no":
        resp = client.place_no_limit_at_best_ask(
            ticker=ticker,
            max_cost_cents=max_cost_cents,
        )
    else:
        return {"error": "side must be 'yes' or 'no'"}

    try:
        body = resp.json()
    except Exception:
        body = resp.text
    body_obj = body if isinstance(body, dict) else {}
    price_cents = _maybe_int(
        body_obj.get("yes_price")
        or body_obj.get("no_price")
        or body_obj.get("price")
        or body_obj.get("limit_price")
    )
    count = _maybe_int(body_obj.get("count") or body_obj.get("quantity") or body_obj.get("filled_count"))
    _log_trade_ledger(
        source="/kalshi/place_best_ask_order",
        run_mode="live",
        action="buy",
        side=str(side).lower(),
        ticker=ticker,
        price_cents=price_cents,
        count=count,
        status_code=resp.status_code if resp is not None else None,
        success=bool(resp is not None and resp.status_code in {200, 201}),
        note=f"max_cost_cents={max_cost_cents}",
        payload=body,
    )
    return {"status_code": resp.status_code, "response": body}


@app.get("/kalshi/portfolio/balance")
def get_portfolio_balance():
    client = KalshiClient()
    return client.get_balance()


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


@app.get("/kalshi/portfolio/current")
def get_portfolio_current_orders(status: str | None = "open", limit: int = 200):
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
        price_cents = _price_to_cents(avg_price)
        if price_cents is None:
            price_cents = _extract_order_price_cents({"avg_price": avg_price})

        cost_cents = (
            _parse_dollar_str_to_cents(p.get("total_cost_dollars"))
            or _parse_dollar_str_to_cents(p.get("total_cost"))
            or _parse_dollar_str_to_cents(p.get("market_exposure_dollars"))
            or _parse_dollar_str_to_cents(p.get("market_exposure"))
            or _parse_dollar_str_to_cents(p.get("total_traded_dollars"))
            or _parse_dollar_str_to_cents(p.get("total_traded"))
            or _parse_dollar_str_to_cents(p.get("cost"))
        )
        if cost_cents is None and price_cents is not None and count:
            cost_cents = price_cents * int(count)
        max_payout_cents = 100 * int(count) if count else None
        market_value_cents = _parse_dollar_str_to_cents(p.get("market_value"))
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
            total_traded_cents = _parse_dollar_str_to_cents(p.get("total_traded_dollars")) or _parse_dollar_str_to_cents(p.get("total_traded"))
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
):
    config = FarthestBandConfig(
        direction="lower",
        side=side,
        ask_min_cents=ask_min_cents,
        ask_max_cents=ask_max_cents,
        max_cost_cents=max_cost_cents,
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
):
    global _farthest_auto_thread

    config = FarthestBandConfig(
        direction="lower",
        side=side,
        ask_min_cents=ask_min_cents,
        ask_max_cents=ask_max_cents,
        max_cost_cents=max_cost_cents,
        mode=mode,
        interval_minutes=interval_minutes,
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
        _farthest_auto_state["next_scheduled_run_at"] = datetime.datetime.now(timezone.utc).isoformat()
        _farthest_auto_state["last_result"] = None
        _farthest_auto_state["active_position"] = None
        _farthest_auto_state["active_position_mode"] = None
        _farthest_auto_thread = threading.Thread(
            target=_farthest_band_worker,
            name="farthest-band-auto",
            daemon=True,
        )
        _farthest_auto_thread.start()

    return {
        "running": True,
        "message": "Auto strategy started",
        "state": _farthest_auto_state,
    }


@app.get("/strategy/farthest_band/auto/stop")
def strategy_farthest_band_auto_stop():
    _farthest_auto_stop.set()
    with _farthest_auto_lock:
        _farthest_auto_state["running"] = False
        active_position = _farthest_auto_state.get("active_position")
    return {
        "running": False,
        "message": "Auto strategy stop requested",
        "active_position": active_position,
    }


@app.get("/strategy/farthest_band/reset")
def strategy_farthest_band_reset():
    with _farthest_auto_lock:
        _farthest_auto_state["active_position"] = None
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
        padding: 24px;
      }
      .wrap {
        width: min(1500px, 100%);
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
        height: 260px;
        background: rgba(7, 12, 24, 0.45);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 12px;
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
      .sub {
        color: var(--muted);
        font-size: 14px;
      }
      .meta {
        margin-top: 18px;
        display: flex;
        gap: 18px;
        flex-wrap: wrap;
        font-size: 13px;
        color: var(--muted);
      }
      .pill {
        padding: 6px 10px;
        border-radius: 999px;
        background: rgba(244, 196, 48, 0.12);
        color: var(--accent);
        border: 1px solid rgba(244, 196, 48, 0.35);
      }
      .stats {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 12px;
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
        grid-template-columns: minmax(0, 1fr) minmax(0, 1.35fr);
        gap: 16px;
        align-items: start;
      }
      .portfolio-panel {
        background: rgba(10, 16, 32, 0.6);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 12px;
        min-height: 260px;
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
      .trade-status {
        margin-top: 8px;
        font-size: 12px;
        color: var(--muted);
      }
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
      .stat {
        background: rgba(255, 255, 255, 0.04);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 12px;
      }
      .stat .label {
        color: var(--muted);
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }
      .stat .value {
        font-size: 18px;
        margin-top: 6px;
      }
      @media (max-width: 520px) {
        .card { padding: 22px; }
        .meta { gap: 10px; }
        .stats { grid-template-columns: 1fr; }
        .top-panels { grid-template-columns: 1fr; }
      }
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="card">
        <div class="grid">
          <div>
            <div class="title">
              <span class="dot"></span>
              Live BTC Ticker
            </div>
            <div id="price" class="price">$--</div>
            <div id="ts" class="sub">Last update: --</div>
            <div class="meta">
              <span class="pill">Refresh: 5s</span>
              <span id="status">Status: waiting</span>
            </div>
          </div>
          <div class="chart-wrap">
            <canvas id="chart"></canvas>
          </div>
          <div class="stats">
            <div class="stat">
              <div class="label">Last Hour High</div>
              <div id="high" class="value">--</div>
            </div>
            <div class="stat">
              <div class="label">Last Hour Low</div>
              <div id="low" class="value">--</div>
            </div>
            <div class="stat">
              <div class="label">Samples</div>
              <div id="count" class="value">--</div>
            </div>
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
                    <th>Ticker</th>
                    <th>Yes Ask</th>
                    <th>No Ask</th>
                    <th>Subtitle</th>
                  </tr>
                </thead>
                <tbody id="markets-body">
                  <tr><td colspan="5">Click Refresh to load markets.</td></tr>
                </tbody>
              </table>
            </div>
            <div class="strategy-panel">
              <div class="markets-header">
                <h3>Farthest Band Strategy</h3>
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
                <button id="strategy-preview" class="btn secondary" type="button">Preview</button>
                <button id="strategy-run" class="btn" type="button">Run Once</button>
                <button id="strategy-auto-start" class="btn" type="button">Auto Start</button>
                <button id="strategy-auto-status" class="btn secondary" type="button">Auto Status</button>
                <button id="strategy-auto-stop" class="btn secondary" type="button">Auto Stop</button>
              </div>
              <div id="strategy-status" class="strategy-status">Strategy idle.</div>
              <div class="strategy-schedule">
                <div id="strategy-next-run" class="strategy-next-run">Next schedule: --</div>
                <div class="progress-track">
                  <div id="strategy-progress-fill" class="progress-fill"></div>
                </div>
              </div>
              <div id="strategy-planned-order" class="planned-order">Planned order will appear after Preview.</div>
              <div id="strategy-candidates" class="candidate-list"></div>
            </div>
          </div>
          <div class="portfolio-panel">
            <div class="markets-header">
              <h3>Current Portfolio</h3>
              <button id="refresh-portfolio" class="btn" type="button">Refresh</button>
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
                    <th>Note</th>
                  </tr>
                </thead>
                <tbody id="ledger-body">
                  <tr><td colspan="10">Click Refresh to load ledger.</td></tr>
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
      const statusEl = document.getElementById("status");
      const highEl = document.getElementById("high");
      const lowEl = document.getElementById("low");
      const countEl = document.getElementById("count");
      const canvas = document.getElementById("chart");
      const ctx = canvas.getContext("2d");
      const marketsBody = document.getElementById("markets-body");
      const refreshMarketsBtn = document.getElementById("refresh-markets");
      const maxCostEl = document.getElementById("max-cost");
      const refreshPortfolioBtn = document.getElementById("refresh-portfolio");
      const portfolioSummaryEl = document.getElementById("portfolio-summary");
      const portfolioBody = document.getElementById("portfolio-body");
      const refreshLedgerBtn = document.getElementById("refresh-ledger");
      const ledgerBody = document.getElementById("ledger-body");
      const strategyModeEl = document.getElementById("strategy-mode");
      const strategySideEl = document.getElementById("strategy-side");
      const strategyAskMinEl = document.getElementById("strategy-ask-min");
      const strategyAskMaxEl = document.getElementById("strategy-ask-max");
      const strategyMaxCostEl = document.getElementById("strategy-max-cost");
      const strategyIntervalEl = document.getElementById("strategy-interval");
      const strategyPreviewBtn = document.getElementById("strategy-preview");
      const strategyRunBtn = document.getElementById("strategy-run");
      const strategyAutoStartBtn = document.getElementById("strategy-auto-start");
      const strategyAutoStatusBtn = document.getElementById("strategy-auto-status");
      const strategyAutoStopBtn = document.getElementById("strategy-auto-stop");
      const strategyStatusEl = document.getElementById("strategy-status");
      const strategyNextRunEl = document.getElementById("strategy-next-run");
      const strategyProgressFillEl = document.getElementById("strategy-progress-fill");
      const strategyPlannedOrderEl = document.getElementById("strategy-planned-order");
      const strategyCandidatesEl = document.getElementById("strategy-candidates");
      let strategyPreviewInflight = false;
      let strategyAutoStatusInflight = false;

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
        ctx.clearRect(0, 0, w, h);

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

        const stepX = w / Math.max(points.length - 1, 1);

        const gradient = ctx.createLinearGradient(0, 0, w, 0);
        gradient.addColorStop(0, getComputedStyle(document.documentElement).getPropertyValue("--graph-1").trim());
        gradient.addColorStop(0.5, getComputedStyle(document.documentElement).getPropertyValue("--graph-2").trim());
        gradient.addColorStop(1, getComputedStyle(document.documentElement).getPropertyValue("--graph-3").trim());

        ctx.lineWidth = 2;
        ctx.strokeStyle = gradient;
        ctx.beginPath();
        points.forEach((p, i) => {
          const x = i * stepX;
          const y = h - ((p.price - minY) / (maxY - minY)) * h;
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        });
        ctx.stroke();

        ctx.fillStyle = "rgba(255,255,255,0.08)";
        ctx.beginPath();
        points.forEach((p, i) => {
          const x = i * stepX;
          const y = h - ((p.price - minY) / (maxY - minY)) * h;
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        });
        ctx.lineTo(w, h);
        ctx.lineTo(0, h);
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
          statusEl.textContent = "Status: live";
        } catch (err) {
          statusEl.textContent = "Status: error";
        }
      }

      async function refreshChart() {
        try {
          const res = await fetch("/kalshi_ingest/last_hour");
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          const points = (data.records || []).map(r => ({
            ts: r.ts,
            price: Number(r.current_price || 0),
          })).filter(p => p.price > 0);

          const prices = points.map(p => p.price);
          if (prices.length) {
            const high = Math.max(...prices);
            const low = Math.min(...prices);
            highEl.textContent = high.toLocaleString("en-US", { style: "currency", currency: "USD" });
            lowEl.textContent = low.toLocaleString("en-US", { style: "currency", currency: "USD" });
          } else {
            highEl.textContent = "--";
            lowEl.textContent = "--";
          }
          countEl.textContent = String(points.length);
          drawChart(points);
        } catch (err) {
          drawChart([]);
        }
      }

      function renderMarkets(markets) {
        if (!markets || !markets.length) {
          marketsBody.innerHTML = "<tr><td colspan=\\"5\\">No markets found.</td></tr>";
          return;
        }
        const rows = markets.slice(0, 20).map(m => {
          const strike = Number(m.strike || 0);
          const ticker = m.ticker || "";
          const yesAsk = m.yes_ask ?? "--";
          const noAsk = m.no_ask ?? "--";
          const subtitle = m.subtitle || "";
          return `
            <tr>
              <td>${strike ? strike.toLocaleString("en-US", { style: "currency", currency: "USD" }) : "--"}</td>
              <td>${ticker}</td>
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
        marketsBody.innerHTML = rows + `<tr><td colspan="5"><div id="trade-status" class="trade-status">Ready.</div></td></tr>`;
        marketsBody.querySelectorAll("button.trade").forEach(btn => {
          btn.addEventListener("click", () => {
            const side = btn.getAttribute("data-side");
            const ticker = btn.getAttribute("data-ticker");
            placeBestAskOrder(side, ticker);
          });
        });
      }

      async function refreshMarkets() {
        try {
          const res = await fetch("/kalshi_ingest/latest");
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          const records = data.records || [];
          const latest = records.length ? records[0] : null;
          renderMarkets(latest ? (latest.markets || []) : []);
        } catch (err) {
          renderMarkets([]);
        }
      }

      async function placeBestAskOrder(side, ticker) {
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
        if (statusEl) statusEl.textContent = `Placing ${side.toUpperCase()} order...`;
        try {
          const url = `/kalshi/place_best_ask_order?side=${encodeURIComponent(side)}&ticker=${encodeURIComponent(ticker)}&max_cost_cents=${encodeURIComponent(maxCost)}`;
          const res = await fetch(url);
          const data = await res.json();
          if (!res.ok || data.error) {
            if (statusEl) statusEl.textContent = `Error: ${data.error || res.status}`;
            return;
          }
          if (statusEl) statusEl.textContent = `Order submitted (${side.toUpperCase()}) for ${ticker}.`;
        } catch (err) {
          if (statusEl) statusEl.textContent = "Request failed.";
        }
      }

      function formatCents(cents) {
        if (cents === null || cents === undefined) return "--";
        const dollars = cents / 100;
        const sign = dollars >= 0 ? "" : "-";
        return `${sign}$${Math.abs(dollars).toFixed(2)}`;
      }

      function strategyParams(includeMode = false, includeInterval = false) {
        const params = new URLSearchParams();
        params.set("side", strategySideEl.value || "yes");
        params.set("ask_min_cents", String(Number(strategyAskMinEl.value || 95)));
        params.set("ask_max_cents", String(Number(strategyAskMaxEl.value || 99)));
        params.set("max_cost_cents", String(Number(strategyMaxCostEl.value || 500)));
        if (includeMode) params.set("mode", strategyModeEl.value || "paper");
        if (includeInterval) params.set("interval_minutes", String(Number(strategyIntervalEl.value || 15)));
        return params.toString();
      }

      function formatDuration(sec) {
        const s = Math.max(0, Math.floor(Number(sec || 0)));
        const m = Math.floor(s / 60);
        const r = s % 60;
        return `${m}m ${String(r).padStart(2, "0")}s`;
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

      function renderStrategyCandidates(candidates) {
        if (!candidates || !candidates.length) {
          strategyCandidatesEl.innerHTML = "";
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
        strategyCandidatesEl.innerHTML = `
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

      function renderStrategySelection(payload, label) {
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
          if (active) {
            strategyPlannedOrderEl.textContent = [
              `${label} (${mode})`,
              `No new order selected.`,
              `Active Position: ${active.ticker || "--"} (${String(active.side || "--").toUpperCase()})`,
              `Entry: ${active.entry_price_cents ?? "--"}c`,
              `Count: ${active.count ?? "--"}`,
              `Reason: ${cycle?.reason || "holding active position"}`,
            ].join("\\n");
          } else {
            strategyPlannedOrderEl.textContent = `No strategy selection returned. ${cycle?.reason || ""}`.trim();
          }
          strategyCandidatesEl.innerHTML = "";
          return;
        }
        if (!selected) {
          strategyPlannedOrderEl.textContent = `${label}: no exact match. ${selection.reason || ""}`;
          renderStrategyCandidates(selection.nearest_candidates || []);
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
        strategyPlannedOrderEl.textContent = lines.join("\\n");
        renderStrategyCandidates(selection.nearest_candidates || []);
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
          renderStrategySelection(data, "Planned order");
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
          renderStrategySelection(data, "Order placed/plan");
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
          if (data.last_result) renderStrategySelection(data.last_result, "Last auto plan");
        } catch (err) {
          if (!quiet) strategyStatusEl.textContent = `Auto status failed: ${err.message || "error"}`;
          updateScheduleProgress(null);
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
          const cash = formatCents(balance.cash_balance_cents ?? balance.cash_balance);
          const portfolio = formatCents(balance.portfolio_value_cents ?? balance.portfolio_value);
          portfolioSummaryEl.textContent = `Cash: ${cash} | Portfolio: ${portfolio}`;

          const orders = data.orders || [];
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
          portfolioSummaryEl.textContent = "Failed to load portfolio.";
          portfolioBody.innerHTML = "<tr><td colspan=\\"5\\">Error loading orders.</td></tr>";
        }
      }

      async function refreshLedger() {
        ledgerBody.innerHTML = "<tr><td colspan=\\"10\\">Loading ledger...</td></tr>";
        try {
          const res = await fetch("/ledger/trades?limit=50");
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          const records = data.records || [];
          if (!records.length) {
            ledgerBody.innerHTML = "<tr><td colspan=\\"10\\">No ledger records.</td></tr>";
            return;
          }
          const rows = records.map(r => {
            const ts = r.ts || "--";
            const source = r.source || "--";
            const action = r.action || "--";
            const side = r.side || "--";
            const ticker = r.ticker || "--";
            const price = r.price_cents !== null && r.price_cents !== undefined ? `${r.price_cents}c` : "--";
            const count = r.count ?? "--";
            const cost = r.cost_cents !== null && r.cost_cents !== undefined ? formatCents(r.cost_cents) : "--";
            const status = r.status_code ?? "--";
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
                <td>${note}</td>
              </tr>
            `;
          }).join("");
          ledgerBody.innerHTML = rows;
        } catch (err) {
          ledgerBody.innerHTML = "<tr><td colspan=\\"10\\">Error loading ledger.</td></tr>";
        }
      }

      refresh();
      refreshChart();
      refreshMarketsBtn.addEventListener("click", refreshMarkets);
      refreshPortfolioBtn.addEventListener("click", refreshPortfolio);
      refreshLedgerBtn.addEventListener("click", refreshLedger);
      strategyPreviewBtn.addEventListener("click", strategyPreview);
      strategyRunBtn.addEventListener("click", strategyRun);
      strategyAutoStartBtn.addEventListener("click", strategyAutoStart);
      strategyAutoStatusBtn.addEventListener("click", strategyAutoStatus);
      strategyAutoStopBtn.addEventListener("click", strategyAutoStop);
      setInterval(() => {
        refresh();
        refreshChart();
      }, 1000);
      setInterval(() => {
        strategyPreview(true);
        strategyAutoStatus(true);
      }, 1000);
      strategyPreview();
      refreshLedger();
      window.addEventListener("resize", refreshChart);
    </script>
  </body>
</html>
    """


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8090)
