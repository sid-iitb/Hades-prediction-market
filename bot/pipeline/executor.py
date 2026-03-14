"""
Pipeline executor for Bot V2: runs exit actions then places orders (DRY_RUN or live).
Uses OrderRegistry for ownership; when dry_run=False performs real Kalshi REST calls.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, List, Optional, Tuple

from bot.pipeline.intents import EXIT_ACTION_CANCEL_ONLY, EXIT_ACTION_MARKET_SELL, EXIT_ACTION_STOP_LOSS, EXIT_ACTION_TAKE_PROFIT, ExitAction, OrderIntent, OrderRecord
from bot.pipeline.registry import OrderRegistry
from src.client.kalshi_client import KalshiClient

logger = logging.getLogger(__name__)


def _sl_audit_db_path() -> Path:
    """
    Shared v2 state DB used for telemetry; reuse it for SL execution audit.
    """
    return Path(__file__).resolve().parents[2].parent / "data" / "v2_state.db"


def _ensure_sl_audit_table() -> None:
    """
    Create v2_sl_execution_audit table if it does not exist.
    Stores one row per SL/TP market-sell attempt with payload + error details.
    """
    path = _sl_audit_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = sqlite3.connect(str(path), check_same_thread=False)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS v2_sl_execution_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL,
                    order_id TEXT,
                    exit_action TEXT,
                    ticker TEXT,
                    side TEXT,
                    sell_count INTEGER,
                    attempt INTEGER,
                    success INTEGER,
                    http_status INTEGER,
                    error_message TEXT,
                    response_body TEXT,
                    payload_json TEXT
                )
                """
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.debug("[SL_AUDIT] Failed to ensure v2_sl_execution_audit table: %s", e)


def _record_sl_execution_audit(
    *,
    order_id: str,
    exit_action: str,
    ticker: str,
    side: str,
    sell_count: int,
    attempt: int,
    success: bool,
    http_status: Optional[int],
    error_message: Optional[str],
    response_body: Optional[str],
    payload: dict,
) -> None:
    """
    Insert a single SL/TP execution attempt into v2_sl_execution_audit.
    Best-effort only: failures are logged but never raise.
    """
    _ensure_sl_audit_table()
    path = _sl_audit_db_path()
    try:
        payload_json = json.dumps(payload, sort_keys=True, default=str)
    except Exception:
        payload_json = "{}"
    try:
        conn = sqlite3.connect(str(path), check_same_thread=False)
        try:
            conn.execute(
                """
                INSERT INTO v2_sl_execution_audit
                (ts, order_id, exit_action, ticker, side, sell_count, attempt, success, http_status, error_message, response_body, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    time.time(),
                    str(order_id),
                    str(exit_action),
                    str(ticker),
                    str(side),
                    int(sell_count),
                    int(attempt),
                    1 if success else 0,
                    int(http_status) if http_status is not None else None,
                    str(error_message) if error_message is not None else None,
                    str(response_body) if response_body is not None else None,
                    payload_json,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.debug("[SL_AUDIT] Failed to record SL execution audit for order_id=%s: %s", order_id, e)


class PipelineExecutor:
    """
    Executes ExitActions first (cancel/sell), then places OrderIntents.
    When dry_run=True, only logs; when False, uses kalshi_client to cancel/place orders.
    """

    def __init__(
        self,
        registry: OrderRegistry,
        dry_run: bool = True,
        kalshi_client: Optional[Any] = None,
    ) -> None:
        self._registry = registry
        self._dry_run = dry_run
        self._kalshi_client = kalshi_client

    def execute_cycle(
        self,
        intents: List[Tuple[OrderIntent, str]],
        exit_actions: List[ExitAction],
        interval: str,
        market_id: str,
        asset: str,
        ticker: str = "",
    ) -> None:
        """
        Step 1: Process exit_actions (cancel + sell). Step 2: Place intents (register on success).
        ticker: optional, used when placing orders; can be derived from context if needed.
        """
        # Step 1 — Exits
        for exit_action in exit_actions:
            if self._dry_run:
                extra = f" limit_price_cents={exit_action.limit_price_cents}" if exit_action.limit_price_cents else ""
                logger.info(
                    "[DRY RUN] Would execute ExitAction: order_id=%s action=%s%s",
                    exit_action.order_id,
                    exit_action.action,
                    extra,
                )
            else:
                self._execute_exit_action(exit_action)

        # Step 2 — Entries
        for intent, strategy_id in intents:
            if self._dry_run:
                logger.info(
                    "[DRY RUN] Would place OrderIntent: strategy_id=%s side=%s price_cents=%s count=%s client_order_id=%s",
                    strategy_id,
                    intent.side,
                    intent.price_cents,
                    intent.count,
                    intent.client_order_id,
                )
            else:
                self._place_intent(intent, strategy_id, interval, market_id, asset, ticker)

    def _execute_exit_action(self, exit_action: ExitAction) -> None:
        """
        Bifurcated exit handling for binary microstructure:
        - take_profit: provide liquidity — place resting limit sell at limit_price_cents (dedupe if already on book).
        - stop_loss / market_sell: take liquidity — cancel any resting orders for this position, then market sell.
        """
        if not self._kalshi_client:
            logger.error("[EXECUTION FATAL] No Kalshi client for exit order_id=%s", exit_action.order_id)
            return

        # Resolve position details from the entry order (ticker, side, fill_count, status).
        try:
            order_info = self._kalshi_client.get_order(exit_action.order_id)
            order = order_info.get("order", order_info) if isinstance(order_info, dict) else {}
        except Exception as e:
            logger.error("[EXECUTION FATAL] get_order failed order_id=%s: %s", exit_action.order_id, e)
            return

        ticker_sell = order.get("ticker")
        # Side: from the ENTRY order (we bought this side; we sell same side to close).
        side = (order.get("side") or "no").lower()
        status = (order.get("status") or "").lower()
        # Resolve contracts we can sell.
        # IMPORTANT: do NOT use remaining_count as filled size. remaining_count is unfilled remainder.
        fill_count = int(order.get("fill_count") or order.get("filled_count") or 0)
        if fill_count < 1:
            # Only fall back to count if the entry order is actually filled/executed.
            if status in ("executed", "filled", "complete"):
                fill_count = int(order.get("count") or 0)

        logger.info(
            "[EXECUTION AUDIT] SL/TP order_id=%s get_order → ticker=%s side=%s status=%s fill_count=%s (from fill_count/fill_count_fp or count when executed)",
            exit_action.order_id, ticker_sell, side, status, fill_count,
        )

        if not ticker_sell or side not in ("yes", "no"):
            logger.error("[EXECUTION FATAL] Invalid order shape order_id=%s ticker=%s side=%s", exit_action.order_id, ticker_sell, side)
            return

        # Cancel the entry order only if still resting (e.g. partially filled).
        # Only then mark registry as "canceled". If already executed/filled, do NOT overwrite to
        # "canceled" — we need to keep status "filled" so evaluate_exit keeps monitoring for stop_loss.
        if status == "resting":
            try:
                self._kalshi_client.cancel_order(exit_action.order_id)
            except Exception as e:
                logger.warning("[EXECUTION] Cancel entry order_id=%s failed (may already be filled): %s", exit_action.order_id, e)
            try:
                self._registry.update_order_status(exit_action.order_id, "canceled", 0)
            except Exception as e:
                logger.debug("Registry update_order_status failed order_id=%s: %s", exit_action.order_id, e)

        if exit_action.action == EXIT_ACTION_CANCEL_ONLY:
            return

        if exit_action.action in (EXIT_ACTION_STOP_LOSS, EXIT_ACTION_MARKET_SELL, EXIT_ACTION_TAKE_PROFIT):
            # TP and SL both close via market sell (fill at best available). No resting limit TP anymore.
            # Cancel any resting sells for this position first (legacy/no-op if none), then market sell.
            canceled = self._cancel_resting_sells_for_position(ticker_sell, side)
            if canceled:
                # Give the exchange a brief window to fully release inventory after cancel.
                time.sleep(0.5)
            if fill_count < 1:
                logger.warning(
                    "[EXECUTION] Stop-loss skipped: fill_count=0 for order_id=%s ticker=%s (status=%s); cannot market sell.",
                    exit_action.order_id, ticker_sell, status,
                )
                return
            # Clamp sell size to what we currently hold to avoid overselling (Kalshi will reject oversell).
            # If position is already 0 (e.g. take-profit limit filled), mark entry as exited and skip.
            pos_count = self._get_position_contracts(ticker_sell, side)
            if pos_count is not None:
                sell_count = min(fill_count, pos_count)
                logger.info(
                    "[EXECUTION AUDIT] count: fill_count=%s pos_count=%s → sell_count=min(%s,%s)=%s",
                    fill_count, pos_count, fill_count, pos_count, sell_count,
                )
                if sell_count < 1:
                    logger.info(
                        "[EXECUTION] Stop-loss skipped: no current position (already closed, e.g. TP filled) "
                        "order_id=%s ticker=%s side=%s filled=%s pos=%s; marking entry as exited.",
                        exit_action.order_id, ticker_sell, side, fill_count, pos_count,
                    )
                    try:
                        self._registry.update_order_status(exit_action.order_id, "exited", 0)
                    except Exception:
                        pass
                    return
            else:
                sell_count = fill_count
                logger.info(
                    "[EXECUTION AUDIT] count: pos_count=None (get_positions failed or no match) → sell_count=fill_count=%s",
                    sell_count,
                )
            # Payload we send: emulated market order as aggressive limit (sell down to 1c).
            payload = {
                "ticker": ticker_sell,
                "action": "sell",
                "side": side,
                "count": int(sell_count),
                "type": "limit",
                f"{side}_price": 1,
                "reduce_only": True,
                "time_in_force": "immediate_or_cancel",
            }
            logger.info(
                "[EXECUTION AUDIT] place_market_order payload: %s",
                payload,
            )
            # Retry stop-loss market sell until success or max attempts — when market is against us we must exit.
            max_stop_loss_retries = 10
            sleep_between_retries = 0.5
            retryable_keywords = (
                "insufficient", "balance", "locked", "inventory", "position",
                "timeout", "rate", "unavailable", "503", "502", "504", "connection",
            )
            last_error = None
            stop_loss_success = False
            for attempt in range(1, max_stop_loss_retries + 1):
                try:
                    resp = self._kalshi_client.place_market_order(
                        ticker=ticker_sell,
                        action="sell",
                        side=side,
                        count=sell_count,
                    )
                    sell_order_id = (resp or {}).get("order", {}).get("order_id") or (resp or {}).get("order_id")
                    reason = "take_profit" if exit_action.action == EXIT_ACTION_TAKE_PROFIT else "stop_loss/market_sell"
                    logger.info(
                        "[EXECUTION] %s: market sell ORDER PLACED (entry_order_id=%s sell_order_id=%s) ticker=%s side=%s count=%s attempt=%d",
                        reason, exit_action.order_id, sell_order_id, ticker_sell, side, sell_count, attempt,
                    )
                    # Record successful attempt in SL execution audit table.
                    _record_sl_execution_audit(
                        order_id=exit_action.order_id,
                        exit_action=exit_action.action,
                        ticker=ticker_sell,
                        side=side,
                        sell_count=sell_count,
                        attempt=attempt,
                        success=True,
                        http_status=getattr(getattr(resp, "status_code", None), "__int__", lambda: None)(),
                        error_message=None,
                        response_body=getattr(resp, "text", None),
                        payload=payload,
                    )
                    # Optional verification: if Kalshi instantly cancels market orders due to protections,
                    # detect it quickly so we can retry within this stop-loss loop.
                    if sell_order_id:
                        try:
                            time.sleep(0.25)
                            so = self._kalshi_client.get_order(str(sell_order_id))
                            so_d = so.get("order", so) if isinstance(so, dict) else {}
                            s_status = str(so_d.get("status") or "").strip().lower()
                            if s_status == "canceled":
                                raise RuntimeError(f"sell_market_order_canceled order_id={sell_order_id}")
                        except Exception as ve:
                            # Treat verification failures as retryable within the stop-loss loop.
                            raise ve
                    # Position closed; mark entry as exited so we don't keep emitting stop_loss.
                    stop_loss_success = True
                    try:
                        self._registry.update_order_status(exit_action.order_id, "exited", sell_count)
                    except Exception:
                        pass
                    break
                except Exception as e:
                    last_error = e
                    msg = str(e).lower()
                    retryable = any(k in msg for k in retryable_keywords)
                    if attempt < max_stop_loss_retries and retryable:
                        logger.warning(
                            "[EXECUTION] Stop-loss market sell attempt %d/%d failed for order_id=%s ticker=%s side=%s: %s; "
                            "sleeping %.1fs before retry",
                            attempt, max_stop_loss_retries, exit_action.order_id, ticker_sell, side, e,
                            sleep_between_retries,
                        )
                        time.sleep(sleep_between_retries)
                        continue
                    # Record failed attempt (non-retryable or last attempt) in SL execution audit table.
                    http_status = None
                    body = None
                    if hasattr(e, "response") and getattr(e, "response") is not None:
                        try:
                            http_status = getattr(e.response, "status_code", None)
                            body = getattr(e.response, "text", None)
                        except Exception:
                            body = None
                    _record_sl_execution_audit(
                        order_id=exit_action.order_id,
                        exit_action=exit_action.action,
                        ticker=ticker_sell,
                        side=side,
                        sell_count=sell_count,
                        attempt=attempt,
                        success=False,
                        http_status=http_status,
                        error_message=str(e),
                        response_body=body,
                        payload=payload,
                    )
                    logger.error(
                        "[EXECUTION FATAL] Stop-loss market sell failed order_id=%s (attempt=%d/%d): %s",
                        exit_action.order_id, attempt, max_stop_loss_retries, e,
                    )
                    break
            if not stop_loss_success:
                if last_error is not None:
                    logger.error(
                        "[EXECUTION FATAL] Stop-loss market sell failed for order_id=%s: %s",
                        exit_action.order_id, last_error,
                    )
                # Mark as exited so we stop emitting take_profit and avoid TP spam (e.g. 400 after duplicate cancels).
                try:
                    self._registry.update_order_status(exit_action.order_id, "exited", 0)
                    logger.warning(
                        "[EXECUTION] Marked order_id=%s as exited after stop-loss failure to stop TP spam.",
                        exit_action.order_id,
                    )
                except Exception:
                    pass
            return

        # take_profit is handled above (same path as stop_loss: market sell). No resting limit TP.

    def _cancel_resting_sells_for_position(self, ticker: str, side: str) -> int:
        """
        Cancel any resting sell orders for this ticker/side (e.g. take-profit limit at 80c).
        Must be called before market sell on stop_loss so we don't leave the limit on the book.
        Returns number of orders canceled.
        """
        canceled = 0
        try:
            data = self._kalshi_client.get_orders(status="resting", ticker=ticker, limit=100)
            orders = data.get("orders") if isinstance(data, dict) else None
            if orders is None and isinstance(data, list):
                orders = data
            if not isinstance(orders, list):
                orders = []
            for o in orders:
                if not isinstance(o, dict):
                    continue
                # Kalshi: action is "buy" or "sell"; side is "yes" or "no".
                act = str(o.get("action", "")).strip().lower()
                sid = str(o.get("side", "")).strip().lower()
                if act != "sell" or sid != side:
                    continue
                oid = o.get("order_id") or o.get("id")
                if not oid:
                    continue
                try:
                    self._kalshi_client.cancel_order(str(oid))
                    canceled += 1
                    logger.info(
                        "[EXECUTION] Canceled resting take-profit sell order_id=%s ticker=%s side=%s (before stop-loss market sell)",
                        oid, ticker, side,
                    )
                except Exception as e:
                    logger.warning("[EXECUTION] Cancel resting sell order_id=%s failed: %s", oid, e)
        except Exception as e:
            logger.warning("[EXECUTION] get_orders(resting) failed ticker=%s: %s", ticker, e)
        return canceled

    def _get_position_contracts(self, ticker: str, side: str) -> Optional[int]:
        """
        Best-effort: return current position contracts for (ticker, side).
        This is the safest source of truth for how much we can sell on stop-loss.
        Returns None if positions cannot be fetched/parsed.
        """
        if not self._kalshi_client:
            return None
        try:
            data = self._kalshi_client.get_positions(limit=200)
            if not isinstance(data, dict):
                return None

            want_ticker = (ticker or "").strip()
            want_side = (side or "").strip().lower()
            if want_side not in ("yes", "no") or not want_ticker:
                return None

            # Preferred (per Kalshi docs): market_positions[].position_fp
            market_positions = data.get("market_positions")
            if isinstance(market_positions, list):
                for mp in market_positions:
                    if not isinstance(mp, dict):
                        continue
                    pt = (mp.get("ticker") or "").strip()
                    if pt != want_ticker:
                        continue
                    pos_fp = mp.get("position_fp")
                    try:
                        pos = float(str(pos_fp)) if pos_fp is not None else 0.0
                    except (TypeError, ValueError):
                        pos = 0.0
                    # Docs: negative means NO, positive means YES.
                    if want_side == "yes":
                        return int(pos) if pos > 0 else 0
                    return int(abs(pos)) if pos < 0 else 0

            # Fallback: legacy/alt shapes some code paths may return ("positions" list)
            positions = data.get("positions", [])
            if isinstance(positions, list):
                total = 0
                for p in positions:
                    if not isinstance(p, dict):
                        continue
                    pt = (p.get("ticker") or p.get("market_ticker") or p.get("event_ticker") or "").strip()
                    if not pt or pt != want_ticker:
                        continue
                    raw_side = p.get("side") if p.get("side") is not None else p.get("position")
                    side_l = str(raw_side).lower() if raw_side is not None else ""
                    if isinstance(raw_side, (int, float)) and raw_side != 0:
                        side_l = "yes" if raw_side > 0 else "no"
                    if side_l != want_side:
                        continue
                    cnt = p.get("contracts") or p.get("quantity") or p.get("count") or 0
                    try:
                        c = int(abs(float(cnt)))
                    except (TypeError, ValueError):
                        c = 0
                    total += c
                return int(total)

            return None
        except Exception as e:
            logger.debug("[EXECUTION] get_positions failed for stop-loss clamp ticker=%s side=%s: %s", ticker, side, e)
            return None

    def _has_resting_limit_sell_at_price(self, ticker: str, side: str, price_cents: int) -> bool:
        """Return True if we already have a resting limit sell at the given price (avoid duplicate API calls)."""
        try:
            data = self._kalshi_client.get_orders(status="resting", ticker=ticker, limit=100)
            orders = data.get("orders") if isinstance(data, dict) else None
            if orders is None and isinstance(data, list):
                orders = data
            if not isinstance(orders, list):
                orders = []
            for o in orders:
                if not isinstance(o, dict):
                    continue
                if str(o.get("action", "")).lower() != "sell":
                    continue
                if str(o.get("side", "")).lower() != side:
                    continue
                # Limit price for this sell: legacy yes_price/no_price (cents); V2 yes_price_dollars/no_price_dollars.
                if side == "yes":
                    order_price = o.get("yes_price")
                    if order_price is None:
                        order_price = KalshiClient._extract_price(o.get("yes_price_dollars"))
                else:
                    order_price = o.get("no_price")
                    if order_price is None:
                        order_price = KalshiClient._extract_price(o.get("no_price_dollars"))
                if order_price is None:
                    order_price = 0
                else:
                    order_price = int(order_price)
                if order_price == price_cents:
                    return True
            return False
        except Exception as e:
            logger.warning("[EXECUTION] get_orders(resting) for dedupe failed ticker=%s: %s", ticker, e)
            return False

    def _verify_order_on_kalshi(self, order_id: str, expected_ticker: str, strategy_id: str) -> None:
        """Verify the order appears on Kalshi (catches wrong env e.g. demo vs prod)."""
        if not self._kalshi_client:
            return
        try:
            order_info = self._kalshi_client.get_order(order_id)
            order = order_info.get("order", order_info) if isinstance(order_info, dict) else {}
            got_ticker = (order.get("ticker") or "").strip()
            if got_ticker and got_ticker != expected_ticker:
                logger.warning(
                    "[EXECUTION] Verify: order_id=%s ticker mismatch (got %s, expected %s)",
                    order_id, got_ticker, expected_ticker,
                )
            elif got_ticker:
                logger.debug("[EXECUTION] Verify: order_id=%s found on Kalshi ticker=%s", order_id, got_ticker)
        except Exception as e:
            logger.warning(
                "[EXECUTION] Verify failed for order_id=%s (order may be on different env or already filled): %s",
                order_id, e,
            )

    def _place_intent(
        self,
        intent: OrderIntent,
        strategy_id: str,
        interval: str,
        market_id: str,
        asset: str,
        ticker: str,
    ) -> None:
        """Place limit order on Kalshi; capture order_id from response; register in v2_state.db."""
        if not self._kalshi_client:
            logger.error("[EXECUTION FATAL] No Kalshi client for place intent strategy_id=%s", strategy_id)
            return
        ticker_place = ticker or market_id
        if not ticker_place:
            logger.error("[EXECUTION FATAL] No ticker for place intent strategy_id=%s", strategy_id)
            return
        try:
            resp = self._kalshi_client.place_limit_order(
                ticker=ticker_place,
                action="buy",
                side=intent.side,
                price_cents=intent.price_cents,
                count=intent.count,
                client_order_id=intent.client_order_id,
            )
            resp.raise_for_status()
            data = resp.json()
            order = data.get("order", data) if isinstance(data, dict) else {}
            order_id = order.get("order_id") or data.get("order_id")
            if not order_id:
                logger.error("[EXECUTION FATAL] No order_id in Kalshi place response strategy_id=%s", strategy_id)
                return
            self._registry.register_order(
                order_id=str(order_id),
                strategy_id=strategy_id,
                interval=interval,
                market_id=market_id,
                asset=asset,
                ticker=ticker_place,
                side=intent.side,
                count=intent.count,
                placed_at=time.time(),
                limit_price_cents=intent.price_cents if intent.order_type == "limit" else None,
                client_order_id=intent.client_order_id,
            )
            logger.info(
                "[EXECUTION] Order PLACED on Kalshi — order_id=%s ticker=%s side=%s count=%s price_cents=%s (check Kalshi app/website to verify)",
                order_id, ticker_place, intent.side, intent.count, intent.price_cents,
            )
            self._verify_order_on_kalshi(order_id, ticker_place, strategy_id)
        except Exception as e:
            # Log Kalshi response body on HTTP errors (e.g. 400 Bad Request) to diagnose market closed / invalid params.
            err_detail = str(e)
            if hasattr(e, "response") and e.response is not None:
                try:
                    body = e.response.text
                    if body and len(body) > 500:
                        body = body[:500] + "..."
                    err_detail = f"{e} | response_body={body!r}"
                except Exception:
                    pass
            logger.error(
                "[EXECUTION FATAL] Place order failed strategy_id=%s ticker=%s price_cents=%s count=%s: %s",
                strategy_id, ticker_place, getattr(intent, "price_cents", None), getattr(intent, "count", None), err_detail,
            )
