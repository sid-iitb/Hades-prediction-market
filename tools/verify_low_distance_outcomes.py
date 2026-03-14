"""Cross-verify low-distance ETH/BTC orders (would be blocked by floors) against Kalshi API."""
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Load project .env so KALSHI_API_KEY / KALSHI_PRIVATE_KEY are set before importing KalshiClient
from dotenv import load_dotenv

_root = Path(__file__).resolve().parent.parent
load_dotenv(_root / ".env")

from src.client.kalshi_client import KalshiClient


DB_PATH = str(_root / "data" / "bot_state.db")


def _load_low_distance_orders(since_iso: str):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # ETH: distance < 1.5
    cur.execute(
        """
        SELECT asset, order_id, window_id, ts_utc, distance, is_stop_loss, final_outcome, pnl_cents
        FROM strategy_report_last_90s
        WHERE asset = 'eth' AND placed = 1 AND ts_utc >= ? AND distance < 1.5
        ORDER BY ts_utc
        """,
        (since_iso,),
    )
    eth_rows = [r for r in cur.fetchall() if r["order_id"]]

    # BTC: distance < 10.0
    cur.execute(
        """
        SELECT asset, order_id, window_id, ts_utc, distance, is_stop_loss, final_outcome, pnl_cents
        FROM strategy_report_last_90s
        WHERE asset = 'btc' AND placed = 1 AND ts_utc >= ? AND distance < 10.0
        ORDER BY ts_utc
        """,
        (since_iso,),
    )
    btc_rows = [r for r in cur.fetchall() if r["order_id"]]

    conn.close()
    return eth_rows, btc_rows


def _check_with_kalshi(label: str, rows, client: KalshiClient) -> None:
    print(f"=== {label} (n={len(rows)}) ===")
    for r in rows:
        oid = r["order_id"]
        try:
            o = client.get_order(oid)
        except Exception as e:
            print(f"  order_id={oid[:8]} ... Kalshi error: {e}")
            continue
        status = str(o.get("status", "")).lower()
        fill_count = int(o.get("fill_count") or o.get("filled_count") or 0)
        fill_price = o.get("fill_price") or o.get("avg_fill_price")
        print(
            "  order_id=%s asset=%s window_id=%s ts=%s dist=%.2f | "
            "status=%s fill_count=%s fill_price=%s db_is_stop_loss=%s db_final_outcome=%s db_pnl=%s"
            % (
                oid[:8],
                (r["asset"] or "").upper(),
                r["window_id"],
                r["ts_utc"],
                r["distance"],
                status,
                fill_count,
                fill_price,
                r["is_stop_loss"],
                r["final_outcome"] or "?",
                r["pnl_cents"],
            )
        )
    print()


def main(hours: int = 24) -> None:
    since_dt = datetime.now(timezone.utc) - timedelta(hours=hours)
    since_iso = since_dt.strftime("%Y-%m-%dT%H:%M:%S")
    print(f"Window: last {hours}h (ts_utc >= {since_iso})\n")

    eth_rows, btc_rows = _load_low_distance_orders(since_iso)

    client = KalshiClient()

    _check_with_kalshi("ETH (distance < 1.5)", eth_rows, client)
    _check_with_kalshi("BTC (distance < 10.0)", btc_rows, client)


if __name__ == "__main__":
    main()

