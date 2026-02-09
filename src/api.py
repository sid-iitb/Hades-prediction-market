import datetime
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


_ingest_thread: threading.Thread | None = None


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

@app.get("/kalshi/place_best_ask_order")
def place_best_ask_order(side: str, ticker: str, max_cost_cents: int = 500):
    client = KalshiClient()
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
    return {"status_code": resp.status_code, "response": body}


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
        width: min(820px, 100%);
      }
      .card {
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 20px;
        padding: 28px;
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

      refresh();
      refreshChart();
      refreshMarketsBtn.addEventListener("click", refreshMarkets);
      setInterval(() => {
        refresh();
        refreshChart();
      }, 1000);
      window.addEventListener("resize", refreshChart);
    </script>
  </body>
</html>
    """


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8090)
