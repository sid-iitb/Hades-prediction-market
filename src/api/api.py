import datetime
import os
import sqlite3
from datetime import timedelta, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from src.client.kraken_client import KrakenClient

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
        margin: 0 0 10px 0;
        font-size: 13px;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
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
            <h3>Latest 10 Kalshi Markets</h3>
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
                <tr><td colspan="4">Loading...</td></tr>
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
        const rows = markets.slice(0, 10).map(m => {
          const strike = Number(m.strike || 0);
          const ticker = m.ticker || "";
          const yesAsk = m.yes_ask ?? "--";
          const noAsk = m.no_ask ?? "--";
          const subtitle = m.subtitle || "";
          return `
            <tr>
              <td>${strike ? strike.toLocaleString("en-US", { style: "currency", currency: "USD" }) : "--"}</td>
              <td>${ticker}c</td>
              <td>${yesAsk}c</td>
              <td>${noAsk}c</td>
              <td>${subtitle}</td>
            </tr>
          `;
        }).join("");
        marketsBody.innerHTML = rows;
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

      refresh();
      refreshChart();
      refreshMarkets();
      setInterval(() => {
        refresh();
        refreshChart();
        refreshMarkets();
      }, 1000);
      window.addEventListener("resize", refreshChart);
    </script>
  </body>
</html>
    """


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8090)
