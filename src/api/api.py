import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8090)
