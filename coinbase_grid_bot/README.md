# Coinbase Grid Bot

Grid trading bot using live prices from Coinbase Advanced Trade API. Optionally places real GTC limit orders when CDP credentials are configured.

## Modes

- **Mock orders (default)**  
  No credentials: live price feed only; `place_limit_order` and `check_fills` are simulated (log only).

- **Live orders**  
  CDP credentials set: real GTC limit orders are placed and fill status is polled via the API.

## CDP authentication (live orders)

The Coinbase Advanced Trade API uses **JWT auth** with:

1. **API Key Name** – e.g. `organizations/{org_id}/apiKeys/{key_id}`
2. **PEM private key** – EC private key (download from Coinbase when you create the key)

Create keys at: **https://cloud.coinbase.com/access/api**

### Option A: Key file (recommended)

1. In Coinbase Developer Platform, create an API key and download the **JSON key file**.
2. Save it somewhere safe (e.g. `coinbase_key.json` in this folder or outside the repo).
3. Set in `.env`:

   ```env
   COINBASE_KEY_FILE=/path/to/coinbase_key.json
   ```

   Or pass `key_file=/path/to/coinbase_key.json` when constructing `LiveCoinbaseGridClient`.

The JSON file must contain `"name"` (API key name) and `"privateKey"` (PEM string).

### Option B: API key name + PEM in `.env`

1. Create an API key at https://cloud.coinbase.com/access/api and copy:
   - **Key name** (e.g. `organizations/.../apiKeys/...`)
   - **Private key** (PEM block: `-----BEGIN EC PRIVATE KEY-----` … `-----END EC PRIVATE KEY-----`)
2. In `.env`, put the key name and the PEM on **one line** by replacing real newlines with `\n`:

   ```env
   COINBASE_API_KEY=organizations/your-org/apiKeys/your-key-id
   COINBASE_API_SECRET="-----BEGIN EC PRIVATE KEY-----\nMIIE...your...key...\n-----END EC PRIVATE KEY-----"
   ```

   Use quotes so the value is a single line; the bot will turn `\n` back into newlines.

**Important:** Do **not** use the old “API key + API secret” (base64) from the legacy Exchange API. For Advanced Trade you must use the **CDP key name + PEM** from Cloud Coinbase.

## Error handling

The bot catches and logs:

- **Rate limit (429)** – logs a warning and skips the offending call (e.g. that order or that tick).
- **Insufficient funds** – logs and skips that order.
- **Minimum order size** – logs and skips that order.

Other API errors are logged and raised as `CoinbaseAPIError`.

## Requirements

```bash
pip install -r requirements.txt
```

See `requirements.txt` for `python-dotenv` and `coinbase-advanced-py`.
