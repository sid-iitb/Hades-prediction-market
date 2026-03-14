import datetime
import pytz

# Base URL for Kalshi events
BASE_URL = "https://kalshi.com/markets/kxbtcd/bitcoin-price-abovebelow/"

# Kalshi event ticker prefixes for hourly price markets (above/below)
ASSET_PREFIXES = {
    "btc": "kxbtcd",
    "eth": "kxethd",
    "sol": "kxsold",
    "xrp": "kxxrpd",
    "doge": "kxdoge",  # doge has range only (no above/below)
}

# Hourly range market prefixes (2 per asset for btc/eth/sol/xrp; 1 for doge -> 9 markets total)
ASSET_PREFIXES_HOURLY_RANGE = {
    "btc": "kxbtc",
    "eth": "kxeth",
    "sol": "kxsol",
    "xrp": "kxxrp",
    "doge": "kxdoge",
}

# 15-minute crypto market prefixes (format: YYmmmDDHHMM)
ASSET_PREFIXES_15M = {
    "btc": "kxbtc15m",
    "eth": "kxeth15m",
    "sol": "kxsol15m",
    "xrp": "kxxrp15m",
}


def _slug_for_prefix_and_time(prefix: str, target_time) -> str:
    """Build hourly slug for a given prefix and Eastern time. Format: prefix-YYmmmDDHH."""
    et_tz = pytz.timezone("US/Eastern")
    if target_time.tzinfo is None:
        target_time = pytz.utc.localize(target_time).astimezone(et_tz)
    else:
        target_time = target_time.astimezone(et_tz)
    year = target_time.strftime("%y")
    month = target_time.strftime("%b").lower()
    day = target_time.strftime("%d")
    hour = target_time.strftime("%H")
    return f"{prefix}-{year}{month}{day}{hour}"


def generate_kalshi_slug(target_time, asset: str = "btc"):
    """
    Generates the Kalshi event slug for a given datetime and asset (above/below).
    Format: {prefix}-[YY][MMM][DD][HH]
    Example: kxbtcd-25nov2614 (Nov 26, 2025, 14:00 ET), kxethd-25nov2614 for ETH
    """
    prefix = ASSET_PREFIXES.get(str(asset).lower(), "kxbtcd")
    return _slug_for_prefix_and_time(prefix, target_time)


def generate_kalshi_slug_range(target_time, asset: str):
    """
    Generates the Kalshi hourly range event slug (e.g. kxbtc-26jan2817).
    Returns None if asset has no range prefix.
    """
    prefix = ASSET_PREFIXES_HOURLY_RANGE.get(str(asset).lower())
    if not prefix:
        return None
    return _slug_for_prefix_and_time(prefix, target_time)


def generate_15min_slug(target_time, asset: str = "btc"):
    """
    Generates the Kalshi event slug for 15-min crypto markets.
    Format: {prefix}-YYmmmDDHHMM (minute = 00, 15, 30, 45)
    Example: kxbtc15m-26feb141430 (Feb 14, 2026, 14:30 ET)
    """
    prefix = ASSET_PREFIXES_15M.get(str(asset).lower(), "kxbtc15m")
    et_tz = pytz.timezone("US/Eastern")
    if target_time.tzinfo is None:
        target_time = pytz.utc.localize(target_time).astimezone(et_tz)
    else:
        target_time = target_time.astimezone(et_tz)

    year = target_time.strftime("%y")
    month = target_time.strftime("%b").lower()
    day = target_time.strftime("%d")
    hour = target_time.strftime("%H")
    minute = target_time.strftime("%M")
    slug = f"{prefix}-{year}{month}{day}{hour}{minute}"
    return slug


_ASSET_BASE_URLS = {
    "btc": "https://kalshi.com/markets/kxbtcd/bitcoin-price-abovebelow/",
    "eth": "https://kalshi.com/markets/kxethd/ethereum-price-abovebelow/",
    "sol": "https://kalshi.com/markets/kxsold/solana-price-abovebelow/",
    "xrp": "https://kalshi.com/markets/kxxrpd/xrp-price-abovebelow/",
}


def generate_kalshi_url(target_time, asset: str = "btc"):
    """
    Generates the full Kalshi URL for a given datetime.
    """
    slug = generate_kalshi_slug(target_time, asset)
    base = _ASSET_BASE_URLS.get(str(asset).lower(), _ASSET_BASE_URLS["btc"])
    return f"{base}{slug}"

def generate_urls_until_year_end():
    """
    Generates URLs for every hour from now until Jan 1, 2026.
    Saves them to 'kalshi_urls_2025.txt'.
    """
    urls = []
    now = datetime.datetime.now(pytz.utc)
    
    # Start from the next full hour
    current_target = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
    
    # End date: Jan 1, 2026 00:00 UTC (approx, depends on ET)
    et_tz = pytz.timezone('US/Eastern')
    
    print(f"Generating URLs starting from: {current_target.astimezone(et_tz)}")
    
    while True:
        # Check if we reached 2026 in ET
        et_time = current_target.astimezone(et_tz)
        if et_time.year >= 2027:
            break
            
        urls.append(generate_kalshi_url(current_target))
        current_target += datetime.timedelta(hours=1)
        
    with open("kalshi_urls_2026.txt", "w") as f:
        for url in urls:
            f.write(url + "\n")
            
    print(f"Generated {len(urls)} URLs and saved to 'kalshi_urls_2025.txt'")

if __name__ == "__main__":
    print("--- Kalshi URL Generator ---")
    
    # Test with the user's specific example time to verify logic
    # User example: kxbtcd-25nov2614 -> Nov 26, 2025, 14:00 ET
    
    et_tz = pytz.timezone('US/Eastern')
    test_time = et_tz.localize(datetime.datetime(2026, 2, 7, 14, 0, 0))
    print(f"Test Time (ET): {test_time}")
    print(f"Generated URL: {generate_kalshi_url(test_time)}")
    
    print("\n--- Generating URLs until 2026 ---")
    generate_urls_until_year_end()
