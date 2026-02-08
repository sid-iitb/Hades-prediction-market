import datetime
import pytz
from src.offline_processing.generate_all_kalshi_urls import generate_kalshi_url

def get_current_market_urls():
    """
    Returns a dictionary with the current active market URLs for Polymarket and Kalshi.
    'Current' is defined as the market expiring/resolving at the top of the next hour.
    """
    now = datetime.datetime.now(pytz.utc)
    
    # Target time is the current full hour
    # Example: If now is 12:15, target is 12:00.
    target_time = now.replace(minute=0, second=0, microsecond=0)

    # Kalshi seems to use the *next* hour for the current market identifier
    # If it's 13:XX, the market is ...14
    kalshi_target_time = target_time + datetime.timedelta(hours=1)
    kalshi_url = generate_kalshi_url(kalshi_target_time)
    
    return {
        "kalshi": kalshi_url,
        "target_time_utc": target_time,
        "target_time_et": target_time.astimezone(pytz.timezone('US/Eastern'))
    }

if __name__ == "__main__":
    urls = get_current_market_urls()
    
    print(f"Current Time (UTC): {datetime.datetime.now(pytz.utc)}")
    print(f"Target Market Time (ET): {urls['target_time_et']}")
    print("-" * 50)
    print(f"Kalshi:     {urls['kalshi']}")
