"""
Fetch 30 days of historical wildfire data from NASA FIRMS API.
Downloads daily CSV files and stores them in ml_model/data/ directory.
"""

import os
import httpx
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configuration
NASA_FIRMS_API_KEY = os.getenv('NASA_FIRMS_API_KEY')
BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
SATELLITE = "VIIRS_SNPP_NRT"
REGION = "world"

# Output directory
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

def fetch_data_for_date(date_str: str):
    """
    Fetch fire data for a specific date.
    
    Args:
        date_str: Date in YYYY-MM-DD format
    
    Returns:
        CSV content as string, or None if failed
    """
    url = f"{BASE_URL}/{NASA_FIRMS_API_KEY}/{SATELLITE}/{REGION}/1/{date_str}"
    
    print(f"Fetching data for {date_str}...")
    
    try:
        with httpx.Client(timeout=60.0) as client:
            response = client.get(url)
            response.raise_for_status()
            
            # Check if data is valid
            if len(response.text) < 100:  # Too short to be valid CSV
                print(f"  ⚠️  No data available for {date_str}")
                return None
            
            print(f"  ✅ Downloaded {len(response.text.splitlines())} lines")
            return response.text
    
    except httpx.HTTPError as e:
        print(f"  ❌ HTTP error: {e}")
        return None
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None

def save_data(date_str: str, csv_content: str):
    """Save CSV data to file."""
    output_file = DATA_DIR / f"fires_{date_str}.csv"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(csv_content)
    
    print(f"  💾 Saved to {output_file}")

def fetch_30_days_data():
    """Fetch historical data for the last 30 days."""
    
    if not NASA_FIRMS_API_KEY:
        print("❌ ERROR: NASA_FIRMS_API_KEY not found in .env file")
        return
    
    print("=" * 60)
    print("NASA FIRMS Historical Data Fetcher")
    print("=" * 60)
    print(f"API Key: {NASA_FIRMS_API_KEY[:10]}...")
    print(f"Satellite: {SATELLITE}")
    print(f"Region: {REGION}")
    print(f"Output: {DATA_DIR}")
    print("=" * 60)
    
    # Calculate date range (30 days ago to today)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    print(f"\nFetching data from {start_date.date()} to {end_date.date()}")
    print(f"Total days: 30\n")
    
    successful = 0
    failed = 0
    
    # Fetch data for each day
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        
        csv_content = fetch_data_for_date(date_str)
        
        if csv_content:
            save_data(date_str, csv_content)
            successful += 1
        else:
            failed += 1
        
        # Move to next day
        current_date += timedelta(days=1)
        
        # Rate limiting: wait 2 seconds between requests
        time.sleep(2)
    
    print("\n" + "=" * 60)
    print(f"✅ Successfully downloaded: {successful} days")
    print(f"❌ Failed/No data: {failed} days")
    print(f"📁 Data saved to: {DATA_DIR}")
    print("=" * 60)

if __name__ == "__main__":
    fetch_30_days_data()
