"""
Fetch Historical Fire Data from NASA FIRMS API
Downloads fire detection data for model training

Uses DATE parameter to fetch historical data beyond the default 10 day window.
"""

import os
import httpx
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv
from io import StringIO

# Load environment variables
load_dotenv(Path(__file__).parent.parent / '.env')

# Configuration
NASA_FIRMS_API_KEY = os.getenv('NASA_FIRMS_API_KEY')
DATA_DIR = Path(__file__).parent / 'data'
DAYS_TO_FETCH = 20  # Total days to fetch (will make multiple API calls)
BATCH_SIZE = 10     # NASA FIRMS API max per request

# NASA FIRMS API URL templates
# Without date: gets most recent N days
NASA_FIRMS_URL_RECENT = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/{api_key}/VIIRS_SNPP_NRT/world/{day_range}"
# With date: gets N days starting from DATE
NASA_FIRMS_URL_DATED = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/{api_key}/VIIRS_SNPP_NRT/world/{day_range}/{date}"


async def fetch_data_batch(client: httpx.AsyncClient, start_date: str = None, days: int = 10) -> pd.DataFrame:
    """Fetch fire data for a batch of days"""
    if start_date:
        url = NASA_FIRMS_URL_DATED.format(api_key=NASA_FIRMS_API_KEY, day_range=days, date=start_date)
        print(f"  📡 Fetching {days} days starting from {start_date}...")
    else:
        url = NASA_FIRMS_URL_RECENT.format(api_key=NASA_FIRMS_API_KEY, day_range=days)
        print(f"  📡 Fetching most recent {days} days...")
    
    try:
        response = await client.get(url, timeout=180.0)
        response.raise_for_status()
        
        df = pd.read_csv(StringIO(response.text))
        print(f"  ✅ Received {len(df):,} records")
        return df
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return pd.DataFrame()


async def fetch_all_historical_data():
    """Fetch historical fire data in batches"""
    print("=" * 60)
    print("🔥 NASA FIRMS Historical Data Fetcher")
    print("=" * 60)
    
    if not NASA_FIRMS_API_KEY:
        print("❌ ERROR: NASA_FIRMS_API_KEY not found in .env file")
        return
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    print(f"\n📅 Fetching {DAYS_TO_FETCH} days of fire data in batches of {BATCH_SIZE}...")
    print(f"📁 Saving to: {DATA_DIR}\n")
    
    all_data = []
    
    async with httpx.AsyncClient() as client:
        # Calculate how many batches we need
        num_batches = (DAYS_TO_FETCH + BATCH_SIZE - 1) // BATCH_SIZE
        
        for batch in range(num_batches):
            days_in_batch = min(BATCH_SIZE, DAYS_TO_FETCH - batch * BATCH_SIZE)
            
            if batch == 0:
                # First batch: get most recent data
                df = await fetch_data_batch(client, start_date=None, days=days_in_batch)
            else:
                # Subsequent batches: calculate start date
                # We need to go back from the earliest date we have
                days_back = batch * BATCH_SIZE
                start_date = (datetime.now(timezone.utc) - timedelta(days=days_back + days_in_batch)).strftime('%Y-%m-%d')
                df = await fetch_data_batch(client, start_date=start_date, days=days_in_batch)
            
            if not df.empty:
                all_data.append(df)
            
            # Small delay between batches
            if batch < num_batches - 1:
                await asyncio.sleep(1)
    
    if not all_data:
        print("❌ No data received!")
        return
    
    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"\n📊 Total records fetched: {len(combined_df):,}")
    
    # Check for acq_date column
    if 'acq_date' not in combined_df.columns:
        print(f"  ❌ No 'acq_date' column found. Columns: {combined_df.columns.tolist()}")
        return
    
    # Remove duplicates (in case of overlapping batches)
    combined_df['acq_date'] = pd.to_datetime(combined_df['acq_date'])
    before_dedup = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['latitude', 'longitude', 'acq_date', 'acq_time'])
    print(f"  🔄 Removed {before_dedup - len(combined_df):,} duplicates")
    
    # Group by date and save separate files
    print("\n📆 Splitting by date...")
    for date, group_df in combined_df.groupby(combined_df['acq_date'].dt.date):
        date_str = date.strftime('%Y-%m-%d')
        filepath = DATA_DIR / f"fires_{date_str}.csv"
        group_df.to_csv(filepath, index=False)
        print(f"  ✅ {date_str}: {len(group_df):,} fires")
    
    # Summary
    csv_files = list(DATA_DIR.glob('fires_*.csv'))
    total_size = sum(f.stat().st_size for f in csv_files) / (1024 * 1024)
    
    print("\n" + "=" * 60)
    print("✅ Download Complete!")
    print(f"   📁 Files: {len(csv_files)} CSV files")
    print(f"   🔥 Total fires: {len(combined_df):,}")
    print(f"   💾 Size: {total_size:.1f} MB")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(fetch_all_historical_data())
