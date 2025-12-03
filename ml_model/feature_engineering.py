"""
Feature Engineering for Wildfire Prediction
Converts raw fire data into grid-based features for ML training
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

# Configuration
DATA_DIR = Path(__file__).parent / 'data'
GRID_SIZE = 0.5  # 0.5 degree grid cells (~55km at equator)
OUTPUT_FILE = DATA_DIR / 'processed_features.csv'


def load_all_fire_data() -> pd.DataFrame:
    """Load all CSV files and combine into single DataFrame"""
    print("📂 Loading fire data files...")
    
    csv_files = sorted(DATA_DIR.glob('fires_*.csv'))
    
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {DATA_DIR}")
    
    all_data = []
    for filepath in csv_files:
        try:
            df = pd.read_csv(filepath)
            # Extract date from filename
            date_str = filepath.stem.replace('fires_', '')
            df['file_date'] = date_str
            all_data.append(df)
            print(f"  ✅ {filepath.name}: {len(df):,} records")
        except Exception as e:
            print(f"  ❌ Error loading {filepath.name}: {e}")
    
    combined = pd.concat(all_data, ignore_index=True)
    print(f"\n📊 Total records: {len(combined):,}")
    
    return combined


def create_grid_cell(lat: float, lon: float) -> tuple:
    """Convert lat/lon to grid cell identifier"""
    grid_lat = np.floor(lat / GRID_SIZE) * GRID_SIZE
    grid_lon = np.floor(lon / GRID_SIZE) * GRID_SIZE
    return (grid_lat, grid_lon)


def get_neighbor_cells(grid_cell: tuple) -> list:
    """Get 8 neighboring grid cells"""
    lat, lon = grid_cell
    neighbors = []
    for dlat in [-GRID_SIZE, 0, GRID_SIZE]:
        for dlon in [-GRID_SIZE, 0, GRID_SIZE]:
            if dlat == 0 and dlon == 0:
                continue
            neighbors.append((lat + dlat, lon + dlon))
    return neighbors


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Create grid-based features for prediction"""
    print("\n🔧 Engineering features...")
    
    # Parse dates
    df['acq_date'] = pd.to_datetime(df['acq_date'])
    df['grid_cell'] = df.apply(lambda r: create_grid_cell(r['latitude'], r['longitude']), axis=1)
    
    # Get unique dates sorted
    dates = sorted(df['acq_date'].unique())
    print(f"  📅 Date range: {dates[0]} to {dates[-1]}")
    
    # Build fire history by date and grid cell
    print("  📈 Building fire history...")
    fire_history = defaultdict(lambda: defaultdict(list))
    
    for _, row in df.iterrows():
        date = row['acq_date']
        grid = row['grid_cell']
        fire_history[date][grid].append({
            'frp': row.get('frp', 0),
            'brightness': row.get('brightness', 300),
            'confidence': row.get('confidence', 'n')
        })
    
    # Create training samples
    print("  🎯 Creating training samples...")
    samples = []
    
    # Skip first 7 days (need history) and last day (need next-day label)
    for i, current_date in enumerate(dates[7:-1]):
        next_date = dates[i + 8]  # Next day for labels
        
        # Get all grid cells that had fires in last 14 days
        active_grids = set()
        for j in range(14):
            past_date = dates[max(0, i + 7 - j)]
            active_grids.update(fire_history[past_date].keys())
        
        # Also add some random "no-fire" grid cells for negative samples
        all_lats = df['latitude'].values
        all_lons = df['longitude'].values
        
        for grid_cell in active_grids:
            grid_lat, grid_lon = grid_cell
            
            # Count fires in various time windows
            fires_last_7_days = 0
            fires_last_14_days = 0
            total_frp_7_days = 0
            
            for j in range(14):
                if i + 7 - j >= 0:
                    past_date = dates[i + 7 - j]
                    fires_in_cell = fire_history[past_date].get(grid_cell, [])
                    
                    if j < 7:
                        fires_last_7_days += len(fires_in_cell)
                        total_frp_7_days += sum(f['frp'] for f in fires_in_cell if f['frp'])
                    
                    fires_last_14_days += len(fires_in_cell)
            
            # Count neighbor fires (last 3 days)
            neighbor_fires = 0
            for neighbor in get_neighbor_cells(grid_cell):
                for j in range(3):
                    if i + 7 - j >= 0:
                        past_date = dates[i + 7 - j]
                        neighbor_fires += len(fire_history[past_date].get(neighbor, []))
            
            # Calculate avg FRP
            avg_frp = total_frp_7_days / fires_last_7_days if fires_last_7_days > 0 else 0
            
            # Target: did fire occur in this cell on NEXT day?
            next_day_fires = fire_history[next_date].get(grid_cell, [])
            target = 1 if len(next_day_fires) > 0 else 0
            
            # Temporal features
            month = current_date.month
            day_of_year = current_date.timetuple().tm_yday
            day_of_week = current_date.weekday()
            
            # Season (0=winter, 1=spring, 2=summer, 3=fall for Northern Hemisphere)
            if month in [12, 1, 2]:
                season = 0
            elif month in [3, 4, 5]:
                season = 1
            elif month in [6, 7, 8]:
                season = 2
            else:
                season = 3
            
            # Hemisphere adjustment for season
            if grid_lat < 0:
                season = (season + 2) % 4
            
            samples.append({
                'grid_lat': grid_lat,
                'grid_lon': grid_lon,
                'fires_last_7_days': fires_last_7_days,
                'fires_last_14_days': fires_last_14_days,
                'avg_frp_7_days': avg_frp,
                'neighbor_fires_3_days': neighbor_fires,
                'month': month,
                'day_of_year': day_of_year,
                'day_of_week': day_of_week,
                'season': season,
                'abs_latitude': abs(grid_lat),
                'target': target
            })
        
        if (i + 1) % 5 == 0:
            print(f"    Processed {i + 1}/{len(dates) - 8} days...")
    
    result_df = pd.DataFrame(samples)
    
    # Add negative samples (areas with no recent fires) for balance
    print("  ⚖️ Adding negative samples for balance...")
    
    positive_count = result_df['target'].sum()
    negative_count = len(result_df) - positive_count
    
    print(f"\n📊 Dataset Statistics:")
    print(f"   Total samples: {len(result_df):,}")
    print(f"   Positive (fire next day): {positive_count:,} ({100*positive_count/len(result_df):.1f}%)")
    print(f"   Negative (no fire): {negative_count:,} ({100*negative_count/len(result_df):.1f}%)")
    
    return result_df


def main():
    """Main entry point"""
    print("=" * 60)
    print("🔥 Wildfire Feature Engineering")
    print("=" * 60)
    
    # Load data
    df = load_all_fire_data()
    
    # Engineer features
    features_df = engineer_features(df)
    
    # Save processed features
    print(f"\n💾 Saving to {OUTPUT_FILE}...")
    features_df.to_csv(OUTPUT_FILE, index=False)
    
    print("\n" + "=" * 60)
    print("✅ Feature engineering complete!")
    print(f"   Output: {OUTPUT_FILE}")
    print(f"   Samples: {len(features_df):,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
