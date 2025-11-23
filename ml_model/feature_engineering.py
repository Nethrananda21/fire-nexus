"""
Feature Engineering for Fire Prediction Model.

Processes raw NASA FIRMS data and creates features for ML training:
- Temporal features (day of week, month, season)
- Spatial features (latitude, longitude, clustering)
- Fire history features (past fire counts in region)
- Environmental features (FRP, brightness, confidence)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import glob

# Directories
DATA_DIR = Path(__file__).parent / "data"
OUTPUT_FILE = Path(__file__).parent / "data" / "processed_features.csv"

def load_all_fire_data():
    """Load all CSV files from data directory into single DataFrame."""
    
    csv_files = glob.glob(str(DATA_DIR / "fires_*.csv"))
    
    if not csv_files:
        print("❌ No CSV files found in data directory!")
        return None
    
    print(f"Loading {len(csv_files)} CSV files...")
    
    dataframes = []
    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path)
            dataframes.append(df)
        except Exception as e:
            print(f"  ⚠️  Error loading {file_path}: {e}")
    
    if not dataframes:
        return None
    
    # Combine all dataframes
    combined_df = pd.concat(dataframes, ignore_index=True)
    print(f"✅ Loaded {len(combined_df)} fire detections")
    
    return combined_df

def create_temporal_features(df):
    """Extract temporal features from acquisition date/time."""
    
    print("Creating temporal features...")
    
    # Combine acq_date and acq_time to create datetime
    df['datetime'] = pd.to_datetime(
        df['acq_date'] + ' ' + df['acq_time'].astype(str).str.zfill(4),
        format='%Y-%m-%d %H%M',
        errors='coerce'
    )
    
    # Extract temporal features
    df['day_of_week'] = df['datetime'].dt.dayofweek  # 0=Monday, 6=Sunday
    df['day_of_month'] = df['datetime'].dt.day
    df['month'] = df['datetime'].dt.month
    df['hour'] = df['datetime'].dt.hour
    df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
    
    # Season (simplified)
    df['season'] = df['month'].apply(lambda x: 
        0 if x in [12, 1, 2] else  # Winter
        1 if x in [3, 4, 5] else    # Spring
        2 if x in [6, 7, 8] else    # Summer
        3                           # Fall
    )
    
    print("  ✅ Temporal features created")
    return df

def create_spatial_features(df):
    """Create spatial binning features for location-based patterns."""
    
    print("Creating spatial features...")
    
    # Create spatial grid (5-degree resolution)
    df['lat_grid'] = (df['latitude'] // 5) * 5
    df['lon_grid'] = (df['longitude'] // 5) * 5
    df['grid_cell'] = df['lat_grid'].astype(str) + '_' + df['lon_grid'].astype(str)
    
    # Distance from equator (normalized)
    df['abs_latitude'] = df['latitude'].abs()
    
    print("  ✅ Spatial features created")
    return df

def create_fire_history_features(df):
    """Create features based on historical fire activity in same region."""
    
    print("Creating fire history features (this may take a few minutes)...")
    
    # Sort by grid cell and datetime for efficient processing
    df = df.sort_values(['grid_cell', 'datetime']).reset_index(drop=True)
    
    # Initialize fires_last_7days column
    df['fires_last_7days'] = 0
    
    # Use vectorized approach with merge_asof for efficiency
    print("  Processing fire history counts...")
    
    # For each grid cell, count fires in past 7 days
    grid_counts = []
    
    for grid_cell, group in df.groupby('grid_cell'):
        if len(grid_counts) % 1000 == 0 and len(grid_counts) > 0:
            print(f"    Processed {len(grid_counts)} grid cells...")
        
        # Create a copy to avoid SettingWithCopyWarning
        group = group.copy()
        
        # For each fire, count how many fires in past 7 days
        counts = []
        times = group['datetime'].values
        
        for i, current_time in enumerate(times):
            # Use numpy for fast comparison
            seven_days_ago = current_time - pd.Timedelta(days=7)
            # Count fires between 7 days ago and current time (excluding current)
            count = ((times < current_time) & (times >= seven_days_ago)).sum()
            counts.append(count)
        
        group['fires_last_7days'] = counts
        grid_counts.append(group)
    
    # Combine all groups back together
    df = pd.concat(grid_counts, ignore_index=True)
    
    print("  ✅ Fire history features created")
    return df

def create_environmental_features(df):
    """Process environmental features from NASA FIRMS data."""
    
    print("Creating environmental features...")
    
    # FRP (Fire Radiative Power) is key indicator
    df['frp'] = df['frp'].fillna(0)
    df['frp_log'] = np.log1p(df['frp'])  # Log transform for better distribution
    
    # Brightness features (VIIRS uses bright_ti4 and bright_ti5)
    # Map to expected column names for consistency
    if 'bright_ti4' in df.columns:
        df['brightness'] = df['bright_ti4'].fillna(df['bright_ti4'].mean())
    elif 'brightness' in df.columns:
        df['brightness'] = df['brightness'].fillna(df['brightness'].mean())
    else:
        df['brightness'] = 300.0  # Default value
    
    if 'bright_ti5' in df.columns:
        df['bright_t31'] = df['bright_ti5'].fillna(df['bright_ti5'].mean())
    elif 'bright_t31' in df.columns:
        df['bright_t31'] = df['bright_t31'].fillna(df['bright_t31'].mean())
    else:
        df['bright_t31'] = 280.0  # Default value
    
    # Confidence encoding (l=0, n=1, h=2)
    confidence_map = {'l': 0, 'n': 1, 'h': 2}
    df['confidence_encoded'] = df['confidence'].map(confidence_map).fillna(1)
    
    # Scan and track features
    df['scan'] = df['scan'].fillna(df['scan'].mean())
    df['track'] = df['track'].fillna(df['track'].mean())
    
    # Day/Night indicator (D=1, N=0)
    df['is_daytime'] = (df['daynight'] == 'D').astype(int)
    
    print("  ✅ Environmental features created")
    return df

def create_target_variable(df):
    """Create target variable: will fire occur in next 24 hours in same grid cell?"""
    
    print("Creating target variable (this may take a few minutes)...")
    
    # Sort by grid cell and datetime
    df = df.sort_values(['grid_cell', 'datetime']).reset_index(drop=True)
    
    # Initialize target column
    df['fire_next_24h'] = 0
    
    print("  Processing target variable...")
    
    # Process each grid cell
    grid_results = []
    
    for grid_cell, group in df.groupby('grid_cell'):
        if len(grid_results) % 1000 == 0 and len(grid_results) > 0:
            print(f"    Processed {len(grid_results)} grid cells...")
        
        # Create a copy
        group = group.copy()
        
        # For each fire, check if fire occurs in next 24 hours
        targets = []
        times = group['datetime'].values
        
        for i, current_time in enumerate(times):
            # Use numpy for fast comparison
            twenty_four_hours_later = current_time + pd.Timedelta(hours=24)
            # Check if any fire occurs between now and 24 hours later (excluding current)
            has_future_fire = ((times > current_time) & (times <= twenty_four_hours_later)).any()
            targets.append(1 if has_future_fire else 0)
        
        group['fire_next_24h'] = targets
        grid_results.append(group)
    
    # Combine all groups
    df = pd.concat(grid_results, ignore_index=True)
    
    positive_ratio = df['fire_next_24h'].mean()
    print(f"  ✅ Target variable created (positive rate: {positive_ratio:.2%})")
    
    return df

def select_final_features(df):
    """Select and order final features for ML model."""
    
    print("Selecting final features...")
    
    # Feature columns to keep
    feature_cols = [
        # Temporal
        'day_of_week', 'day_of_month', 'month', 'hour', 'is_weekend', 'season',
        
        # Spatial
        'latitude', 'longitude', 'abs_latitude', 'lat_grid', 'lon_grid',
        
        # Fire history
        'fires_last_7days',
        
        # Environmental
        'frp', 'frp_log', 'brightness', 'bright_t31', 
        'confidence_encoded', 'scan', 'track', 'is_daytime',
        
        # Target
        'fire_next_24h'
    ]
    
    # Keep only selected columns
    df_final = df[feature_cols].copy()
    
    # Remove rows with missing values
    df_final = df_final.dropna()
    
    print(f"  ✅ Final dataset: {len(df_final)} samples, {len(feature_cols)-1} features")
    
    return df_final

def process_and_save():
    """Main processing pipeline."""
    
    print("=" * 60)
    print("Fire Prediction Feature Engineering")
    print("=" * 60)
    
    # Load data
    df = load_all_fire_data()
    if df is None:
        return
    
    print(f"\nRaw data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Feature engineering pipeline
    df = create_temporal_features(df)
    df = create_spatial_features(df)
    df = create_environmental_features(df)
    df = create_fire_history_features(df)
    df = create_target_variable(df)
    df_final = select_final_features(df)
    
    # Save processed data
    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    df_final.to_csv(OUTPUT_FILE, index=False)
    
    print("\n" + "=" * 60)
    print(f"✅ Processing complete!")
    print(f"📁 Output saved to: {OUTPUT_FILE}")
    print(f"📊 Dataset size: {len(df_final)} samples")
    print(f"📈 Features: {len(df_final.columns) - 1}")
    print(f"🎯 Target distribution:")
    print(f"   - No fire (0): {(df_final['fire_next_24h'] == 0).sum()} ({(df_final['fire_next_24h'] == 0).mean():.2%})")
    print(f"   - Fire (1): {(df_final['fire_next_24h'] == 1).sum()} ({(df_final['fire_next_24h'] == 1).mean():.2%})")
    print("=" * 60)

if __name__ == "__main__":
    process_and_save()
