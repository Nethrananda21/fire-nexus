"""
Wildfire Prediction Module
Makes predictions for high-risk grid cells
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import joblib

# Configuration
MODELS_DIR = Path(__file__).parent / 'models'
GRID_SIZE = 0.5

# Feature columns (must match training)
FEATURE_COLS = [
    'grid_lat', 'grid_lon',
    'fires_last_7_days', 'fires_last_14_days',
    'avg_frp_7_days', 'neighbor_fires_3_days',
    'month', 'day_of_year', 'day_of_week',
    'season', 'abs_latitude'
]


def load_model():
    """Load the latest trained model"""
    model_files = list(MODELS_DIR.glob('xgboost_wildfire_*.joblib'))
    
    if not model_files:
        raise FileNotFoundError("No trained model found. Run train_model.py first!")
    
    # Get most recent model
    latest_model = max(model_files, key=lambda p: p.stat().st_mtime)
    print(f"📂 Loading model: {latest_model.name}")
    
    return joblib.load(latest_model)


def create_grid_cell(lat: float, lon: float) -> tuple:
    """Convert lat/lon to grid cell"""
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


def prepare_prediction_features(fire_history: dict, current_fires: list) -> pd.DataFrame:
    """
    Prepare features for prediction based on current fire data
    
    fire_history: dict of {date: {grid_cell: [fire_data]}}
    current_fires: list of current fire locations [{lat, lon, frp}, ...]
    """
    # Get all grid cells with recent activity
    active_grids = set()
    
    # Add grids from recent fires
    for fires in fire_history.values():
        active_grids.update(fires.keys())
    
    # Add grids from current fires
    for fire in current_fires:
        grid = create_grid_cell(fire['latitude'], fire['longitude'])
        active_grids.add(grid)
    
    # Also add neighbor grids
    all_grids = set(active_grids)
    for grid in active_grids:
        all_grids.update(get_neighbor_cells(grid))
    
    # Current date info
    now = datetime.utcnow()
    month = now.month
    day_of_year = now.timetuple().tm_yday
    day_of_week = now.weekday()
    
    samples = []
    for grid_cell in all_grids:
        grid_lat, grid_lon = grid_cell
        
        # Count fires in various time windows
        fires_last_7_days = 0
        fires_last_14_days = 0
        total_frp = 0
        
        dates_sorted = sorted(fire_history.keys(), reverse=True)
        for i, date in enumerate(dates_sorted[:14]):
            fires_in_cell = fire_history[date].get(grid_cell, [])
            count = len(fires_in_cell)
            
            if i < 7:
                fires_last_7_days += count
                total_frp += sum(f.get('frp', 0) for f in fires_in_cell)
            
            fires_last_14_days += count
        
        # Count neighbor fires (last 3 days)
        neighbor_fires = 0
        for neighbor in get_neighbor_cells(grid_cell):
            for date in dates_sorted[:3]:
                neighbor_fires += len(fire_history[date].get(neighbor, []))
        
        # Calculate features
        avg_frp = total_frp / fires_last_7_days if fires_last_7_days > 0 else 0
        
        # Season
        if month in [12, 1, 2]:
            season = 0
        elif month in [3, 4, 5]:
            season = 1
        elif month in [6, 7, 8]:
            season = 2
        else:
            season = 3
        
        # Hemisphere adjustment
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
            'abs_latitude': abs(grid_lat)
        })
    
    return pd.DataFrame(samples)


def predict_high_risk_zones(model, features_df: pd.DataFrame, 
                            current_fire_grids: set = None,
                            min_probability: float = 0.4) -> list:
    """
    Predict high-risk zones and return sorted by probability
    
    Returns list of dicts with grid info and probability
    """
    if len(features_df) == 0:
        return []
    
    # Make predictions
    X = features_df[FEATURE_COLS]
    probabilities = model.predict_proba(X)[:, 1]
    
    # Add probabilities to dataframe
    features_df = features_df.copy()
    features_df['probability'] = probabilities
    
    # Filter by minimum probability
    high_risk = features_df[features_df['probability'] >= min_probability].copy()
    
    # Exclude cells with current fires if provided
    if current_fire_grids:
        high_risk = high_risk[
            ~high_risk.apply(
                lambda r: (r['grid_lat'], r['grid_lon']) in current_fire_grids, 
                axis=1
            )
        ]
    
    # Classify risk levels
    def get_risk_level(prob):
        if prob >= 0.7:
            return 'HIGH'
        elif prob >= 0.5:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    high_risk['risk_level'] = high_risk['probability'].apply(get_risk_level)
    
    # Sort by probability descending
    high_risk = high_risk.sort_values('probability', ascending=False)
    
    # Convert to list of dicts
    results = []
    for _, row in high_risk.iterrows():
        results.append({
            'grid_lat': float(row['grid_lat']),
            'grid_lon': float(row['grid_lon']),
            'center_lat': float(row['grid_lat'] + GRID_SIZE / 2),
            'center_lon': float(row['grid_lon'] + GRID_SIZE / 2),
            'probability': float(row['probability']),
            'risk_level': row['risk_level'],
            'fires_last_7_days': int(row['fires_last_7_days']),
            'neighbor_fires': int(row['neighbor_fires_3_days'])
        })
    
    return results


# For testing
if __name__ == "__main__":
    print("=" * 60)
    print("🔥 Wildfire Prediction Test")
    print("=" * 60)
    
    # Load model
    model = load_model()
    
    # Create sample fire history (simulated)
    from collections import defaultdict
    fire_history = defaultdict(lambda: defaultdict(list))
    
    # Add some sample fires
    sample_fires = [
        {'latitude': 34.05, 'longitude': -118.25, 'frp': 50},
        {'latitude': 34.10, 'longitude': -118.20, 'frp': 75},
        {'latitude': 36.75, 'longitude': -119.75, 'frp': 100},
    ]
    
    today = datetime.utcnow().date()
    for i in range(7):
        date = today - timedelta(days=i)
        for fire in sample_fires:
            grid = create_grid_cell(fire['latitude'], fire['longitude'])
            fire_history[date][grid].append(fire)
    
    # Prepare features
    features = prepare_prediction_features(fire_history, sample_fires)
    print(f"\n📊 Prepared {len(features)} grid cells for prediction")
    
    # Make predictions
    predictions = predict_high_risk_zones(model, features, min_probability=0.3)
    
    print(f"\n🎯 Found {len(predictions)} high-risk zones:\n")
    for i, pred in enumerate(predictions[:10]):
        print(f"  {i+1}. ({pred['center_lat']:.2f}, {pred['center_lon']:.2f})")
        print(f"     Probability: {pred['probability']:.1%}")
        print(f"     Risk Level: {pred['risk_level']}")
        print()
