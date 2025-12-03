"""
Wildfire Prediction Verification Script
========================================
This script validates predictions by backtesting against historical data.

How it works:
1. Uses data from days 1-N to make predictions
2. Checks if fires actually occurred in predicted zones on day N+1
3. Calculates accuracy metrics
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from glob import glob
import joblib

# Configuration
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
MODELS_DIR = os.path.join(os.path.dirname(__file__), 'models')
GRID_SIZE = 0.5  # degrees

def load_fires_for_date(date_str):
    """Load fire data for a specific date."""
    file_path = os.path.join(DATA_DIR, f'fires_{date_str}.csv')
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        return df
    return None

def get_available_dates():
    """Get all available dates from data files."""
    files = glob(os.path.join(DATA_DIR, 'fires_*.csv'))
    dates = []
    for f in files:
        basename = os.path.basename(f)
        date_str = basename.replace('fires_', '').replace('.csv', '')
        try:
            dates.append(datetime.strptime(date_str, '%Y-%m-%d'))
        except:
            pass
    return sorted(dates)

def get_grid_cell(lat, lon):
    """Convert lat/lon to grid cell."""
    grid_lat = round(lat / GRID_SIZE) * GRID_SIZE
    grid_lon = round(lon / GRID_SIZE) * GRID_SIZE
    return (grid_lat, grid_lon)

def calculate_zone_features(fires_df, days_back=7):
    """Calculate features for prediction zones based on recent fires."""
    zones = {}
    
    for _, fire in fires_df.iterrows():
        cell = get_grid_cell(fire['latitude'], fire['longitude'])
        if cell not in zones:
            zones[cell] = {
                'fires': 0,
                'total_frp': 0,
                'high_conf': 0
            }
        zones[cell]['fires'] += 1
        zones[cell]['total_frp'] += fire.get('frp', 0) or 0
        if fire.get('confidence', '') == 'h':
            zones[cell]['high_conf'] += 1
    
    return zones

def make_heuristic_predictions(recent_fires_df, threshold=40):
    """Make predictions using heuristic method."""
    zones = calculate_zone_features(recent_fires_df)
    
    predictions = []
    for (lat, lon), data in zones.items():
        # Simple heuristic: score based on fire count and FRP
        avg_frp = data['total_frp'] / max(data['fires'], 1)
        score = min(100, 30 + (data['fires'] * 0.5) + (avg_frp * 0.3) + (data['high_conf'] * 2))
        
        if score >= threshold:
            predictions.append({
                'lat': lat,
                'lon': lon,
                'score': score,
                'fires_nearby': data['fires']
            })
    
    return predictions

def check_predictions_against_actual(predictions, actual_fires_df):
    """Check how many predictions matched actual fires."""
    if actual_fires_df is None or len(actual_fires_df) == 0:
        return None
    
    # Get actual fire grid cells
    actual_cells = set()
    for _, fire in actual_fires_df.iterrows():
        cell = get_grid_cell(fire['latitude'], fire['longitude'])
        actual_cells.add(cell)
    
    # Check predictions
    true_positives = 0
    false_positives = 0
    predicted_cells = set()
    
    for pred in predictions:
        cell = (pred['lat'], pred['lon'])
        predicted_cells.add(cell)
        
        # Check if fire occurred in this cell or adjacent cells
        hit = False
        for dlat in [-GRID_SIZE, 0, GRID_SIZE]:
            for dlon in [-GRID_SIZE, 0, GRID_SIZE]:
                check_cell = (cell[0] + dlat, cell[1] + dlon)
                if check_cell in actual_cells:
                    hit = True
                    break
            if hit:
                break
        
        if hit:
            true_positives += 1
        else:
            false_positives += 1
    
    # False negatives: actual fires not predicted
    false_negatives = len(actual_cells - predicted_cells)
    
    return {
        'true_positives': true_positives,
        'false_positives': false_positives,
        'false_negatives': false_negatives,
        'total_predictions': len(predictions),
        'total_actual_fires': len(actual_cells),
        'precision': true_positives / max(len(predictions), 1),
        'recall': true_positives / max(len(actual_cells), 1)
    }

def run_backtest():
    """Run backtesting on historical data."""
    print("=" * 60)
    print("WILDFIRE PREDICTION VERIFICATION")
    print("=" * 60)
    
    dates = get_available_dates()
    print(f"\nFound {len(dates)} days of data")
    print(f"Date range: {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}")
    
    if len(dates) < 8:
        print("\nNeed at least 8 days of data for backtesting")
        return
    
    results = []
    
    # Test on the last 7 days (use previous 7 days to predict each)
    test_dates = dates[-7:]
    
    print(f"\nBacktesting on {len(test_dates)} days...")
    print("-" * 60)
    
    for test_date in test_dates:
        # Get training data (7 days before test date)
        train_end = test_date - timedelta(days=1)
        train_start = train_end - timedelta(days=6)
        
        # Collect training fires
        train_fires = []
        current = train_start
        while current <= train_end:
            df = load_fires_for_date(current.strftime('%Y-%m-%d'))
            if df is not None:
                train_fires.append(df)
            current += timedelta(days=1)
        
        if not train_fires:
            continue
            
        train_df = pd.concat(train_fires, ignore_index=True)
        
        # Make predictions
        predictions = make_heuristic_predictions(train_df, threshold=40)
        
        # Load actual fires for test date
        actual_df = load_fires_for_date(test_date.strftime('%Y-%m-%d'))
        
        # Check predictions
        metrics = check_predictions_against_actual(predictions, actual_df)
        
        if metrics:
            results.append({
                'date': test_date.strftime('%Y-%m-%d'),
                **metrics
            })
            
            print(f"\n{test_date.strftime('%Y-%m-%d')}:")
            print(f"  Predictions made: {metrics['total_predictions']}")
            print(f"  Actual fire zones: {metrics['total_actual_fires']}")
            print(f"  True Positives (hits): {metrics['true_positives']}")
            print(f"  Precision: {metrics['precision']:.1%}")
            print(f"  Recall: {metrics['recall']:.1%}")
    
    # Summary
    if results:
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        
        avg_precision = np.mean([r['precision'] for r in results])
        avg_recall = np.mean([r['recall'] for r in results])
        total_hits = sum(r['true_positives'] for r in results)
        total_preds = sum(r['total_predictions'] for r in results)
        
        print(f"\nAverage Precision: {avg_precision:.1%}")
        print(f"  (Of zones we predicted, {avg_precision:.1%} had fires)")
        
        print(f"\nAverage Recall: {avg_recall:.1%}")
        print(f"  (Of actual fire zones, we predicted {avg_recall:.1%})")
        
        print(f"\nTotal Hits: {total_hits} / {total_preds} predictions")
        
        # F1 Score
        if avg_precision + avg_recall > 0:
            f1 = 2 * (avg_precision * avg_recall) / (avg_precision + avg_recall)
            print(f"F1 Score: {f1:.3f}")
        
        print("\n" + "=" * 60)
        print("INTERPRETATION")
        print("=" * 60)
        print("""
- Precision > 50%: Good - most predictions are valid
- Recall > 30%: Acceptable - catching significant fire zones
- High precision + low recall: Conservative (fewer false alarms)
- Low precision + high recall: Aggressive (catches more but more noise)

Note: Wildfire prediction is inherently uncertain. A 50%+ precision
with 30%+ recall is considered useful for early warning systems.
        """)

def check_model_metrics():
    """Display the trained model's metrics."""
    print("\n" + "=" * 60)
    print("TRAINED MODEL METRICS")
    print("=" * 60)
    
    # Find model metadata
    meta_files = glob(os.path.join(MODELS_DIR, '*_metadata.txt'))
    
    if not meta_files:
        print("No model metadata found.")
        return
    
    latest = sorted(meta_files)[-1]
    print(f"\nModel: {os.path.basename(latest).replace('_metadata.txt', '')}")
    print("-" * 40)
    
    with open(latest, 'r') as f:
        print(f.read())

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--metrics':
        check_model_metrics()
    else:
        run_backtest()
        print("\n")
        check_model_metrics()
