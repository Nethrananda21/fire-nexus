"""
Make predictions using trained fire prediction model.

Load a trained model and make predictions on new fire detection data.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import joblib
from datetime import datetime

# Paths
MODELS_DIR = Path(__file__).parent / "models"

def load_latest_model(model_type="random_forest"):
    """Load the most recently trained model."""
    
    print(f"Loading latest {model_type} model...")
    
    # Find all model files of specified type
    model_files = list(MODELS_DIR.glob(f"{model_type}_*.joblib"))
    
    if not model_files:
        print(f"❌ No {model_type} models found in {MODELS_DIR}")
        return None
    
    # Get most recent model
    latest_model = max(model_files, key=lambda p: p.stat().st_mtime)
    
    # Load model
    model = joblib.load(latest_model)
    
    print(f"✅ Loaded model: {latest_model.name}")
    
    return model

def prepare_prediction_data(fire_data):
    """
    Prepare fire detection data for prediction.
    
    Args:
        fire_data: Dictionary with fire detection attributes
    
    Returns:
        DataFrame with required features
    """
    
    # Create datetime
    dt = datetime.strptime(
        f"{fire_data['acq_date']} {str(fire_data['acq_time']).zfill(4)}",
        "%Y-%m-%d %H%M"
    )
    
    # Extract features
    features = {
        # Temporal
        'day_of_week': dt.weekday(),
        'day_of_month': dt.day,
        'month': dt.month,
        'hour': dt.hour,
        'is_weekend': 1 if dt.weekday() >= 5 else 0,
        'season': 0 if dt.month in [12,1,2] else 1 if dt.month in [3,4,5] else 2 if dt.month in [6,7,8] else 3,
        
        # Spatial
        'latitude': fire_data['latitude'],
        'longitude': fire_data['longitude'],
        'abs_latitude': abs(fire_data['latitude']),
        'lat_grid': (fire_data['latitude'] // 5) * 5,
        'lon_grid': (fire_data['longitude'] // 5) * 5,
        
        # Fire history (default to 0 if not provided)
        'fires_last_7days': fire_data.get('fires_last_7days', 0),
        
        # Environmental
        'frp': fire_data.get('frp', 0),
        'frp_log': np.log1p(fire_data.get('frp', 0)),
        'brightness': fire_data.get('brightness', 300),
        'bright_t31': fire_data.get('bright_t31', 280),
        'confidence_encoded': {'l': 0, 'n': 1, 'h': 2}.get(fire_data.get('confidence', 'n'), 1),
        'scan': fire_data.get('scan', 1.0),
        'track': fire_data.get('track', 1.0),
        'is_daytime': 1 if fire_data.get('daynight', 'D') == 'D' else 0,
    }
    
    return pd.DataFrame([features])

def predict_fire_risk(model, fire_data):
    """
    Predict fire risk for a given location.
    
    Args:
        model: Trained ML model
        fire_data: Dictionary with fire detection attributes
    
    Returns:
        Prediction result dictionary
    """
    
    # Prepare features
    X = prepare_prediction_data(fire_data)
    
    # Make prediction
    prediction = model.predict(X)[0]
    probability = model.predict_proba(X)[0, 1]
    
    # Determine risk level
    if probability >= 0.7:
        risk_level = "HIGH"
    elif probability >= 0.4:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"
    
    return {
        'prediction': int(prediction),
        'probability': float(probability),
        'risk_level': risk_level,
        'location': f"({fire_data['latitude']}, {fire_data['longitude']})",
        'timestamp': f"{fire_data['acq_date']} {fire_data['acq_time']}"
    }

def example_prediction():
    """Example usage of the prediction system."""
    
    print("=" * 60)
    print("🔥 Fire Prediction Example")
    print("=" * 60)
    
    # Load model
    model = load_latest_model("random_forest")
    if model is None:
        print("\n⚠️  No trained model found. Please run train_model.py first.")
        return
    
    # Example fire detection data
    example_fires = [
        {
            'latitude': 34.0522,
            'longitude': -118.2437,
            'acq_date': '2025-01-15',
            'acq_time': 1430,
            'frp': 85.5,
            'brightness': 340.0,
            'bright_t31': 305.0,
            'confidence': 'h',
            'daynight': 'D',
            'scan': 1.2,
            'track': 1.1,
            'fires_last_7days': 5
        },
        {
            'latitude': 40.7128,
            'longitude': -74.0060,
            'acq_date': '2025-01-15',
            'acq_time': 2130,
            'frp': 15.3,
            'brightness': 310.0,
            'bright_t31': 290.0,
            'confidence': 'l',
            'daynight': 'N',
            'scan': 0.9,
            'track': 1.0,
            'fires_last_7days': 0
        },
        {
            'latitude': -23.5505,
            'longitude': -46.6333,
            'acq_date': '2025-01-15',
            'acq_time': 1600,
            'frp': 120.0,
            'brightness': 360.0,
            'bright_t31': 320.0,
            'confidence': 'h',
            'daynight': 'D',
            'scan': 1.5,
            'track': 1.3,
            'fires_last_7days': 12
        }
    ]
    
    print("\n📍 Making predictions for 3 locations...\n")
    
    for i, fire in enumerate(example_fires, 1):
        result = predict_fire_risk(model, fire)
        
        print(f"🔥 Fire Detection #{i}")
        print(f"   Location: {result['location']}")
        print(f"   Timestamp: {result['timestamp']}")
        print(f"   Prediction: {'Fire will continue' if result['prediction'] == 1 else 'Fire will not continue'}")
        print(f"   Probability: {result['probability']:.2%}")
        print(f"   Risk Level: {result['risk_level']}")
        print()
    
    print("=" * 60)

if __name__ == "__main__":
    example_prediction()
