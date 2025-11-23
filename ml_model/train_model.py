"""
Train Machine Learning Model for Fire Prediction.

Trains Random Forest and XGBoost models to predict fire occurrence
in the next 24 hours based on historical NASA FIRMS data.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, 
    f1_score, roc_auc_score, classification_report, confusion_matrix
)
import joblib
from datetime import datetime

# Try to import XGBoost (optional)
try:
    import xgboost as xgb
    XGBOOST_AVAILABLE = True
except ImportError:
    XGBOOST_AVAILABLE = False
    print("⚠️  XGBoost not installed. Only Random Forest will be trained.")
    print("   Install with: pip install xgboost")

# Paths
DATA_FILE = Path(__file__).parent / "data" / "processed_features.csv"
MODELS_DIR = Path(__file__).parent / "models"
MODELS_DIR.mkdir(exist_ok=True)

def load_processed_data():
    """Load the processed feature dataset."""
    
    print("Loading processed data...")
    
    if not DATA_FILE.exists():
        print(f"❌ ERROR: {DATA_FILE} not found!")
        print("   Please run feature_engineering.py first.")
        return None, None
    
    df = pd.read_csv(DATA_FILE)
    print(f"✅ Loaded {len(df)} samples")
    
    # Separate features and target
    X = df.drop('fire_next_24h', axis=1)
    y = df['fire_next_24h']
    
    print(f"   Features: {X.shape[1]}")
    print(f"   Target distribution: {y.value_counts().to_dict()}")
    
    return X, y

def split_data(X, y, test_size=0.2, random_state=42):
    """Split data into training and testing sets."""
    
    print(f"\nSplitting data (test_size={test_size})...")
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )
    
    print(f"✅ Train set: {len(X_train)} samples")
    print(f"✅ Test set: {len(X_test)} samples")
    
    return X_train, X_test, y_train, y_test

def train_random_forest(X_train, y_train):
    """Train Random Forest classifier."""
    
    print("\n" + "=" * 60)
    print("Training Random Forest Classifier...")
    print("=" * 60)
    
    # Initialize model with balanced class weights
    rf_model = RandomForestClassifier(
        n_estimators=100,
        max_depth=20,
        min_samples_split=10,
        min_samples_leaf=5,
        class_weight='balanced',
        random_state=42,
        n_jobs=-1,
        verbose=1
    )
    
    # Train model
    rf_model.fit(X_train, y_train)
    
    print("✅ Random Forest training complete")
    
    return rf_model

def train_xgboost(X_train, y_train):
    """Train XGBoost classifier (if available)."""
    
    if not XGBOOST_AVAILABLE:
        return None
    
    print("\n" + "=" * 60)
    print("Training XGBoost Classifier...")
    print("=" * 60)
    
    # Calculate scale_pos_weight for imbalanced data
    negative_count = (y_train == 0).sum()
    positive_count = (y_train == 1).sum()
    scale_pos_weight = negative_count / positive_count
    
    # Initialize model
    xgb_model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=8,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        random_state=42,
        n_jobs=-1,
        verbosity=1
    )
    
    # Train model
    xgb_model.fit(X_train, y_train)
    
    print("✅ XGBoost training complete")
    
    return xgb_model

def evaluate_model(model, X_test, y_test, model_name):
    """Evaluate model performance."""
    
    print("\n" + "=" * 60)
    print(f"{model_name} - Evaluation Results")
    print("=" * 60)
    
    # Make predictions
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    roc_auc = roc_auc_score(y_test, y_pred_proba)
    
    print(f"\n📊 Performance Metrics:")
    print(f"   Accuracy:  {accuracy:.4f}")
    print(f"   Precision: {precision:.4f}")
    print(f"   Recall:    {recall:.4f}")
    print(f"   F1-Score:  {f1:.4f}")
    print(f"   ROC-AUC:   {roc_auc:.4f}")
    
    print(f"\n📋 Classification Report:")
    print(classification_report(y_test, y_pred, target_names=['No Fire', 'Fire']))
    
    print(f"\n🔢 Confusion Matrix:")
    cm = confusion_matrix(y_test, y_pred)
    print(f"   True Negatives:  {cm[0,0]}")
    print(f"   False Positives: {cm[0,1]}")
    print(f"   False Negatives: {cm[1,0]}")
    print(f"   True Positives:  {cm[1,1]}")
    
    # Feature importance (if available)
    if hasattr(model, 'feature_importances_'):
        print(f"\n⭐ Top 10 Important Features:")
        feature_names = X_test.columns
        importances = model.feature_importances_
        
        # Sort by importance
        indices = np.argsort(importances)[::-1][:10]
        
        for i, idx in enumerate(indices, 1):
            print(f"   {i}. {feature_names[idx]}: {importances[idx]:.4f}")
    
    return {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'roc_auc': roc_auc
    }

def save_model(model, model_name, metrics):
    """Save trained model to disk."""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_filename = f"{model_name}_{timestamp}.joblib"
    model_path = MODELS_DIR / model_filename
    
    # Save model
    joblib.dump(model, model_path)
    
    # Save metadata
    metadata = {
        'model_name': model_name,
        'timestamp': timestamp,
        'metrics': metrics,
        'model_file': model_filename
    }
    
    metadata_path = MODELS_DIR / f"{model_name}_{timestamp}_metadata.txt"
    with open(metadata_path, 'w') as f:
        for key, value in metadata.items():
            f.write(f"{key}: {value}\n")
    
    print(f"\n💾 Model saved to: {model_path}")
    print(f"📄 Metadata saved to: {metadata_path}")

def main():
    """Main training pipeline."""
    
    print("=" * 60)
    print("🔥 Fire Prediction Model Training")
    print("=" * 60)
    
    # Load data
    X, y = load_processed_data()
    if X is None:
        return
    
    # Split data
    X_train, X_test, y_train, y_test = split_data(X, y)
    
    # Train Random Forest
    rf_model = train_random_forest(X_train, y_train)
    rf_metrics = evaluate_model(rf_model, X_test, y_test, "Random Forest")
    save_model(rf_model, "random_forest", rf_metrics)
    
    # Train XGBoost (if available)
    if XGBOOST_AVAILABLE:
        xgb_model = train_xgboost(X_train, y_train)
        xgb_metrics = evaluate_model(xgb_model, X_test, y_test, "XGBoost")
        save_model(xgb_model, "xgboost", xgb_metrics)
    
    print("\n" + "=" * 60)
    print("✅ Training Complete!")
    print("=" * 60)
    
    # Compare models
    print("\n📊 Model Comparison:")
    print(f"\nRandom Forest:")
    for metric, value in rf_metrics.items():
        print(f"   {metric}: {value:.4f}")
    
    if XGBOOST_AVAILABLE:
        print(f"\nXGBoost:")
        for metric, value in xgb_metrics.items():
            print(f"   {metric}: {value:.4f}")
        
        # Determine best model
        if xgb_metrics['f1'] > rf_metrics['f1']:
            print(f"\n🏆 Best Model: XGBoost (F1: {xgb_metrics['f1']:.4f})")
        else:
            print(f"\n🏆 Best Model: Random Forest (F1: {rf_metrics['f1']:.4f})")

if __name__ == "__main__":
    main()
