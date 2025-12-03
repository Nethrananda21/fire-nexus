"""
Train XGBoost Model for Wildfire Prediction
Predicts probability of fire occurring in a grid cell in next 24 hours
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import joblib
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, 
    f1_score, roc_auc_score, classification_report,
    confusion_matrix
)
import xgboost as xgb

# Configuration
DATA_DIR = Path(__file__).parent / 'data'
MODELS_DIR = Path(__file__).parent / 'models'
FEATURES_FILE = DATA_DIR / 'processed_features.csv'

# Feature columns
FEATURE_COLS = [
    'grid_lat', 'grid_lon',
    'fires_last_7_days', 'fires_last_14_days',
    'avg_frp_7_days', 'neighbor_fires_3_days',
    'month', 'day_of_year', 'day_of_week',
    'season', 'abs_latitude'
]

TARGET_COL = 'target'


def load_data() -> tuple:
    """Load and prepare training data"""
    print("📂 Loading processed features...")
    
    df = pd.read_csv(FEATURES_FILE)
    print(f"   Total samples: {len(df):,}")
    
    X = df[FEATURE_COLS]
    y = df[TARGET_COL]
    
    # Check class balance
    pos_rate = y.mean()
    print(f"   Positive rate: {pos_rate:.2%}")
    
    return X, y


def train_model(X: pd.DataFrame, y: pd.Series) -> xgb.XGBClassifier:
    """Train XGBoost classifier"""
    print("\n🎯 Training XGBoost model...")
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    print(f"   Train set: {len(X_train):,} samples")
    print(f"   Test set: {len(X_test):,} samples")
    
    # Calculate scale_pos_weight for imbalanced data
    neg_count = (y_train == 0).sum()
    pos_count = (y_train == 1).sum()
    scale_pos_weight = neg_count / pos_count if pos_count > 0 else 1
    
    print(f"   Scale pos weight: {scale_pos_weight:.2f}")
    
    # Initialize XGBoost with GPU support if available
    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=8,
        learning_rate=0.1,
        scale_pos_weight=scale_pos_weight,
        tree_method='hist',  # Use 'gpu_hist' if GPU available
        random_state=42,
        n_jobs=-1,
        eval_metric='auc'
    )
    
    # Train with early stopping
    print("\n   Training...")
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False
    )
    
    # Evaluate
    print("\n📊 Model Evaluation:")
    
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    roc_auc = roc_auc_score(y_test, y_pred_proba)
    
    print(f"   Accuracy:  {accuracy:.4f}")
    print(f"   Precision: {precision:.4f}")
    print(f"   Recall:    {recall:.4f}")
    print(f"   F1 Score:  {f1:.4f}")
    print(f"   ROC-AUC:   {roc_auc:.4f}")
    
    # Confusion Matrix
    print("\n   Confusion Matrix:")
    cm = confusion_matrix(y_test, y_pred)
    print(f"   TN: {cm[0,0]:,}  FP: {cm[0,1]:,}")
    print(f"   FN: {cm[1,0]:,}  TP: {cm[1,1]:,}")
    
    # Feature importance
    print("\n   Feature Importance:")
    importance = dict(zip(FEATURE_COLS, model.feature_importances_))
    sorted_importance = sorted(importance.items(), key=lambda x: x[1], reverse=True)
    for feat, imp in sorted_importance[:5]:
        print(f"      {feat}: {imp:.4f}")
    
    return model, {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'roc_auc': roc_auc
    }


def save_model(model: xgb.XGBClassifier, metrics: dict):
    """Save trained model and metadata"""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    model_path = MODELS_DIR / f'xgboost_wildfire_{timestamp}.joblib'
    
    print(f"\n💾 Saving model to {model_path}...")
    joblib.dump(model, model_path)
    
    # Save metadata
    metadata_path = MODELS_DIR / f'xgboost_wildfire_{timestamp}_metadata.txt'
    with open(metadata_path, 'w') as f:
        f.write(f"Model: XGBoost Wildfire Predictor\n")
        f.write(f"Trained: {datetime.now().isoformat()}\n")
        f.write(f"Features: {', '.join(FEATURE_COLS)}\n")
        f.write(f"\nMetrics:\n")
        for metric, value in metrics.items():
            f.write(f"  {metric}: {value:.4f}\n")
    
    print(f"   Metadata saved to {metadata_path}")
    
    return model_path


def main():
    """Main entry point"""
    print("=" * 60)
    print("🔥 XGBoost Wildfire Prediction Model Training")
    print("=" * 60)
    
    # Check if features file exists
    if not FEATURES_FILE.exists():
        print(f"❌ Features file not found: {FEATURES_FILE}")
        print("   Run feature_engineering.py first!")
        return
    
    # Load data
    X, y = load_data()
    
    # Train model
    model, metrics = train_model(X, y)
    
    # Save model
    model_path = save_model(model, metrics)
    
    print("\n" + "=" * 60)
    print("✅ Training complete!")
    print(f"   Model: {model_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
