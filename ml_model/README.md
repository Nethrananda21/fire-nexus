# Fire Prediction ML Model

Standalone machine learning system for predicting wildfire continuation using 30 days of historical NASA FIRMS data.

## 🎯 Purpose

Train and evaluate ML models to predict whether a detected fire will continue burning in the next 24 hours based on:
- Historical fire patterns
- Environmental conditions (FRP, brightness, confidence)
- Temporal features (time of day, season, day of week)
- Spatial features (location, past fire activity)

## 📂 Directory Structure

```
ml_model/
├── data/               # Downloaded NASA FIRMS datasets
├── models/             # Trained model files (.joblib)
├── notebooks/          # Jupyter notebooks (optional)
├── fetch_historical_data.py
├── feature_engineering.py
├── train_model.py
├── predict.py
└── README.md
```

## 🚀 Quick Start

### Step 1: Fetch Historical Data

Download 30 days of NASA FIRMS fire detection data:

```powershell
cd ml_model
python fetch_historical_data.py
```

**Output**: 30 CSV files in `data/` directory (e.g., `fires_2025-01-15.csv`)

**Note**: Requires `NASA_FIRMS_API_KEY` in `.env` file (uses same key as main project)

---

### Step 2: Feature Engineering

Process raw data and create ML features:

```powershell
python feature_engineering.py
```

**Output**: `data/processed_features.csv` with engineered features

**Features created**:
- **Temporal**: day_of_week, month, hour, season, is_weekend
- **Spatial**: latitude, longitude, grid cells, distance from equator
- **Fire History**: fires_last_7days (count in same region)
- **Environmental**: FRP, brightness, confidence, scan/track, day/night
- **Target**: fire_next_24h (binary: will fire continue?)

---

### Step 3: Train Models

Train Random Forest and XGBoost models:

```powershell
python train_model.py
```

**Output**: Trained models saved to `models/` directory

**Models trained**:
- Random Forest (always available)
- XGBoost (if installed: `pip install xgboost`)

**Evaluation metrics**:
- Accuracy, Precision, Recall, F1-Score
- ROC-AUC score
- Confusion matrix
- Feature importance rankings

---

### Step 4: Make Predictions

Use trained model to predict fire risk:

```powershell
python predict.py
```

**Example output**:
```
🔥 Fire Detection #1
   Location: (34.0522, -118.2437)
   Timestamp: 2025-01-15 1430
   Prediction: Fire will continue
   Probability: 78.5%
   Risk Level: HIGH
```

---

## 📊 Understanding the Data

### Input Data (NASA FIRMS)

Each fire detection includes:
- `latitude`, `longitude`: Location
- `acq_date`, `acq_time`: Detection timestamp
- `frp`: Fire Radiative Power (MW)
- `brightness`, `bright_t31`: Temperature readings
- `confidence`: 'l' (low), 'n' (nominal), 'h' (high)
- `daynight`: 'D' (day) or 'N' (night)
- `scan`, `track`: Satellite scan geometry

### Target Variable

**`fire_next_24h`**: Binary classification
- **1** (Positive): Another fire detected in same grid cell within 24 hours
- **0** (Negative): No fire detected in same grid cell within 24 hours

This indicates **fire persistence** rather than new fire ignition.

---

## 🧠 Model Details

### Random Forest Classifier

**Hyperparameters**:
- `n_estimators=100`: 100 decision trees
- `max_depth=20`: Maximum tree depth
- `class_weight='balanced'`: Handle imbalanced data
- `n_jobs=-1`: Use all CPU cores

**Best for**: Interpretability, feature importance analysis

---

### XGBoost Classifier (Optional)

**Hyperparameters**:
- `n_estimators=100`: 100 boosting rounds
- `max_depth=8`: Maximum tree depth
- `learning_rate=0.1`: Step size
- `scale_pos_weight`: Automatic balancing for minority class

**Best for**: Performance, handling imbalanced data

---

## 📈 Feature Importance

Top features typically include:
1. **frp** (Fire Radiative Power) - strongest indicator
2. **fires_last_7days** - historical fire activity
3. **confidence_encoded** - satellite confidence level
4. **brightness** - fire temperature
5. **latitude/longitude** - geographic patterns

---

## 🔧 Configuration

All settings use environment variables from `.env` (shared with main project):

```env
NASA_FIRMS_API_KEY=your_api_key_here
```

### Customization Options

**In `fetch_historical_data.py`**:
- Change `timedelta(days=30)` to fetch different date range
- Modify `SATELLITE` to use different sensor (e.g., MODIS)

**In `feature_engineering.py`**:
- Adjust grid size: `(df['latitude'] // 5) * 5` → change `5` to different degree resolution
- Modify fire history window: `timedelta(days=7)` → change lookback period

**In `train_model.py`**:
- Tune hyperparameters in model initialization
- Change `test_size=0.2` for different train/test split

---

## 📝 Dependencies

Install required packages:

```powershell
pip install pandas numpy scikit-learn joblib httpx python-dotenv
pip install xgboost  # Optional, for XGBoost model
```

**Note**: Main project dependencies already include most of these.

---

## 🎓 Interpretation Guide

### Prediction Output

```python
{
    'prediction': 1,           # 1 = fire continues, 0 = fire stops
    'probability': 0.785,      # Confidence (0.0 to 1.0)
    'risk_level': 'HIGH',      # HIGH (>70%), MEDIUM (40-70%), LOW (<40%)
    'location': '(34.05, -118.24)',
    'timestamp': '2025-01-15 1430'
}
```

### Risk Levels

- **HIGH (≥70%)**: Strong evidence fire will persist
- **MEDIUM (40-69%)**: Uncertain, monitor closely
- **LOW (<40%)**: Fire likely to stop or not spread

---

## 🧪 Evaluation Metrics

### Confusion Matrix

```
                Predicted No Fire    Predicted Fire
Actual No Fire      TN                  FP
Actual Fire         FN                  TP
```

### Key Metrics

- **Accuracy**: Overall correctness (TP+TN)/(TP+TN+FP+FN)
- **Precision**: How many predicted fires were correct TP/(TP+FP)
- **Recall**: How many actual fires were caught TP/(TP+FN)
- **F1-Score**: Harmonic mean of precision and recall
- **ROC-AUC**: Model's ability to distinguish classes

---

## 🚨 Limitations

1. **Data Scope**: Only predicts fire persistence, not new ignitions
2. **Grid Resolution**: 5-degree grid cells (~555 km) - coarse spatial resolution
3. **Time Window**: 24-hour prediction window - no hourly predictions
4. **Imbalanced Data**: Many more "no fire" cases than "fire continues"
5. **Static Model**: Requires retraining with new data periodically

---

## 🔮 Future Improvements

1. **Weather Data Integration**: Add temperature, humidity, wind speed
2. **Vegetation Indices**: Include NDVI (Normalized Difference Vegetation Index)
3. **Terrain Features**: Elevation, slope, land cover type
4. **Time Series Models**: LSTM/GRU for sequential fire spread patterns
5. **Ensemble Methods**: Combine multiple models for better predictions
6. **Real-time Updates**: Automated retraining pipeline

---

## 📊 Example Workflow

```powershell
# 1. Activate virtual environment
.\venv\Scripts\activate

# 2. Fetch data (takes ~2 minutes with rate limiting)
cd ml_model
python fetch_historical_data.py

# 3. Process features (takes 1-5 minutes depending on data size)
python feature_engineering.py

# 4. Train models (takes 2-10 minutes)
python train_model.py

# 5. Make predictions
python predict.py
```

---

## 🐛 Troubleshooting

### "NASA_FIRMS_API_KEY not found"
- Check `.env` file in root directory
- Ensure `load_dotenv()` is working
- Test API key: `curl "https://firms.modaps.eosdis.nasa.gov/api/area/csv/YOUR_KEY/VIIRS_SNPP_NRT/world/1/2025-01-15"`

### "No CSV files found in data directory"
- Run `fetch_historical_data.py` first
- Check for errors during download
- Verify CSV files in `ml_model/data/`

### "processed_features.csv not found"
- Run `feature_engineering.py` before training
- Check for errors during processing

### XGBoost import error
- Optional: Install with `pip install xgboost`
- Random Forest will still work without it

### Low model performance
- Check class imbalance (target distribution)
- Try collecting more days of data (60, 90 days)
- Tune hyperparameters in `train_model.py`
- Add more features (weather, vegetation, etc.)

---

## 📚 Resources

- **NASA FIRMS**: https://firms.modaps.eosdis.nasa.gov/
- **Scikit-learn**: https://scikit-learn.org/
- **XGBoost**: https://xgboost.readthedocs.io/
- **Fire Prediction Papers**: Search "wildfire prediction machine learning"

---

## ⚠️ Important Notes

- **Standalone System**: Does NOT integrate with main FastAPI application
- **No Frontend**: Predictions are made via command line only
- **No Database**: Does not modify `fire_detections` table
- **Training Data**: Uses historical NASA FIRMS API data, not database records
- **Purpose**: Research and model development, not production deployment

---

**Created**: 2025-01-15  
**Last Updated**: 2025-01-15
