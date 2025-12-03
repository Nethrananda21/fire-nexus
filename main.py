from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from geoalchemy2 import Geometry
from datetime import datetime, timedelta
import httpx
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from contextlib import asynccontextmanager
import logging
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
import os
from dotenv import load_dotenv
from pathlib import Path
from collections import defaultdict
import numpy as np
import joblib

# Load environment variables
load_dotenv()

# Configuration from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')
NASA_FIRMS_API_KEY = os.getenv('NASA_FIRMS_API_KEY')
NASA_FIRMS_URL = os.getenv('NASA_FIRMS_URL', 'https://firms.modaps.eosdis.nasa.gov/api/active_fire/viirs-snpp-nrt/csv')
ETL_INTERVAL_MINUTES = int(os.getenv('ETL_INTERVAL_MINUTES', 10))
SEVERE_FRP_THRESHOLD = float(os.getenv('SEVERE_FRP_THRESHOLD', 100.0))
SEVERE_FRP_WITH_CONFIDENCE = float(os.getenv('SEVERE_FRP_WITH_CONFIDENCE', 50.0))
MODERATE_FRP_THRESHOLD = float(os.getenv('MODERATE_FRP_THRESHOLD', 20.0))

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Track last ETL run time
last_etl_run = None

# Database Models
class FireDetection(Base):
    __tablename__ = "fire_detections"
    
    id = Column(Integer, primary_key=True, index=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    geom = Column(Geometry('POINT', srid=4326), nullable=False)
    brightness = Column(Float)
    scan = Column(Float)
    track = Column(Float)
    acq_date = Column(String)
    acq_time = Column(String)
    satellite = Column(String)
    confidence = Column(String)
    version = Column(String)
    bright_t31 = Column(Float)
    frp = Column(Float)  # Fire Radiative Power
    daynight = Column(String)
    severity = Column(String)  # 'severe' or 'moderate'
    detected_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow)  # Last time this fire was detected in NASA data

# Pydantic models
class FireDetectionResponse(BaseModel):
    id: int
    latitude: float
    longitude: float
    brightness: Optional[float]
    frp: Optional[float]
    confidence: Optional[str]
    severity: str
    acq_date: Optional[str]
    acq_time: Optional[str]
    satellite: Optional[str]
    detected_at: datetime

    class Config:
        from_attributes = True

class FireStats(BaseModel):
    total_fires: int
    severe_fires: int
    moderate_fires: int
    last_update: Optional[datetime]

class PredictionZone(BaseModel):
    lat: float
    lon: float
    probability: float
    risk_level: str
    fires_nearby: int
    avg_frp: float
    heuristic_score: Optional[float] = None
    ml_score: Optional[float] = None
    prediction_method: Optional[str] = None

class PredictionResponse(BaseModel):
    timestamp: datetime
    model_name: str
    high_risk_zones: List[PredictionZone]
    total_predictions: int
    model_accuracy: Optional[float]
    prediction_method: str  # 'hybrid', 'ml_only', 'heuristic_only'

# ML Model Management
class WildfirePredictor:
    def __init__(self):
        self.model = None
        self.model_path = None
        self.metadata = {}
        self._load_latest_model()
    
    def _load_latest_model(self):
        """Load the latest trained model"""
        models_dir = Path(__file__).parent / "ml_model" / "models"
        if not models_dir.exists():
            logger.warning("ML models directory not found")
            return
        
        # Find latest model file (support multiple naming patterns)
        model_files = list(models_dir.glob("*.joblib"))
        # Filter out non-model files
        model_files = [f for f in model_files if 'wildfire' in f.name.lower() or 'predictor' in f.name.lower()]
        
        if not model_files:
            logger.warning("No trained ML model found")
            return
        
        # Sort by modification time and get latest
        latest_model = max(model_files, key=lambda p: p.stat().st_mtime)
        
        try:
            self.model = joblib.load(latest_model)
            self.model_path = latest_model
            
            # Load metadata if exists (try multiple naming patterns)
            metadata_path = latest_model.with_name(latest_model.stem + '_metadata.txt')
            if not metadata_path.exists():
                # Try alternative naming
                metadata_path = latest_model.with_suffix('.txt').with_name(
                    latest_model.stem.replace('wildfire_predictor', 'random_forest') + '_metadata.txt'
                )
            
            if metadata_path.exists():
                with open(metadata_path) as f:
                    for line in f:
                        if ':' in line:
                            key, value = line.strip().split(':', 1)
                            self.metadata[key.strip()] = value.strip()
            
            logger.info(f"Loaded ML model: {latest_model.name}")
        except Exception as e:
            logger.error(f"Error loading ML model: {e}")
    
    def reload_model(self):
        """Reload the model (call after retraining)"""
        self._load_latest_model()
    
    def predict(self, fires_data: List[dict], grid_size: float = 0.5) -> List[dict]:
        """Generate predictions for grid cells based on recent fire data"""
        if self.model is None:
            return []
        
        if not fires_data:
            return []
        
        # Build grid from fire locations
        predictions = []
        grid_fires = defaultdict(list)
        
        # Group fires by grid cell
        for fire in fires_data:
            lat = fire.get('latitude', 0)
            lon = fire.get('longitude', 0)
            grid_lat = round(lat / grid_size) * grid_size
            grid_lon = round(lon / grid_size) * grid_size
            grid_fires[(grid_lat, grid_lon)].append(fire)
        
        # Generate predictions for cells with fires and their neighbors
        cells_to_predict = set()
        for (lat, lon) in grid_fires.keys():
            # Add the cell itself
            cells_to_predict.add((lat, lon))
            # Add neighboring cells
            for dlat in [-grid_size, 0, grid_size]:
                for dlon in [-grid_size, 0, grid_size]:
                    cells_to_predict.add((lat + dlat, lon + dlon))
        
        # Create features for each cell
        for (lat, lon) in cells_to_predict:
            fires_in_cell = grid_fires.get((lat, lon), [])
            
            # Count fires in neighboring cells
            neighbor_fires = 0
            for dlat in [-grid_size, 0, grid_size]:
                for dlon in [-grid_size, 0, grid_size]:
                    if dlat != 0 or dlon != 0:
                        neighbor_fires += len(grid_fires.get((lat + dlat, lon + dlon), []))
            
            # Calculate features
            fires_count = len(fires_in_cell)
            avg_frp = np.mean([f.get('frp', 0) or 0 for f in fires_in_cell]) if fires_in_cell else 0
            max_frp = max([f.get('frp', 0) or 0 for f in fires_in_cell]) if fires_in_cell else 0
            
            # Season (1-4)
            month = datetime.utcnow().month
            season = (month % 12) // 3 + 1
            
            # Build feature vector (must match training features)
            features = np.array([[
                lat,                    # latitude
                lon,                    # longitude
                fires_count,            # fires_last_7days (approximation)
                fires_count,            # fires_last_14days
                fires_count,            # fires_last_30days
                neighbor_fires,         # neighbor_fires
                avg_frp,                # avg_frp
                max_frp,                # max_frp
                season                  # season
            ]])
            
            try:
                # Get probability prediction
                proba = self.model.predict_proba(features)[0][1]  # Probability of fire
                
                # Determine risk level
                if proba >= 0.7:
                    risk_level = "critical"
                elif proba >= 0.5:
                    risk_level = "high"
                elif proba >= 0.3:
                    risk_level = "moderate"
                else:
                    risk_level = "low"
                
                # Only include moderate+ risk zones
                if proba >= 0.25:
                    predictions.append({
                        'lat': lat,
                        'lon': lon,
                        'probability': round(proba * 100, 1),
                        'risk_level': risk_level,
                        'fires_nearby': fires_count + neighbor_fires,
                        'avg_frp': round(avg_frp, 1)
                    })
            except Exception as e:
                logger.error(f"Prediction error for cell ({lat}, {lon}): {e}")
                continue
        
        # Sort by probability (highest first)
        predictions.sort(key=lambda x: x['probability'], reverse=True)
        
        return predictions[:100]  # Return top 100 high-risk zones

# Global predictor instance
wildfire_predictor = None

def get_predictor() -> WildfirePredictor:
    global wildfire_predictor
    if wildfire_predictor is None:
        wildfire_predictor = WildfirePredictor()
    return wildfire_predictor

# ETL Service
class FireDataETL:
    def __init__(self, db: Session):
        self.db = db
        
    def classify_severity(self, frp: float, confidence: str) -> str:
        """Classify fire severity based on FRP and confidence"""
        if frp is None:
            frp = 0
        
        # Severe: High FRP or high FRP with high confidence (from env)
        if frp > SEVERE_FRP_THRESHOLD or (frp > SEVERE_FRP_WITH_CONFIDENCE and confidence == 'h'):
            return 'severe'
        # Moderate: Medium FRP or any fire with nominal/high confidence
        elif frp > MODERATE_FRP_THRESHOLD or confidence in ['n', 'h']:
            return 'moderate'
        else:
            return 'moderate'
    
    async def fetch_nasa_firms_data(self, days: int = 1) -> List[dict]:
        """Fetch fire data from NASA FIRMS API"""
        try:
            # Use the complete URL from environment variable
            url = NASA_FIRMS_URL
            
            logger.info(f"Fetching fire data from: {url}")
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                
                # Parse CSV data
                lines = response.text.strip().split('\n')
                if len(lines) < 2:
                    logger.warning("No fire data received from NASA FIRMS")
                    return []
                
                headers = lines[0].split(',')
                data = []
                
                for line in lines[1:]:
                    values = line.split(',')
                    if len(values) == len(headers):
                        record = dict(zip(headers, values))
                        data.append(record)
                
                logger.info(f"Fetched {len(data)} fire detections from NASA FIRMS")
                return data
                
        except Exception as e:
            logger.error(f"Error fetching NASA FIRMS data: {e}")
            return []
    
    def clean_and_validate(self, raw_data: List[dict]) -> List[dict]:
        """Clean and validate fire data"""
        cleaned_data = []
        
        for record in raw_data:
            try:
                # Validate required fields
                lat = float(record.get('latitude', 0))
                lon = float(record.get('longitude', 0))
                
                if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                    continue
                
                # Clean and convert data (NASA FIRMS column names)
                cleaned_record = {
                    'latitude': lat,
                    'longitude': lon,
                    'brightness': float(record.get('bright_ti4', 0)) if record.get('bright_ti4') else None,
                    'scan': float(record.get('scan', 0)) if record.get('scan') else None,
                    'track': float(record.get('track', 0)) if record.get('track') else None,
                    'acq_date': record.get('acq_date', ''),
                    'acq_time': record.get('acq_time', ''),
                    'satellite': record.get('satellite', ''),
                    'confidence': record.get('confidence', 'l'),
                    'version': record.get('version', ''),
                    'bright_t31': float(record.get('bright_ti5', 0)) if record.get('bright_ti5') else None,
                    'frp': float(record.get('frp', 0)) if record.get('frp') else None,
                    'daynight': record.get('daynight', 'D')
                }
                
                # Classify severity
                cleaned_record['severity'] = self.classify_severity(
                    cleaned_record['frp'], 
                    cleaned_record['confidence']
                )
                
                cleaned_data.append(cleaned_record)
                
            except Exception as e:
                logger.warning(f"Error processing record: {e}")
                continue
        
        logger.info(f"Cleaned {len(cleaned_data)} valid fire detections")
        return cleaned_data
    
    def update_database(self, cleaned_data: List[dict]):
        """Update database with cleaned fire data"""
        try:
            # Remove old detections (older than 24 hours)
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            deleted_count = self.db.query(FireDetection).filter(
                FireDetection.detected_at < cutoff_time
            ).delete()
            self.db.commit()
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old fire detections")
            
            # Build dictionary of existing fires for fast lookup
            existing_fires_dict = {}
            existing_fires = self.db.query(FireDetection).all()
            for fire in existing_fires:
                key = (fire.latitude, fire.longitude, fire.acq_date, fire.acq_time)
                existing_fires_dict[key] = fire
            
            # Track fires seen in current fetch and dedupe incoming data
            seen_in_batch = set()
            new_fires = []
            updated_count = 0
            skipped_dupes = 0
            
            for record in cleaned_data:
                key = (record['latitude'], record['longitude'], record['acq_date'], record['acq_time'])
                
                # Skip duplicates within the same batch
                if key in seen_in_batch:
                    skipped_dupes += 1
                    continue
                seen_in_batch.add(key)
                
                if key in existing_fires_dict:
                    # Update last_seen for existing fire
                    existing_fires_dict[key].last_seen = datetime.utcnow()
                    updated_count += 1
                else:
                    # Create new record with last_seen set to now
                    fire = FireDetection(
                        **record,
                        geom=f"SRID=4326;POINT({record['longitude']} {record['latitude']})",
                        last_seen=datetime.utcnow()
                    )
                    new_fires.append(fire)
                    # Add to existing dict to prevent duplicates within new_fires
                    existing_fires_dict[key] = fire
                    
            # Insert new fires one at a time to handle any remaining conflicts
            inserted_count = 0
            for fire in new_fires:
                try:
                    self.db.add(fire)
                    self.db.flush()
                    inserted_count += 1
                except Exception:
                    self.db.rollback()
                    # Skip this fire if it still causes conflict
                    continue
            
            self.db.commit()
            logger.info(f"Database updated: {inserted_count} new fires, {updated_count} updated, {skipped_dupes} batch dupes skipped")
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating database: {e}")
            raise
    
    async def run_etl_pipeline(self):
        """Execute complete ETL pipeline"""
        global last_etl_run
        logger.info("Starting ETL pipeline...")
        
        # Fetch data
        raw_data = await self.fetch_nasa_firms_data(days=1)
        
        if not raw_data:
            logger.warning("No data fetched, skipping ETL pipeline")
            last_etl_run = datetime.utcnow()
            return
        
        # Clean and validate
        cleaned_data = self.clean_and_validate(raw_data)
        
        if not cleaned_data:
            logger.warning("No valid data after cleaning, skipping database update")
            last_etl_run = datetime.utcnow()
            return
        
        # Update database
        self.update_database(cleaned_data)
        
        # Update last run timestamp
        last_etl_run = datetime.utcnow()
        logger.info("ETL pipeline completed successfully")
        
        # Run severity analysis SQL
        self.run_severity_analysis()
        
        logger.info("ETL pipeline completed successfully")
    
    def run_severity_analysis(self):
        """Run SQL queries to analyze fire severity distribution"""
        try:
            # Update severity based on spatial clustering and FRP
            query = text(f"""
            UPDATE fire_detections
            SET severity = CASE
                WHEN frp > {SEVERE_FRP_THRESHOLD} OR (frp > {SEVERE_FRP_WITH_CONFIDENCE} AND confidence = 'h') THEN 'severe'
                WHEN frp > {MODERATE_FRP_THRESHOLD} OR confidence IN ('n', 'h') THEN 'moderate'
                ELSE 'moderate'
            END
            WHERE detected_at > NOW() - INTERVAL '24 hours';
            """)
            self.db.execute(query)
            self.db.commit()
            
            logger.info("Severity analysis completed")
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error in severity analysis: {e}")

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Scheduler setup
scheduler = AsyncIOScheduler()

async def scheduled_etl_job():
    """Background job to run ETL pipeline every 10 minutes"""
    db = SessionLocal()
    try:
        etl = FireDataETL(db)
        await etl.run_etl_pipeline()
    finally:
        db.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting application...")
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    # Create PostGIS extension
    try:
        db = SessionLocal()
        db.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        db.commit()
        db.close()
    except Exception as e:
        logger.warning(f"PostGIS extension setup: {e}")
    
    # Run initial ETL
    logger.info("Running initial ETL pipeline...")
    await scheduled_etl_job()
    
    # Schedule ETL job
    scheduler.add_job(
        scheduled_etl_job,
        trigger=IntervalTrigger(minutes=ETL_INTERVAL_MINUTES),
        id='etl_job',
        name=f'Fetch and process fire data every {ETL_INTERVAL_MINUTES} minutes',
        replace_existing=True
    )
    scheduler.start()
    logger.info(f"Scheduler started - ETL will run every {ETL_INTERVAL_MINUTES} minutes")
    
    yield
    
    # Shutdown
    scheduler.shutdown()
    logger.info("Application shutdown")

# FastAPI app
app = FastAPI(
    title="Wildfire Detection System",
    description="Real-time wildfire detection using NASA FIRMS data",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Endpoints
@app.get("/")
async def root():
    return {
        "message": "Wildfire Detection System API",
        "version": "1.0.0",
        "endpoints": {
            "fires": "/api/fires",
            "stats": "/api/stats",
            "predictions": "/api/predictions",
            "manual_update": "/api/update"
        }
    }

@app.get("/api/fires", response_model=List[FireDetectionResponse])
async def get_fires(
    severity: Optional[str] = None,
    limit: int = 1000,
    db: Session = Depends(get_db)
):
    """Get active fire detections"""
    try:
        # Only show fires seen in last 30 minutes (actively detected by NASA)
        query = db.query(FireDetection).filter(
            FireDetection.last_seen > datetime.utcnow() - timedelta(minutes=30)
        )
        
        if severity:
            query = query.filter(FireDetection.severity == severity)
        
        fires = query.order_by(FireDetection.detected_at.desc()).limit(limit).all()
        
        return fires
        
    except Exception as e:
        logger.error(f"Error fetching fires: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats", response_model=FireStats)
async def get_stats(db: Session = Depends(get_db)):
    """Get fire statistics"""
    try:
        # Count only fires seen in last 30 minutes
        total = db.query(FireDetection).filter(
            FireDetection.last_seen > datetime.utcnow() - timedelta(minutes=30)
        ).count()
        
        severe = db.query(FireDetection).filter(
            FireDetection.last_seen > datetime.utcnow() - timedelta(minutes=30),
            FireDetection.severity == 'severe'
        ).count()
        
        moderate = db.query(FireDetection).filter(
            FireDetection.last_seen > datetime.utcnow() - timedelta(minutes=30),
            FireDetection.severity == 'moderate'
        ).count()
        
        # Use last ETL run time if available, otherwise fall back to database timestamp
        last_update = last_etl_run if last_etl_run else db.query(func.max(FireDetection.updated_at)).scalar()
        
        return FireStats(
            total_fires=total,
            severe_fires=severe,
            moderate_fires=moderate,
            last_update=last_update
        )
        
    except Exception as e:
        logger.error(f"Error fetching stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/update")
async def manual_update(db: Session = Depends(get_db)):
    """Manually trigger ETL pipeline"""
    try:
        etl = FireDataETL(db)
        await etl.run_etl_pipeline()
        return {"message": "ETL pipeline executed successfully"}
    except Exception as e:
        logger.error(f"Error in manual update: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/predictions", response_model=PredictionResponse)
async def get_predictions(db: Session = Depends(get_db)):
    """Get AI-powered wildfire risk predictions using hybrid model"""
    try:
        predictor = get_predictor()
        
        # Get recent fires from database (last 24 hours for prediction)
        recent_fires = db.query(FireDetection).filter(
            FireDetection.detected_at > datetime.utcnow() - timedelta(hours=24)
        ).all()
        
        # Convert to dict for predictor
        fires_data = [
            {
                'latitude': f.latitude,
                'longitude': f.longitude,
                'frp': f.frp,
                'confidence': f.confidence,
                'severity': f.severity
            }
            for f in recent_fires
        ]
        
        # Use hybrid prediction system
        predictions, prediction_method = hybrid_predict(fires_data, predictor)
        
        # Determine model name
        if predictor.model is not None:
            model_name = f"hybrid-{predictor.model_path.name}" if predictor.model_path else "hybrid-xgboost"
        else:
            model_name = "heuristic-v1"
        
        # Get model accuracy from metadata (if ML model is used)
        accuracy = None
        if predictor.model is not None and 'Accuracy' in predictor.metadata:
            try:
                accuracy = float(predictor.metadata['Accuracy'].replace('%', ''))
            except:
                pass
        
        high_risk_zones = [PredictionZone(**p) for p in predictions]
        
        return PredictionResponse(
            timestamp=datetime.utcnow(),
            model_name=model_name,
            high_risk_zones=high_risk_zones,
            total_predictions=len(high_risk_zones),
            model_accuracy=accuracy,
            prediction_method=prediction_method
        )
        
    except Exception as e:
        logger.error(f"Error generating predictions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================================
# HYBRID PREDICTION SYSTEM
# Combines heuristic rules with ML model for robust predictions
# ============================================================================

# Configurable weights for hybrid model
HEURISTIC_WEIGHT = float(os.getenv('HEURISTIC_WEIGHT', '0.4'))  # 40% heuristic
ML_WEIGHT = float(os.getenv('ML_WEIGHT', '0.6'))  # 60% ML model

def calculate_heuristic_score(fires_count: int, neighbor_fires: int, avg_frp: float, 
                               severe_count: int, high_conf_count: int) -> float:
    """
    Calculate heuristic risk score (0-100) based on fire characteristics.
    
    Components:
    - Base risk: Fire density in cell (max 40%)
    - Neighbor risk: Fire activity in adjacent cells (max 20%)
    - FRP risk: Fire intensity (max 20%)
    - Severity risk: Proportion of severe fires (max 15%)
    - Confidence risk: Proportion of high-confidence detections (max 5%)
    """
    # Base risk from fire count (0-40)
    base_risk = min(fires_count * 10, 40)
    
    # Neighbor risk (0-20)
    neighbor_risk = min(neighbor_fires * 5, 20)
    
    # FRP risk (0-20)
    frp_risk = min(avg_frp / 10, 20) if avg_frp > 0 else 0
    
    # Severity risk (0-15)
    severity_risk = (severe_count / fires_count * 15) if fires_count > 0 else 0
    
    # Confidence risk (0-5)
    confidence_risk = (high_conf_count / fires_count * 5) if fires_count > 0 else 0
    
    # Total (capped at 95)
    return min(base_risk + neighbor_risk + frp_risk + severity_risk + confidence_risk, 95)

def calculate_ml_score(predictor, lat: float, lon: float, fires_count: int, 
                       neighbor_fires: int, avg_frp: float, max_frp: float) -> Optional[float]:
    """
    Calculate ML-based risk score (0-100) using trained XGBoost model.
    Returns None if no model is available.
    
    Model features (11 total):
    grid_lat, grid_lon, fires_last_7_days, fires_last_14_days, avg_frp_7_days,
    neighbor_fires_3_days, month, day_of_year, day_of_week, season, abs_latitude
    """
    if predictor.model is None:
        return None
    
    # Temporal features
    now = datetime.utcnow()
    month = now.month
    day_of_year = now.timetuple().tm_yday
    day_of_week = now.weekday()
    
    # Season (0=winter, 1=spring, 2=summer, 3=fall) - adjusted for hemisphere
    if month in [12, 1, 2]:
        season = 0
    elif month in [3, 4, 5]:
        season = 1
    elif month in [6, 7, 8]:
        season = 2
    else:
        season = 3
    
    # Hemisphere adjustment
    if lat < 0:
        season = (season + 2) % 4
    
    # Build feature vector (must match training features exactly!)
    # Features: grid_lat, grid_lon, fires_last_7_days, fires_last_14_days, avg_frp_7_days,
    #           neighbor_fires_3_days, month, day_of_year, day_of_week, season, abs_latitude
    features = np.array([[
        lat,                    # grid_lat
        lon,                    # grid_lon
        fires_count,            # fires_last_7_days
        fires_count,            # fires_last_14_days (approximation)
        avg_frp,                # avg_frp_7_days
        neighbor_fires,         # neighbor_fires_3_days
        month,                  # month
        day_of_year,            # day_of_year
        day_of_week,            # day_of_week
        season,                 # season
        abs(lat)                # abs_latitude
    ]])
    
    try:
        # Get probability prediction from model
        proba = predictor.model.predict_proba(features)[0][1]
        return round(proba * 100, 1)
    except Exception as e:
        logger.error(f"ML prediction error: {e}")
        return None

def determine_risk_level(probability: float) -> str:
    """Determine risk level from probability score"""
    if probability >= 70:
        return "critical"
    elif probability >= 50:
        return "high"
    elif probability >= 30:
        return "moderate"
    else:
        return "low"

def hybrid_predict(fires_data: List[dict], predictor, grid_size: float = 0.5) -> tuple:
    """
    Generate hybrid predictions combining heuristic rules and ML model.
    
    Returns:
        tuple: (predictions_list, prediction_method)
        - prediction_method: 'hybrid', 'ml_only', or 'heuristic_only'
    """
    if not fires_data:
        return [], 'heuristic_only'
    
    predictions = []
    grid_fires = defaultdict(list)
    has_ml = predictor.model is not None
    
    # Group fires by grid cell
    for fire in fires_data:
        lat = fire.get('latitude', 0)
        lon = fire.get('longitude', 0)
        grid_lat = round(lat / grid_size) * grid_size
        grid_lon = round(lon / grid_size) * grid_size
        grid_fires[(grid_lat, grid_lon)].append(fire)
    
    # Generate predictions for cells with fires and their neighbors
    cells_to_predict = set()
    for (lat, lon) in grid_fires.keys():
        cells_to_predict.add((lat, lon))
        # Add neighboring cells for spread prediction
        for dlat in [-grid_size, 0, grid_size]:
            for dlon in [-grid_size, 0, grid_size]:
                cells_to_predict.add((lat + dlat, lon + dlon))
    
    # Process each cell
    for (lat, lon) in cells_to_predict:
        cell_fires = grid_fires.get((lat, lon), [])
        
        # Count fires in neighboring cells
        neighbor_fires = 0
        for dlat in [-grid_size, 0, grid_size]:
            for dlon in [-grid_size, 0, grid_size]:
                if dlat != 0 or dlon != 0:
                    neighbor_fires += len(grid_fires.get((lat + dlat, lon + dlon), []))
        
        # Calculate features
        fires_count = len(cell_fires)
        avg_frp = np.mean([f.get('frp', 0) or 0 for f in cell_fires]) if cell_fires else 0
        max_frp = max([f.get('frp', 0) or 0 for f in cell_fires]) if cell_fires else 0
        severe_count = sum(1 for f in cell_fires if f.get('severity') == 'severe')
        high_conf_count = sum(1 for f in cell_fires if f.get('confidence') == 'h')
        
        # Calculate heuristic score
        heuristic_score = calculate_heuristic_score(
            fires_count, neighbor_fires, avg_frp, severe_count, high_conf_count
        )
        
        # Calculate ML score (if model available)
        ml_score = calculate_ml_score(
            predictor, lat, lon, fires_count, neighbor_fires, avg_frp, max_frp
        ) if has_ml else None
        
        # Combine scores using weighted average
        if ml_score is not None:
            # Hybrid: weighted combination
            combined_probability = (HEURISTIC_WEIGHT * heuristic_score) + (ML_WEIGHT * ml_score)
            method = 'hybrid'
        else:
            # Heuristic only
            combined_probability = heuristic_score
            method = 'heuristic'
        
        # Cap at 95%
        combined_probability = min(combined_probability, 95)
        
        # Determine risk level
        risk_level = determine_risk_level(combined_probability)
        
        # Only include moderate+ risk zones (>= 25%)
        if combined_probability >= 25:
            predictions.append({
                'lat': lat,
                'lon': lon,
                'probability': round(combined_probability, 1),
                'risk_level': risk_level,
                'fires_nearby': fires_count + neighbor_fires,
                'avg_frp': round(avg_frp, 1),
                'heuristic_score': round(heuristic_score, 1),
                'ml_score': ml_score,
                'prediction_method': method
            })
    
    # Sort by probability (highest first)
    predictions.sort(key=lambda x: x['probability'], reverse=True)
    
    # Determine overall prediction method
    if has_ml:
        overall_method = 'hybrid'
    else:
        overall_method = 'heuristic_only'
    
    return predictions[:100], overall_method

def heuristic_predict(fires_data: List[dict], grid_size: float = 0.5) -> List[dict]:
    """
    Legacy heuristic-only predictions (kept for backwards compatibility).
    Use hybrid_predict for new implementations.
    """
    if not fires_data:
        return []
    
    predictions = []
    grid_fires = defaultdict(list)
    
    # Group fires by grid cell
    for fire in fires_data:
        lat = fire.get('latitude', 0)
        lon = fire.get('longitude', 0)
        grid_lat = round(lat / grid_size) * grid_size
        grid_lon = round(lon / grid_size) * grid_size
        grid_fires[(grid_lat, grid_lon)].append(fire)
    
    # Calculate risk for each cell based on heuristics
    for (lat, lon), cell_fires in grid_fires.items():
        neighbor_fires = 0
        for dlat in [-grid_size, 0, grid_size]:
            for dlon in [-grid_size, 0, grid_size]:
                if dlat != 0 or dlon != 0:
                    neighbor_fires += len(grid_fires.get((lat + dlat, lon + dlon), []))
        
        fires_count = len(cell_fires)
        avg_frp = np.mean([f.get('frp', 0) or 0 for f in cell_fires]) if cell_fires else 0
        severe_count = sum(1 for f in cell_fires if f.get('severity') == 'severe')
        high_conf_count = sum(1 for f in cell_fires if f.get('confidence') == 'h')
        
        probability = calculate_heuristic_score(
            fires_count, neighbor_fires, avg_frp, severe_count, high_conf_count
        )
        risk_level = determine_risk_level(probability)
        
        if probability >= 25:
            predictions.append({
                'lat': lat,
                'lon': lon,
                'probability': round(probability, 1),
                'risk_level': risk_level,
                'fires_nearby': fires_count + neighbor_fires,
                'avg_frp': round(avg_frp, 1),
                'heuristic_score': round(probability, 1),
                'ml_score': None,
                'prediction_method': 'heuristic'
            })
    
    predictions.sort(key=lambda x: x['probability'], reverse=True)
    return predictions[:100]

@app.post("/api/predictions/reload")
async def reload_prediction_model():
    """Reload the ML model after retraining"""
    try:
        predictor = get_predictor()
        predictor.reload_model()
        
        if predictor.model is None:
            return {"status": "error", "message": "No model found to load"}
        
        return {
            "status": "success", 
            "message": f"Model reloaded: {predictor.model_path.name if predictor.model_path else 'unknown'}"
        }
    except Exception as e:
        logger.error(f"Error reloading model: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)