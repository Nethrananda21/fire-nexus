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
from typing import List, Optional
from pydantic import BaseModel
import os
from dotenv import load_dotenv

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
            
            # Build sets of existing fire keys for fast lookup
            existing_keys = set()
            existing_fires = self.db.query(
                FireDetection.latitude,
                FireDetection.longitude,
                FireDetection.acq_date,
                FireDetection.acq_time
            ).all()
            for fire in existing_fires:
                key = (fire.latitude, fire.longitude, fire.acq_date, fire.acq_time)
                existing_keys.add(key)
            
            # Separate new and existing records
            new_fires = []
            inserted_count = 0
            skipped_count = 0
            
            for record in cleaned_data:
                key = (record['latitude'], record['longitude'], record['acq_date'], record['acq_time'])
                
                if key not in existing_keys:
                    # Create new record
                    fire = FireDetection(
                        **record,
                        geom=f"SRID=4326;POINT({record['longitude']} {record['latitude']})"
                    )
                    new_fires.append(fire)
                    
            # Batch insert all new fires
            if new_fires:
                self.db.bulk_save_objects(new_fires)
                inserted_count = len(new_fires)
            
            self.db.commit()
            logger.info(f"Database updated: {inserted_count} inserted, {skipped_count} skipped (using batch insert)")
            
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
        query = db.query(FireDetection).filter(
            FireDetection.detected_at > datetime.utcnow() - timedelta(hours=24)
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
        total = db.query(FireDetection).filter(
            FireDetection.detected_at > datetime.utcnow() - timedelta(hours=24)
        ).count()
        
        severe = db.query(FireDetection).filter(
            FireDetection.detected_at > datetime.utcnow() - timedelta(hours=24),
            FireDetection.severity == 'severe'
        ).count()
        
        moderate = db.query(FireDetection).filter(
            FireDetection.detected_at > datetime.utcnow() - timedelta(hours=24),
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

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)