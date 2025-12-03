# Wildfire Detection System - AI Agent Instructions

## Project Overview

This is a **real-time wildfire detection and monitoring system** that fetches satellite data from NASA FIRMS API, processes it through an ETL pipeline, stores it in PostgreSQL with PostGIS,and visualizes active fires on an interactive web map.

**Tech Stack**: FastAPI (Python), PostgreSQL + PostGIS, Leaflet.js, NASA FIRMS API

---

## Architecture & Data Flow

### ETL Pipeline (main.py: `FireDataETL` class)
1. **Extract**: Fetch CSV data from NASA FIRMS API (`NASA_FIRMS_URL`)
2. **Transform**: Parse CSV, validate coordinates, classify severity based on FRP thresholds
3. **Load**: Upsert to `fire_detections` table with PostGIS geometry
4. **Schedule**: Runs every `ETL_INTERVAL_MINUTES` (default: 10 min) via APScheduler

### Database Schema (`fire_detections` table)
```sql
- id (SERIAL PRIMARY KEY)
- latitude, longitude (FLOAT)
- geom (GEOMETRY(POINT, 4326)) -- PostGIS spatial column
- brightness, scan, track (FLOAT)
- acq_date, acq_time (STRING)
- satellite, confidence, version (STRING)
- bright_t31, frp (FLOAT) -- Fire Radiative Power in MW
- daynight (STRING)
- severity (STRING) -- 'severe' or 'moderate'
- detected_at, updated_at (TIMESTAMP)
```

### Fire Severity Classification Logic
Located in `main.py: FireDataETL.classify_severity()`

**Severe**: 
- FRP > `SEVERE_FRP_THRESHOLD` (100 MW) OR
- FRP > `SEVERE_FRP_WITH_CONFIDENCE` (50 MW) AND confidence = 'h'

**Moderate**:
- FRP > `MODERATE_FRP_THRESHOLD` (20 MW) OR
- confidence in ['n', 'h']

**Important**: These thresholds are configurable via `.env` variables.

---

## Environment Variables (.env)

All configuration is loaded via `python-dotenv`. **Never hardcode credentials!**

### Critical Variables (Required)
```env
DATABASE_URL=postgresql://user:password@host:port/dbname
NASA_FIRMS_API_KEY=your_api_key_here
```

### Complete Variable Reference
| Variable | Used In | Default | Description |
|----------|---------|---------|-------------|
| `DATABASE_URL` | `main.py`, `config.py` | (required) | PostgreSQL connection string |
| `NASA_FIRMS_API_KEY` | `main.py` | (required) | NASA FIRMS API key |
| `NASA_FIRMS_URL` | `main.py` | `https://firms...csv` | FIRMS CSV endpoint |
| `ETL_INTERVAL_MINUTES` | `main.py` | 10 | ETL schedule interval |
| `SEVERE_FRP_THRESHOLD` | `main.py` | 100.0 | FRP threshold for severe (MW) |
| `SEVERE_FRP_WITH_CONFIDENCE` | `main.py` | 50.0 | FRP threshold with high confidence |
| `MODERATE_FRP_THRESHOLD` | `main.py` | 20.0 | FRP threshold for moderate |
| `API_HOST`, `API_PORT` | `config.py` | 0.0.0.0:8000 | FastAPI server config |
| `LOG_LEVEL` | `config.py` | INFO | Logging level |

### Variable Consistency Check
- ✅ `main.py` uses `os.getenv()` to read all variables
- ✅ `config.py` uses Pydantic BaseSettings with `.env` file
- ✅ Both load via `load_dotenv()` at startup
- ⚠️ **Always use environment variables**, never hardcode!

---

## Key Development Workflows

### 1. Running the Application
```powershell
# Activate virtual environment
.\venv\Scripts\activate

# Start FastAPI server (with auto-reload)
uvicorn main:app --reload

# Frontend: Open frontend/index.html in browser
# (or serve with: python -m http.server 8080)
```

### 2. Database Setup
```powershell
# Create database
psql -U postgres -c "CREATE DATABASE wildfire_db;"

# Run schema setup (creates tables, indexes, PostGIS)
psql -U postgres -d wildfire_db -f database/setup_database.sql
```

**Important**: PostGIS extension is auto-created on app startup via:
```python
db.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
```

### 3. Manual ETL Trigger
```bash
# Via API endpoint
curl -X POST http://localhost:8000/api/update

# Or programmatically
await scheduled_etl_job()
```

### 4. Testing
```powershell
python test_installation.py  # Validates setup
pytest tests/                # Run unit tests
```

---

## Critical Code Patterns

### 1. Accessing Environment Variables
**Correct**:
```python
from dotenv import load_dotenv
import os

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')
```

**Incorrect**: Don't hardcode values in `main.py` or `config.py`

### 2. PostGIS Geometry Handling
```python
# Insert with WKT format
fire = FireDetection(
    geom=f"SRID=4326;POINT({longitude} {latitude})"
)

# Query within radius
ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography, radius_meters)
```

### 3. ETL Duplicate Prevention
```python
# Check for existing record (same location + date)
existing = db.query(FireDetection).filter(
    FireDetection.latitude == record['latitude'],
    FireDetection.longitude == record['longitude'],
    FireDetection.acq_date == record['acq_date']
).first()
```

### 4. Background Scheduling
```python
# In lifespan context manager
scheduler.add_job(
    scheduled_etl_job,
    trigger=IntervalTrigger(minutes=ETL_INTERVAL_MINUTES),
    id='etl_job'
)
```

---

## Common Pitfalls & Solutions

### Issue: "DATABASE_URL not found"
**Cause**: `.env` file missing or not loaded  
**Fix**: Ensure `load_dotenv()` is called before `os.getenv()`

### Issue: "PostGIS extension not found"
**Cause**: PostGIS not installed on PostgreSQL  
**Fix**: 
```bash
sudo apt install postgresql-postgis  # Linux
# Or download from postgis.net/windows_downloads/
```

### Issue: ETL returns no data
**Check**:
1. NASA FIRMS API key valid? Test: `curl "https://firms.../csv/YOUR_KEY/..."`
2. CSV parsing logic (lines 102-128 in `main.py`)
3. Logs: `grep "Fetched" logs/wildfire.log`

### Issue: Frontend shows "Error loading fire data"
**Check**:
1. API running on port 8000? `curl http://localhost:8000/health`
2. CORS enabled? Check `CORSMiddleware` in `main.py`
3. Browser console for exact error

---

## API Endpoints Reference

| Endpoint | Method | Purpose | Key Parameters |
|----------|--------|---------|----------------|
| `/api/fires` | GET | Get fire detections | `?severity=severe&limit=5000` |
| `/api/stats` | GET | Get statistics | None |
| `/api/update` | POST | Trigger ETL manually | None |
| `/health` | GET | Health check | None |
| `/docs` | GET | OpenAPI docs | None |

**Response Format** (`/api/fires`):
```json
[{
  "id": 123,
  "latitude": 34.5678,
  "longitude": -118.1234,
  "frp": 65.3,
  "severity": "severe",
  "confidence": "h",
  "satellite": "NOAA-20",
  "detected_at": "2025-11-23T10:30:00"
}]
```

---

## Project-Specific Conventions

### File Organization
- **`main.py`**: Entire FastAPI app + ETL (437 lines) - monolithic by design for simplicity
- **`config.py`**: Pydantic settings (use for adding new config)
- **`database/setup_database.sql`**: Schema, indexes, triggers
- **`frontend/index.html`**: Self-contained SPA (no build step)

### Naming Conventions
- Database table: `fire_detections` (plural, snake_case)
- API model: `FireDetection` (singular, PascalCase)
- Severity values: `'severe'` or `'moderate'` (lowercase strings)
- Confidence values: `'l'` (low), `'n'` (nominal), `'h'` (high)

### Data Retention
- Default: Keep fires from last 24 hours (controlled by `DATA_RETENTION_HOURS`)
- Cleanup runs in `update_database()` method
- For custom retention, modify:
```python
cutoff_time = datetime.utcnow() - timedelta(hours=DATA_RETENTION_HOURS)
```

---

## Adding New Features

### To add a new environment variable:
1. Add to `.env` and `.env.example`
2. Add to `config.py` Settings class
3. Access via `os.getenv('VAR_NAME')` in code
4. Document in this file

### To modify severity thresholds:
1. Update values in `.env`
2. No code changes needed (already uses env vars)
3. Restart application to apply

### To add new API endpoint:
```python
@app.get("/api/your-endpoint")
async def your_endpoint(db: Session = Depends(get_db)):
    # Implementation
    return result
```

### To modify database schema:
1. Update `database/setup_database.sql`
2. Update `FireDetection` model in `main.py`
3. Update `FireDetectionResponse` Pydantic model
4. Run migrations or recreate database

---

## Dependencies & Installation

### Core Dependencies
```txt
fastapi==0.103.0
uvicorn==0.23.0
sqlalchemy==2.0.20
geoalchemy2==0.14.1
psycopg2-binary==2.9.7
httpx==0.24.1
apscheduler==3.10.4
pydantic==2.3.0
pydantic-settings==2.0.0
python-dotenv==1.0.0
```

**Note**: Older versions used to avoid Rust compilation issues. Don't upgrade pydantic past 2.5 without testing.

### Installation Commands
```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

---

## Production Considerations

### Security
- Never commit `.env` to git (in `.gitignore`)
- Use strong PostgreSQL passwords
- Restrict CORS origins: `CORS_ORIGINS=["https://yourdomain.com"]`
- Run app as non-root user

### Performance
- Database indexes already configured in `setup_database.sql`
- For >10K fires, consider pagination on `/api/fires`
- Monitor ETL execution time in logs

### Monitoring
- Check `/health` endpoint
- Monitor PostgreSQL connections
- Watch `logs/wildfire.log` for ETL errors

---

## Questions to Ask Before Making Changes

1. **Is this configuration?** → Add to `.env`, not code
2. **Does it affect severity?** → Use existing threshold vars
3. **New database field?** → Update both model and SQL schema
4. **Frontend change?** → Check `frontend/index.html` (no build step)
5. **ETL modification?** → Test with small dataset first

---

## Useful Commands Cheat Sheet

```powershell
# Check database
psql -U postgres -d wildfire_db -c "SELECT COUNT(*) FROM fire_detections;"

# View logs
Get-Content logs/wildfire.log -Tail 50 -Wait

# Test API
curl http://localhost:8000/api/stats

# Check Python environment
pip list | Select-String "fastapi|pydantic|sqlalchemy"

# Restart with fresh data
psql -U postgres -d wildfire_db -c "TRUNCATE fire_detections;"
curl -X POST http://localhost:8000/api/update
```

---

**Last Updated**: 2025-11-23  
**Maintainer**: Reference this file for all development decisions!
