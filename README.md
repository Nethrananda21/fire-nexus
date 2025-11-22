# 🔥 Wildfire Detection System

Real-time wildfire detection and monitoring system using NASA FIRMS satellite data with FastAPI backend and interactive web interface.

## Features

- **Real-time Data**: Automatically fetches wildfire data from NASA FIRMS API
- **Smart Classification**: Categorizes fires by severity (Critical, High, Moderate, Low)
- **Interactive Map**: Leaflet.js-powered map with clustered markers
- **REST API**: FastAPI backend with comprehensive endpoints
- **PostGIS Integration**: Geospatial database with location-based queries
- **Automated ETL**: Background scheduler for periodic data updates
- **Live Statistics**: Real-time dashboard with fire counts and trends

## Quick Start

### Prerequisites

- Python 3.8+
- PostgreSQL 12+ with PostGIS extension
- NASA FIRMS API key ([Get one here](https://firms.modaps.eosdis.nasa.gov/api/area/))

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd wildfire_detection
   ```

2. **Set up Python environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials and FIRMS API key
   ```

4. **Set up database**
   ```bash
   createdb wildfire_db
   psql -U postgres -d wildfire_db -f database/setup_database.sql
   ```

5. **Run the application**
   ```bash
   uvicorn main:app --reload
   ```

6. **Open the frontend**
   - Open `frontend/index.html` in your browser
   - Or serve it: `cd frontend && python -m http.server 8080`

## API Documentation

### Base URL
```
http://localhost:8000
```

### Endpoints

#### Get Fire Detections
```http
GET /api/fires?severity=critical&limit=1000&hours=24
```

**Parameters:**
- `severity` (optional): Filter by severity (critical, high, moderate, low)
- `limit` (optional): Maximum records to return (default: 1000, max: 10000)
- `hours` (optional): Hours to look back (default: 24)

**Response:**
```json
[
  {
    "id": 1,
    "latitude": 34.5678,
    "longitude": -118.1234,
    "brightness": 385.2,
    "acquisition_date": "2024-01-15T14:30:00",
    "satellite": "NOAA-20",
    "confidence": "high",
    "frp": 65.3,
    "severity": "high"
  }
]
```

#### Get Statistics
```http
GET /api/stats?hours=24
```

**Response:**
```json
{
  "total_fires": 1234,
  "critical_fires": 45,
  "high_fires": 234,
  "moderate_fires": 567,
  "low_fires": 388,
  "latest_update": "2024-01-15T14:30:00"
}
```

#### Trigger ETL Pipeline
```http
POST /api/etl/run
```

#### Health Check
```http
GET /health
```

## Architecture

### Backend (`main.py`)
- **FastAPI**: RESTful API with automatic OpenAPI documentation
- **SQLAlchemy**: ORM for database interactions
- **GeoAlchemy2**: Geospatial queries and PostGIS integration
- **APScheduler**: Background task scheduling for ETL pipeline
- **HTTPX**: Async HTTP client for NASA FIRMS API

### Database (`database/setup_database.sql`)
- **PostgreSQL**: Primary database
- **PostGIS**: Geospatial extension for location queries
- **Indexes**: Optimized for date and severity queries
- **Spatial Indexes**: GIST index on geometry column

### Frontend (`frontend/index.html`)
- **Leaflet.js**: Interactive map visualization
- **Marker Clustering**: Groups nearby fires for better performance
- **Bootstrap**: Responsive UI components
- **Real-time Updates**: Auto-refresh every 5 minutes

### ETL Pipeline
1. **Extract**: Fetch data from NASA FIRMS API
2. **Transform**: 
   - Parse date/time fields
   - Calculate fire severity
   - Create geometry points
3. **Load**: Insert new records into PostgreSQL
4. **Schedule**: Runs every 6 hours (configurable)

## Fire Severity Classification

The system classifies fires based on three factors:

| Severity | Brightness (K) | FRP (MW) | Confidence |
|----------|---------------|----------|------------|
| Critical | ≥400          | ≥100     | High       |
| High     | ≥360          | ≥50      | High       |
| Moderate | ≥330          | ≥20      | Nominal    |
| Low      | <330          | <20      | Any        |

Scoring system:
- **Critical**: Score ≥6
- **High**: Score 4-5
- **Moderate**: Score 2-3
- **Low**: Score 0-1

## Configuration

Edit `config.py` or set environment variables in `.env`:

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/wildfire_db

# NASA FIRMS
FIRMS_API_KEY=your_api_key_here
FIRMS_AREA_ID=world
FIRMS_DAYS=1

# ETL
ETL_INTERVAL_HOURS=6

# Severity Thresholds (optional)
CRITICAL_BRIGHTNESS=400
HIGH_BRIGHTNESS=360
MODERATE_BRIGHTNESS=330
CRITICAL_FRP=100
HIGH_FRP=50
MODERATE_FRP=20
```

## Testing

Run the installation test:
```bash
python test_installation.py
```

## Development

### Project Structure
```
wildfire_detection/
├── main.py                 # FastAPI application + ETL
├── config.py               # Configuration management
├── requirements.txt        # Dependencies
├── database/
│   └── setup_database.sql  # Database schema
├── frontend/
│   └── index.html          # Web interface
├── logs/                   # Application logs
└── tests/                  # Unit tests
```

### Adding Features

1. **New API Endpoint**: Add to `main.py` with route decorator
2. **Database Model**: Modify `WildfireDetection` class
3. **Severity Logic**: Update `classify_severity()` function
4. **Frontend**: Edit `frontend/index.html`

## Troubleshooting

### Database Connection Error
```bash
# Check PostgreSQL is running
sudo systemctl status postgresql

# Verify connection
psql -U postgres -d wildfire_db -c "SELECT 1"
```

### API Key Issues
- Ensure your FIRMS API key is valid
- Check the area ID is correct
- Verify the API URL format

### No Data Appearing
- Check logs: `tail -f logs/wildfire.log`
- Manually trigger ETL: `POST http://localhost:8000/api/etl/run`
- Verify database has records: `SELECT COUNT(*) FROM wildfire_detections`

## Production Deployment

See `SETUP.md` for detailed production deployment instructions including:
- Nginx reverse proxy configuration
- SSL certificate setup
- Systemd service configuration
- Database optimization
- Security hardening

## License

MIT License - See LICENSE file for details

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## Support

For issues and questions:
- Create an issue on GitHub
- Check existing documentation in `docs/`

## Credits

- **Data Source**: NASA FIRMS (Fire Information for Resource Management System)
- **Maps**: OpenStreetMap contributors
- **Icons**: Leaflet.js default icons
