# Wildfire Detection System - API Documentation

Complete API reference for the Wildfire Detection System.

## Base URL

```
http://localhost:8000
```

For production, replace with your domain.

## Authentication

Currently, the API does not require authentication. For production deployment, consider implementing:
- API keys
- OAuth2
- JWT tokens

## Endpoints

### 1. Root Endpoint

**GET** `/`

Returns API information and available endpoints.

**Response:**
```json
{
  "message": "Wildfire Detection API",
  "version": "1.0.0",
  "endpoints": {
    "fires": "/api/fires",
    "stats": "/api/stats",
    "health": "/health"
  }
}
```

---

### 2. Health Check

**GET** `/health`

Check if the API is running.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T14:30:00.123456"
}
```

---

### 3. Get Fire Detections

**GET** `/api/fires`

Retrieve wildfire detection data with optional filtering.

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `severity` | string | null | Filter by severity: `critical`, `high`, `moderate`, `low` |
| `limit` | integer | 1000 | Maximum records to return (max: 10000) |
| `hours` | integer | 24 | Number of hours to look back |

**Examples:**

```bash
# Get all fires in last 24 hours
GET /api/fires

# Get critical fires only
GET /api/fires?severity=critical

# Get fires from last 48 hours, limit 500
GET /api/fires?hours=48&limit=500

# Get high severity fires from last 12 hours
GET /api/fires?severity=high&hours=12&limit=100
```

**Response:**
```json
[
  {
    "id": 1234,
    "latitude": 34.5678,
    "longitude": -118.1234,
    "brightness": 385.2,
    "acquisition_date": "2024-01-15T14:30:00",
    "satellite": "NOAA-20",
    "confidence": "high",
    "frp": 65.3,
    "severity": "high"
  },
  {
    "id": 1235,
    "latitude": 36.7890,
    "longitude": -119.4567,
    "brightness": 420.5,
    "acquisition_date": "2024-01-15T14:25:00",
    "satellite": "Suomi-NPP",
    "confidence": "high",
    "frp": 125.7,
    "severity": "critical"
  }
]
```

**Response Fields:**

- `id`: Unique detection ID
- `latitude`: Latitude coordinate
- `longitude`: Longitude coordinate
- `brightness`: Brightness temperature in Kelvin
- `acquisition_date`: When the fire was detected (ISO 8601 format)
- `satellite`: Satellite name (NOAA-20, Suomi-NPP, etc.)
- `confidence`: Detection confidence (low, nominal, high)
- `frp`: Fire Radiative Power in MW (can be null)
- `severity`: Calculated severity (critical, high, moderate, low)

---

### 4. Get Statistics

**GET** `/api/stats`

Get fire detection statistics and counts by severity.

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `hours` | integer | 24 | Number of hours to look back |

**Examples:**

```bash
# Get stats for last 24 hours
GET /api/stats

# Get stats for last week
GET /api/stats?hours=168
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

**Response Fields:**

- `total_fires`: Total fire detections in time period
- `critical_fires`: Count of critical severity fires
- `high_fires`: Count of high severity fires
- `moderate_fires`: Count of moderate severity fires
- `low_fires`: Count of low severity fires
- `latest_update`: Timestamp of most recent data update (can be null)

---

### 5. Trigger ETL Pipeline

**POST** `/api/etl/run`

Manually trigger the ETL pipeline to fetch new data from NASA FIRMS.

**Request:**
```bash
POST /api/etl/run
```

No request body required.

**Response:**
```json
{
  "status": "success",
  "message": "ETL pipeline executed"
}
```

**Error Response:**
```json
{
  "detail": "Error message here"
}
```

**Note:** The ETL pipeline runs automatically every 6 hours (configurable). Use this endpoint sparingly.

---

## Error Responses

All endpoints may return error responses in the following format:

**400 Bad Request:**
```json
{
  "detail": "Invalid parameter value"
}
```

**422 Unprocessable Entity:**
```json
{
  "detail": [
    {
      "loc": ["query", "limit"],
      "msg": "ensure this value is less than or equal to 10000",
      "type": "value_error.number.not_le"
    }
  ]
}
```

**500 Internal Server Error:**
```json
{
  "detail": "Internal server error message"
}
```

---

## Rate Limiting

Currently, no rate limiting is implemented. For production:

```python
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.get("/api/fires")
@limiter.limit("100/minute")
async def get_fires():
    ...
```

---

## CORS Configuration

The API currently allows all origins:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

For production, restrict to specific domains:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
```

---

## Interactive Documentation

FastAPI provides automatic interactive API documentation:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

These interfaces allow you to:
- Explore all endpoints
- Test API calls directly from the browser
- View request/response schemas
- See example responses

---

## Code Examples

### Python (using requests)

```python
import requests

# Get critical fires
response = requests.get(
    "http://localhost:8000/api/fires",
    params={"severity": "critical", "limit": 100}
)
fires = response.json()

for fire in fires:
    print(f"Fire at {fire['latitude']}, {fire['longitude']} - Severity: {fire['severity']}")
```

### JavaScript (using fetch)

```javascript
// Get statistics
fetch('http://localhost:8000/api/stats?hours=24')
  .then(response => response.json())
  .then(data => {
    console.log(`Total fires: ${data.total_fires}`);
    console.log(`Critical: ${data.critical_fires}`);
  });

// Get fires with async/await
async function getFires() {
  const response = await fetch('http://localhost:8000/api/fires?severity=high');
  const fires = await response.json();
  return fires;
}
```

### cURL

```bash
# Get fires
curl "http://localhost:8000/api/fires?severity=critical&limit=10"

# Get statistics
curl "http://localhost:8000/api/stats?hours=24"

# Trigger ETL
curl -X POST "http://localhost:8000/api/etl/run"

# Health check
curl "http://localhost:8000/health"
```

---

## WebSocket Support (Future Enhancement)

For real-time updates, consider implementing WebSocket endpoints:

```python
from fastapi import WebSocket

@app.websocket("/ws/fires")
async def websocket_fires(websocket: WebSocket):
    await websocket.accept()
    while True:
        # Send updates when new fires are detected
        data = await get_latest_fires()
        await websocket.send_json(data)
        await asyncio.sleep(60)
```

---

## Data Sources

- **NASA FIRMS**: Fire Information for Resource Management System
- **Satellites**: VIIRS aboard NOAA-20 and Suomi-NPP
- **Update Frequency**: Near real-time (within 3 hours of satellite overpass)
- **Coverage**: Global

---

## Troubleshooting

### No Data Returned

If `/api/fires` returns an empty array:

1. Check if ETL has run: Look at `logs/wildfire.log`
2. Manually trigger ETL: `POST /api/etl/run`
3. Verify database has data: `SELECT COUNT(*) FROM wildfire_detections`
4. Check FIRMS API key is valid in `.env`

### Slow Response Times

For large datasets:

1. Use appropriate `limit` parameter
2. Filter by `severity` to reduce results
3. Add database indexes (already included in setup)
4. Consider implementing pagination

### CORS Errors

If frontend can't access API:

1. Check CORS configuration in `main.py`
2. Ensure API is running on correct host/port
3. Update `API_BASE_URL` in frontend code
