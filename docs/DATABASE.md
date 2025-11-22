# Wildfire Detection System - Database Documentation

Complete database schema and query reference.

## Database Technology

- **RDBMS**: PostgreSQL 12+
- **Extension**: PostGIS 3.0+ (for geospatial functionality)
- **ORM**: SQLAlchemy 2.0
- **Driver**: psycopg2

## Connection String Format

```
postgresql://username:password@host:port/database_name
```

Example:
```
postgresql://postgres:mypassword@localhost:5432/wildfire_db
```

---

## Schema

### Table: `wildfire_detections`

Primary table storing all fire detection records.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | SERIAL | No | Primary key, auto-increment |
| `latitude` | DOUBLE PRECISION | No | Latitude coordinate |
| `longitude` | DOUBLE PRECISION | No | Longitude coordinate |
| `brightness` | DOUBLE PRECISION | Yes | Brightness temperature (Kelvin) |
| `scan` | DOUBLE PRECISION | Yes | Scan pixel size |
| `track` | DOUBLE PRECISION | Yes | Track pixel size |
| `acquisition_date` | TIMESTAMP | No | Detection timestamp |
| `satellite` | VARCHAR(50) | Yes | Satellite name |
| `confidence` | VARCHAR(10) | Yes | Detection confidence |
| `version` | VARCHAR(10) | Yes | FIRMS data version |
| `bright_t31` | DOUBLE PRECISION | Yes | Brightness temperature channel 31 |
| `frp` | DOUBLE PRECISION | Yes | Fire Radiative Power (MW) |
| `daynight` | VARCHAR(1) | Yes | Day (D) or Night (N) |
| `severity` | VARCHAR(20) | Yes | Calculated severity level |
| `geom` | GEOMETRY(Point, 4326) | Yes | PostGIS geometry point |
| `created_at` | TIMESTAMP | No | Record insertion timestamp |

---

## Indexes

Performance-optimized indexes:

```sql
-- Date index (most common query)
CREATE INDEX idx_wildfire_acquisition_date ON wildfire_detections(acquisition_date DESC);

-- Severity index
CREATE INDEX idx_wildfire_severity ON wildfire_detections(severity);

-- Satellite index
CREATE INDEX idx_wildfire_satellite ON wildfire_detections(satellite);

-- Creation timestamp index
CREATE INDEX idx_wildfire_created_at ON wildfire_detections(created_at DESC);

-- Spatial index (for location queries)
CREATE INDEX idx_wildfire_geom ON wildfire_detections USING GIST(geom);

-- Composite index for common queries
CREATE INDEX idx_wildfire_date_severity ON wildfire_detections(acquisition_date DESC, severity);
```

---

## Views

### `recent_fires`

Shows fires from the last 24 hours with GeoJSON geometry.

```sql
CREATE VIEW recent_fires AS
SELECT 
    id,
    latitude,
    longitude,
    brightness,
    acquisition_date,
    satellite,
    confidence,
    frp,
    severity,
    ST_AsGeoJSON(geom) as geojson,
    created_at
FROM wildfire_detections
WHERE acquisition_date >= NOW() - INTERVAL '24 hours'
ORDER BY acquisition_date DESC;
```

**Usage:**
```sql
SELECT * FROM recent_fires WHERE severity = 'critical';
```

### `fire_stats_by_severity`

Aggregated statistics by severity level.

```sql
CREATE VIEW fire_stats_by_severity AS
SELECT 
    severity,
    COUNT(*) as count,
    AVG(brightness) as avg_brightness,
    AVG(frp) as avg_frp,
    MAX(acquisition_date) as latest_detection
FROM wildfire_detections
WHERE acquisition_date >= NOW() - INTERVAL '24 hours'
GROUP BY severity
ORDER BY 
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'moderate' THEN 3
        WHEN 'low' THEN 4
    END;
```

**Usage:**
```sql
SELECT * FROM fire_stats_by_severity;
```

---

## Materialized Views

### `daily_fire_stats`

Pre-computed daily statistics for faster queries.

```sql
CREATE MATERIALIZED VIEW daily_fire_stats AS
SELECT 
    DATE(acquisition_date) as fire_date,
    COUNT(*) as total_fires,
    COUNT(*) FILTER (WHERE severity = 'critical') as critical_fires,
    COUNT(*) FILTER (WHERE severity = 'high') as high_fires,
    COUNT(*) FILTER (WHERE severity = 'moderate') as moderate_fires,
    COUNT(*) FILTER (WHERE severity = 'low') as low_fires,
    AVG(brightness) as avg_brightness,
    AVG(frp) FILTER (WHERE frp IS NOT NULL) as avg_frp,
    MAX(brightness) as max_brightness,
    MAX(frp) as max_frp
FROM wildfire_detections
GROUP BY DATE(acquisition_date)
ORDER BY fire_date DESC;
```

**Refresh the view:**
```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY daily_fire_stats;
```

Or use the function:
```sql
SELECT refresh_daily_stats();
```

---

## Functions

### `get_fires_within_radius()`

Find fires within a specified radius of a point.

**Signature:**
```sql
get_fires_within_radius(
    center_lat DOUBLE PRECISION,
    center_lon DOUBLE PRECISION,
    radius_meters DOUBLE PRECISION
)
```

**Returns:**
- `id`, `latitude`, `longitude`, `brightness`, `acquisition_date`, `severity`, `distance_meters`

**Example:**
```sql
-- Find fires within 50km of Los Angeles
SELECT * FROM get_fires_within_radius(34.0522, -118.2437, 50000);
```

### `cleanup_old_detections()`

Remove fire detection records older than specified days.

**Signature:**
```sql
cleanup_old_detections(days_to_keep INTEGER DEFAULT 90)
```

**Returns:** Number of deleted records

**Example:**
```sql
-- Delete records older than 90 days
SELECT cleanup_old_detections(90);

-- Delete records older than 30 days
SELECT cleanup_old_detections(30);
```

---

## Triggers

### `wildfire_detections_geom_trigger`

Automatically populate the `geom` column from `latitude` and `longitude`.

```sql
CREATE TRIGGER wildfire_detections_geom_trigger
    BEFORE INSERT OR UPDATE ON wildfire_detections
    FOR EACH ROW
    WHEN (NEW.geom IS NULL)
    EXECUTE FUNCTION update_geom_column();
```

This trigger ensures geospatial queries work even if `geom` is not explicitly set.

---

## Common Queries

### Get fires by location

```sql
-- Fires in a bounding box (e.g., California)
SELECT * FROM wildfire_detections
WHERE latitude BETWEEN 32.5 AND 42.0
  AND longitude BETWEEN -124.5 AND -114.0
  AND acquisition_date >= NOW() - INTERVAL '24 hours';
```

### Get fires within distance

```sql
-- Using PostGIS distance
SELECT 
    id,
    latitude,
    longitude,
    severity,
    ST_Distance(
        geom::geography,
        ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography
    ) / 1000 as distance_km
FROM wildfire_detections
WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(-118.2437, 34.0522), 4326)::geography,
    100000  -- 100km in meters
)
ORDER BY distance_km;
```

### Get hottest fires

```sql
SELECT 
    latitude,
    longitude,
    brightness,
    frp,
    severity,
    acquisition_date
FROM wildfire_detections
WHERE acquisition_date >= NOW() - INTERVAL '7 days'
ORDER BY brightness DESC
LIMIT 10;
```

### Get fires by satellite

```sql
SELECT 
    satellite,
    COUNT(*) as fire_count,
    AVG(confidence) as avg_confidence
FROM wildfire_detections
WHERE acquisition_date >= NOW() - INTERVAL '24 hours'
GROUP BY satellite;
```

### Temporal analysis

```sql
-- Fires per hour over last 24 hours
SELECT 
    DATE_TRUNC('hour', acquisition_date) as hour,
    COUNT(*) as fire_count,
    AVG(brightness) as avg_brightness
FROM wildfire_detections
WHERE acquisition_date >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', acquisition_date)
ORDER BY hour DESC;
```

### Severity distribution

```sql
SELECT 
    severity,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM wildfire_detections
WHERE acquisition_date >= NOW() - INTERVAL '24 hours'
GROUP BY severity
ORDER BY 
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'high' THEN 2
        WHEN 'moderate' THEN 3
        WHEN 'low' THEN 4
    END;
```

---

## Maintenance

### Vacuum and Analyze

Regular maintenance for optimal performance:

```sql
-- Analyze table statistics
ANALYZE wildfire_detections;

-- Vacuum to reclaim space
VACUUM wildfire_detections;

-- Full vacuum with analyze
VACUUM ANALYZE wildfire_detections;
```

### Check Table Size

```sql
SELECT 
    pg_size_pretty(pg_total_relation_size('wildfire_detections')) as total_size,
    pg_size_pretty(pg_relation_size('wildfire_detections')) as table_size,
    pg_size_pretty(pg_indexes_size('wildfire_detections')) as indexes_size;
```

### Index Usage Statistics

```sql
SELECT 
    indexrelname as index_name,
    idx_scan as times_used,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
  AND relname = 'wildfire_detections'
ORDER BY idx_scan DESC;
```

---

## Backup and Restore

### Backup

```bash
# Full database backup
pg_dump -U postgres wildfire_db > backup.sql

# Compressed backup
pg_dump -U postgres wildfire_db | gzip > backup.sql.gz

# Table-specific backup
pg_dump -U postgres -t wildfire_detections wildfire_db > detections_backup.sql

# Custom format (recommended for large databases)
pg_dump -U postgres -Fc wildfire_db > backup.dump
```

### Restore

```bash
# From SQL file
psql -U postgres -d wildfire_db < backup.sql

# From compressed file
gunzip < backup.sql.gz | psql -U postgres -d wildfire_db

# From custom format
pg_restore -U postgres -d wildfire_db backup.dump
```

### Automated Backup Script

```bash
#!/bin/bash
# backup_wildfire_db.sh

BACKUP_DIR="/var/backups/wildfire"
DATE=$(date +%Y%m%d_%H%M%S)
FILENAME="wildfire_db_$DATE.sql.gz"

mkdir -p $BACKUP_DIR
pg_dump -U postgres wildfire_db | gzip > "$BACKUP_DIR/$FILENAME"

# Keep only last 7 days of backups
find $BACKUP_DIR -name "wildfire_db_*.sql.gz" -mtime +7 -delete

echo "Backup completed: $FILENAME"
```

Add to crontab for daily backups:
```bash
0 2 * * * /path/to/backup_wildfire_db.sh
```

---

## Performance Tuning

### PostgreSQL Configuration

Edit `postgresql.conf`:

```ini
# Memory
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 4MB

# Checkpoints
checkpoint_completion_target = 0.9
wal_buffers = 16MB

# Query Planning
default_statistics_target = 100
random_page_cost = 1.1
effective_io_concurrency = 200

# Write Ahead Log
min_wal_size = 1GB
max_wal_size = 4GB
```

### Connection Pooling

Use pgBouncer for connection pooling:

```ini
# pgbouncer.ini
[databases]
wildfire_db = host=localhost port=5432 dbname=wildfire_db

[pgbouncer]
pool_mode = transaction
max_client_conn = 100
default_pool_size = 20
```

---

## Data Retention

Implement data retention policy:

```sql
-- Keep only 90 days of data
SELECT cleanup_old_detections(90);

-- Or schedule with cron/scheduler
-- Run daily at 3 AM
```

Add to application scheduler:

```python
scheduler.add_job(
    lambda: cleanup_old_detections(90),
    'cron',
    hour=3,
    id='cleanup_old_data'
)
```

---

## Monitoring

### Active Connections

```sql
SELECT count(*) FROM pg_stat_activity;
```

### Long Running Queries

```sql
SELECT 
    pid,
    now() - query_start as duration,
    query
FROM pg_stat_activity
WHERE state = 'active'
  AND now() - query_start > interval '1 minute'
ORDER BY duration DESC;
```

### Table Bloat

```sql
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
    n_live_tup,
    n_dead_tup,
    ROUND(100 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_ratio
FROM pg_stat_user_tables
WHERE schemaname = 'public'
ORDER BY n_dead_tup DESC;
```
