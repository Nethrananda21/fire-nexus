-- Database Setup Script for Wildfire Detection System
-- Run this script as PostgreSQL superuser

-- Create database
CREATE DATABASE wildfire_db;

-- Connect to the database
\c wildfire_db;

-- Create PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create fire_detections table
CREATE TABLE IF NOT EXISTS fire_detections (
    id SERIAL PRIMARY KEY,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    geom GEOMETRY(POINT, 4326) NOT NULL,
    brightness DOUBLE PRECISION,
    scan DOUBLE PRECISION,
    track DOUBLE PRECISION,
    acq_date VARCHAR(50),
    acq_time VARCHAR(50),
    satellite VARCHAR(50),
    confidence VARCHAR(10),
    version VARCHAR(10),
    bright_t31 DOUBLE PRECISION,
    frp DOUBLE PRECISION,
    daynight VARCHAR(5),
    severity VARCHAR(20),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create spatial index on geometry column
CREATE INDEX idx_fire_geom ON fire_detections USING GIST(geom);

-- Create index on severity for faster filtering
CREATE INDEX idx_fire_severity ON fire_detections(severity);

-- Create index on detected_at for time-based queries
CREATE INDEX idx_fire_detected_at ON fire_detections(detected_at);

-- Create index on last_seen for active fire queries
CREATE INDEX idx_fire_last_seen ON fire_detections(last_seen);

-- Create composite index for common queries
CREATE INDEX idx_fire_severity_date ON fire_detections(severity, detected_at);

-- Create index on acquisition date
CREATE INDEX idx_fire_acq_date ON fire_detections(acq_date);

-- Create unique constraint to prevent duplicates
CREATE UNIQUE INDEX idx_fire_unique ON fire_detections(latitude, longitude, acq_date, acq_time);

-- Create function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_fire_detections_updated_at 
    BEFORE UPDATE ON fire_detections 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Create view for severe fires
CREATE OR REPLACE VIEW severe_fires AS
SELECT 
    id,
    latitude,
    longitude,
    ST_AsGeoJSON(geom)::json as geojson,
    frp,
    confidence,
    acq_date,
    acq_time,
    satellite,
    detected_at,
    last_seen
FROM fire_detections
WHERE severity = 'severe' 
    AND last_seen > NOW() - INTERVAL '30 minutes'
ORDER BY last_seen DESC;

-- Create view for moderate fires
CREATE OR REPLACE VIEW moderate_fires AS
SELECT 
    id,
    latitude,
    longitude,
    ST_AsGeoJSON(geom)::json as geojson,
    frp,
    confidence,
    acq_date,
    acq_time,
    satellite,
    detected_at,
    last_seen
FROM fire_detections
WHERE severity = 'moderate' 
    AND last_seen > NOW() - INTERVAL '30 minutes'
ORDER BY last_seen DESC;

-- Create view for fire statistics
CREATE OR REPLACE VIEW fire_statistics AS
SELECT 
    COUNT(*) as total_fires,
    COUNT(*) FILTER (WHERE severity = 'severe') as severe_fires,
    COUNT(*) FILTER (WHERE severity = 'moderate') as moderate_fires,
    AVG(frp) as avg_frp,
    MAX(frp) as max_frp,
    MAX(last_seen) as last_detection
FROM fire_detections
WHERE last_seen > NOW() - INTERVAL '30 minutes';

-- Create materialized view for spatial clustering analysis
CREATE MATERIALIZED VIEW fire_clusters AS
WITH clustered_fires AS (
    SELECT 
        id,
        latitude,
        longitude,
        geom,
        severity,
        frp,
        ST_ClusterDBSCAN(geom, eps := 0.1, minpoints := 3) OVER() as cluster_id
    FROM fire_detections
    WHERE last_seen > NOW() - INTERVAL '30 minutes'
)
SELECT 
    cluster_id,
    COUNT(*) as fire_count,
    AVG(latitude) as center_lat,
    AVG(longitude) as center_lon,
    ST_Centroid(ST_Collect(geom)) as center_geom,
    MAX(severity) as max_severity,
    AVG(frp) as avg_frp,
    ST_ConvexHull(ST_Collect(geom)) as cluster_boundary
FROM clustered_fires
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id;

-- Create index on materialized view
CREATE INDEX idx_fire_clusters_geom ON fire_clusters USING GIST(center_geom);

-- Function to refresh fire clusters
CREATE OR REPLACE FUNCTION refresh_fire_clusters()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW fire_clusters;
END;
$$ LANGUAGE plpgsql;

-- Create stored procedure for severity classification
-- NOTE: This function uses hardcoded thresholds matching the default .env values:
-- SEVERE_FRP_THRESHOLD=100.0, SEVERE_FRP_WITH_CONFIDENCE=50.0, MODERATE_FRP_THRESHOLD=20.0
-- If you change .env thresholds, update these values accordingly or rely on Python classification
CREATE OR REPLACE FUNCTION classify_fire_severity(
    p_frp DOUBLE PRECISION,
    p_confidence VARCHAR
)
RETURNS VARCHAR AS $$
BEGIN
    -- Severe: FRP > 100 OR (FRP > 50 AND confidence = 'h')
    IF p_frp > 100 OR (p_frp > 50 AND p_confidence = 'h') THEN
        RETURN 'severe';
    -- Moderate: FRP > 20 OR confidence IN ('n', 'h')
    ELSIF p_frp > 20 OR p_confidence IN ('n', 'h') THEN
        RETURN 'moderate';
    ELSE
        RETURN 'moderate';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Create function to get nearby fires
CREATE OR REPLACE FUNCTION get_nearby_fires(
    p_latitude DOUBLE PRECISION,
    p_longitude DOUBLE PRECISION,
    p_radius_km DOUBLE PRECISION DEFAULT 50
)
RETURNS TABLE (
    id INTEGER,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    severity VARCHAR,
    distance_km DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.id,
        f.latitude,
        f.longitude,
        f.severity,
        ST_Distance(
            f.geom::geography,
            ST_SetSRID(ST_MakePoint(p_longitude, p_latitude), 4326)::geography
        ) / 1000 as distance_km
    FROM fire_detections f
    WHERE f.last_seen > NOW() - INTERVAL '30 minutes'
        AND ST_DWithin(
            f.geom::geography,
            ST_SetSRID(ST_MakePoint(p_longitude, p_latitude), 4326)::geography,
            p_radius_km * 1000
        )
    ORDER BY distance_km;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions (adjust username as needed)
-- GRANT ALL PRIVILEGES ON DATABASE wildfire_db TO your_username;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO your_username;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO your_username;

-- Create user and grant permissions (optional)
-- CREATE USER wildfire_user WITH PASSWORD 'your_secure_password';
-- GRANT CONNECT ON DATABASE wildfire_db TO wildfire_user;
-- GRANT USAGE ON SCHEMA public TO wildfire_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO wildfire_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO wildfire_user;

COMMENT ON TABLE fire_detections IS 'Active fire detections from NASA FIRMS VIIRS satellite data';
COMMENT ON COLUMN fire_detections.frp IS 'Fire Radiative Power in MW (megawatts)';
COMMENT ON COLUMN fire_detections.severity IS 'Fire severity classification: severe or moderate';
COMMENT ON COLUMN fire_detections.geom IS 'PostGIS geometry point (SRID 4326)';
COMMENT ON COLUMN fire_detections.last_seen IS 'Last time this fire was detected in NASA FIRMS data fetch';
COMMENT ON VIEW fire_statistics IS 'Real-time statistics of active fires seen in the last 30 minutes';