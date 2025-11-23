-- Migration script to add last_seen column to existing database
-- Run this if you already have fire_detections table

-- Add last_seen column
ALTER TABLE fire_detections 
ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Initialize last_seen with current detected_at values for existing records
UPDATE fire_detections 
SET last_seen = detected_at 
WHERE last_seen IS NULL;

-- Create index on last_seen for performance
CREATE INDEX IF NOT EXISTS idx_fire_last_seen ON fire_detections(last_seen);

-- Update views to use last_seen (30 minutes) instead of detected_at (24 hours)
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

-- Add comment
COMMENT ON COLUMN fire_detections.last_seen IS 'Last time this fire was detected in NASA FIRMS data fetch';

-- Verify migration
SELECT 
    COUNT(*) as total_records,
    COUNT(last_seen) as records_with_last_seen,
    MIN(last_seen) as earliest_last_seen,
    MAX(last_seen) as latest_last_seen
FROM fire_detections;

-- Show sample records
SELECT id, latitude, longitude, detected_at, last_seen 
FROM fire_detections 
LIMIT 5;

COMMENT ON COLUMN fire_detections.last_seen IS 'Tracks when fire was last seen in NASA data. Fires not updated for 30+ minutes are hidden from active view but kept for historical analysis.';
