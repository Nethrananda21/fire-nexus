"""
Database migration script to add last_seen column
Run this to update existing database with Smart TTL feature
"""

import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

def run_migration():
    """Run migration to add last_seen column"""
    print("🔄 Starting database migration...")
    
    try:
        # Connect to database
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Add last_seen column
        print("📝 Adding last_seen column...")
        cur.execute("""
            ALTER TABLE fire_detections 
            ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        """)
        
        # Initialize last_seen with detected_at values
        print("🔄 Initializing last_seen values...")
        cur.execute("""
            UPDATE fire_detections 
            SET last_seen = detected_at 
            WHERE last_seen IS NULL;
        """)
        updated_rows = cur.rowcount
        
        # Create index
        print("📊 Creating index on last_seen...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_last_seen ON fire_detections(last_seen);
        """)
        
        # Update views
        print("🔄 Updating database views...")
        
        # Severe fires view
        cur.execute("""
            CREATE OR REPLACE VIEW severe_fires AS
            SELECT 
                id, latitude, longitude,
                ST_AsGeoJSON(geom)::json as geojson,
                frp, confidence, acq_date, acq_time, satellite,
                detected_at, last_seen
            FROM fire_detections
            WHERE severity = 'severe' 
                AND last_seen > NOW() - INTERVAL '30 minutes'
            ORDER BY last_seen DESC;
        """)
        
        # Moderate fires view
        cur.execute("""
            CREATE OR REPLACE VIEW moderate_fires AS
            SELECT 
                id, latitude, longitude,
                ST_AsGeoJSON(geom)::json as geojson,
                frp, confidence, acq_date, acq_time, satellite,
                detected_at, last_seen
            FROM fire_detections
            WHERE severity = 'moderate' 
                AND last_seen > NOW() - INTERVAL '30 minutes'
            ORDER BY last_seen DESC;
        """)
        
        # Fire statistics view
        cur.execute("""
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
        """)
        
        # Add comment
        cur.execute("""
            COMMENT ON COLUMN fire_detections.last_seen IS 
            'Tracks when fire was last seen in NASA data. Fires not updated for 30+ minutes are hidden from active view.';
        """)
        
        # Commit changes
        conn.commit()
        
        # Verify migration
        print("\n✅ Migration completed successfully!")
        print(f"   Updated {updated_rows} existing records")
        
        # Show statistics
        cur.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(last_seen) as records_with_last_seen,
                MIN(last_seen) as earliest_last_seen,
                MAX(last_seen) as latest_last_seen
            FROM fire_detections;
        """)
        stats = cur.fetchone()
        
        print("\n📊 Database Statistics:")
        print(f"   Total records: {stats[0]}")
        print(f"   Records with last_seen: {stats[1]}")
        if stats[2]:
            print(f"   Earliest last_seen: {stats[2]}")
            print(f"   Latest last_seen: {stats[3]}")
        
        # Show active fires
        cur.execute("""
            SELECT COUNT(*) FROM fire_detections
            WHERE last_seen > NOW() - INTERVAL '30 minutes';
        """)
        active_count = cur.fetchone()[0]
        print(f"\n🔥 Active fires (last 30 min): {active_count}")
        
        # Close connection
        cur.close()
        conn.close()
        
        print("\n✨ Migration complete! Restart FastAPI server to apply changes.")
        print("   Fires will now disappear from map after 30 minutes of not being detected.")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        if conn:
            conn.rollback()
        raise

if __name__ == "__main__":
    run_migration()
