# Fire Nexus - Fire Data Ingestion System

## Project Structure
```
fire-ingestion/
├── etl/          # ETL scripts for data ingestion
├── db/           # Database schemas and migrations
├── api/          # API endpoints
├── docker/       # Docker configuration files
├── .env          # Environment variables (not in git)
├── .gitignore    # Git ignore rules
└── README.md     # This file
```

## Setup Instructions

### 1. Initialize Git Repository
```bash
git init
```

### 2. Create Project Folders
```bash
mkdir etl db api docker
```

### 3. Configure Environment Variables
```bash
cp .env.example .env
```
Edit `.env` and add your actual credentials:
- `MAP_KEY`: Get from NASA FIRMS (https://firms.modaps.eosdis.nasa.gov/api/area/)
- `DB_URL`: PostgreSQL connection string

### 4. Install Dependencies
```bash
pip install requests pandas python-dotenv psycopg2-binary
```

### 5. Run ETL Script
```bash
python etl/fire_data.py
```

## Environment Variables
- `MAP_KEY`: NASA FIRMS API key
- `DB_URL`: PostgreSQL database connection string
- `SOURCE`: Fire data source (default: VIIRS_SNPP_NRT)
- `AREA`: Geographic area (default: world)
- `DAY_RANGE`: Number of days to retrieve (default: 1)
