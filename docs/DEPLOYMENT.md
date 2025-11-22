# Wildfire Detection System - Deployment Guide

Production deployment guide for the Wildfire Detection System.

## Table of Contents

1. [Deployment Overview](#deployment-overview)
2. [Server Requirements](#server-requirements)
3. [Deployment Options](#deployment-options)
4. [Production Configuration](#production-configuration)
5. [Security Hardening](#security-hardening)
6. [Monitoring and Logging](#monitoring-and-logging)
7. [Backup Strategy](#backup-strategy)
8. [Scaling Considerations](#scaling-considerations)

---

## Deployment Overview

### Architecture

```
┌─────────────┐
│   Nginx     │  (Reverse Proxy + SSL)
│   Port 80   │
│   Port 443  │
└──────┬──────┘
       │
┌──────▼──────┐
│   Uvicorn   │  (FastAPI Application)
│   Port 8000 │
└──────┬──────┘
       │
┌──────▼──────┐
│ PostgreSQL  │  (Database + PostGIS)
│   Port 5432 │
└─────────────┘
```

### Components

- **Web Server**: Nginx (reverse proxy, SSL termination, static files)
- **App Server**: Uvicorn (ASGI server for FastAPI)
- **Database**: PostgreSQL with PostGIS
- **Process Manager**: Systemd or Supervisor
- **Firewall**: UFW (Ubuntu) or firewalld (CentOS)

---

## Server Requirements

### Minimum Requirements

- **OS**: Ubuntu 20.04/22.04 LTS or CentOS 8+
- **CPU**: 2 cores
- **RAM**: 4GB
- **Disk**: 20GB SSD
- **Network**: 100 Mbps

### Recommended for Production

- **CPU**: 4+ cores
- **RAM**: 8GB+
- **Disk**: 50GB+ SSD
- **Network**: 1 Gbps
- **Backup**: Separate backup storage

---

## Deployment Options

### Option 1: Traditional VPS (Recommended for Beginners)

Providers: DigitalOcean, Linode, Vultr, AWS EC2

**Pros:**
- Full control
- Predictable costs
- Easy to understand

**Cons:**
- Manual management required
- Single server (until you scale)

### Option 2: Docker Containers

**Pros:**
- Isolated environments
- Easy to replicate
- Portable

**Cons:**
- Additional complexity
- Resource overhead

### Option 3: Cloud Platforms (PaaS)

Providers: Heroku, Google App Engine, AWS Elastic Beanstalk

**Pros:**
- Managed infrastructure
- Auto-scaling
- Less operational overhead

**Cons:**
- Higher costs
- Less control
- Vendor lock-in

---

## Production Configuration

### 1. Server Setup (Ubuntu 22.04)

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install dependencies
sudo apt install -y python3.10 python3-pip python3-venv
sudo apt install -y postgresql postgresql-contrib postgis
sudo apt install -y nginx
sudo apt install -y git certbot python3-certbot-nginx

# Create application user
sudo adduser --system --group --home /opt/wildfire wildfire

# Create application directory
sudo mkdir -p /opt/wildfire
sudo chown wildfire:wildfire /opt/wildfire
```

### 2. Application Deployment

```bash
# Switch to application user
sudo su - wildfire

# Clone repository
cd /opt/wildfire
git clone <repository-url> app
cd app

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Configure environment
cp .env.example .env
nano .env  # Edit with production values

# Create required directories
mkdir -p logs
chmod 755 logs
```

### 3. Database Setup

```bash
# Create database and user
sudo -u postgres psql << EOF
CREATE USER wildfire_prod WITH PASSWORD 'secure_production_password';
CREATE DATABASE wildfire_prod OWNER wildfire_prod;
\c wildfire_prod
CREATE EXTENSION postgis;
GRANT ALL PRIVILEGES ON DATABASE wildfire_prod TO wildfire_prod;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO wildfire_prod;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO wildfire_prod;
EOF

# Run database setup
sudo -u postgres psql -d wildfire_prod -f database/setup_database.sql

# Configure PostgreSQL for production
sudo nano /etc/postgresql/14/main/postgresql.conf
```

**PostgreSQL production settings:**

```ini
# Connection Settings
listen_addresses = 'localhost'
max_connections = 100

# Memory Settings
shared_buffers = 1GB
effective_cache_size = 3GB
maintenance_work_mem = 256MB
work_mem = 10MB

# Write Ahead Log
wal_buffers = 16MB
checkpoint_completion_target = 0.9
min_wal_size = 1GB
max_wal_size = 4GB

# Query Tuning
random_page_cost = 1.1
effective_io_concurrency = 200
```

Restart PostgreSQL:
```bash
sudo systemctl restart postgresql
```

### 4. Systemd Service

Create `/etc/systemd/system/wildfire-api.service`:

```ini
[Unit]
Description=Wildfire Detection API
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=wildfire
Group=wildfire
WorkingDirectory=/opt/wildfire/app
Environment="PATH=/opt/wildfire/app/venv/bin"
ExecStart=/opt/wildfire/app/venv/bin/uvicorn main:app \
    --host 127.0.0.1 \
    --port 8000 \
    --workers 4 \
    --log-level info \
    --access-log \
    --proxy-headers \
    --forwarded-allow-ips='*'

# Restart policy
Restart=always
RestartSec=10

# Security
NoNewPrivileges=true
PrivateTmp=true

# Logging
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

**Enable and start the service:**

```bash
sudo systemctl daemon-reload
sudo systemctl enable wildfire-api
sudo systemctl start wildfire-api
sudo systemctl status wildfire-api
```

### 5. Nginx Configuration

Create `/etc/nginx/sites-available/wildfire`:

```nginx
# Rate limiting
limit_req_zone $binary_remote_addr zone=api_limit:10m rate=100r/m;

# Upstream
upstream wildfire_api {
    server 127.0.0.1:8000 fail_timeout=0;
}

# HTTP -> HTTPS redirect
server {
    listen 80;
    listen [::]:80;
    server_name yourdomain.com www.yourdomain.com;
    
    location /.well-known/acme-challenge/ {
        root /var/www/html;
    }
    
    location / {
        return 301 https://$server_name$request_uri;
    }
}

# HTTPS server
server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name yourdomain.com www.yourdomain.com;
    
    # SSL configuration (Certbot will add these)
    ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_prefer_server_ciphers on;
    ssl_ciphers 'ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256';
    ssl_session_cache shared:SSL:10m;
    ssl_session_timeout 10m;
    
    # Security headers
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;
    
    # Logging
    access_log /var/log/nginx/wildfire_access.log;
    error_log /var/log/nginx/wildfire_error.log;
    
    # API endpoints
    location /api/ {
        limit_req zone=api_limit burst=20 nodelay;
        
        proxy_pass http://wildfire_api;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }
    
    # Health check
    location /health {
        proxy_pass http://wildfire_api;
        proxy_set_header Host $host;
        access_log off;
    }
    
    # API docs
    location ~ ^/(docs|redoc|openapi.json) {
        proxy_pass http://wildfire_api;
        proxy_set_header Host $host;
    }
    
    # Frontend
    location / {
        root /opt/wildfire/app/frontend;
        index index.html;
        try_files $uri $uri/ /index.html;
        
        # Cache static files
        location ~* \.(jpg|jpeg|png|gif|ico|css|js)$ {
            expires 30d;
            add_header Cache-Control "public, immutable";
        }
    }
    
    # Deny access to hidden files
    location ~ /\. {
        deny all;
    }
}
```

**Enable the site:**

```bash
sudo ln -s /etc/nginx/sites-available/wildfire /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

### 6. SSL Certificate

```bash
# Get SSL certificate from Let's Encrypt
sudo certbot --nginx -d yourdomain.com -d www.yourdomain.com

# Test auto-renewal
sudo certbot renew --dry-run
```

### 7. Firewall Configuration

```bash
# Install UFW
sudo apt install ufw

# Allow SSH, HTTP, HTTPS
sudo ufw allow 22/tcp
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Enable firewall
sudo ufw enable
sudo ufw status
```

---

## Security Hardening

### 1. Environment Variables

Never commit `.env` to version control. Use secure values:

```env
DATABASE_URL=postgresql://wildfire_prod:$(openssl rand -base64 32)@localhost:5432/wildfire_prod
FIRMS_API_KEY=your_actual_firms_api_key
```

### 2. Database Security

```sql
-- Restrict database user permissions
REVOKE ALL ON DATABASE wildfire_prod FROM PUBLIC;
GRANT CONNECT ON DATABASE wildfire_prod TO wildfire_prod;

-- Use read-only user for queries if needed
CREATE USER wildfire_readonly WITH PASSWORD 'readonly_password';
GRANT CONNECT ON DATABASE wildfire_prod TO wildfire_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO wildfire_readonly;
```

**Configure pg_hba.conf:**

```
# /etc/postgresql/14/main/pg_hba.conf
local   wildfire_prod   wildfire_prod                   md5
host    wildfire_prod   wildfire_prod   127.0.0.1/32   md5
```

### 3. Application Security

```python
# In config.py - use secrets management
from secrets import token_urlsafe

# Generate secure session key
SESSION_SECRET = token_urlsafe(32)

# CORS - restrict origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
```

### 4. Rate Limiting

Install slowapi:

```bash
pip install slowapi
```

Add to `main.py`:

```python
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

@app.get("/api/fires")
@limiter.limit("100/minute")
async def get_fires(...):
    ...
```

### 5. Fail2Ban

Protect against brute force attacks:

```bash
sudo apt install fail2ban

# Create jail for nginx
sudo nano /etc/fail2ban/jail.local
```

```ini
[nginx-limit-req]
enabled = true
filter = nginx-limit-req
logpath = /var/log/nginx/wildfire_error.log
maxretry = 5
findtime = 300
bantime = 3600
```

---

## Monitoring and Logging

### 1. Application Logs

View logs:

```bash
# Application logs
sudo journalctl -u wildfire-api -f

# Nginx access logs
sudo tail -f /var/log/nginx/wildfire_access.log

# Nginx error logs
sudo tail -f /var/log/nginx/wildfire_error.log

# Application file logs
sudo tail -f /opt/wildfire/app/logs/wildfire.log
```

### 2. Log Rotation

Create `/etc/logrotate.d/wildfire`:

```
/opt/wildfire/app/logs/*.log {
    daily
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 wildfire wildfire
    sharedscripts
    postrotate
        systemctl reload wildfire-api > /dev/null 2>&1 || true
    endscript
}
```

### 3. Monitoring with Prometheus (Optional)

```bash
pip install prometheus-fastapi-instrumentator
```

Add to `main.py`:

```python
from prometheus_fastapi_instrumentator import Instrumentator

@app.on_event("startup")
async def startup():
    Instrumentator().instrument(app).expose(app)
```

---

## Backup Strategy

### 1. Database Backups

Create `/opt/wildfire/scripts/backup_db.sh`:

```bash
#!/bin/bash

BACKUP_DIR="/var/backups/wildfire"
DATE=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=30

mkdir -p $BACKUP_DIR

# Backup database
pg_dump -U wildfire_prod -h localhost wildfire_prod | gzip > "$BACKUP_DIR/wildfire_db_$DATE.sql.gz"

# Remove old backups
find $BACKUP_DIR -name "wildfire_db_*.sql.gz" -mtime +$RETENTION_DAYS -delete

echo "Backup completed: wildfire_db_$DATE.sql.gz"
```

Make executable and add to cron:

```bash
chmod +x /opt/wildfire/scripts/backup_db.sh

# Add to crontab (daily at 2 AM)
sudo crontab -e
0 2 * * * /opt/wildfire/scripts/backup_db.sh
```

### 2. Off-site Backups

Use rsync or cloud storage:

```bash
# Rsync to remote server
rsync -avz /var/backups/wildfire/ user@backup-server:/backups/wildfire/

# Or use AWS S3
aws s3 sync /var/backups/wildfire/ s3://your-bucket/wildfire-backups/
```

---

## Scaling Considerations

### Horizontal Scaling

**Load Balancer Setup:**

```nginx
upstream wildfire_cluster {
    least_conn;
    server 10.0.1.10:8000;
    server 10.0.1.11:8000;
    server 10.0.1.12:8000;
}
```

### Database Replication

Set up PostgreSQL streaming replication for read scaling.

### Caching Layer

Add Redis for caching:

```python
import redis
from functools import lru_cache

redis_client = redis.Redis(host='localhost', port=6379, db=0)

@lru_cache(maxsize=128)
def get_cached_stats(hours: int):
    cache_key = f"stats:{hours}"
    cached = redis_client.get(cache_key)
    if cached:
        return json.loads(cached)
    # ... fetch from database
    redis_client.setex(cache_key, 300, json.dumps(data))
    return data
```

---

## Maintenance

### Update Application

```bash
sudo su - wildfire
cd /opt/wildfire/app
git pull
source venv/bin/activate
pip install -r requirements.txt
exit

sudo systemctl restart wildfire-api
```

### Database Maintenance

```bash
# Run weekly
sudo -u postgres psql wildfire_prod -c "VACUUM ANALYZE;"

# Run monthly
sudo -u postgres psql wildfire_prod -c "REINDEX DATABASE wildfire_prod;"
```
