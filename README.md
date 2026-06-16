# Deep Scrape Tender Details (Stage 2) Pipeline Setup Guide

This guide provides step-by-step instructions for deploying the FastAPI production server on your VPS, configuring PostgreSQL, and running the distributed GitHub Actions scraper pipeline.

---

## 🏛️ Architecture Overview

The system distributes scraping tasks across **20 parallel GitHub Actions runner VMs**. 
* **GHA Runners:** Make requests, rotate proxies, bypass CAPTCHA, parse HTML, and upload clean JSON records to the VPS.
* **VPS API Gateway (FastAPI):** Exposes secure endpoints for querying progress and bulk uploading scraped data.
* **Database (PostgreSQL via PgBouncer):** Stores final details in a queryable `JSONB` column, utilizing connection pooling to handle high write rates.

---

## ⚙️ VPS Setup Instructions (Ubuntu)

### 1. Install System Dependencies
Connect to your VPS via SSH and install the required packages:
```bash
sudo apt update
sudo apt install -y postgresql postgresql-contrib pgbouncer nginx openssl python3-venv python3-pip
```

### 2. Configure PostgreSQL Database
1. Access the PostgreSQL console:
   ```bash
   sudo -i -u postgres psql
   ```
2. Run these SQL commands to initialize the schema:
   ```sql
   CREATE DATABASE tender_db;
   CREATE USER scraper_user WITH PASSWORD 'your_database_password';
   GRANT ALL PRIVILEGES ON DATABASE tender_db TO scraper_user;
   ALTER SCHEMA public OWNER TO scraper_user;
   \c tender_db

   CREATE TABLE tender_details (
       internal_id VARCHAR PRIMARY KEY,
       tender_id VARCHAR NOT NULL,
       details_json JSONB NOT NULL,
       scraped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
   );
   CREATE INDEX idx_tender_details_internal_id ON tender_details (internal_id);
   GRANT ALL PRIVILEGES ON TABLE tender_details TO scraper_user;
   \q
   ```

### 3. Tune PostgreSQL (Optional, but highly recommended for 5M+ writes)
Open `/etc/postgresql/14/main/postgresql.conf` (adjust path for your PG version) and configure:
```ini
shared_buffers = 1500MB
effective_cache_size = 4500MB
synchronous_commit = off      # Boosts write performance significantly
```
Restart PostgreSQL:
```bash
sudo systemctl restart postgresql
```

### 4. Configure PgBouncer (Connection Pooler)
1. Edit `/etc/pgbouncer/pgbouncer.ini`:
   ```ini
   [databases]
   tender_db = host=127.0.0.1 port=5432 dbname=tender_db user=scraper_user auth_user=postgres

   [pgbouncer]
   listen_addr = 127.0.0.1
   listen_port = 6432
   auth_type = md5
   auth_file = /etc/pgbouncer/userlist.txt
   pool_mode = transaction
   max_client_conn = 500
   default_pool_size = 50
   ```
2. Create `/etc/pgbouncer/userlist.txt`:
   ```text
   "scraper_user" "your_database_password"
   ```
3. Start and enable PgBouncer:
   ```bash
   sudo systemctl restart pgbouncer
   sudo systemctl enable pgbouncer
   ```

### 5. Setup the FastAPI Service
1. Copy `main.py` to `~/opt/tender/final/main.py`.
2. Inside `~/opt/tender/final`, set up a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install fastapi uvicorn psycopg2-binary gunicorn pydantic
   ```
3. Generate a secure API token:
   ```bash
   openssl rand -hex 24
   ```
4. Create the systemd service `/etc/systemd/system/tender-api.service`:
   ```ini
   [Unit]
   Description=FastAPI Scraping Backend
   After=network.target postgresql.service pgbouncer.service

   [Service]
   User=root
   WorkingDirectory=/root/opt/tender/final
   ExecStart=/root/opt/tender/final/venv/bin/gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8000
   Restart=always
   Environment="DATABASE_URL=postgresql://scraper_user:your_database_password@127.0.0.1:6432/tender_db"
   Environment="SCRAPER_API_KEY=your_generated_openssl_token"

   [Install]
   WantedBy=multi-user.target
   ```
5. Enable and start the service:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl start tender-api
   sudo systemctl enable tender-api
   ```

### 6. Configure Self-Signed SSL on Nginx
1. Generate certificates (ignoring domain requirements):
   ```bash
   sudo openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /etc/ssl/private/nginx-selfsigned.key -out /etc/ssl/certs/nginx-selfsigned.crt
   ```
2. Create Nginx site configuration `/etc/nginx/sites-available/tender-api`:
   ```nginx
   server {
       listen 80 default_server;
       listen [::]:80 default_server;
       server_name 154.38.170.134; # Replace with your VPS IP
       return 301 https://$host$request_uri;
   }

   server {
       listen 443 ssl default_server;
       listen [::]:443 ssl default_server;
       server_name 154.38.170.134; # Replace with your VPS IP

       ssl_certificate /etc/ssl/certs/nginx-selfsigned.crt;
       ssl_certificate_key /etc/ssl/private/nginx-selfsigned.key;
       ssl_protocols TLSv1.2 TLSv1.3;
       ssl_ciphers HIGH:!aNULL:!MD5;

       location / {
           proxy_pass http://127.0.0.1:8000;
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
       }
   }
   ```
3. Remove Nginx's default site, enable the new configuration, and restart Nginx:
   ```bash
   sudo rm /etc/nginx/sites-enabled/default
   sudo ln -sf /etc/nginx/sites-available/tender-api /etc/nginx/sites-enabled/
   sudo nginx -t
   sudo systemctl restart nginx
   ```

---

## 🏃 GHA Integration & Launch Checklist

1. **Upload Metadata (`tenders.db`) to PostgreSQL:**
   Copy your local `tenders.db` SQLite database to the VPS and run the memory-safe migration script:
   ```bash
   DATABASE_URL="postgresql://scraper_user:your_database_password@127.0.0.1:6432/tender_db" ./venv/bin/python import_tenders.py
   ```
2. **Add GitHub Repository Secrets:**
   In your repository Settings -> Secrets and variables -> Actions, add:
   * `SCRAPER_API_URL` ➔ `https://154.38.170.134` (Use your VPS IP, ensure it starts with `https://`)
   * `SCRAPER_API_KEY` ➔ `your_generated_openssl_token` (The key matches the one set in your systemd service)
3. **Trigger Workflow:**
   Navigate to the **Actions** tab on GitHub, select **Deep Scrape Tender Details (Stage 2)**, and click **Run workflow**.
