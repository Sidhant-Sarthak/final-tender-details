import sqlite3
import psycopg2
from psycopg2.extras import execute_values
import os

# Path to your SQLite DB on the VPS
SQLITE_DB = "tenders.db" # Update if named differently, e.g. "tenders_replicated.db"

# Connection to PostgreSQL (reads the same environment variable as FastAPI)
PG_URL = os.getenv("DATABASE_URL", "postgresql://scraper_user:your_secure_password@127.0.0.1:6432/tender_db")

def main():
    print(f"[*] Reading metadata from SQLite: {SQLITE_DB}")
    if not os.path.exists(SQLITE_DB):
        print(f"[-] SQLite file {SQLITE_DB} not found! Please check path.")
        return
        
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_cursor = sqlite_conn.cursor()

    # 1. Fetch metadata columns from SQLite
    try:
        sqlite_cursor.execute("SELECT internal_id, tender_id, detail_url, status, organisation_name, title, reference_number, portal_type, serial_number, e_published_date, bid_submission_closing_date, tender_opening_date, corrigendum_url FROM tenders;")
        rows = sqlite_cursor.fetchall()
        print(f"[+] Loaded {len(rows)} tender records from SQLite.")
    except Exception as e:
        print(f"[-] Failed to read SQLite database: {e}")
        sqlite_conn.close()
        return

    sqlite_conn.close()

    # 2. Connect to PostgreSQL
    print("[*] Connecting to PostgreSQL...")
    try:
        pg_conn = psycopg2.connect(PG_URL)
        pg_cursor = pg_conn.cursor()
    except Exception as e:
        print(f"[-] Failed to connect to PostgreSQL: {e}")
        return

    # Create the tenders table in PostgreSQL if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS tenders (
        internal_id VARCHAR PRIMARY KEY,
        tender_id VARCHAR,
        detail_url TEXT,
        status VARCHAR,
        organisation_name VARCHAR,
        title TEXT,
        reference_number VARCHAR,
        portal_type VARCHAR,
        serial_number VARCHAR,
        e_published_date VARCHAR,
        bid_submission_closing_date VARCHAR,
        tender_opening_date VARCHAR,
        corrigendum_url TEXT,
        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_tenders_status ON tenders (status);
    """
    try:
        pg_cursor.execute(create_table_query)
        pg_conn.commit()
    except Exception as e:
        print(f"[-] Failed to create tenders schema in PostgreSQL: {e}")
        pg_conn.close()
        return

    # 3. Perform bulk insert
    print("[*] Performing bulk insert into PostgreSQL...")
    insert_query = """
        INSERT INTO tenders (
            internal_id, tender_id, detail_url, status, organisation_name, title, 
            reference_number, portal_type, serial_number, e_published_date, 
            bid_submission_closing_date, tender_opening_date, corrigendum_url
        ) VALUES %s
        ON CONFLICT (internal_id) DO NOTHING;
    """

    try:
        execute_values(pg_cursor, insert_query, rows)
        pg_conn.commit()
        print("[+] SUCCESS: Imported all metadata records into PostgreSQL.")
    except Exception as e:
        pg_conn.rollback()
        print(f"[-] Error writing to PostgreSQL: {e}")
    finally:
        pg_cursor.close()
        pg_conn.close()

if __name__ == "__main__":
    main()
