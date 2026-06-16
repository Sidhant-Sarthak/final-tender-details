import sqlite3
import psycopg2
from psycopg2.extras import execute_values
import os

SQLITE_DB = "tenders.db"
PG_URL = os.getenv("DATABASE_URL", "postgresql://scraper_user:your_secure_password@127.0.0.1:6432/tender_db")
CHUNK_SIZE = 50000

def main():
    if not os.path.exists(SQLITE_DB):
        print(f"[-] SQLite file {SQLITE_DB} not found! Please check path.")
        return
        
    print(f"[*] Connecting to databases...")
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_cursor = sqlite_conn.cursor()
    
    try:
        pg_conn = psycopg2.connect(PG_URL)
        pg_cursor = pg_conn.cursor()
    except Exception as e:
        print(f"[-] Failed to connect to PostgreSQL: {e}")
        sqlite_conn.close()
        return

    # Create schema if it doesn't exist
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
        sqlite_conn.close()
        pg_conn.close()
        return

    # Fetch and insert in chunks using a generator
    print("[*] Starting chunked migration...")
    try:
        sqlite_cursor.execute("SELECT internal_id, tender_id, detail_url, status, organisation_name, title, reference_number, portal_type, serial_number, e_published_date, bid_submission_closing_date, tender_opening_date, corrigendum_url FROM tenders;")
    except Exception as e:
        print(f"[-] Failed to read SQLite database: {e}")
        sqlite_conn.close()
        pg_conn.close()
        return
        
    total_imported = 0
    while True:
        rows = sqlite_cursor.fetchmany(CHUNK_SIZE)
        if not rows:
            break
            
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
            total_imported += len(rows)
            print(f"[+] Imported {total_imported} records...")
        except Exception as e:
            pg_conn.rollback()
            print(f"[-] Error writing chunk: {e}")
            break

    sqlite_conn.close()
    pg_cursor.close()
    pg_conn.close()
    print(f"[+] DONE: Successfully imported a total of {total_imported} records into PostgreSQL.")

if __name__ == "__main__":
    main()
