import psycopg2
import time
import os

PG_URL = os.getenv("DATABASE_URL", "postgresql://scraper_user:your_secure_password@127.0.0.1:6432/tender_db")
BATCH_SIZE = 100000

def main():
    print("[*] Connecting to PostgreSQL...")
    try:
        conn = psycopg2.connect(PG_URL)
        cursor = conn.cursor()
    except Exception as e:
        print(f"[-] Failed to connect: {e}")
        return

    # 1. Check if column exists, if not add it
    print("[*] Ensuring partition_id column exists...")
    try:
        cursor.execute("ALTER TABLE tenders ADD COLUMN IF NOT EXISTS partition_id INTEGER;")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[-] Failed to add column: {e}")
        conn.close()
        return

    # 2. Update in batches of 100,000 to prevent OOM/crashing
    print("[*] Starting chunked partition_id updates...")
    total_updated = 0
    
    while True:
        try:
            # Query IDs that still need updating
            cursor.execute(
                "SELECT internal_id FROM tenders WHERE partition_id IS NULL LIMIT %s;",
                (BATCH_SIZE,)
            )
            rows = cursor.fetchall()
            if not rows:
                break
                
            ids = [row[0] for row in rows]
            
            # Update partition_id for this batch using MD5 modulo calculation
            cursor.execute("""
                UPDATE tenders 
                SET partition_id = MOD(abs(('x' || substring(md5(internal_id) from 1 for 8))::bit(32)::int), 50)
                WHERE internal_id IN %s;
            """, (tuple(ids),))
            
            conn.commit()
            total_updated += len(ids)
            print(f"[+] Updated {total_updated} rows...")
            time.sleep(0.1)  # Brief pause to let Postgres release lock pages
            
        except Exception as e:
            conn.rollback()
            print(f"[-] Error during update batch: {e}")
            break

    # 3. Create the index
    print("[*] Creating index idx_tenders_partition_id...")
    try:
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tenders_partition_id ON tenders (partition_id);")
        conn.commit()
        print("[+] SUCCESS: Created partition index.")
    except Exception as e:
        conn.rollback()
        print(f"[-] Failed to create index: {e}")
        
    cursor.close()
    conn.close()
    print(f"[+] DONE: Successfully populated partition_id on {total_updated} records.")

if __name__ == "__main__":
    main()
