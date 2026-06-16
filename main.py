import os
from typing import Dict, Any, List
from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor

app = FastAPI(title="CPPP Scraper Production API")
security = HTTPBearer()

# Connect to PgBouncer port (6432) instead of Postgres (5432) for connection pooling
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://scraper_user:your_secure_password@127.0.0.1:6432/tender_db")
API_KEY = os.getenv("SCRAPER_API_KEY", "your_secret_token_here")

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")

class DetailPayload(BaseModel):
    internal_id: str
    tender_id: str
    details: Dict[str, Any]

# Efficient connection handling
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
    finally:
        conn.close()

@app.get("/api/tenders", dependencies=[Depends(verify_token)])
def get_tenders_partition(job_index: int, total_jobs: int, conn = Depends(get_db)):
    """
    Queries and returns the assigned tenders for a specific runner index.
    Utilizes SQL Hash Modulo Partitioning to guarantee 0% duplicate overlap
    between parallel runners, independent of boot-time differences.
    """
    if total_jobs <= 0 or job_index < 0 or job_index >= total_jobs:
        raise HTTPException(status_code=400, detail="Invalid partition arguments")
        
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Query pre-indexed partition directly (runs in < 2ms)
        query = """
            SELECT t.internal_id, t.tender_id, t.detail_url
            FROM tenders t
            WHERE t.partition_id = %s
              AND NOT EXISTS (
                  SELECT 1 FROM tender_details d WHERE d.internal_id = t.internal_id
              )
            ORDER BY t.internal_id;
        """
        cursor.execute(query, (job_index,))
        my_tenders = cursor.fetchall()
        
        # 2. Get total remaining count for statistics
        cursor.execute("""
            SELECT count(*) as count 
            FROM tenders t
            LEFT JOIN tender_details d ON t.internal_id = d.internal_id
            WHERE d.internal_id IS NULL;
        """)
        total_stats = cursor.fetchone()
        total_unscraped = total_stats["count"] if total_stats else len(my_tenders)
        
        return {
            "total_unscraped": total_unscraped,
            "partition_size": len(my_tenders),
            "tenders": my_tenders
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()

@app.post("/api/tender-details/check-batch", dependencies=[Depends(verify_token)])
def check_batch_existence(internal_ids: List[str], conn = Depends(get_db)):
    """
    Checks multiple internal_ids in one single request.
    """
    if not internal_ids:
        return {"exists": []}
    
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT internal_id FROM tender_details WHERE internal_id IN %s;",
            (tuple(internal_ids),)
        )
        existing = [row[0] for row in cursor.fetchall()]
        return {"exists": existing}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()

@app.post("/api/tender-details", dependencies=[Depends(verify_token)])
def upload_tender_details(records: List[DetailPayload], conn = Depends(get_db)):
    """
    Performs high-speed bulk insert/upserts.
    """
    if not records:
        return {"status": "success", "inserted": 0}
        
    cursor = conn.cursor()
    import json
    
    query = """
        INSERT INTO tender_details (internal_id, tender_id, details_json)
        VALUES %s
        ON CONFLICT (internal_id) DO UPDATE SET
            details_json = EXCLUDED.details_json,
            scraped_at = CURRENT_TIMESTAMP;
    """
    
    values = [
        (r.internal_id, r.tender_id, json.dumps(r.details))
        for r in records
    ]
    
    try:
        execute_values(cursor, query, values)
        conn.commit()
        return {"status": "success", "inserted": len(records)}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
