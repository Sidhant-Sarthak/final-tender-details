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
    Fetches the remaining active tenders that have NOT been scraped yet,
    and returns a specific partition slice for the requested runner.
    """
    if total_jobs <= 0 or job_index < 0 or job_index >= total_jobs:
        raise HTTPException(status_code=400, detail="Invalid partition arguments")
        
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Get active tenders that don't have scraped details yet
        query = """
            SELECT t.internal_id, t.tender_id, t.detail_url
            FROM tenders t
            LEFT JOIN tender_details d ON t.internal_id = d.internal_id
            WHERE t.status = 'active' AND d.internal_id IS NULL
            ORDER BY t.internal_id;
        """
        cursor.execute(query)
        all_tenders = cursor.fetchall()
        
        total_tenders = len(all_tenders)
        if total_tenders == 0:
            return {"tenders": []}
            
        # Slice into chunks
        chunk_size = (total_tenders + total_jobs - 1) // total_jobs
        start_idx = job_index * chunk_size
        end_idx = min(start_idx + chunk_size, total_tenders)
        
        my_tenders = all_tenders[start_idx:end_idx]
        return {
            "total_unscraped": total_tenders,
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
