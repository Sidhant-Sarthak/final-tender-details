#!/usr/bin/env python3
import os
import re
import sys
import time
import json
import base64
import random
import logging
import sqlite3
import threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
import requests
import urllib3

# Suppress SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
SOURCE_DB = "../tenders.db"  # Path to the source metadata DB relative to final-tender-details
LOG_FILE = "../logs/scrape_details.log"
MAX_THREADS = 10
MAX_RETRIES = 5
BACKOFF_FACTOR = 1.5

# GHA Graceful time limit (5.5 hours)
START_TIME = time.time()
MAX_RUN_TIME = 5.5 * 3600  # 5.5 hours in seconds

# VPS API configuration from environment variables
API_URL = os.getenv("SCRAPER_API_URL", "https://154.38.170.134:8000")
API_KEY = os.getenv("SCRAPER_API_KEY", "your_secret_token_here")

# Static MD5 key used by eprocure globally to verify CAPTCHA solves
BYPASS_KEY_B64 = "OGQ2NzAxYTMwZTJhNTIxMGNiNmEwM2EzNmNhYWZhODk="

# Base search URL used as referer
BASE_REFERER = "https://eprocure.gov.in/cppp/tendersearch/cpppdata/bydGVuZGVyQTEzaDFBRFZBTkNFRCBXRUFQT05TIEFORCBFUVVJUE1FTlQgSU5ESUEgTFRELUFXRUlMQTEzaDFzZWxlY3RBMTNoMW51bGxBMTNoMW51bGw="

# Configure logging
os.makedirs("../logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8")
    ]
)
logger = logging.getLogger("ScrapeDetails")

# DB locks and single writer queue to send writes to VPS
write_queue = Queue()

def api_writer_worker():
    """Consumes write_queue and sends records to the VPS API in batches."""
    logger.info("API writer thread started.")
    
    batch_size = 50
    batch = []
    last_commit = time.time()
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    endpoint = f"{API_URL.rstrip('/')}/api/tender-details"
    
    def send_batch(records_batch):
        for attempt in range(3):
            try:
                # verify=False ignores self-signed certificate warnings
                response = requests.post(endpoint, json=records_batch, headers=headers, timeout=25, verify=False)
                if response.status_code == 200:
                    logger.info(f"Successfully uploaded batch of {len(records_batch)} records to VPS.")
                    return True
                else:
                    logger.error(f"VPS returned HTTP {response.status_code}: {response.text}")
            except Exception as e:
                logger.error(f"Error sending batch to VPS: {e}")
            time.sleep(2 ** attempt)
        return False

    while True:
        try:
            item = write_queue.get(timeout=1.0)
            if item is None:  # Sentinel value to exit
                break
            
            batch.append(item)
            
            if len(batch) >= batch_size or (time.time() - last_commit > 2.0 and batch):
                send_batch(batch)
                batch = []
                last_commit = time.time()
                
            write_queue.task_done()
        except Empty:
            if batch:
                send_batch(batch)
                batch = []
                last_commit = time.time()
            continue
            
    # Final flush
    if batch:
        send_batch(batch)
    logger.info("API writer thread stopped.")

def get_already_scraped_batch(internal_ids: List[str]) -> set:
    """Queries the VPS in one bulk batch request to check which IDs already exist."""
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    endpoint = f"{API_URL.rstrip('/')}/api/tender-details/check-batch"
    
    for attempt in range(3):
        try:
            response = requests.post(endpoint, json=internal_ids, headers=headers, timeout=30, verify=False)
            if response.status_code == 200:
                return set(response.json().get("exists", []))
        except Exception as e:
            logger.error(f"Error checking batch existence from VPS: {e}")
        time.sleep(2 ** attempt)
    return set()

def construct_bypass_url(detail_url):
    """Transforms captcha-bound URL into a bypassed URL."""
    try:
        url_base, b64_hash = detail_url.split("/tendersfullview/")
        parts = b64_hash.split("A13h1")
        
        # Update timestamp block (index 3) with current Unix epoch
        current_ts = str(int(time.time()))
        parts[3] = base64.b64encode(current_ts.encode('utf-8')).decode('utf-8')
        
        # Append or replace the bypass token
        if len(parts) == 6:
            parts.append(BYPASS_KEY_B64)
        elif len(parts) > 6:
            parts[6] = BYPASS_KEY_B64
            
        return f"{url_base}/tendersfullview/{'A13h1'.join(parts)}"
    except Exception as e:
        logger.error(f"Error constructing bypass URL from {detail_url}: {e}")
        return detail_url

def parse_detail_page(html_content):
    """Parses table details from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")
    details = {}
    
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            cell_texts = [c.get_text(separator=" ", strip=True) for c in cells]
            if not cell_texts:
                continue
                
            has_colon = ":" in cell_texts
            if has_colon:
                idx = 0
                while idx < len(cell_texts):
                    if cell_texts[idx] == ":":
                        if idx > 0 and idx + 1 < len(cell_texts):
                            key = cell_texts[idx - 1].strip()
                            val = cell_texts[idx + 1].strip()
                            if key:
                                details[key] = val
                        idx += 2
                    else:
                        idx += 1
            elif len(cell_texts) == 2:
                key = cell_texts[0].strip()
                val = cell_texts[1].strip()
                if key:
                    details[key] = val
                    
    cleaned_details = {}
    for k, v in details.items():
        if k and k != ":" and not k.startswith("*"):
            cleaned_details[k] = v
            
    return cleaned_details

def worker_thread(row, stats):
    """Processes a single tender row, parses, and queues writes."""
    internal_id, tender_id, detail_url = row
    
    # Check if we are approaching the GHA runner time limit
    if time.time() - START_TIME > MAX_RUN_TIME:
        return
        
    bypass_url = construct_bypass_url(detail_url)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": BASE_REFERER
    }
    
    session = requests.Session()
    session.cookies.set("cookieWorked", "yes", domain="eprocure.gov.in", path="/")
    
    try:
        session.get(BASE_REFERER, headers=headers, timeout=10)
    except Exception:
        pass
        
    retries = 0
    success = False
    
    while retries < MAX_RETRIES:
        if time.time() - START_TIME > MAX_RUN_TIME:
            return
            
        try:
            time.sleep(random.uniform(0.1, 0.3))
            response = session.get(bypass_url, headers=headers, timeout=15)
            
            if response.status_code == 429 or response.status_code >= 500:
                time.sleep((BACKOFF_FACTOR ** retries) + random.uniform(1.0, 3.0))
                retries += 1
                continue
                
            if response.status_code == 200:
                if "tendersfullview" in response.url or "Organisation Name" in response.text or "Tender Title" in response.text:
                    parsed_data = parse_detail_page(response.text)
                    if parsed_data:
                        # Queue payload for API writer thread
                        write_queue.put({
                            "internal_id": internal_id,
                            "tender_id": tender_id,
                            "details": parsed_data
                        })
                        success = True
                        break
            
            retries += 1
            time.sleep(random.uniform(0.5, 1.5))
        except Exception:
            retries += 1
            time.sleep((BACKOFF_FACTOR ** retries) + random.uniform(0.5, 1.5))
            
    with stats["lock"]:
        if success:
            stats["success"] += 1
        else:
            stats["failed"] += 1
        stats["processed"] += 1
        total = stats["total"]
        processed = stats["processed"]
        if processed % 100 == 0 or processed == total:
            logger.info(f"Progress: {processed}/{total} processed ({stats['success']} successful, {stats['failed']} failed)")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Distributed Deep Details Scraper")
    parser.add_argument("--job-index", type=int, default=0, help="0-based runner index")
    parser.add_argument("--total-jobs", type=int, default=20, help="Total number of runners")
    args = parser.parse_args()
    
    if not os.path.exists(SOURCE_DB):
        logger.error(f"Source database {SOURCE_DB} not found!")
        sys.exit(1)
        
    # Fetch all active metadata from local SQLite DB
    conn = sqlite3.connect(SOURCE_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT internal_id, tender_id, detail_url FROM tenders WHERE status = 'active' ORDER BY internal_id")
    all_tenders = cursor.fetchall()
    conn.close()
    
    # 1. Distribute tasks dynamically via job-index partition allocation
    total_tenders = len(all_tenders)
    chunk_size = (total_tenders + args.total_jobs - 1) // args.total_jobs
    start_idx = args.job_index * chunk_size
    end_idx = min(start_idx + chunk_size, total_tenders)
    my_tenders = all_tenders[start_idx:end_idx]
    
    logger.info(f"Runner {args.job_index}/{args.total_jobs} assigned to tenders range: {start_idx} to {end_idx} (Total: {len(my_tenders)})")
    
    if not my_tenders:
        logger.info("No tasks assigned to this runner.")
        return
        
    # 2. Query VPS in batch requests to check which assigned IDs already exist in Postgres
    logger.info("Checking existing records against VPS...")
    my_ids = [row[0] for row in my_tenders]
    
    # Split queries into batches of 500 to keep HTTP payloads safe
    scraped_ids = set()
    for i in range(0, len(my_ids), 500):
        batch = my_ids[i:i+500]
        scraped_ids.update(get_already_scraped_batch(batch))
        
    # Filter out already scraped records
    tenders_to_scrape = [t for t in my_tenders if t[0] not in scraped_ids]
    total_to_scrape = len(tenders_to_scrape)
    
    logger.info(f"Already scraped: {len(scraped_ids)} | Left to scrape: {total_to_scrape}")
    
    if total_to_scrape == 0:
        logger.info("All tenders in this partition are already scraped.")
        return
        
    # Start database writer thread
    writer_thread = threading.Thread(target=api_writer_worker, name="APIWriter")
    writer_thread.daemon = True
    writer_thread.start()
    
    stats = {
        "total": total_to_scrape,
        "processed": 0,
        "success": 0,
        "failed": 0,
        "lock": threading.Lock()
    }
    
    logger.info(f"Starting ThreadPoolExecutor with {MAX_THREADS} threads...")
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_THREADS, thread_name_prefix="ScraperThread") as executor:
            futures = [executor.submit(worker_thread, row, stats) for row in tenders_to_scrape]
            for fut in futures:
                if time.time() - START_TIME > MAX_RUN_TIME:
                    logger.warning("Approaching GHA runner limit. Exiting pool execution.")
                    break
                fut.result()
    except KeyboardInterrupt:
        logger.warning("Scraper interrupted by user.")
    finally:
        # Stop database writer thread
        write_queue.put(None)
        writer_thread.join()
        logger.info(f"Job runner completed. Successful: {stats['success']}, Failed: {stats['failed']}")

if __name__ == "__main__":
    main()
