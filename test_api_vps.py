#!/usr/bin/env python3
import sys
import requests
import urllib3

# Suppress self-signed certificate warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def test_api(api_url, api_key):
    print(f"[*] Testing connection to FastAPI VPS server at: {api_url}")
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Endpoint URLs
    check_batch_url = f"{api_url.rstrip('/')}/api/tender-details/check-batch"
    upload_url = f"{api_url.rstrip('/')}/api/tender-details"
    
    # 1. Test batch check endpoint
    dummy_ids = ["test_id_1", "test_id_2"]
    print(f"[*] Sending batch check request to: {check_batch_url}...")
    try:
        response = requests.post(check_batch_url, json=dummy_ids, headers=headers, timeout=10, verify=False)
        if response.status_code == 200:
            print(f"[+] SUCCESS: Connected to check-batch endpoint. Response: {response.json()}")
        else:
            print(f"[-] FAILURE: check-batch endpoint returned status code {response.status_code}. Response: {response.text}")
            return
    except Exception as e:
        print(f"[-] ERROR: Failed to connect to check-batch endpoint: {e}")
        return

    # 2. Test batch upload endpoint
    dummy_payload = [
        {
            "internal_id": "test_id_1",
            "tender_id": "TENDER_TEST_9999",
            "details": {
                "Title": "Test Tender",
                "Value": "100000",
                "Status": "Active-Testing"
            }
        }
    ]
    print(f"[*] Sending dummy upload request to: {upload_url}...")
    try:
        response = requests.post(upload_url, json=dummy_payload, headers=headers, timeout=10, verify=False)
        if response.status_code == 200:
            print(f"[+] SUCCESS: Connected to upload endpoint. Response: {response.json()}")
        else:
            print(f"[-] FAILURE: upload endpoint returned status code {response.status_code}. Response: {response.text}")
            return
    except Exception as e:
        print(f"[-] ERROR: Failed to connect to upload endpoint: {e}")
        return

    # 3. Test verification check again
    print("[*] Re-checking batch existence to confirm dummy write succeeded...")
    try:
        response = requests.post(check_batch_url, json=dummy_ids, headers=headers, timeout=10, verify=False)
        if response.status_code == 200:
            existing = response.json().get("exists", [])
            if "test_id_1" in existing:
                print("[+] SUCCESS: Verified database write propagation. Data was stored and queried successfully.")
            else:
                print("[-] FAILURE: Write succeeded but data was not found in check-batch lookup.")
        else:
            print(f"[-] FAILURE: Recheck query failed with status code {response.status_code}.")
    except Exception as e:
        print(f"[-] ERROR: Recheck query failed: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python test_api_vps.py <API_URL> <API_KEY>")
        print("Example: python test_api_vps.py https://154.38.170.134:8000 your_secret_token_here")
        sys.exit(1)
        
    url = sys.argv[1]
    key = sys.argv[2]
    test_api(url, key)
