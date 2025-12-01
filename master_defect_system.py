import sqlite3
import requests
import json
from requests.auth import HTTPBasicAuth
import os
import sys

INVENTORY_API_URL = "https://dummyjson.com/products"
DB_FILE = "supply_chain_inventory.db"


JIRA_URL    = os.environ.get("JIRA_URL") 
JIRA_EMAIL  = os.environ.get("JIRA_EMAIL")
PROJECT_KEY = os.environ.get("PROJECT_KEY")
JIRA_TOKEN  = os.environ.get("JIRA_TOKEN")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def setup_stale_database():
    print("\n[1/3] Building Local Database...")
    
    try:
        response = requests.get(INVENTORY_API_URL, headers=HEADERS)
        response.raise_for_status() 

        data = response.json()
        products = data['products'] 
        
    except Exception as e:
        print(f"CRITICAL API ERROR: {e}")
        print("Stopping execution to prevent database corruption.")
        sys.exit(1)
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS products")
    cursor.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            title TEXT,
            price REAL
        )
    ''')

    for item in products:
        pid = item['id']
        price = item['price']

        if pid == 1:
            price = 999.99 
            
        cursor.execute("INSERT INTO products VALUES (?, ?, ?)", (pid, item['title'], price))
    
    conn.commit()
    conn.close()
    print("Database created with 1 intentional defect.")

def run_integrity_scan():
    print("\n[2/3] Scanning for Data Mismatches...")
    
    if not os.path.exists(DB_FILE):
        print("Error: Database file not found. Setup failed.")
        return []

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    defects = []
    cursor.execute("SELECT id, price FROM products")
    local_data = cursor.fetchall()
    
 
    print(f"      Checking first 5 of {len(local_data)} records...")
    
    for row in local_data[:5]: 
        local_id = row[0]
        local_price = row[1]
        
        try:
            api_resp = requests.get(f"{INVENTORY_API_URL}/{local_id}", headers=HEADERS)
            if api_resp.status_code == 200:
                live_price = api_resp.json()['price']
                
                if local_price != live_price:
                    msg = f"Price Mismatch for Item {local_id}. DB: ${local_price}, API: ${live_price}"
                    print(f"Found Issue: {msg}")
                    defects.append({
                        "id": local_id,
                        "desc": msg,
                        "expected": live_price,
                        "actual": local_price
                    })
        except Exception as e:
            print(f"Error checking Item {local_id}: {e}")
            
    conn.close()
    return defects

def log_defects_to_jira(defects_list):
    print(f"\n[3/3] Auto-Logging {len(defects_list)} Tickets to Jira...")
    
    if not defects_list:
        print("No defects found. System healthy.")
        return
        
    if not JIRA_TOKEN:
        print("Skipping Jira Log: No Token Found (Running locally?)")
        return

    url = f"{JIRA_URL}/rest/api/3/issue"
    auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_TOKEN)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    for defect in defects_list:
        payload = json.dumps({
            "fields": {
                "project": {"key": PROJECT_KEY},
                "summary": f"[Auto-Alert] Data Integrity Fail: Item {defect['id']}",
                "description": {
                    "type": "doc",
                    "version": 1,
                    "content": [{
                        "type": "paragraph",
                        "content": [{
                            "type": "text",
                            "text": f"{defect['desc']}. Please trigger inventory sync."
                        }]
                    }]
                },
                "issuetype": {"name": "Task"}, 
                "priority": {"name": "High"}
            }
        })

        response = requests.post(url, data=payload, headers=headers, auth=auth)
        
        if response.status_code == 201:
            key = response.json()['key']
            print(f"Jira Ticket Created: {key}")
        else:
            print(f"Failed to create ticket. Code: {response.status_code}")
            print(f"Response: {response.text}")

if __name__ == "__main__":
    print("="*50)
    print("STARTING AUTOMATED DEFECT LIFECYCLE SYSTEM")
    print("="*50)
    
    setup_stale_database()
    found_defects = run_integrity_scan()
    log_defects_to_jira(found_defects)
    
    print("\n" + "="*50)
    print("PROCESS COMPLETE")
    print("="*50)
