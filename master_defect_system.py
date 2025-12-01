import sqlite3
import requests
import json
from requests.auth import HTTPBasicAuth
import time
import os

INVENTORY_API_URL = "https://fakestoreapi.com/products"
DB_FILE = "supply_chain_inventory.db"

JIRA_URL   = os.environ.get("JIRA_URL")
JIRA_EMAIL = os.environ.get("JIRA_EMAIL")
PROJECT_KEY = os.environ.get("PROJECT_KEY")
JIRA_TOKEN  = os.environ.get("JIRA_TOKEN")

if not JIRA_TOKEN:
    raise ValueError("Error: JIRA_TOKEN is missing! Set it in GitHub Secrets.")

def setup_stale_database():
    print("\n[1/3] Building Local Database...")
    response = requests.get(INVENTORY_API_URL)
    products = response.json()
    
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
            price = 9.99 
            
        cursor.execute("INSERT INTO products VALUES (?, ?, ?)", (pid, item['title'], price))
    
    conn.commit()
    conn.close()
    print("Database created with 1 intentional defect.")


def run_integrity_scan():
    print("\n[2/3] Scanning for Data Mismatches...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    defects = []
    cursor.execute("SELECT id, price FROM products")
    local_data = cursor.fetchall()
    
    for row in local_data:
        local_id = row[0]
        local_price = row[1]
        
       
        try:
            api_resp = requests.get(f"{INVENTORY_API_URL}/{local_id}")
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
        print(" No defects found. System healthy.")
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
            print(f"Link: {JIRA_URL}/browse/{key}")
        else:
            print(f"Failed to create ticket. Code: {response.status_code}")


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