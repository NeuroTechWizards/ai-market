import httpx
import json
import sys

BASE_URL = "http://127.0.0.1:8000"

def run_tests():
    print(f"Checking {BASE_URL}...\n")
    
    # Общий клиент с увеличенным таймаутом
    with httpx.Client(base_url=BASE_URL, timeout=60.0) as client:
        # 1. Health Check
        print("== HEALTH CHECK ==")
        try:
            resp = client.get("/health")
            resp.raise_for_status()
            print(f"Status: {resp.status_code}")
            print(f"Response: {resp.json()}")
        except Exception as e:
            print(f"FAILED: {e}")
            return

        print("\n== SAMPLE (3 rows) ==")
        try:
            resp = client.get("/rfsd/sample", params={
                "year": 2023, 
                "limit": 3, 
                "fields": "inn,year,okved_section,okved"
            })
            resp.raise_for_status()
            data = resp.json()
            print(f"Status: {resp.status_code}")
            print(f"Columns: {data.get('columns')}")
            print(f"Rows count: {len(data.get('rows', []))}")
            # print(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception as e:
            print(f"FAILED: {e}")

        print("\n== COMPANY TIMESERIES (inn 7722514880, year 2023) ==")
        payload = {
            "inn": "7722514880",
            "years": [2023],
            "fields": [
                "inn", "year", "region", "okved_section", 
                "okved", "line_2110", "line_2300", "line_2400"
            ],
            "limit": 50
        }
        
        try:
            resp = client.post("/rfsd/company_timeseries", json=payload)
            resp.raise_for_status()
            data = resp.json()
            print(f"Status: {resp.status_code}")
            meta = data.get("meta", {})
            print(f"Meta: {meta}")
            print(f"Rows matched: {len(data.get('rows', []))}")
        except Exception as e:
            print(f"FAILED: {e}")
            print(resp.text)

        print("\n== COMPANY TIMESERIES (Implicit Year Check) ==")
        # Request multiple years WITHOUT 'year' in fields. 
        # Expectation: 'year' should be auto-added to columns.
        payload = {
            "inn": "7722514880",
            "years": [2022, 2023],
            "fields": ["inn", "region", "okved_section", "okved"],  # без "year"
            "limit": 1
        }
        
        try:
            # Увеличенный таймаут специально для тяжелого запроса
            resp = client.post("/rfsd/company_timeseries", json=payload, timeout=60.0)
            resp.raise_for_status()
            data = resp.json()
            columns = data.get("columns", [])
            print(f"Status: {resp.status_code}")
            print(f"Columns: {columns}")
            if "year" in columns:
                print("SUCCESS: 'year' column was automatically added.")
            else:
                print("FAILURE: 'year' column is MISSING.")
            print(f"Rows matched: {len(data.get('rows', []))}")
        except Exception as e:
            print(f"FAILED: {e}")
            if hasattr(e, "response") and e.response:
                print(e.response.text)

if __name__ == "__main__":
    try:
        run_tests()
    except KeyboardInterrupt:
        sys.exit(0)
