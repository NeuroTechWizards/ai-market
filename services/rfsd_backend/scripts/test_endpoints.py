import httpx
import json
import sys
import os

BASE_URL = "http://127.0.0.1:8000"

def run_tests():
    print(f"Checking {BASE_URL}...\n")
    
    with httpx.Client(base_url=BASE_URL, timeout=60.0) as client:
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

        print("\n== COMPANY TIMESERIES (Implicit Year Check) ==")
        payload = {
            "inn": "7722514880",
            "years": [2022, 2023],
            "fields": ["inn", "region", "okved_section", "okved"],
            "limit": 1
        }
        
        try:
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

        print("\n== COMPANY REVENUE TIMESERIES ==")
        payload = {"inn": "7722514880"}
        try:
            resp = client.post("/rfsd/company_revenue_timeseries", json=payload, timeout=60.0)
            resp.raise_for_status()
            data = resp.json()
            print(f"Status: {resp.status_code}")
            series = data.get("series", [])
            print(f"Years returned: {[item['year'] for item in series]}")
            print(f"Rows count: {len(series)}")
            if len(series) > 0 and "revenue" in series[0]:
                print("SUCCESS: Revenue data received.")
        except Exception as e:
            print(f"FAILED: {e}")

        print("\n== EXPORT FULL PROFILE XLSX (Final) ==")
        payload = {"inn": "7722514880"}
        export_dir = "exports"
        if not os.path.exists(export_dir):
            os.makedirs(export_dir, exist_ok=True)

        try:
            resp = client.post("/rfsd/export_full_profile_xlsx", json=payload, timeout=120.0)
            resp.raise_for_status()
            print(f"Status: {resp.status_code}")
            filename = f"rfsd_profile_{payload['inn']}.xlsx"
            filepath = os.path.join(export_dir, filename)
            with open(filepath, "wb") as f:
                f.write(resp.content)
            file_size = os.path.getsize(filepath)
            print(f"Saved to {filepath}")
            print(f"File size: {file_size} bytes")
            if file_size > 5000:
                print("SUCCESS")
        except Exception as e:
            print(f"FAILED: {e}")

        print("\n== SECTOR BENCHMARK ==")
        payload = {
            "inn": "7722514880",
            "years": [2023],
            "metrics": ["line_2110", "line_2400"]
        }
        try:
            # Увеличенный таймаут для MVP (цель - уложиться в 20-30 сек после оптимизации)
            resp = client.post("/rfsd/sector_benchmark", json=payload, timeout=120.0)
            resp.raise_for_status()
            data = resp.json()
            
            print(f"Status: {resp.status_code}")
            rows = data.get("rows", [])
            print(f"Rows count: {len(rows)}")
            if rows:
                r = rows[0]
                print(f"Year: {r.get('year')}, Section: {r.get('okved_section')}")
                print(f"Company Revenue: {r.get('company_line_2110')}")
                print(f"Sector Median Revenue: {r.get('sector_median_line_2110')}")
                print(f"Sector Count: {r.get('sector_count')}")
                print("SUCCESS (Benchmark)")
            else:
                print("FAILURE: No benchmark data returned")
                
        except Exception as e:
            print(f"FAILED: {e}")
            if hasattr(e, "response") and e.response:
                print(e.response.text)

if __name__ == "__main__":
    try:
        run_tests()
    except KeyboardInterrupt:
        sys.exit(0)
