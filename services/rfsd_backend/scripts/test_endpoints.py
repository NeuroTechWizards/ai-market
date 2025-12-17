import httpx
import json
import sys
import os

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

        print("\n== COMPANY REVENUE TIMESERIES ==")
        payload = {
            "inn": "7722514880",
            # years по умолчанию [2019-2023]
        }
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
            else:
                print("FAILURE: No revenue data or wrong format.")
                
        except Exception as e:
            print(f"FAILED: {e}")
            if hasattr(e, "response") and e.response:
                print(e.response.text)

        print("\n== EXPORT FULL PROFILE XLSX (Final) ==")
        payload = {
            "inn": "7722514880",
            # years по умолчанию [2019-2023]
        }
        
        # Создаем папку для экспорта (в корне, если запускаем из services/rfsd_backend)
        export_dir = "exports"
        if not os.path.exists(export_dir):
            try:
                os.makedirs(export_dir, exist_ok=True)
            except OSError as e:
                print(f"Warning: could not create directory {export_dir}: {e}")

        try:
            # Таймаут побольше
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
            else:
                print("FAILURE: File seems too small")
                
        except Exception as e:
            print(f"FAILED: {e}")
            if hasattr(e, "response") and e.response:
                print(e.response.text)

        print("\n== EXPORT FULL PROFILE XLSX (DEBUG 1 YEAR) ==")
        # Пробуем скачать только 1 год, чтобы проверить, работает ли сам механизм генерации
        payload_debug = {
            "inn": "7722514880",
            "years": [2023]
        }
        
        try:
            resp = client.post("/rfsd/export_full_profile_xlsx", json=payload_debug, timeout=120.0)
            
            if resp.status_code == 200:
                print(f"Status: {resp.status_code}")
                filename_debug = f"debug_profile_{payload_debug['inn']}_2023.xlsx"
                filepath_debug = os.path.join(export_dir, filename_debug)
                
                with open(filepath_debug, "wb") as f:
                    f.write(resp.content)
                    
                file_size_debug = os.path.getsize(filepath_debug)
                print(f"Saved to {filepath_debug}")
                print(f"File size: {file_size_debug} bytes")
                print("SUCCESS (DEBUG)")
            else:
                print(f"FAILED: Status {resp.status_code}")
                print(resp.text[:500])
                
        except Exception as e:
            print(f"FAILED (Exception): {e}")

if __name__ == "__main__":
    try:
        run_tests()
    except KeyboardInterrupt:
        sys.exit(0)
