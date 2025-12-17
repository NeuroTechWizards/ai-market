import sys
import os
import time
import logging

# Настройка логирования, чтобы видеть процесс
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Inspector")

# Добавляем путь к app
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

print("--- STARTING COLUMN INSPECTION ---")
print("Importing modules...")

try:
    from app.rfsd_loader import get_schema_columns
    print("Modules imported successfully.")
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

def inspect():
    year = 2023
    print(f"Connecting to Hugging Face to get schema for year {year}...")
    print("This may take 10-30 seconds depending on connection speed...")
    
    start_time = time.time()
    try:
        # Эта операция требует сетевого запроса
        cols = get_schema_columns(year)
        elapsed = time.time() - start_time
        
        print(f"\nSUCCESS! Schema received in {elapsed:.1f} seconds.")
        print(f"Total columns found: {len(cols)}")
        
        # Фильтруем
        lines = [c for c in cols if c.startswith("line_")]
        print(f"Financial lines found: {len(lines)}")
        print("-" * 50)
        print("List of financial indicators:")
        print(sorted(lines))
        print("-" * 50)
        
    except Exception as e:
        print(f"\nERROR: Could not get schema. Reason: {e}")
        print("Check your internet connection or try again later.")

if __name__ == "__main__":
    inspect()
