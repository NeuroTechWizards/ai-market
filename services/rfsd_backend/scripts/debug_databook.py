import polars as pl
import os
import sys

# Ищем файл
possible_paths = [
    r"D:\OneDrive\Работа\AI consalting\ИИ-агент инвест дир\Программа и Код\AI market\docs\databook\rfsd_databook_ru.xlsx",
    "docs/databook/rfsd_databook_ru.xlsx",
    "../docs/databook/rfsd_databook_ru.xlsx"
]

path = None
for p in possible_paths:
    if os.path.exists(p):
        path = p
        break

if not path:
    print("File NOT found!")
    sys.exit(1)

print(f"Reading: {path}")

try:
    # Пробуем прочитать через Polars
    # Polars read_excel читает первый лист по умолчанию
    df = pl.read_excel(path)
    print("\nColumns found:", df.columns)
    print("\nFirst 5 rows:")
    print(df.head(5))
    
    # Проверяем наличие нужных колонок
    if "code" in df.columns and "name_ru" in df.columns:
        print("\nSUCCESS: 'code' and 'name_ru' columns exist.")
    else:
        print("\nFAILURE: Required columns missing.")
        
except Exception as e:
    print(f"\nError reading with Polars: {e}")
    
    # Попробуем через openpyxl напрямую, если Polars подвел
    try:
        import openpyxl
        print("\nTrying openpyxl...")
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        print(f"Sheet name: {ws.title}")
        
        rows = list(ws.iter_rows(min_row=1, max_row=5, values_only=True))
        print("First 5 rows (raw):")
        for r in rows:
            print(r)
    except Exception as e2:
        print(f"Error with openpyxl: {e2}")
