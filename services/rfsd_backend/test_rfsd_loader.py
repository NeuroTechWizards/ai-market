"""Минимальный тест для rfsd_loader."""

from app.rfsd_loader import load_year

# Минимально: читаем только inn и year, и берём 5 строк
df = load_year(2023, columns=["inn", "year"])
print(df.head(5))
print("OK, rows:", df.height, "cols:", df.width)
