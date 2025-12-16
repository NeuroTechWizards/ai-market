"""Простой демонстрационный скрипт для RFSD.

Запуск из корня проекта (при активированном .venv):

    python -m rfsd.demo data/raw/sample.csv

Скрипт:
- загружает указанный CSV-файл с помощью rfsd.data.load_financial_statements,
- печатает размерность датафрейма (df.shape),
- выводит первые 5 строк (df.head(5)).

Файлы данных не модифицируются.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from .data import load_financial_statements


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Demo RFSD: загрузка финансовых данных из CSV и вывод "
            "основной информации (shape, head)."
        )
    )

    parser.add_argument(
        "csv_path",
        type=str,
        help=(
            "Путь к CSV-файлу с финансовой отчётностью (например, "
            "data/raw/sample.csv)."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv_path)

    # Файлы данных не создаём и не модифицируем — только читаем.
    df = load_financial_statements(csv_path)

    print("=== RFSD demo ===")
    print(f"Файл: {csv_path}")
    print(f"shape: {df.shape}")
    print("head(5):")
    print(df.head(5))


if __name__ == "__main__":  # pragma: no cover - CLI-вход
    main()
