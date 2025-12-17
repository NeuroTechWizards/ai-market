"""Загрузка RFSD данных (Parquet) по годам из Hugging Face.

На первом этапе используем захардкоженный список годов.
"""

from __future__ import annotations

from typing import Iterable, Sequence

import polars as pl


_AVAILABLE_YEARS = list(range(2011, 2025))


def list_available_years() -> list[int]:
    """Возвращает список доступных годов (пока захардкоженный)."""

    return _AVAILABLE_YEARS.copy()


def _validate_year(year: int) -> None:
    if year not in _AVAILABLE_YEARS:
        raise ValueError(f"Год {year} недоступен. Допустимо: {min(_AVAILABLE_YEARS)}–{max(_AVAILABLE_YEARS)}")


def _scan_year(year: int, columns: Sequence[str] | None = None) -> pl.LazyFrame:
    """Возвращает ленивый скан по году с добавленной колонкой year."""

    _validate_year(year)

    path = f"hf://datasets/irlspbru/RFSD/RFSD/year={year}/*.parquet"
    scan = pl.scan_parquet(path)
    # Добавляем партиционный год как колонку, чтобы им можно было пользоваться и в select.
    scan = scan.with_columns(pl.lit(year).alias("year"))

    if columns is not None:
        scan = scan.select(list(columns))

    return scan


def load_year(year: int, columns: Sequence[str] | None = None) -> pl.DataFrame:
    """Загружает один год RFSD из Hugging Face Parquet.

    Параметры
    ---------
    year:
        Год отчётности (2011–2024 на текущем этапе).
    columns:
        Список колонок для выборки. Если None — читаются все.
    """

    return _scan_year(year, columns=columns).collect()


def load_years(years: Iterable[int], columns: Sequence[str] | None = None) -> pl.DataFrame:
    """Загружает несколько лет и конкатенирует результаты вертикально."""

    years_list = list(years)
    if not years_list:
        raise ValueError("Список годов пуст")

    frames: list[pl.DataFrame] = []
    for y in years_list:
        frames.append(load_year(y, columns=columns))

    return pl.concat(frames, how="vertical")


def sample_year(year: int, columns: Sequence[str] | None = None, n: int = 5) -> pl.DataFrame:
    """Возвращает первые n строк указанного года без полного collect."""

    return _scan_year(year, columns=columns).limit(n).collect()


def filter_inn_year(
    year: int,
    inn: str,
    columns: Sequence[str],
    limit: int = 200,
) -> pl.DataFrame:
    """Фильтр по ИНН для указанного года, с выборкой колонок и ограничением количества строк."""

    cols = list(columns)
    if "inn" not in cols:
        cols.append("inn")

    return (
        _scan_year(year, columns=cols)
        .filter(pl.col("inn") == inn)
        .limit(limit)
        .collect()
    )


def get_schema_columns(year: int) -> list[str]:
    """Возвращает список доступных колонок для указанного года без чтения данных."""

    _validate_year(year)
    path = f"hf://datasets/irlspbru/RFSD/RFSD/year={year}/*.parquet"
    scan = pl.scan_parquet(path)
    schema = scan.collect_schema()
    # year добавляется виртуально, но его нет в schema, поэтому добавляем вручную
    columns = list(schema.keys())
    if "year" not in columns:
        columns.append("year")
    return columns


def load_indicators_dict() -> dict[str, str]:
    """Загружает справочник индикаторов из Excel (docs/databook/rfsd_databook_ru.xlsx).
    
    Возвращает словарь {code: name_ru}. Использует openpyxl напрямую.
    """
    import os
    import openpyxl
    
    # Ищем корень проекта
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    
    possible_paths = [
        os.path.join(project_root, "docs", "databook", "rfsd_databook_ru.xlsx"),
        os.path.join(project_root, "..", "docs", "databook", "rfsd_databook_ru.xlsx"),
        r"D:\OneDrive\Работа\AI consalting\ИИ-агент инвест дир\Программа и Код\AI market\docs\databook\rfsd_databook_ru.xlsx"
    ]
    
    databook_path = None
    for p in possible_paths:
        if os.path.exists(p):
            databook_path = p
            break
            
    if not databook_path:
        return {}

    try:
        # Используем openpyxl вместо polars.read_excel, чтобы избежать проблем с движками
        wb = openpyxl.load_workbook(databook_path, read_only=True, data_only=True)
        # Ищем лист 'databook' или берем активный
        if 'databook' in wb.sheetnames:
            ws = wb['databook']
        else:
            ws = wb.active
            
        # Читаем заголовки (1-я строка)
        headers = [cell.value for cell in ws[1]]
        try:
            code_idx = headers.index('code')
            name_idx = headers.index('name_ru')
        except ValueError:
            return {}
            
        result = {}
        # Читаем данные (с 2-й строки)
        for row in ws.iter_rows(min_row=2, values_only=True):
            code = row[code_idx]
            name = row[name_idx]
            if code:
                result[str(code)] = str(name) if name else ""
                
        return result
    except Exception as e:
        # Логируем ошибку, но не падаем
        print(f"Error loading databook: {e}")
        return {}
