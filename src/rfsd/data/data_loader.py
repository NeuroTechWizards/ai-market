"""Модуль базовой загрузки финансовой отчётности РФ (RFSD).

На этом этапе реализуется минимальный интерфейс для чтения табличных данных
(например, CSV) в `pandas.DataFrame`. Логика парсинга специфичных форм
(РСБУ, МСФО и т.п.) будет добавлена на последующих этапах.
"""

from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd

PathLike = Union[str, Path]


def load_financial_statements(path: PathLike, *, encoding: str = "utf-8", **read_csv_kwargs) -> pd.DataFrame:
    """Загружает финансовые данные из табличного файла в `pandas.DataFrame`.

    Параметры
    ---------
    path:
        Путь до файла с данными (обычно CSV, на старте).
    encoding:
        Кодировка входного файла. По умолчанию ``"utf-8"``.
    **read_csv_kwargs:
        Дополнительные параметры, которые будут переданы в :func:`pandas.read_csv`.

    Возвращает
    ----------
    pandas.DataFrame
        Таблица с загруженными данными.

    Исключения
    ----------
    FileNotFoundError
        Если указанный файл не существует.
    pd.errors.EmptyDataError
        Если файл существует, но не содержит данных.
    """

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Файл с данными не найден: {path}")

    # На базовом уровне считаем, что входной формат — CSV.
    # Поддержка других форматов (Excel, Parquet, БД) будет добавлена позже.
    df = pd.read_csv(path, encoding=encoding, **read_csv_kwargs)
    return df
