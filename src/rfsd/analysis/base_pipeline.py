"""Базовая заготовка аналитического пайплайна RFSD.

Задача этого модуля — описать минимальный интерфейс для последующих
анализов (например, расчёт показателей, агрегирование по отраслям,
формирование витрин для моделей и т.п.).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class BaseAnalysisPipeline(ABC):
    """Абстрактный базовый класс для аналитических пайплайнов RFSD.

    Типичный сценарий использования:

    1. Загрузить данные с помощью :func:`rfsd.data.load_financial_statements`.
    2. Передать :class:`pandas.DataFrame` в конкретную реализацию пайплайна.
    3. Вызвать метод :meth:`run` для выполнения анализа.
    """

    def __init__(self, data: pd.DataFrame) -> None:
        self._data = data

    @property
    def data(self) -> pd.DataFrame:
        """Исходные данные для анализа (read-only)."""

        return self._data

    @abstractmethod
    def run(self, **kwargs: Any) -> Any:  # pragma: no cover - логика появится позже
        """Запускает основной аналитический пайплайн.

        Конкретные реализации должны переопределить этот метод и вернуть
        результат анализа (таблицу, словарь метрик, модель и т.п.).
        """

        raise NotImplementedError
