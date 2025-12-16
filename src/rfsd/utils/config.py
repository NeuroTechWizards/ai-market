"""Минимальная конфигурация для RFSD.

На старте конфигурация предельно простая: корневой путь данных, каталог
для моделей и артефактов и т.п. В дальнейшем может быть расширена
(например, чтением параметров из переменных окружения или файлов).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class RFSDConfig:
    """Базовая конфигурация RFSD.

    Параметры
    ---------
    data_root:
        Корневой каталог с данными (сырыми или предобработанными).
    models_root:
        Каталог для сохранения моделей и связанных артефактов.
    """

    data_root: Path
    models_root: Path

    @classmethod
    def from_project_root(cls, project_root: Path) -> "RFSDConfig":
        """Создаёт конфигурацию с типовой структурой каталогов.

        Параметры
        ---------
        project_root:
            Корневая директория проекта (например, Path(__file__).resolve().parents[n]).
        """

        data_root = project_root / "data"
        models_root = project_root / "models"
        return cls(data_root=data_root, models_root=models_root)
