"""Pydantic схемы для API RFSD Backend."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


_DEFAULT_FIELDS = [
    "inn",
    "year",
    "region",
    "okved_section",
    "okved",
    "line_2110",
    "line_2300",
    "line_2400",
]


class CompanyTimeseriesRequest(BaseModel):
    """Запрос на получение временного ряда по компании."""

    inn: str = Field(..., description="ИНН компании")
    years: list[int] | None = Field(
        default=[2022, 2023, 2024],
        description="Список годов для поиска. По умолчанию [2022, 2023, 2024]",
    )
    fields: list[str] | None = Field(
        default=None,
        description=f"Список полей для выборки. По умолчанию: {_DEFAULT_FIELDS}",
    )
    limit: int = Field(
        default=200,
        ge=1,
        le=1000,
        description="Максимальное количество строк в результате (1-1000)",
    )


class TableResponse(BaseModel):
    """Ответ с табличными данными."""

    columns: list[str] = Field(..., description="Список колонок")
    rows: list[dict[str, Any]] = Field(..., description="Строки данных")
    meta: dict[str, Any] = Field(..., description="Метаданные запроса")
    files: list[dict[str, Any]] | None = Field(
        default=None,
        description="Список файлов (пока не используется)",
    )
