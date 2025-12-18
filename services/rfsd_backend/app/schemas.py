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
    years: list[int] | None = Field(default=[2022, 2023, 2024], description="Список годов")
    fields: list[str] | None = Field(default=None, description=f"Список полей")
    limit: int = Field(default=200, ge=1, le=1000, description="Максимальное количество строк")


class TableResponse(BaseModel):
    """Ответ с табличными данными."""
    columns: list[str] = Field(..., description="Список колонок")
    rows: list[dict[str, Any]] = Field(..., description="Строки данных")
    meta: dict[str, Any] = Field(..., description="Метаданные запроса")
    files: list[dict[str, Any]] | None = Field(default=None, description="Список файлов")


class CompanyRevenueTimeseriesRequest(BaseModel):
    """Запрос на получение выручки (line_2110) по годам."""
    inn: str = Field(..., description="ИНН компании")
    years: list[int] | None = Field(default=None, description="Список годов")


class RevenueYear(BaseModel):
    """Данные о выручке за один год."""
    year: int
    revenue: float | None = None


class CompanyRevenueTimeseriesResponse(BaseModel):
    """Ответ с временным рядом выручки."""
    inn: str
    series: list[RevenueYear]
    meta: dict[str, Any]


class ExportCompanyRevenueRequest(BaseModel):
    """Запрос на экспорт выручки в Excel."""
    inn: str = Field(..., description="ИНН компании")
    years: list[int] | None = Field(default=None, description="Список годов")


class ExportFullProfileRequest(BaseModel):
    """Запрос на экспорт ВСЕХ показателей в Excel."""
    inn: str = Field(..., description="ИНН компании")
    years: list[int] | None = Field(default=None, description="Список годов")


class SectorBenchmarkRequest(BaseModel):
    """Запрос на сравнение с отраслью."""
    inn: str = Field(..., description="ИНН компании")
    years: list[int] | None = Field(default=None, description="Список годов. По умолчанию последние 5 лет.")
    metrics: list[str] | None = Field(default=["line_2110", "line_2400"], description="Коды метрик")
    limit_sector: int = Field(default=200000, description="Лимит строк сектора для безопасности")


class AgentQueryRequest(BaseModel):
    """Запрос к AI-агенту."""
    query: str = Field(..., description="Текст запроса на естественном языке")
    context: dict[str, Any] | None = Field(default=None, description="Контекст (user_id, etc)")


class AgentQueryResponse(BaseModel):
    """Ответ AI-агента."""
    answer: str = Field(..., description="Текстовый ответ LLM")
    meta: dict[str, Any] = Field(default_factory=dict, description="Метаданные")
