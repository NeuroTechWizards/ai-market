"""Smoke tests для API endpoints."""

import pytest
from httpx import AsyncClient

from app.main import app


@pytest.mark.asyncio
async def test_health():
    """Тест /health endpoint."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_sample():
    """Тест /rfsd/sample endpoint."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/rfsd/sample?year=2023&limit=3&fields=inn,year")
        assert response.status_code == 200
        data = response.json()
        assert "year" in data
        assert "columns" in data
        assert "rows" in data
        assert data["year"] == 2023
        assert len(data["rows"]) <= 3
        # Проверяем, что year присутствует в данных
        if data["rows"]:
            assert "year" in data["rows"][0]


@pytest.mark.asyncio
async def test_company_timeseries():
    """Тест /rfsd/company_timeseries endpoint."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        # Используем ИНН из задания
        response = await client.post(
            "/rfsd/company_timeseries",
            json={
                "inn": "0100000011",
                "years": [2023],
                "fields": ["inn", "year", "region"],
                "limit": 10,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "columns" in data
        assert "rows" in data
        assert "meta" in data
        assert isinstance(data["columns"], list)
        assert isinstance(data["rows"], list)
        assert isinstance(data["meta"], dict)
        assert "years_scanned" in data["meta"]
        assert "matched_rows" in data["meta"]
        assert "elapsed_ms" in data["meta"]
