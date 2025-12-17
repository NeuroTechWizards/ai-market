"""Основной модуль FastAPI приложения RFSD Backend."""

import logging
from time import perf_counter
from typing import Any
from datetime import datetime
from io import BytesIO

from fastapi import FastAPI, HTTPException, Query, Response
import polars as pl

from . import schemas
from .rfsd_loader import filter_inn_year, get_schema_columns, sample_year, load_indicators_dict
from .settings import settings

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="RFSD Backend",
    description="Backend сервис для Russian Financial Statements Database",
    version="0.1.0",
)

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


_INDICATORS_DICT = {}

@app.on_event("startup")
async def startup_event():
    """Загружаем справочники при старте."""
    global _INDICATORS_DICT
    logger.info("Loading indicators databook...")
    _INDICATORS_DICT = load_indicators_dict()
    logger.info(f"Loaded {len(_INDICATORS_DICT)} indicators.")

@app.get("/health")
async def health_check() -> dict[str, str]:
    """Проверка здоровья сервиса."""
    return {"status": "ok"}


@app.get("/rfsd/sample")
async def get_sample(
    year: int = Query(default=2023, ge=2011, le=2024, description="Год данных"),
    limit: int = Query(default=5, ge=1, le=100, description="Количество строк (1-100)"),
    fields: str | None = Query(
        default="inn,year",
        description="Список полей через запятую",
    ),
) -> dict[str, Any]:
    """Быстрый endpoint для получения сэмпла данных без полной загрузки."""

    # Парсим fields
    if fields:
        fields_list = [f.strip() for f in fields.split(",") if f.strip()]
    else:
        fields_list = ["inn", "year"]

    try:
        df = sample_year(year=year, columns=fields_list, n=limit)
        rows = df.to_dicts()

        return {
            "year": year,
            "columns": fields_list,
            "rows": rows,
        }
    except Exception as e:
        logger.error(f"Ошибка при получении sample: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/rfsd/company_timeseries", response_model=schemas.TableResponse)
async def company_timeseries(request: schemas.CompanyTimeseriesRequest) -> schemas.TableResponse:
    """Поиск компании по ИНН в нескольких годах."""

    # Определяем годы для сканирования
    years_to_scan = request.years if request.years is not None else [2022, 2023, 2024]

    # Определяем поля
    fields = request.fields if request.fields is not None else _DEFAULT_FIELDS.copy()

    # Если сканируем несколько лет, нужно обязательно вернуть year, чтобы различать данные
    if len(years_to_scan) > 1 and "year" not in fields:
        fields.append("year")

    # Проверяем схему и фильтруем несуществующие колонки
    dropped_fields: list[str] = []
    if years_to_scan:
        # Берем схему первого года как эталон
        schema_columns = get_schema_columns(years_to_scan[0])
        # year всегда доступен (виртуальная колонка)
        valid_fields = [f for f in fields if f in schema_columns or f == "year"]
        dropped_fields = [f for f in fields if f not in valid_fields]
        fields = valid_fields

    logger.info(
        f"company_timeseries: inn={request.inn}, years={years_to_scan}, "
        f"fields={fields}, limit={request.limit}"
    )

    start_time = perf_counter()
    frames: list = []
    per_year_elapsed_ms: dict[int, float] = {}

    # Сканируем по годам
    for year in years_to_scan:
        year_start = perf_counter()
        try:
            df = filter_inn_year(
                year=year,
                inn=request.inn,
                columns=fields,
                limit=request.limit,
            )
            if df.height > 0:
                frames.append(df)
            per_year_elapsed_ms[year] = (perf_counter() - year_start) * 1000
        except Exception as e:
            logger.warning(f"Ошибка при обработке года {year}: {e}")
            per_year_elapsed_ms[year] = (perf_counter() - year_start) * 1000

    # Конкатенируем результаты
    if frames:
        if len(frames) > 1:
            result_df = pl.concat(frames, how="diagonal")
        else:
            result_df = frames[0]

        # Сортируем по year, если колонка есть
        if "year" in result_df.columns:
            result_df = result_df.sort("year")

        # Обрезаем до limit
        if result_df.height > request.limit:
            result_df = result_df.head(request.limit)

        columns = result_df.columns
        rows = result_df.to_dicts()
        matched_rows = result_df.height
    else:
        # Ничего не найдено
        columns = fields
        rows = []
        matched_rows = 0

    elapsed_ms = (perf_counter() - start_time) * 1000

    logger.info(
        f"company_timeseries завершен: inn={request.inn}, "
        f"matched_rows={matched_rows}, elapsed_ms={elapsed_ms:.2f}"
    )

    meta = {
        "years_scanned": years_to_scan,
        "matched_rows": matched_rows,
        "elapsed_ms": round(elapsed_ms, 2),
        "per_year_elapsed_ms": {str(k): round(v, 2) for k, v in per_year_elapsed_ms.items()},
    }

    if dropped_fields:
        meta["dropped_fields"] = dropped_fields

    return schemas.TableResponse(
        columns=columns,
        rows=rows,
        meta=meta,
        files=None,
    )


@app.post("/rfsd/company_revenue_timeseries", response_model=schemas.CompanyRevenueTimeseriesResponse)
async def company_revenue_timeseries(request: schemas.CompanyRevenueTimeseriesRequest) -> schemas.CompanyRevenueTimeseriesResponse:
    """Вернуть выручку компании (line_2110) по годам."""
    
    # 1. Годы по умолчанию
    years_to_scan = request.years if request.years is not None else [2019, 2020, 2021, 2022, 2023]
    
    logger.info(f"company_revenue_timeseries: inn={request.inn}, years={years_to_scan}")
    
    start_time = perf_counter()
    series: list[schemas.RevenueYear] = []
    matched_rows = 0
    per_year_elapsed_ms: dict[int, float] = {}
    
    # Поля строго фиксированы
    fields = ["inn", "year", "line_2110"]
    
    # 2. Цикл по годам (без join'ов, просто сбор данных)
    for year in years_to_scan:
        year_start = perf_counter()
        try:
            # Используем существующий loader
            df = filter_inn_year(
                year=year,
                inn=request.inn,
                columns=fields,
                limit=1  # Нам нужна только 1 строка на год для компании
            )
            
            if df.height > 0:
                row = df.row(0, named=True)
                # Извлекаем выручку (может быть None)
                revenue_val = row.get("line_2110")
                # Преобразуем к float, если не None
                if revenue_val is not None:
                    try:
                        revenue_val = float(revenue_val)
                    except (ValueError, TypeError):
                        revenue_val = None
                        
                series.append(schemas.RevenueYear(year=year, revenue=revenue_val))
                matched_rows += 1
                
            per_year_elapsed_ms[year] = (perf_counter() - year_start) * 1000
            
        except Exception as e:
            # Ошибки (например, файла за год нет) игнорируем, просто идем дальше
            logger.warning(f"Ошибка при получении выручки за {year}: {e}")
            per_year_elapsed_ms[year] = (perf_counter() - year_start) * 1000

    # 3. Сортировка по year ASC
    series.sort(key=lambda x: x.year)
    
    elapsed_ms = (perf_counter() - start_time) * 1000
    
    # 4. Формирование ответа
    return schemas.CompanyRevenueTimeseriesResponse(
        inn=request.inn,
        series=series,
        meta={
            "years_scanned": years_to_scan,
            "matched_rows": matched_rows,
            "elapsed_ms": round(elapsed_ms, 2),
            "per_year_elapsed_ms": {str(k): round(v, 2) for k, v in per_year_elapsed_ms.items()},
        }
    )


@app.post("/rfsd/export_company_revenue_xlsx")
async def export_company_revenue_xlsx(request: schemas.ExportCompanyRevenueRequest):
    """Экспорт выручки компании в Excel файл."""
    
    # 1. Повторно используем логику получения данных
    # Формируем внутренний запрос
    internal_req = schemas.CompanyRevenueTimeseriesRequest(
        inn=request.inn,
        years=request.years
    )
    
    # Вызываем функцию получения данных (напрямую, как корутину)
    data = await company_revenue_timeseries(internal_req)
    
    # 2. Формируем Excel
    try:
        import openpyxl
        
        wb = openpyxl.Workbook()
        
        # Лист 1: revenue_timeseries
        ws1 = wb.active
        ws1.title = "revenue_timeseries"
        ws1.append(["year", "revenue"])
        for item in data.series:
            ws1.append([item.year, item.revenue])
            
        # Лист 2: meta
        ws2 = wb.create_sheet("meta")
        ws2.append(["inn", "elapsed_ms", "generated_at"])
        ws2.append([data.inn, data.meta.get("elapsed_ms"), datetime.now().isoformat()])
        
        # Сохраняем в буфер
        output = BytesIO()
        wb.save(output)
        output.seek(0)
        
        filename = f"rfsd_revenue_{request.inn}.xlsx"
        
        return Response(
            content=output.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        logger.error(f"Ошибка при генерации Excel: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Ошибка генерации Excel: {str(e)}")


@app.post("/rfsd/export_full_profile_xlsx")
async def export_full_profile_xlsx(request: schemas.ExportFullProfileRequest):
    """Экспорт ВСЕХ финансовых показателей компании в Excel (Profile)."""

    years_to_scan = request.years if request.years is not None else [2019, 2020, 2021, 2022, 2023]
    logger.info(f"export_full_profile_xlsx: inn={request.inn}, years={years_to_scan}")
    
    start_time = perf_counter()

    try:
        # 1. Получаем список всех доступных колонок из схемы последнего года
        schema_cols = get_schema_columns(2023)
        # Фильтруем только line_...
        financial_lines = [c for c in schema_cols if c.startswith("line_")]
        financial_lines.sort() 
        
        fields_to_load = ["inn", "year"] + financial_lines
        
        # 2. Скачиваем данные по годам
        frames = []
        for year in years_to_scan:
            try:
                # Фильтруем те поля, которые реально есть в ЭТОМ году
                current_year_schema = get_schema_columns(year)
                current_fields = [f for f in fields_to_load if f in current_year_schema or f == "year"]
                
                df = filter_inn_year(
                    year=year,
                    inn=request.inn,
                    columns=current_fields,
                    limit=1
                )
                if df.height > 0:
                    frames.append(df)
            except Exception as e:
                logger.warning(f"Error loading year {year}: {e}")
        
        # 3. Объединяем
        if not frames:
             raise HTTPException(status_code=404, detail="Data not found for this INN")
             
        full_df = pl.concat(frames, how="diagonal")
        full_df = full_df.sort("year")
        
        # 4. Трансформируем в формат: Строки = Показатели, Столбцы = Годы
        matrix = {code: {} for code in financial_lines}
        
        for row in full_df.to_dicts():
            y = row["year"]
            for k, v in row.items():
                if k.startswith("line_") and v is not None:
                     matrix[k][y] = v
                     
        # Используем глобальный справочник, загруженный при старте
        # indicators_dict = load_indicators_dict() <- БЫЛО
        indicators_dict = _INDICATORS_DICT
                     
        # 5. Генерируем Excel
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Financial Profile"
        
        # Заголовки: Indicator Code | Indicator Name (RU) | 2019 | 2020 | ...
        headers = ["Indicator Code", "Indicator Name (RU)"] + [str(y) for y in years_to_scan]
        ws.append(headers)
        
        # Заполняем строки
        for code in financial_lines:
            name_ru = indicators_dict.get(code, "")
            row_data = [code, name_ru]
            for year in years_to_scan:
                val = matrix.get(code, {}).get(year)
                row_data.append(val)
            ws.append(row_data)
            
        # Лист 2: meta
        ws2 = wb.create_sheet("meta")
        ws2.append(["inn", "generated_at", "elapsed_s"])
        elapsed = perf_counter() - start_time
        ws2.append([request.inn, datetime.now().isoformat(), round(elapsed, 2)])
        
        output = BytesIO()
        wb.save(output)
        output.seek(0)
        
        filename = f"rfsd_profile_{request.inn}.xlsx"
        
        return Response(
            content=output.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Full profile export error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
