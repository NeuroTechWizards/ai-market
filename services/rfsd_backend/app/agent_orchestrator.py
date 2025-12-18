import re
import logging
import asyncio
from typing import Any
import polars as pl
from openai import AsyncOpenAI

from .settings import settings
from .rfsd_loader import filter_inn_year, _scan_year

logger = logging.getLogger(__name__)

class AgentOrchestrator:
    def __init__(self):
        self.api_key = settings.OPENAI_API_KEY
        if self.api_key:
            self.client = AsyncOpenAI(api_key=self.api_key)
        else:
            self.client = None
            logger.warning("OPENAI_API_KEY is not set. Agent will not work.")

    def extract_inn(self, text: str) -> str | None:
        match = re.search(r'\b\d{10}\b|\b\d{12}\b', text)
        if match:
            return match.group(0)
        return None

    def parse_years(self, text: str) -> list[int]:
        # Диапазон
        range_match = re.search(r'(\d{4})\s*[-–—]\s*(\d{4})', text)
        if range_match:
            start = int(range_match.group(1))
            end = int(range_match.group(2))
            if start > end: start, end = end, start
            return list(range(start, end + 1))
        
        # Одиночный год
        single_year_match = re.search(r'\b(20[12][0-9])\b', text)
        if single_year_match:
             return [int(single_year_match.group(1))]

        # Default
        return [2020, 2021, 2022, 2023, 2024]

    async def _get_benchmark_data(self, inn: str, years: list[int]) -> list[dict]:
        """Сбор данных бенчмарка (упрощенная копия логики из main.py)."""
        metrics = ["line_2110", "line_2400"]
        results = []
        
        for idx, year in enumerate(years):
            if idx > 0: await asyncio.sleep(0.5) # Небольшая задержка
            try:
                # 1. Данные компании
                comp_df = filter_inn_year(year, inn, ["inn", "year", "okved_section"] + metrics, limit=1)
                if comp_df.height == 0: continue
                
                comp_row = comp_df.row(0, named=True)
                section = comp_row.get("okved_section")
                if not section: continue

                # 2. Бенчмарк
                scan = _scan_year(year, columns=["okved_section"] + metrics)
                scan = scan.filter(pl.col("okved_section") == section)
                
                # MVP Aggregations
                agg_exprs = [pl.len().alias("count")]
                for m in metrics:
                    agg_exprs.append(pl.col(m).quantile(0.5, interpolation="nearest").alias(f"median_{m}"))
                
                stats_df = scan.select(agg_exprs).collect()
                if stats_df.height == 0: continue
                
                stats_row = stats_df.row(0, named=True)
                
                item = {
                    "year": year,
                    "section": section,
                    "revenue": comp_row.get("line_2110"),
                    "profit": comp_row.get("line_2400"),
                    "market_revenue_median": stats_row.get("median_line_2110"),
                    "market_profit_median": stats_row.get("median_line_2400"),
                    "market_count": stats_row.get("count")
                }
                results.append(item)
            except Exception as e:
                logger.error(f"Agent data fetch error {year}: {e}")
        
        return results

    async def process_query(self, query: str) -> str:
        if not self.client:
            return "Ошибка: API ключ OpenAI не настроен."

        inn = self.extract_inn(query)
        if not inn:
            return "Не могу найти ИНН в запросе. Укажите ИНН (10 или 12 цифр)."
            
        years = self.parse_years(query)
        
        # Сбор данных
        data = await self._get_benchmark_data(inn, years)
        
        if not data:
            return f"Не удалось найти данные для ИНН {inn} за указанные годы."

        # Формирование промпта
        system_prompt = (
            "Ты — опытный финансовый аналитик. Твоя задача — проанализировать финансовые показатели компании "
            "и сравнить их с медианными показателями по отрасли (рынку).\n"
            "Стиль: деловой, конкретный, без воды. Сначала факты/цифры, потом выводы."
        )
        
        user_prompt = f"""
        Запрос пользователя: "{query}"
        
        Данные по компании (ИНН {inn}) и рынку:
        {data}
        
        Задание:
        1. Сделай краткий финансовый профиль компании (динамика выручки и прибыли).
        2. Сравни с рынком (выше/ниже медианы, тренды).
        3. Дай 2-3 ключевых вывода о состоянии бизнеса.
        Если данных мало или есть странности, отметь это.
        """

        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini", 
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3,
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"OpenAI error: {e}")
            return f"Ошибка при генерации ответа LLM: {str(e)}"

agent = AgentOrchestrator()
