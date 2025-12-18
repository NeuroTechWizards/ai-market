import re
import logging
from datetime import datetime
from typing import Any
from .rfsd_client import rfsd_client

logger = logging.getLogger(__name__)

class Router:
    """Парсер и роутер сообщений (NL-интерпретатор)."""
    
    def extract_inn(self, text: str) -> str | None:
        match = re.search(r'\b\d{10}\b|\b\d{12}\b', text)
        if match:
            return match.group(0)
        return None

    def parse_years(self, text: str) -> list[int]:
        """Извлекает годы из текста или возвращает дефолт."""
        # Сначала ищем диапазон годов (например, "2021-2023")
        range_match = re.search(r'(\d{4})\s*[-–—]\s*(\d{4})', text)
        if range_match:
            start = int(range_match.group(1))
            end = int(range_match.group(2))
            if start > end:
                start, end = end, start
            return list(range(start, end + 1))

        # Затем ищем "последние N лет"
        last_match = re.search(r'(?:за|последни[ех])\s*(\d+)\s*(?:лет|год|г\.?)', text, re.IGNORECASE)
        if last_match:
            n = int(last_match.group(1))
            end_year = 2023 
            start_year = end_year - n + 1
            return list(range(start_year, end_year + 1))

        # Ищем одиночный год (4 цифры в диапазоне 2010-2024)
        single_year_match = re.search(r'\b(20[12][0-9])\b', text)
        if single_year_match:
            year = int(single_year_match.group(1))
            if 2010 <= year <= 2024:
                return [year]

        # Если ничего не найдено, возвращаем дефолт (последний год)
        return [2023]

    def parse_intent_format(self, text: str) -> tuple[str, str]:
        """Определяет намерение и формат ответа."""
        text_lower = text.lower()
        
        fmt = "text"
        if any(kw in text_lower for kw in ["xlsx", "эксель", "excel"]):
            fmt = "xlsx"
            
        if "выруч" in text_lower:
            intent = "revenue"
        elif "прибыл" in text_lower:
            intent = "profit"
        elif any(kw in text_lower for kw in ["сравни", "бенчмарк", "benchmark", "отрасл"]):
            intent = "benchmark"
        elif any(kw in text_lower for kw in ["профиль", "все показатели", "все поля", "полный"]):
            intent = "full_profile"
        else:
            intent = "full_profile"
            
        return intent, fmt

    def _format_number(self, val: Any) -> str:
        if val is None:
            return "-"
        try:
            return f"{float(val):,.0f}".replace(",", " ")
        except (ValueError, TypeError):
            return str(val)

    async def route_message(self, text: str) -> dict[str, Any]:
        """Главный метод маршрутизации."""
        inn = self.extract_inn(text)
        if not inn:
            return {
                "type": "text",
                "content": "Пришлите ИНН (10 или 12 цифр) для поиска."
            }

        years = self.parse_years(text)
        intent, fmt = self.parse_intent_format(text)
        
        logger.info(f"Routing: inn={inn}, intent={intent}, fmt={fmt}, years={years}")

        if fmt == "xlsx":
            filename_map = {
                "revenue": "revenue",
                "profit": "profit",
                "full_profile": "profile"
            }
            fname_prefix = filename_map.get(intent, "report")
            
            file_bytes = await rfsd_client.export_full_profile_xlsx(inn, years)
            if file_bytes:
                return {
                    "type": "document",
                    "content": file_bytes,
                    "filename": f"rfsd_{fname_prefix}_{inn}.xlsx",
                    "caption": f"Отчет ({intent}) по ИНН {inn} за {min(years)}-{max(years)}"
                }
            else:
                return {
                    "type": "text", 
                    "content": "Не удалось сгенерировать файл. Возможно, нет данных или сервис недоступен."
                }

        if intent == "revenue":
            data = await rfsd_client.company_revenue_timeseries(inn, years)
            if not data or not data.get("series"):
                return {"type": "text", "content": f"Нет данных по выручке для ИНН {inn}."}
            
            lines = [f"📊 Выручка ИНН {inn}", ""]
            series = data["series"]
            series.sort(key=lambda x: x["year"])
            
            count = 0
            for item in series:
                if count >= 10:
                    lines.append("... (показано первые 10)")
                    break
                val = self._format_number(item.get("revenue"))
                lines.append(f"{item['year']}: {val}")
                count += 1
            
            if len(series) > 10:
                lines.append("\nℹ️ Скажите 'xlsx' чтобы получить всё.")
                
            return {"type": "text", "content": "\n".join(lines)}

        elif intent == "profit":
            fields = ["inn", "year", "line_2400"]
            data = await rfsd_client.company_timeseries(inn, years, fields, limit=100)
            
            if not data or not data.get("rows"):
                return {"type": "text", "content": f"Нет данных по прибыли для ИНН {inn}."}
                
            lines = [f"💰 Чистая прибыль ИНН {inn}", ""]
            rows = data["rows"]
            rows.sort(key=lambda x: x.get("year", 0))
            
            count = 0
            for row in rows:
                if count >= 10:
                    lines.append("... (показано первые 10)")
                    break
                val = self._format_number(row.get("line_2400"))
                lines.append(f"{row.get('year')}: {val}")
                count += 1
                
            if len(rows) > 10:
                lines.append("\nℹ️ Скажите 'xlsx' чтобы получить всё.")
                
            return {"type": "text", "content": "\n".join(lines)}

        elif intent == "benchmark":
            data = await rfsd_client.sector_benchmark(inn, years)
            if not data or not data.get("rows"):
                # Проверяем, есть ли информация о rate limiting
                meta = data.get("meta", {}) if data else {}
                warning = meta.get("warning", "")
                rate_limit_errors = meta.get("rate_limit_errors", [])
                
                if rate_limit_errors:
                    return {
                        "type": "text", 
                        "content": f"⚠️ Rate limiting от Hugging Face для годов: {rate_limit_errors}.\n\n"
                                 f"Попробуйте запросить один год, например: 'ИНН {inn} сравни с отраслью 2023'"
                    }
                
                return {
                    "type": "text", 
                    "content": f"Не удалось построить бенчмарк для ИНН {inn}. Возможно, нет данных или не определена отрасль."
                }
            
            rows = data["rows"]
            rows.sort(key=lambda x: x.get("year", 0))
            section = rows[0].get("okved_section", "?")
            
            lines = [f"📊 Бенчмарк по отрасли (секция {section}), ИНН {inn}", ""]
            
            for r in rows:
                y = r.get("year")
                rev_comp = self._format_number(r.get("company_line_2110"))
                rev_med = self._format_number(r.get("sector_median_line_2110"))
                prof_comp = self._format_number(r.get("company_line_2400"))
                prof_med = self._format_number(r.get("sector_median_line_2400"))
                
                lines.append(f"📅 {y}")
                lines.append(f"  Выручка: {rev_comp} (Рынок: {rev_med})")
                lines.append(f"  Прибыль: {prof_comp} (Рынок: {prof_med})")
                lines.append("")
                
            lines.append(f"Всего компаний в выборке: {self._format_number(rows[0].get('sector_count'))}")
            
            # Добавляем предупреждение о rate limiting, если есть
            meta = data.get("meta", {})
            if meta.get("rate_limit_errors"):
                lines.append("")
                lines.append(f"⚠️ Некоторые годы ({meta['rate_limit_errors']}) не обработаны из-за rate limiting.")
            
            return {"type": "text", "content": "\n".join(lines)}

        else:
            fields = ["inn", "year", "line_2110", "line_2400"]
            data = await rfsd_client.company_timeseries(inn, years, fields, limit=100)
            
            if not data or not data.get("rows"):
                return {"type": "text", "content": f"Данные для ИНН {inn} не найдены."}
                
            lines = [f"🏢 Профиль ИНН {inn}", "Год | Выручка | Прибыль", "--- | --- | ---"]
            rows = data["rows"]
            rows.sort(key=lambda x: x.get("year", 0))
            
            count = 0
            for row in rows:
                if count >= 10:
                    lines.append("... (показано первые 10)")
                    break
                y = row.get("year", "")
                rev = self._format_number(row.get("line_2110"))
                prof = self._format_number(row.get("line_2400"))
                lines.append(f"{y} | {rev} | {prof}")
                count += 1
                
            lines.append("\n💡 Напишите 'xlsx', чтобы скачать полный отчет.")
            
            return {"type": "text", "content": "\n".join(lines)}

router = Router()
