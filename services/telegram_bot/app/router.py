import re
import logging
from datetime import datetime
from typing import Any
from .rfsd_client import rfsd_client

logger = logging.getLogger(__name__)

class Router:
    """Парсер и роутер сообщений (NL-интерпретатор)."""
    
    def extract_inn(self, text: str) -> str | None:
        # Ищем 10 или 12 цифр
        match = re.search(r'\b\d{10}\b|\b\d{12}\b', text)
        if match:
            return match.group(0)
        return None

    def parse_years(self, text: str) -> list[int]:
        """Извлекает годы из текста или возвращает дефолт."""
        # 1. Диапазон: 2021-2023, 2021–2023
        range_match = re.search(r'(\d{4})\s*[-–—]\s*(\d{4})', text)
        if range_match:
            start = int(range_match.group(1))
            end = int(range_match.group(2))
            if start > end:
                start, end = end, start
            return list(range(start, end + 1))

        # 2. "За N лет", "последние N лет"
        # Ищем число рядом со словом лет/год
        last_match = re.search(r'(?:за|последни[ех])\s*(\d+)\s*(?:лет|год|г\.?)', text, re.IGNORECASE)
        if last_match:
            n = int(last_match.group(1))
            # Ограничиваем верхней границей (данные есть пока до 2023/2024)
            # Берем 2023 как безопасный максимум
            end_year = 2023 
            start_year = end_year - n + 1
            return list(range(start_year, end_year + 1))

        # 3. Default (последние 5 лет)
        return [2019, 2020, 2021, 2022, 2023]

    def parse_intent_format(self, text: str) -> tuple[str, str]:
        """Определяет намерение и формат ответа."""
        text_lower = text.lower()
        
        # Формат
        fmt = "text"
        if any(kw in text_lower for kw in ["xlsx", "эксель", "excel"]):
            fmt = "xlsx"
            
        # Интент
        if "выруч" in text_lower:
            intent = "revenue"
        elif "прибыл" in text_lower:
            intent = "profit"
        elif any(kw in text_lower for kw in ["профиль", "все показатели", "все поля", "полный"]):
            intent = "full_profile"
        else:
            intent = "full_profile" # Дефолтное поведение (базовый профиль)
            
        return intent, fmt

    def _format_number(self, val: Any) -> str:
        if val is None:
            return "-"
        try:
            # Форматируем с разделителем тысяч
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

        # --- Обработка XLSX (единая точка входа) ---
        if fmt == "xlsx":
            # Для любого интента в XLSX формате пока используем полный профиль, 
            # так как он содержит всё (и выручку, и прибыль).
            # filename делаем понятным
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

        # --- Обработка Текстовых запросов ---
        
        # 1. Выручка
        if intent == "revenue":
            data = await rfsd_client.company_revenue_timeseries(inn, years)
            if not data or not data.get("series"):
                return {"type": "text", "content": f"Нет данных по выручке для ИНН {inn}."}
            
            lines = [f"📊 Выручка ИНН {inn}", ""]
            series = data["series"]
            # Гарантируем сортировку
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

        # 2. Прибыль (line_2400)
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

        # 3. Базовый профиль (Full Profile Text)
        else:
            # Дефолтные поля для краткого просмотра
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
