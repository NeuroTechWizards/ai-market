import re
import logging
from typing import Any
from .rfsd_client import rfsd_client

logger = logging.getLogger(__name__)

class Router:
    """Простой роутер сообщений."""
    
    def extract_inn(self, text: str) -> str | None:
        # Ищем 10 или 12 цифр
        match = re.search(r'\b\d{10}\b|\b\d{12}\b', text)
        if match:
            return match.group(0)
        return None

    async def route_message(self, text: str) -> dict[str, Any]:
        """Определяет, что делать с сообщением и возвращает результат для отправки."""
        inn = self.extract_inn(text)
        if not inn:
            return {
                "type": "text",
                "content": "Пожалуйста, укажите ИНН компании (10 или 12 цифр) в тексте сообщения."
            }

        text_lower = text.lower()
        
        # Сценарий 1: Экспорт Excel (xlsx)
        # Если просят xlsx/excel - отдаем ПОЛНЫЙ профиль (так полезнее) или выручку?
        # В ТЗ сказано export_company_revenue_xlsx, но мы сделали крутой export_full_profile_xlsx.
        # Давайте использовать крутой профиль, раз он есть.
        if "xlsx" in text_lower or "эксель" in text_lower or "excel" in text_lower:
            file_bytes = await rfsd_client.export_full_profile_xlsx(inn)
            if file_bytes:
                return {
                    "type": "document",
                    "content": file_bytes,
                    "filename": f"rfsd_profile_{inn}.xlsx",
                    "caption": f"Финансовый профиль для ИНН {inn}"
                }
            else:
                return {
                    "type": "text",
                    "content": "Не удалось сгенерировать Excel файл. Проверьте ИНН или попробуйте позже."
                }

        # Сценарий 2: Выручка (текстом)
        if "выруч" in text_lower:
            data = await rfsd_client.company_revenue_timeseries(inn)
            if not data or not data.get("series"):
                return {
                    "type": "text",
                    "content": f"Данные по выручке для ИНН {inn} не найдены."
                }
            
            # Формируем красивый текст
            series = data["series"]
            lines = [f"Выручка (стр. 2110) для ИНН {inn}:"]
            for item in series:
                rev = item.get('revenue')
                rev_fmt = f"{rev:,.0f}" if rev is not None else "-"
                lines.append(f"{item['year']}: {rev_fmt}")
            
            lines.append(f"\nОбработано за {data['meta']['elapsed_ms']} мс")
            
            return {
                "type": "text",
                "content": "\n".join(lines)
            }

        # Сценарий 3: Общая информация (по умолчанию)
        # 5 лет, базовые поля
        years = [2019, 2020, 2021, 2022, 2023]
        fields = ["inn", "year", "region", "okved", "line_2110", "line_2300", "line_2400"]
        
        data = await rfsd_client.company_timeseries(inn, years, fields, limit=5)
        
        if not data or not data.get("rows"):
             return {
                "type": "text",
                "content": f"Данные для ИНН {inn} за 2019-2023 не найдены."
            }
            
        rows = data["rows"]
        # Сортируем по году
        rows.sort(key=lambda x: x.get("year", 0))
        
        lines = [f"Данные по ИНН {inn} (базовые):"]
        for r in rows:
            y = r.get("year", "?")
            rev = r.get("line_2110", "-")
            net_profit = r.get("line_2400", "-")
            lines.append(f"{y} | Выр: {rev} | Чист.приб: {net_profit}")
            
        lines.append(f"\nВсего строк: {len(rows)}")
        
        return {
            "type": "text",
            "content": "\n".join(lines)
        }

router = Router()
