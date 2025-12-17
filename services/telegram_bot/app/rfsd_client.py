import httpx
import logging
from .settings import settings

logger = logging.getLogger(__name__)

class RFSDClient:
    def __init__(self):
        self.base_url = settings.RFSD_BACKEND_URL
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=30.0)

    async def health(self) -> bool:
        try:
            resp = await self.client.get("/health")
            return resp.status_code == 200
        except Exception:
            return False

    async def company_revenue_timeseries(self, inn: str, years: list[int] | None = None):
        try:
            payload = {"inn": inn, "years": years}
            resp = await self.client.post("/rfsd/company_revenue_timeseries", json=payload)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Error getting revenue: {e}")
            return None

    async def company_timeseries(self, inn: str, years: list[int], fields: list[str], limit: int):
        try:
            payload = {
                "inn": inn,
                "years": years,
                "fields": fields,
                "limit": limit
            }
            resp = await self.client.post("/rfsd/company_timeseries", json=payload)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Error getting company data: {e}")
            return None

    async def export_company_revenue_xlsx(self, inn: str, years: list[int] | None = None) -> bytes | None:
        try:
            payload = {"inn": inn, "years": years}
            resp = await self.client.post("/rfsd/export_company_revenue_xlsx", json=payload, timeout=60.0)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.error(f"Error exporting xlsx: {e}")
            return None
    
    async def export_full_profile_xlsx(self, inn: str, years: list[int] | None = None) -> bytes | None:
        """Экспорт полного профиля (универсальный)"""
        try:
            payload = {"inn": inn, "years": years}
            # Добавляем Connection: close, чтобы избежать проблем с keep-alive при долгих запросах
            resp = await self.client.post(
                "/rfsd/export_full_profile_xlsx", 
                json=payload, 
                timeout=120.0,
                headers={"Connection": "close"}
            )
            
            if resp.status_code != 200:
                logger.error(f"Backend error: {resp.status_code} - {resp.text}")
                return None
                
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.error(f"Error exporting full profile: {e}")
            return None

rfsd_client = RFSDClient()
