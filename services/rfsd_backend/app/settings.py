"""Настройки приложения RFSD Backend."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Настройки приложения."""

    app_name: str = "RFSD Backend"
    debug: bool = False
    HF_TOKEN: str | None = None
    OPENAI_API_KEY: str | None = None
    LLM_ENDPOINT_URL: str | None = None  # Colab или другой совместимый endpoint
    CACHE_YEARS: str = "2020,2021,2022,2023,2024"  # Годы для кэширования (через запятую)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()
