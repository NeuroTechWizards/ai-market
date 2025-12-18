"""Настройки приложения RFSD Backend."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Настройки приложения."""

    app_name: str = "RFSD Backend"
    debug: bool = False
    HF_TOKEN: str | None = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()
