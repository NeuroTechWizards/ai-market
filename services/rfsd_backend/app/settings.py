"""Настройки приложения RFSD Backend."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Настройки приложения."""

    app_name: str = "RFSD Backend"
    debug: bool = False

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
