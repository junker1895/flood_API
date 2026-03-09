from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    app_name: str = "global-hydrology-feed"
    environment: str = "dev"
    database_url: str = "sqlite+pysqlite:///./dev.db"
    log_level: str = "INFO"
    normalization_version: str = "v1"
    api_prefix: str = "/v1"
    default_limit: int = 100
    max_limit: int = 1000

    enable_provider_usgs: bool = True
    enable_provider_ea: bool = True
    enable_provider_geoglows: bool = True
    enable_provider_whos: bool = False

    usgs_poll_minutes: int = 10
    ea_poll_minutes: int = 15
    geoglows_poll_minutes: int = 30

    geoglows_api_key: str | None = None


settings = Settings()
