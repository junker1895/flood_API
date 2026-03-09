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


settings = Settings()
