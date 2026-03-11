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
    geoglows_api_base_url: str = "https://geoglows.ecmwf.int"
    geoglows_reach_ids: str | None = None
    geoglows_region: str | None = None
    geoglows_history_lookback_days: int = 7
    geoglows_timeout_seconds: float = 30.0

    forecast_default_model: str = "geoglows"
    forecast_major_river_threshold: float = 5000.0
    forecast_detail_river_threshold: float = 10000.0
    forecast_priority_region_ids: str = ""
    forecast_detail_region_ids: str = ""

    geoglows_forecast_bucket: str = "geoglows-v2-forecasts"
    geoglows_forecast_prefix: str = ""
    geoglows_metadata_bucket: str = "geoglows-v2"
    geoglows_metadata_tables_prefix: str = "tables"
    geoglows_return_periods_zarr_path: str = "s3://geoglows-v2/retrospective/return-periods.zarr"
    geoglows_aws_region: str = "us-west-2"

    station_fresh_minutes: int = 30
    station_stale_minutes: int = 360
    reach_fresh_minutes: int = 720
    reach_stale_minutes: int = 2880


settings = Settings()
