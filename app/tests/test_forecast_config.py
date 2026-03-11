from app.core.config import Settings


def test_forecast_bucket_settings_from_env(monkeypatch):
    monkeypatch.setenv("GEOGLOWS_FORECAST_BUCKET", "geoglows-v2-forecasts")
    monkeypatch.setenv("GEOGLOWS_FORECAST_PREFIX", "archive")
    monkeypatch.setenv("GEOGLOWS_METADATA_BUCKET", "geoglows-v2")
    monkeypatch.setenv("GEOGLOWS_METADATA_TABLES_PREFIX", "tables")
    monkeypatch.setenv("GEOGLOWS_RETURN_PERIODS_ZARR_PATH", "s3://geoglows-v2/retrospective/return-periods.zarr")
    monkeypatch.setenv("GEOGLOWS_AWS_REGION", "us-west-2")

    settings = Settings()
    assert settings.geoglows_forecast_bucket == "geoglows-v2-forecasts"
    assert settings.geoglows_forecast_prefix == "archive"
    assert settings.geoglows_metadata_bucket == "geoglows-v2"
    assert settings.geoglows_metadata_tables_prefix == "tables"
    assert settings.geoglows_return_periods_zarr_path.endswith("return-periods.zarr")
    assert settings.geoglows_aws_region == "us-west-2"
