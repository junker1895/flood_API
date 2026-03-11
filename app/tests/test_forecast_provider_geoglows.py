import json

import pytest

from app.forecast_models.geoglows import GeoglowsForecastProvider


def test_geoglows_provider_schema_discovery_and_chunking(monkeypatch, tmp_path):
    metadata = {
        "data": [
            {"LINKNO": 101, "lat": 1.0, "lon": 2.0, "uparea": 3.0, "rp2": 5.0},
            {"comid": 102, "latitude": 4.0, "longitude": 5.0, "upstream_area": 6.0, "rp10": 7.0},
        ]
    }
    manifest = {"data": {"forecast_date": "2026-03-11", "timestep_hours": 3, "timesteps": ["2026-03-11T00:00:00Z"]}}
    run_payload = {"data": [{"reach_id": 101, "timesteps": [{"valid_time": "2026-03-11T00:00:00Z"}]}]}

    metadata_file = tmp_path / "metadata.json"
    manifest_file = tmp_path / "manifest.json"
    run_file = tmp_path / "run_2026-03-11.json"
    metadata_file.write_text(json.dumps(metadata))
    manifest_file.write_text(json.dumps(manifest))
    run_file.write_text(json.dumps(run_payload))

    monkeypatch.setattr("app.forecast_models.geoglows.settings.geoglows_forecast_reach_metadata_url", str(metadata_file))
    monkeypatch.setattr("app.forecast_models.geoglows.settings.geoglows_forecast_run_manifest_url", str(manifest_file))
    monkeypatch.setattr(
        "app.forecast_models.geoglows.settings.geoglows_forecast_run_data_url_template",
        str(tmp_path / "run_{forecast_date}.json"),
    )

    provider = GeoglowsForecastProvider()
    chunks = list(provider.iter_reach_metadata_chunks(chunk_size=1))

    assert len(chunks) == 2
    assert chunks[0][0]["reach_id"] == 101
    assert chunks[1][0]["reach_id"] == 102

    run = provider.discover_run()
    assert run.forecast_date.isoformat() == "2026-03-11"

    run_chunks = list(provider.iter_run_forecast_chunks(run, chunk_size=1))
    assert run_chunks[0][0]["reach_id"] == 101


def test_geoglows_provider_fails_fast_on_missing_reach_id(monkeypatch, tmp_path):
    metadata_file = tmp_path / "bad_metadata.json"
    metadata_file.write_text(json.dumps({"data": [{"lat": 1.0, "lon": 2.0}]}))

    monkeypatch.setattr("app.forecast_models.geoglows.settings.geoglows_forecast_reach_metadata_url", str(metadata_file))
    monkeypatch.setattr("app.forecast_models.geoglows.settings.geoglows_forecast_run_manifest_url", str(metadata_file))
    monkeypatch.setattr("app.forecast_models.geoglows.settings.geoglows_forecast_run_data_url_template", str(metadata_file))

    provider = GeoglowsForecastProvider()
    with pytest.raises(ValueError, match="reach identifier"):
        list(provider.iter_reach_metadata_chunks())
