from datetime import UTC

import httpx

from app.adapters.geoglows import GeoglowsAdapter


def test_normalize_reach_metadata_and_stable_id():
    adapter = GeoglowsAdapter()
    raw = {
        "river_id": 902800057,
        "river": "Nile",
        "country_code": "EG",
        "lat": "30.1",
        "lon": "31.2",
        "geometry": {"type": "LineString", "coordinates": [[31.2, 30.1], [31.3, 30.2]]},
    }

    normalized = adapter.normalize_reach(raw)

    assert normalized.reach_id == "geoglows-902800057"
    assert normalized.provider_reach_id == "902800057"
    assert normalized.latitude == 30.1
    assert normalized.longitude == 31.2
    assert normalized.geometry_wkt == "LINESTRING(31.2 30.1, 31.3 30.2)"


def test_normalize_observation_forecast_and_reanalysis_semantics():
    adapter = GeoglowsAdapter()

    forecast = adapter.normalize_observation({"reach_id": "902800057", "datetime": "2024-01-01T00:00:00Z", "flow": 12.3, "series_type": "forecast"})
    reanalysis = adapter.normalize_observation({"reach_id": "902800057", "datetime": "2024-01-01T00:00:00+00:00", "flow": 9.9, "series_type": "reanalysis"})

    assert forecast.reach_id == "geoglows-902800057"
    assert forecast.is_forecast is True
    assert reanalysis.is_forecast is False
    assert reanalysis.observed_at.tzinfo == UTC


def test_fetch_latest_observations_v2_forecaststats(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = ["902800057"]

    calls = []

    async def fake_url(url, params=None):
        calls.append((url, params))
        if "/forecaststats/902800057" in url:
            return {
                "datetime": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"],
                "mean": [10.0, 11.0],
            }
        raise AssertionError("unexpected v2 url")

    async def fake_legacy(endpoint, params=None):
        assert endpoint == adapter.reach_metadata_endpoint
        return {"river_id": "902800057", "lat": 1.0, "lon": 2.0}

    monkeypatch.setattr(adapter, "_request_json_url", fake_url)
    monkeypatch.setattr(adapter, "_request_json_legacy", fake_legacy)

    import asyncio

    items = asyncio.run(adapter.fetch_latest_observations())

    assert len(items) == 1
    assert items[0]["flow"] == 11.0
    assert items[0]["meta"]["product"] == "forecaststats"
    assert any(params in ({}, None) for _, params in calls)


def test_fetch_latest_observations_falls_back_to_v2_ensembles(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = ["902800057"]

    async def fake_url(url, params=None):
        if "/forecaststats/902800057" in url:
            raise httpx.HTTPStatusError("failed", request=httpx.Request("GET", "http://test"), response=httpx.Response(500))
        if "/forecastensemble/902800057" in url:
            return {"datetime": ["2024-01-01T00:00:00Z"], "ensemble_52": [4.2]}
        raise AssertionError("unexpected v2 url")

    async def fake_legacy(_endpoint, params=None):
        return {"river_id": "902800057"}

    monkeypatch.setattr(adapter, "_request_json_url", fake_url)
    monkeypatch.setattr(adapter, "_request_json_legacy", fake_legacy)

    import asyncio

    items = asyncio.run(adapter.fetch_latest_observations())

    assert len(items) == 1
    assert items[0]["flow"] == 4.2
    assert items[0]["meta"]["product"] == "forecastensemble"


def test_fetch_historical_timeseries_v2_retrospective_columnar(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = ["902800057"]
    adapter.history_lookback_days = 36500

    async def fake_url(url, params=None):
        assert "/retrospectivedaily/902800057" in url
        assert params is None
        return {
            "datetime": ["2024-01-01", "2024-01-02"],
            "902800057": [7.1, 7.5],
        }

    async def fake_legacy(_endpoint, params=None):
        return {"river_id": "902800057"}

    monkeypatch.setattr(adapter, "_request_json_url", fake_url)
    monkeypatch.setattr(adapter, "_request_json_legacy", fake_legacy)

    import asyncio

    records = asyncio.run(adapter.fetch_historical_timeseries())

    assert len(records) == 2
    assert all(r["series_type"] == "reanalysis" for r in records)


def test_metadata_best_effort_does_not_block_latest(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = ["902800057"]

    async def fake_url(url, params=None):
        if "/forecaststats/902800057" in url:
            return {"datetime": ["2024-01-01T01:00:00Z"], "mean": [3.4]}
        raise AssertionError("unexpected v2 url")

    async def fake_legacy(_endpoint, params=None):
        raise httpx.HTTPStatusError("failed", request=httpx.Request("GET", "http://test"), response=httpx.Response(500))

    monkeypatch.setattr(adapter, "_request_json_url", fake_url)
    monkeypatch.setattr(adapter, "_request_json_legacy", fake_legacy)

    import asyncio

    items = asyncio.run(adapter.fetch_latest_observations())

    assert len(items) == 1
    assert items[0]["reach_id"] == "902800057"


def test_invalid_configured_ids_are_filtered(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = ["1001", "abc", "123456789", "902800057"]

    seen = []

    async def fake_url(url, params=None):
        seen.append(url)
        if "/forecaststats/902800057" in url:
            return {"datetime": ["2024-01-01T01:00:00Z"], "mean": [1.0]}
        raise AssertionError("unexpected v2 url")

    async def fake_legacy(_endpoint, params=None):
        return {"river_id": "902800057"}

    monkeypatch.setattr(adapter, "_request_json_url", fake_url)
    monkeypatch.setattr(adapter, "_request_json_legacy", fake_legacy)

    import asyncio

    items = asyncio.run(adapter.fetch_latest_observations())

    assert len(items) == 1
    assert any("/forecaststats/902800057" in url for url in seen)
    assert all("1001" not in url and "123456789" not in url for url in seen)
