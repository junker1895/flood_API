from datetime import UTC, datetime

import httpx

from app.adapters.geoglows import GeoglowsAdapter


def test_normalize_reach_metadata_and_stable_id():
    adapter = GeoglowsAdapter()
    raw = {
        "reach_id": 321,
        "river": "Nile",
        "country_code": "EG",
        "lat": "30.1",
        "lon": "31.2",
        "geometry": {"type": "LineString", "coordinates": [[31.2, 30.1], [31.3, 30.2]]},
    }

    normalized = adapter.normalize_reach(raw)

    assert normalized.reach_id == "geoglows-321"
    assert normalized.provider_reach_id == "321"
    assert normalized.latitude == 30.1
    assert normalized.longitude == 31.2
    assert normalized.geometry_wkt == "LINESTRING(31.2 30.1, 31.3 30.2)"
    assert normalized.raw_metadata["modeled_source_type"] == "geoglows_streamflow"


def test_normalize_observation_forecast_and_reanalysis_semantics():
    adapter = GeoglowsAdapter()

    forecast = adapter.normalize_observation({"reach_id": "123456789", "datetime": "2024-01-01T00:00:00Z", "flow": 12.3, "series_type": "forecast"})
    reanalysis = adapter.normalize_observation({"reach_id": "123456789", "datetime": "2024-01-01T00:00:00+00:00", "flow": 9.9, "series_type": "reanalysis"})

    assert forecast.reach_id == "geoglows-123456789"
    assert forecast.is_forecast is True
    assert forecast.property == "discharge"
    assert forecast.unit_canonical == "m3/s"

    assert reanalysis.is_forecast is False
    assert reanalysis.observed_at.tzinfo == UTC


def test_fetch_latest_observations_uses_latest_point_and_river_id_param(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = ["123456789"]

    calls: list[tuple[str, dict | None]] = []

    async def fake_request_json(endpoint, params=None):
        calls.append((endpoint, params))
        if endpoint == adapter.reach_metadata_endpoint:
            assert params == {"river_id": "123456789"}
            return {"reach_id": "123456789", "lat": 1.0, "lon": 2.0}
        if endpoint == adapter.latest_endpoint:
            assert params == {"river_id": "123456789"}
            return {
                "data": [
                    {"datetime": "2024-01-01T00:00:00Z", "flow": 10.0},
                    {"datetime": "2024-01-01T01:00:00Z", "flow": 11.0},
                ]
            }
        raise AssertionError("unexpected call")

    monkeypatch.setattr(adapter, "_request_json", fake_request_json)

    import asyncio

    items = asyncio.run(adapter.fetch_latest_observations())

    assert len(items) == 1
    assert items[0]["flow"] == 11.0
    assert items[0]["series_type"] == "forecast"
    assert any(endpoint == adapter.latest_endpoint and params == {"river_id": "123456789"} for endpoint, params in calls)


def test_fetch_historical_timeseries_parses_reanalysis_with_river_id_param(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = ["123456789"]

    async def fake_request_json(endpoint, params=None):
        if endpoint == adapter.reach_metadata_endpoint:
            assert params == {"river_id": "123456789"}
            return {"reach_id": "123456789"}
        if endpoint == adapter.history_endpoint:
            assert params and params.get("river_id") == "123456789"
            assert "start_date" in params
            return {"2024-01-01T00:00:00Z": 7.1, "2024-01-01T01:00:00Z": 7.5}
        raise AssertionError("unexpected call")

    monkeypatch.setattr(adapter, "_request_json", fake_request_json)

    import asyncio

    records = asyncio.run(adapter.fetch_historical_timeseries())

    assert len(records) == 2
    assert all(r["series_type"] == "reanalysis" for r in records)


def test_fetch_latest_observations_metadata_unavailable_is_best_effort(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = ["123456789"]

    async def fake_request_json(endpoint, params=None):
        if endpoint == adapter.reach_metadata_endpoint:
            raise httpx.HTTPStatusError("failed", request=httpx.Request("GET", "http://test"), response=httpx.Response(500))
        if endpoint == adapter.latest_endpoint:
            assert params == {"river_id": "123456789"}
            return {"data": [{"datetime": "2024-01-01T01:00:00Z", "flow": 3.4}]}
        raise AssertionError("unexpected call")

    monkeypatch.setattr(adapter, "_request_json", fake_request_json)

    import asyncio

    items = asyncio.run(adapter.fetch_latest_observations())

    assert len(items) == 1
    assert items[0]["reach_id"] == "123456789"


def test_fetch_latest_observations_uses_fallback_endpoint(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = ["123456789"]
    adapter.latest_endpoints = ["/api/ForecastStats/", "/api/ForecastEnsembles/"]

    calls = []

    async def fake_request_json(endpoint, params=None):
        calls.append((endpoint, params))
        if endpoint == adapter.reach_metadata_endpoint:
            return {"reach_id": "123456789"}
        if endpoint == "/api/ForecastStats/":
            raise httpx.HTTPStatusError("failed", request=httpx.Request("GET", "http://test"), response=httpx.Response(500))
        if endpoint == "/api/ForecastEnsembles/":
            return {"data": [{"datetime": "2024-01-01T02:00:00Z", "flow": 7.7}]}
        raise AssertionError("unexpected call")

    monkeypatch.setattr(adapter, "_request_json", fake_request_json)

    import asyncio

    items = asyncio.run(adapter.fetch_latest_observations())

    assert len(items) == 1
    assert items[0]["flow"] == 7.7
    assert items[0]["meta"]["endpoint"] == "/api/ForecastEnsembles/"
    assert ("/api/ForecastStats/", {"river_id": "123456789"}) in calls
    assert ("/api/ForecastEnsembles/", {"river_id": "123456789"}) in calls


def test_fetch_reach_catalog_returns_empty_on_catalog_failure(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = []

    async def fake_request_json(endpoint, params=None):
        if endpoint == adapter.reach_catalog_endpoint:
            raise httpx.HTTPStatusError("failed", request=httpx.Request("GET", "http://test"), response=httpx.Response(500))
        raise AssertionError("unexpected call")

    monkeypatch.setattr(adapter, "_request_json", fake_request_json)

    import asyncio

    records = asyncio.run(adapter.fetch_reach_catalog())

    assert records == []


def test_invalid_configured_ids_are_filtered(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = ["1001", "abc", "123456789"]

    seen: list[tuple[str, dict | None]] = []

    async def fake_request_json(endpoint, params=None):
        seen.append((endpoint, params))
        if endpoint == adapter.reach_metadata_endpoint:
            return {"reach_id": "123456789"}
        if endpoint == adapter.latest_endpoint:
            return {"data": [{"datetime": "2024-01-01T01:00:00Z", "flow": 1.0}]}
        raise AssertionError("unexpected call")

    monkeypatch.setattr(adapter, "_request_json", fake_request_json)

    import asyncio

    items = asyncio.run(adapter.fetch_latest_observations())

    assert len(items) == 1
    assert all((params or {}).get("river_id") != "1001" for _, params in seen)
    assert any((params or {}).get("river_id") == "123456789" for _, params in seen)
