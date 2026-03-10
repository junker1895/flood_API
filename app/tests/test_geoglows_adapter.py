from datetime import UTC, datetime

from app.adapters.geoglows import GeoglowsAdapter


async def _fake_request_json_factory(responses):
    async def _fake_request_json(_endpoint, params=None):
        key = (_endpoint, tuple(sorted((params or {}).items())))
        return responses[key]

    return _fake_request_json


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

    forecast = adapter.normalize_observation({"reach_id": "10", "datetime": "2024-01-01T00:00:00Z", "flow": 12.3, "series_type": "forecast"})
    reanalysis = adapter.normalize_observation({"reach_id": "10", "datetime": "2024-01-01T00:00:00+00:00", "flow": 9.9, "series_type": "reanalysis"})

    assert forecast.reach_id == "geoglows-10"
    assert forecast.is_forecast is True
    assert forecast.property == "discharge"
    assert forecast.unit_canonical == "m3/s"

    assert reanalysis.is_forecast is False
    assert reanalysis.observed_at.tzinfo == UTC


def test_fetch_latest_observations_uses_latest_point(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = ["1001"]

    responses = {
        (adapter.reach_metadata_endpoint, (("reach_id", "1001"),)): {"reach_id": "1001", "lat": 1.0, "lon": 2.0},
        (adapter.latest_endpoint, (("reach_id", "1001"),)): {
            "data": [
                {"datetime": "2024-01-01T00:00:00Z", "flow": 10.0},
                {"datetime": "2024-01-01T01:00:00Z", "flow": 11.0},
            ]
        },
    }

    async def fake_request_json(endpoint, params=None):
        return responses[(endpoint, tuple(sorted((params or {}).items())))]

    monkeypatch.setattr(adapter, "_request_json", fake_request_json)

    import asyncio

    items = asyncio.run(adapter.fetch_latest_observations())

    assert len(items) == 1
    assert items[0]["flow"] == 11.0
    assert items[0]["series_type"] == "forecast"


def test_fetch_historical_timeseries_parses_reanalysis(monkeypatch):
    adapter = GeoglowsAdapter()
    adapter.reach_ids = ["1001"]

    responses = {
        (adapter.reach_metadata_endpoint, (("reach_id", "1001"),)): {"reach_id": "1001"},
        (adapter.history_endpoint, (("reach_id", "1001"), ("start_date", (datetime.now(UTC)).date().isoformat()))): {
            "2024-01-01T00:00:00Z": 7.1,
            "2024-01-01T01:00:00Z": 7.5,
        },
    }

    async def fake_request_json(endpoint, params=None):
        key = (endpoint, tuple(sorted((params or {}).items())))
        if endpoint == adapter.history_endpoint:
            # ignore start_date exact value drift
            return {"2024-01-01T00:00:00Z": 7.1, "2024-01-01T01:00:00Z": 7.5}
        return responses[key]

    monkeypatch.setattr(adapter, "_request_json", fake_request_json)

    import asyncio

    records = asyncio.run(adapter.fetch_historical_timeseries())

    assert len(records) == 2
    assert all(r["series_type"] == "reanalysis" for r in records)
