from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from app.api.deps import get_db
from app.api.main import app
from app.services.api_utils import freshness_status


class Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def _client():
    app.dependency_overrides[get_db] = lambda: None
    return TestClient(app)


def test_freshness_status_logic():
    now = datetime.now(UTC)
    assert freshness_status(None, timedelta(minutes=1), timedelta(minutes=2), now=now) == "unknown"
    assert freshness_status(now - timedelta(seconds=30), timedelta(minutes=1), timedelta(minutes=2), now=now) == "fresh"
    assert freshness_status(now - timedelta(minutes=1, seconds=1), timedelta(minutes=1), timedelta(minutes=2), now=now) == "stale"
    assert freshness_status(now - timedelta(minutes=3), timedelta(minutes=1), timedelta(minutes=2), now=now) == "old"


def test_stations_include_latest_and_geometry(monkeypatch):
    import app.api.routes.stations as routes

    station = Obj(
        station_id="s1",
        provider_id="usgs",
        source_type="observed",
        name="S",
        latitude=1.0,
        longitude=2.0,
    )
    monkeypatch.setattr(routes, "list_stations", lambda *args, **kwargs: [(station, '{"type":"Point","coordinates":[2,1]}')])
    monkeypatch.setattr(
        routes,
        "latest_embed_for_station",
        lambda *args, **kwargs: {
            "property": "stage",
            "observed_at": datetime.now(UTC),
            "value_canonical": 1.2,
            "unit_canonical": "m",
            "quality_code": "verified",
            "is_forecast": False,
            "is_provisional": False,
            "is_estimated": False,
            "is_missing": False,
            "is_flagged": False,
            "ingested_at": datetime.now(UTC),
        },
    )

    c = _client()
    r = c.get("/v1/stations?include_latest=true")
    assert r.status_code == 200
    row = r.json()["data"][0]
    assert row["geometry"]["type"] == "Point"
    assert row["latest_observation"]["property"] == "stage"


def test_reaches_latest_bbox_passthrough(monkeypatch):
    import app.api.routes.reaches as routes

    called = {}

    def fake_latest(db, property, limit, bbox=None):
        called["bbox"] = bbox
        return []

    monkeypatch.setattr(routes, "latest_for_reaches", fake_latest)
    c = _client()
    r = c.get("/v1/reaches/latest?bbox=1,2,3,4")
    assert r.status_code == 200
    assert called["bbox"] == (1.0, 2.0, 3.0, 4.0)


def test_stations_map_endpoint(monkeypatch):
    import app.api.routes.stations as routes

    monkeypatch.setattr(
        routes,
        "station_map_rows",
        lambda *args, **kwargs: [
            {
                "station_id": "s1",
                "provider_id": "usgs",
                "source_type": "observed",
                "provider_station_id": "1",
                "name": "A",
                "river_name": None,
                "country_code": None,
                "admin1": None,
                "admin2": None,
                "latitude": 1.0,
                "longitude": 2.0,
                "geometry": '{"type":"Point","coordinates":[2,1]}',
                "canonical_primary_property": "stage",
                "station_status": None,
                "observed_at": None,
                "value_native": None,
                "unit_native": None,
                "value_canonical": None,
                "unit_canonical": None,
                "property": None,
                "quality_code": None,
                "quality_score": None,
                "aggregation": None,
                "is_forecast": False,
                "is_provisional": False,
                "is_estimated": False,
                "is_missing": False,
                "is_flagged": False,
                "ingested_at": None,
                "freshness_status": "unknown",
                "threshold_summary": None,
                "warning_summary": None,
            }
        ],
    )
    c = _client()
    r = c.get("/v1/stations/map?bbox=1,2,3,4")
    assert r.status_code == 200
    assert r.json()["data"][0]["geometry"]["type"] == "Point"


def test_reaches_map_endpoint(monkeypatch):
    import app.api.routes.reaches as routes

    monkeypatch.setattr(
        routes,
        "reach_map_rows",
        lambda *args, **kwargs: [
            {
                "reach_id": "r1",
                "provider_id": "geoglows",
                "source_type": "modeled",
                "provider_reach_id": "1",
                "name": None,
                "river_name": None,
                "country_code": None,
                "network_name": None,
                "latitude": 1.0,
                "longitude": 2.0,
                "geometry": '{"type":"LineString","coordinates":[[2,1],[3,2]]}',
                "observed_at": None,
                "value_native": None,
                "unit_native": None,
                "value_canonical": None,
                "unit_canonical": None,
                "property": None,
                "quality_code": None,
                "quality_score": None,
                "aggregation": None,
                "is_forecast": True,
                "is_provisional": False,
                "is_estimated": False,
                "is_missing": False,
                "is_flagged": False,
                "ingested_at": None,
                "freshness_status": "unknown",
                "threshold_summary": None,
                "warning_summary": None,
            }
        ],
    )
    c = _client()
    r = c.get("/v1/reaches/map?bbox=1,2,3,4")
    assert r.status_code == 200
    assert r.json()["data"][0]["geometry"]["type"] == "LineString"


def test_threshold_detail_response(monkeypatch):
    import app.api.routes.stations as routes

    monkeypatch.setattr(
        routes,
        "station_thresholds",
        lambda *args, **kwargs: [Obj(threshold_id="t1", entity_type="station", station_id="s1", reach_id=None, property="stage", threshold_type="alert", threshold_label="minor", severity_rank=1, value_native=1.0, unit_native="m", value_canonical=1.0, unit_canonical="m", effective_from=None, effective_to=None, source="manual", method=None)],
    )
    c = _client()
    r = c.get("/v1/stations/s1/thresholds")
    assert r.status_code == 200
    assert r.json()["data"][0]["threshold_label"] == "minor"


def test_warning_active_and_geometry(monkeypatch):
    import app.api.routes.warnings as routes

    warning = Obj(
        warning_id="w1",
        provider_id="ea_england",
        country_code="GB",
        warning_type="flood",
        severity="severe",
        title="Flood warning",
        description="desc",
        issued_at=None,
        effective_from=None,
        effective_to=None,
        status="active",
        related_station_ids=["s1"],
        related_reach_ids=None,
        ingested_at=None,
    )
    monkeypatch.setattr(routes, "active_warnings", lambda *args, **kwargs: [(warning, '{"type":"Polygon","coordinates":[]}')])
    c = _client()
    r = c.get("/v1/warnings/active?bbox=1,2,3,4")
    assert r.status_code == 200
    row = r.json()["data"][0]
    assert row["geometry"]["type"] == "Polygon"
    assert row["related_station_ids"] == ["s1"]
