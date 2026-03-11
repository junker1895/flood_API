from datetime import date, datetime, UTC

from fastapi.testclient import TestClient

from app.api.deps import get_db
from app.api.main import app


class Obj:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def _client():
    app.dependency_overrides[get_db] = lambda: None
    return TestClient(app)


def test_forecast_meta_response(monkeypatch):
    import app.api.routes.forecast as routes

    monkeypatch.setattr(
        routes,
        "latest_run",
        lambda db, model: Obj(
            model=model,
            forecast_date=date(2026, 3, 11),
            timestep_count=40,
            timestep_hours=3,
            timesteps_json=["2026-03-11T00:00:00Z"],
        ),
    )

    c = _client()
    r = c.get("/v1/forecast/meta?model=geoglows")
    assert r.status_code == 200
    assert r.json()["model"] == "geoglows"
    assert r.json()["timestep_count"] == 40


def test_forecast_reaches_response(monkeypatch):
    import app.api.routes.forecast as routes

    monkeypatch.setattr(routes, "pick_run_date", lambda db, model, forecast_date: date(2026, 3, 11))
    monkeypatch.setattr(
        routes,
        "reach_risks_in_bbox",
        lambda db, model, forecast_date, bbox: [Obj(reach_id=10000042, risk_class=2, peak_time=datetime(2026, 3, 12, 9, 0, tzinfo=UTC))],
    )

    c = _client()
    r = c.get("/v1/forecast/reaches?model=geoglows&bbox=144,-38,154,-25")
    assert r.status_code == 200
    reaches = r.json()["reaches"]
    assert reaches["10000042"]["risk_class"] == 2


def test_forecast_reach_detail_unavailable(monkeypatch):
    import app.api.routes.forecast as routes

    monkeypatch.setattr(routes, "pick_run_date", lambda db, model, forecast_date: date(2026, 3, 11))
    monkeypatch.setattr(routes, "reach_detail", lambda db, model, forecast_date, reach_id: [])

    c = _client()
    r = c.get("/v1/forecast/reach/10000042?model=geoglows")
    assert r.status_code == 200
    assert r.json()["detail_available"] is False
