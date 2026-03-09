from fastapi.testclient import TestClient

from app.api.main import app


def test_health_live():
    c = TestClient(app)
    r = c.get("/health/live")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
