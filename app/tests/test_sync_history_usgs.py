from contextlib import contextmanager
from datetime import UTC, datetime

from app.ingestion.jobs import sync_history


class FakeDB:
    def __init__(self):
        self.stations = {"usgs-01646500"}
        self.is_active = True

    def get(self, model, key):
        if model.__name__ == "Station":
            return object() if key in self.stations else None
        return object()

    def commit(self):
        return None


class FakeRunState:
    def __init__(self):
        self.records_seen = 0
        self.records_inserted = 0
        self.records_updated = 0
        self.records_failed = 0
        self.error_summary = None


@contextmanager
def fake_tracked_run(_db, _provider_id, _job_type):
    yield FakeRunState()


class FakeUSGSAdapter:
    provider_id = "usgs"
    supports_history = True

    async def fetch_historical_timeseries(self):
        return [{"site": "1"}, {"site": "2"}]

    def normalize_observation(self, raw):
        from app.adapters.base import NormalizedObservation

        station = "usgs-01646500" if raw["site"] == "1" else "usgs-missing"
        return [
            NormalizedObservation(
                entity_type="station",
                station_id=station,
                property="discharge",
                observed_at=datetime.now(UTC),
                quality_code="raw",
                raw_payload=raw,
            )
        ]


class FakeNoHistoryAdapter:
    provider_id = "ea_england"
    supports_history = False


def test_sync_history_partial_station_failure(monkeypatch):
    db = FakeDB()
    calls = {"append": 0, "enrich": 0}

    monkeypatch.setattr(sync_history, "USGSAdapter", lambda: FakeUSGSAdapter())
    monkeypatch.setattr(sync_history, "EAEnglandAdapter", lambda: FakeNoHistoryAdapter())
    monkeypatch.setattr(sync_history, "GeoglowsAdapter", lambda: FakeNoHistoryAdapter())
    monkeypatch.setattr(sync_history, "tracked_run", fake_tracked_run)

    def fake_append(_db, obs):
        calls["append"] += 1
        if obs.station_id == "usgs-missing":
            raise ValueError("entity missing for observation")
        return 1

    async def fake_enrich(_db, _adapter, _obs):
        calls["enrich"] += 1
        return False

    monkeypatch.setattr(sync_history, "append_timeseries", fake_append)
    monkeypatch.setattr(sync_history, "_enrich_usgs_station_if_missing", fake_enrich)

    import asyncio

    asyncio.run(sync_history.run(db, provider_id="usgs"))
    assert calls["append"] == 2
    assert calls["enrich"] == 1
