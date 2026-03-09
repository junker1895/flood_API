from contextlib import contextmanager
from datetime import UTC, datetime

from app.ingestion.jobs import sync_latest


class FakeDB:
    def __init__(self):
        self.providers = set()
        self.stations = {"usgs-01646500"}
        self.reaches = {"geoglows-1001"}
        self.is_active = True

    def rollback(self):
        self.is_active = True

    def get(self, model, key):
        if model.__name__ == "Provider":
            return object() if key in self.providers else None
        if model.__name__ == "Station":
            return object() if key in self.stations else None
        if model.__name__ == "Reach":
            return object() if key in self.reaches else None
        return None

    def add(self, provider):
        self.providers.add(provider.provider_id)

    def flush(self):
        return None

    def merge(self, _entity):
        return None

    @contextmanager
    def begin_nested(self):
        yield

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

    async def fetch_latest_observations(self):
        return [{"kind": "usgs"}]

    def normalize_observation(self, raw):
        from app.adapters.base import NormalizedObservation

        return NormalizedObservation(
            entity_type="station",
            station_id="usgs-01646500",
            property="discharge",
            observed_at=datetime.now(UTC),
            quality_code="raw",
            raw_payload=raw,
        )


class FakeNoopAdapter:
    def __init__(self, provider_id):
        self.provider_id = provider_id

    async def fetch_latest_observations(self):
        return []


def test_sync_latest_bootstrap_upserts_when_inventory_exists(monkeypatch):
    db = FakeDB()
    calls = {"count": 0}

    monkeypatch.setattr(sync_latest, "USGSAdapter", lambda: FakeUSGSAdapter())
    monkeypatch.setattr(sync_latest, "EAEnglandAdapter", lambda: FakeNoopAdapter("ea_england"))
    monkeypatch.setattr(sync_latest, "GeoglowsAdapter", lambda: FakeNoopAdapter("geoglows"))
    monkeypatch.setattr(sync_latest, "tracked_run", fake_tracked_run)

    def fake_upsert(_db, _obs):
        calls["count"] += 1
        return (1, 1)

    monkeypatch.setattr(sync_latest, "upsert_latest_and_append_ts", fake_upsert)

    import asyncio
    asyncio.run(sync_latest.run(db))

    assert calls["count"] == 1
