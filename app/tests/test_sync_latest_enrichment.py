from contextlib import contextmanager
from datetime import UTC, datetime

import pytest

from app.ingestion.jobs import sync_latest


class FakeDB:
    def __init__(self):
        self.providers: set[str] = set()
        self.stations: set[str] = set()
        self.is_active = True
        self.merged = []

    def rollback(self):
        self.is_active = True

    def get(self, model, key):
        name = model.__name__
        if name == "Provider":
            return object() if key in self.providers else None
        if name == "Station":
            return object() if key in self.stations else None
        return None

    def add(self, provider):
        self.providers.add(provider.provider_id)

    def flush(self):
        return None

    def merge(self, station):
        self.stations.add(station.station_id)
        self.merged.append(station)

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
def fake_tracked_run(db, provider_id: str, job_type: str):
    yield FakeRunState()


class FakeEAAdapter:
    provider_id = "ea_england"

    async def fetch_latest_observations(self):
        return [{"stationReference": "A1", "unitName": "m", "latestReading": {"value": 1.1, "dateTime": datetime.now(UTC).isoformat()}}]

    def normalize_observation(self, raw):
        from app.adapters.base import NormalizedObservation

        return NormalizedObservation(
            entity_type="station",
            station_id="ea_england-A1",
            property="stage",
            observed_at=datetime.now(UTC),
            quality_code="raw",
            raw_payload=raw,
        )

    async def fetch_station_by_reference(self, station_reference: str):
        return {"notation": station_reference, "label": "EA Station", "lat": 51.1, "long": -1.2}

    def normalize_station(self, raw):
        from app.adapters.base import NormalizedStation

        return NormalizedStation(
            station_id="ea_england-A1",
            provider_id="ea_england",
            provider_station_id=raw["notation"],
            name=raw["label"],
            latitude=raw["lat"],
            longitude=raw["long"],
            raw_metadata=raw,
        )


class FakeNoopAdapter:
    def __init__(self, provider_id: str):
        self.provider_id = provider_id

    async def fetch_latest_observations(self):
        return []


@pytest.mark.asyncio
async def test_sync_latest_enriches_missing_ea_station_and_retries(monkeypatch):
    db = FakeDB()
    ea = FakeEAAdapter()

    monkeypatch.setattr(sync_latest, "USGSAdapter", lambda: FakeNoopAdapter("usgs"))
    monkeypatch.setattr(sync_latest, "EAEnglandAdapter", lambda: ea)
    monkeypatch.setattr(sync_latest, "GeoglowsAdapter", lambda: FakeNoopAdapter("geoglows"))
    monkeypatch.setattr(sync_latest, "tracked_run", fake_tracked_run)

    calls = {"count": 0}

    def fake_upsert(db, obs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise ValueError("entity missing for observation: station_id=ea_england-A1 reach_id=None")
        return (1, 1)

    monkeypatch.setattr(sync_latest, "upsert_latest_and_append_ts", fake_upsert)

    await sync_latest.run(db)

    assert "ea_england-A1" in db.stations
    assert calls["count"] == 2


@pytest.mark.asyncio
async def test_sync_latest_enrichment_uses_station_reference_for_ids(monkeypatch):
    db = FakeDB()

    class FakeEAAdapterMismatchedNotation(FakeEAAdapter):
        async def fetch_latest_observations(self):
            return [{"stationReference": "E9250", "unitName": "m", "latestReading": {"value": 0.9, "dateTime": datetime.now(UTC).isoformat()}}]

        async def fetch_station_by_reference(self, station_reference: str):
            # Endpoint payload can return notation without the prefix used by stationReference
            return {"notation": "9250", "label": "EA Station", "lat": 51.1, "long": -1.2}

        def normalize_station(self, raw):
            from app.adapters.base import NormalizedStation

            return NormalizedStation(
                station_id="ea_england-9250",
                provider_id="ea_england",
                provider_station_id="9250",
                name=raw["label"],
                latitude=raw["lat"],
                longitude=raw["long"],
                raw_metadata=raw,
            )

    ea = FakeEAAdapterMismatchedNotation()

    monkeypatch.setattr(sync_latest, "USGSAdapter", lambda: FakeNoopAdapter("usgs"))
    monkeypatch.setattr(sync_latest, "EAEnglandAdapter", lambda: ea)
    monkeypatch.setattr(sync_latest, "GeoglowsAdapter", lambda: FakeNoopAdapter("geoglows"))
    monkeypatch.setattr(sync_latest, "tracked_run", fake_tracked_run)

    calls = {"count": 0}

    def fake_upsert(_db, obs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise ValueError(f"entity missing for observation: station_id={obs.station_id} reach_id=None")
        return (1, 1)

    monkeypatch.setattr(sync_latest, "upsert_latest_and_append_ts", fake_upsert)

    await sync_latest.run(db)

    assert "ea_england-E9250" in db.stations
    assert "ea_england-9250" not in db.stations
    assert calls["count"] == 2
