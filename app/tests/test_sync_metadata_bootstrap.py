from contextlib import contextmanager

from app.ingestion.jobs import sync_metadata


class FakeDB:
    def __init__(self):
        self.providers = set()
        self.stations = set()
        self.reaches = set()
        self.committed = False
        self.is_active = True

    def get(self, model, key):
        if model.__name__ == "Provider":
            return object() if key in self.providers else None
        return None

    def add(self, provider):
        self.providers.add(provider.provider_id)

    def flush(self):
        return None

    def merge(self, entity):
        if hasattr(entity, "station_id") and entity.station_id:
            self.stations.add(entity.station_id)
        if hasattr(entity, "reach_id") and entity.reach_id:
            self.reaches.add(entity.reach_id)

    def commit(self):
        self.committed = True


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

    async def fetch_station_catalog(self):
        return [{"id": "01646500"}]

    def normalize_station(self, raw):
        from app.adapters.base import NormalizedStation

        return NormalizedStation(
            station_id="usgs-01646500",
            provider_id="usgs",
            provider_station_id="01646500",
            name="USGS Station",
            latitude=10.0,
            longitude=11.0,
            raw_metadata=raw,
        )


class FakeEAAdapter:
    provider_id = "ea_england"

    async def fetch_station_catalog(self):
        return [{"id": "A1"}]

    def normalize_station(self, raw):
        from app.adapters.base import NormalizedStation

        return NormalizedStation(
            station_id="ea_england-A1",
            provider_id="ea_england",
            provider_station_id="A1",
            name="EA Station",
            latitude=51.1,
            longitude=-1.1,
            raw_metadata=raw,
        )


class FakeGeoglowsAdapter:
    provider_id = "geoglows"

    async def fetch_reach_catalog(self):
        return [{"reach_id": "1001", "lat": 0.0, "lon": 0.0}]

    def normalize_reach(self, raw):
        from app.adapters.base import NormalizedReach

        return NormalizedReach(
            reach_id="geoglows-1001",
            provider_id="geoglows",
            provider_reach_id="1001",
            latitude=raw["lat"],
            longitude=raw["lon"],
            raw_metadata=raw,
        )


def test_sync_metadata_bootstrap_populates_providers_stations_reaches(monkeypatch):
    db = FakeDB()

    monkeypatch.setattr(sync_metadata, "tracked_run", fake_tracked_run)
    monkeypatch.setattr(sync_metadata, "USGSAdapter", lambda: FakeUSGSAdapter())
    monkeypatch.setattr(sync_metadata, "EAEnglandAdapter", lambda: FakeEAAdapter())
    monkeypatch.setattr(sync_metadata, "GeoglowsAdapter", lambda: FakeGeoglowsAdapter())

    import asyncio
    asyncio.run(sync_metadata.run(db))

    assert {"usgs", "ea_england", "geoglows", "whos"}.issubset(db.providers)
    assert "usgs-01646500" in db.stations
    assert "ea_england-A1" in db.stations
    assert "geoglows-1001" in db.reaches
    assert db.committed is True
