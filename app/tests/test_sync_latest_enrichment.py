from contextlib import contextmanager
from datetime import UTC, datetime

from app.ingestion.jobs import sync_latest


class FakeDB:
    def __init__(self):
        self.providers: set[str] = set()
        self.stations: set[str] = set()
        self.reaches: set[str] = set()
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
        if name == "Reach":
            return object() if key in self.reaches else None
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
        self.merged.append(entity)

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


def test_sync_latest_enriches_missing_ea_station_and_retries(monkeypatch):
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

    import asyncio
    asyncio.run(sync_latest.run(db))

    assert "ea_england-A1" in db.stations
    station = next(e for e in db.merged if getattr(e, "station_id", None) == "ea_england-A1")
    assert station.geom is not None
    assert station.geom.data == "POINT(-1.2 51.1)"
    assert calls["count"] == 2


def test_sync_latest_enrichment_uses_station_reference_for_ids(monkeypatch):
    db = FakeDB()

    class FakeEAAdapterMismatchedNotation(FakeEAAdapter):
        async def fetch_latest_observations(self):
            return [{"stationReference": "E9250", "unitName": "m", "latestReading": {"value": 0.9, "dateTime": datetime.now(UTC).isoformat()}}]

        async def fetch_station_by_reference(self, station_reference: str):
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

    import asyncio
    asyncio.run(sync_latest.run(db))

    assert "ea_england-E9250" in db.stations
    assert "ea_england-9250" not in db.stations
    assert calls["count"] == 2


def test_sync_latest_enriches_missing_usgs_station_and_retries(monkeypatch):
    db = FakeDB()

    class FakeUSGSAdapter:
        provider_id = "usgs"

        async def fetch_latest_observations(self):
            return [{"site": "01646500"}]

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

        async def fetch_station_catalog(self):
            return [{"line": "USGS\t01646500\tPotomac\t\t38.9\t-77.0"}]

        def normalize_station(self, raw):
            from app.adapters.base import NormalizedStation

            return NormalizedStation(
                station_id="usgs-01646500",
                provider_id="usgs",
                provider_station_id="01646500",
                name="Potomac",
                latitude=38.9,
                longitude=-77.0,
                raw_metadata=raw,
            )

    monkeypatch.setattr(sync_latest, "USGSAdapter", lambda: FakeUSGSAdapter())
    monkeypatch.setattr(sync_latest, "EAEnglandAdapter", lambda: FakeNoopAdapter("ea_england"))
    monkeypatch.setattr(sync_latest, "GeoglowsAdapter", lambda: FakeNoopAdapter("geoglows"))
    monkeypatch.setattr(sync_latest, "tracked_run", fake_tracked_run)

    calls = {"count": 0}

    def fake_upsert(_db, _obs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise ValueError("entity missing for observation: station_id=usgs-01646500 reach_id=None")
        return (1, 1)

    monkeypatch.setattr(sync_latest, "upsert_latest_and_append_ts", fake_upsert)

    import asyncio
    asyncio.run(sync_latest.run(db))

    assert "usgs-01646500" in db.stations
    station = next(e for e in db.merged if getattr(e, "station_id", None) == "usgs-01646500")
    assert station.geom is not None
    assert station.geom.data == "POINT(-77.0 38.9)"
    assert calls["count"] == 2


def test_sync_latest_enriches_missing_geoglows_reach_and_retries(monkeypatch):
    db = FakeDB()

    class FakeGeoglowsAdapter:
        provider_id = "geoglows"

        async def fetch_latest_observations(self):
            return [{"reach_id": "1001", "datetime": datetime.now(UTC).isoformat(), "flow": 5.2}]

        def normalize_observation(self, raw):
            from app.adapters.base import NormalizedObservation

            return NormalizedObservation(
                entity_type="reach",
                reach_id="geoglows-1001",
                property="discharge",
                observed_at=datetime.now(UTC),
                quality_code="forecast",
                raw_payload=raw,
            )

        async def fetch_reach_catalog(self):
            return [{"reach_id": "1001", "lat": 0.0, "lon": 0.0, "river": "Demo"}]

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

    monkeypatch.setattr(sync_latest, "USGSAdapter", lambda: FakeNoopAdapter("usgs"))
    monkeypatch.setattr(sync_latest, "EAEnglandAdapter", lambda: FakeNoopAdapter("ea_england"))
    monkeypatch.setattr(sync_latest, "GeoglowsAdapter", lambda: FakeGeoglowsAdapter())
    monkeypatch.setattr(sync_latest, "tracked_run", fake_tracked_run)

    calls = {"count": 0}

    def fake_upsert(_db, _obs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise ValueError("entity missing for observation: station_id=None reach_id=geoglows-1001")
        return (1, 1)

    monkeypatch.setattr(sync_latest, "upsert_latest_and_append_ts", fake_upsert)

    import asyncio
    asyncio.run(sync_latest.run(db))

    assert "geoglows-1001" in db.reaches
    assert calls["count"] == 2
