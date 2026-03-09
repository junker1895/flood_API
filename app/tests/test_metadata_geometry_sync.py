from contextlib import contextmanager

from app.ingestion.jobs import sync_metadata


class FakeDB:
    def __init__(self):
        self.providers = set()
        self.stations: dict[str, object] = {}
        self.reaches: dict[str, object] = {}
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
            self.stations[entity.station_id] = entity
        if hasattr(entity, "reach_id") and entity.reach_id:
            self.reaches[entity.reach_id] = entity

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
            latitude=51.1,
            longitude=-1.1,
            raw_metadata=raw,
        )


class FakeEAAdapter:
    provider_id = "ea_england"

    async def fetch_station_catalog(self):
        return [{"notation": "A1"}]

    def normalize_station(self, raw):
        from app.adapters.base import NormalizedStation

        return NormalizedStation(
            station_id="ea_england-A1",
            provider_id="ea_england",
            provider_station_id="A1",
            name="EA Station",
            latitude=52.845991,
            longitude=-0.100848,
            raw_metadata=raw,
        )


class FakeGeoglowsAdapter:
    provider_id = "geoglows"

    async def fetch_reach_catalog(self):
        return [{"reach_id": "1001", "lat": 51.5, "lon": -0.12}]

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


def _patch_adapters(monkeypatch):
    monkeypatch.setattr(sync_metadata, "tracked_run", fake_tracked_run)
    monkeypatch.setattr(sync_metadata, "USGSAdapter", lambda: FakeUSGSAdapter())
    monkeypatch.setattr(sync_metadata, "EAEnglandAdapter", lambda: FakeEAAdapter())
    monkeypatch.setattr(sync_metadata, "GeoglowsAdapter", lambda: FakeGeoglowsAdapter())


def test_sync_metadata_populates_station_geom_for_all_providers(monkeypatch):
    db = FakeDB()
    _patch_adapters(monkeypatch)

    import asyncio

    asyncio.run(sync_metadata.run(db))

    usgs = db.stations["usgs-01646500"]
    assert usgs.geom is not None
    assert usgs.geom.srid == 4326
    assert usgs.geom.data == "POINT(-1.1 51.1)"

    ea = db.stations["ea_england-A1"]
    assert ea.geom is not None
    assert ea.geom.srid == 4326
    assert ea.geom.data == "POINT(-0.100848 52.845991)"


def test_sync_metadata_populates_reach_point_fallback_geom(monkeypatch):
    db = FakeDB()
    _patch_adapters(monkeypatch)

    import asyncio

    asyncio.run(sync_metadata.run(db))

    reach = db.reaches["geoglows-1001"]
    assert reach.geom is not None
    assert reach.geom.srid == 4326
    assert reach.geom.data == "POINT(-0.12 51.5)"


def test_sync_metadata_rerun_backfills_existing_missing_geom(monkeypatch):
    db = FakeDB()
    _patch_adapters(monkeypatch)

    import asyncio

    asyncio.run(sync_metadata.run(db))
    db.stations["ea_england-A1"].geom = None
    db.reaches["geoglows-1001"].geom = None

    asyncio.run(sync_metadata.run(db))

    assert db.stations["ea_england-A1"].geom is not None
    assert db.reaches["geoglows-1001"].geom is not None


def test_station_bbox_envelope_contains_synced_station_points(monkeypatch):
    db = FakeDB()
    _patch_adapters(monkeypatch)

    import asyncio

    asyncio.run(sync_metadata.run(db))

    for sid in ["usgs-01646500", "ea_england-A1"]:
        geom_text = db.stations[sid].geom.data
        lon, lat = (float(part) for part in geom_text.removeprefix("POINT(").removesuffix(")").split())
        assert -10 <= lon <= 3
        assert 49 <= lat <= 61


def test_sync_metadata_preserves_reach_linestring_geometry_when_available(monkeypatch):
    class LineGeomGeoglowsAdapter(FakeGeoglowsAdapter):
        def normalize_reach(self, raw):
            from app.adapters.base import NormalizedReach

            return NormalizedReach(
                reach_id="geoglows-1001",
                provider_id="geoglows",
                provider_reach_id="1001",
                latitude=raw["lat"],
                longitude=raw["lon"],
                geometry_wkt="LINESTRING(-0.12 51.5, -0.11 51.6)",
                raw_metadata=raw,
            )

    db = FakeDB()
    monkeypatch.setattr(sync_metadata, "tracked_run", fake_tracked_run)
    monkeypatch.setattr(sync_metadata, "USGSAdapter", lambda: FakeUSGSAdapter())
    monkeypatch.setattr(sync_metadata, "EAEnglandAdapter", lambda: FakeEAAdapter())
    monkeypatch.setattr(sync_metadata, "GeoglowsAdapter", lambda: LineGeomGeoglowsAdapter())

    import asyncio

    asyncio.run(sync_metadata.run(db))

    assert db.reaches["geoglows-1001"].geom.data == "LINESTRING(-0.12 51.5, -0.11 51.6)"


def test_sync_metadata_module_main_executes_job(monkeypatch):
    events = []

    class FakeSessionContext:
        def __enter__(self):
            events.append("enter")
            return object()

        def __exit__(self, exc_type, exc, tb):
            events.append("exit")

    async def fake_run(_db):
        events.append("run")

    monkeypatch.setattr(sync_metadata, "SessionLocal", lambda: FakeSessionContext())
    monkeypatch.setattr(sync_metadata, "run", fake_run)

    sync_metadata.main()

    assert events == ["enter", "run", "exit"]


def test_ensure_providers_ignores_duplicate_insert_race(monkeypatch):
    from sqlalchemy.exc import IntegrityError

    class FakeDBProviderRace(FakeDB):
        def __init__(self):
            super().__init__()
            self.flush_calls = 0
            self.rollback_calls = 0

        def flush(self):
            self.flush_calls += 1
            if self.flush_calls == 1:
                raise IntegrityError("INSERT", {}, Exception("duplicate key"))

        def rollback(self):
            self.rollback_calls += 1

    db = FakeDBProviderRace()

    created = sync_metadata._ensure_providers(db)

    assert created == 3
    assert db.rollback_calls == 1
