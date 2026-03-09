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
        return []


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


def test_sync_metadata_populates_station_geom_and_lon_lat_order(monkeypatch):
    db = FakeDB()
    _patch_adapters(monkeypatch)

    import asyncio

    asyncio.run(sync_metadata.run(db))

    station = db.stations["usgs-01646500"]
    assert station.geom is not None
    assert station.geom.srid == 4326
    assert station.geom.data == "POINT(-1.1 51.1)"


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
    db.stations["usgs-01646500"].geom = None
    db.reaches["geoglows-1001"].geom = None

    asyncio.run(sync_metadata.run(db))

    assert db.stations["usgs-01646500"].geom is not None
    assert db.reaches["geoglows-1001"].geom is not None


def test_station_bbox_envelope_contains_synced_station_point(monkeypatch):
    db = FakeDB()
    _patch_adapters(monkeypatch)

    import asyncio

    asyncio.run(sync_metadata.run(db))

    geom_text = db.stations["usgs-01646500"].geom.data
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
