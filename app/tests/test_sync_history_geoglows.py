from contextlib import contextmanager
from datetime import UTC, datetime

from app.ingestion.jobs import sync_history


class FakeDB:
    def __init__(self):
        self.reaches = set()
        self.is_active = True

    def get(self, model, key):
        if model.__name__ == "Reach":
            return object() if key in self.reaches else None
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


class FakeNoHistoryAdapter:
    provider_id = "usgs"
    supports_history = False


class FakeGeoglowsAdapter:
    provider_id = "geoglows"
    supports_history = True

    async def fetch_historical_timeseries(self):
        return [
            {"reach_id": "1001", "datetime": "2024-01-01T00:00:00Z", "flow": 1.1, "series_type": "reanalysis"},
            {"reach_id": "1001", "datetime": "2024-01-01T01:00:00Z", "flow": 1.2, "series_type": "reanalysis"},
        ]

    def normalize_observation(self, raw):
        from app.adapters.base import NormalizedObservation

        return NormalizedObservation(
            entity_type="reach",
            reach_id="geoglows-1001",
            property="discharge",
            observed_at=datetime.fromisoformat(raw["datetime"].replace("Z", "+00:00")).astimezone(UTC),
            quality_code="raw",
            is_forecast=False,
            raw_payload=raw,
        )


def test_sync_history_geoglows_enrichment_and_idempotent_rerun(monkeypatch):
    db = FakeDB()
    calls = {"append": 0, "enrich": 0}

    monkeypatch.setattr(sync_history, "USGSAdapter", lambda: FakeNoHistoryAdapter())
    monkeypatch.setattr(sync_history, "EAEnglandAdapter", lambda: FakeNoHistoryAdapter())
    monkeypatch.setattr(sync_history, "GeoglowsAdapter", lambda: FakeGeoglowsAdapter())
    monkeypatch.setattr(sync_history, "tracked_run", fake_tracked_run)

    seen = set()

    def fake_append(_db, obs):
        calls["append"] += 1
        if obs.reach_id not in db.reaches:
            raise ValueError("entity missing for observation")
        key = (obs.reach_id, obs.observed_at)
        if key in seen:
            return 0
        seen.add(key)
        return 1

    async def fake_enrich(_db, _adapter, _raw, obs):
        calls["enrich"] += 1
        db.reaches.add(obs.reach_id)
        return True

    monkeypatch.setattr(sync_history, "append_timeseries", fake_append)
    monkeypatch.setattr(sync_history, "_enrich_geoglows_reach_if_missing", fake_enrich)

    import asyncio

    asyncio.run(sync_history.run(db, provider_id="geoglows"))
    asyncio.run(sync_history.run(db, provider_id="geoglows"))

    assert calls["enrich"] == 1
    assert calls["append"] == 5
