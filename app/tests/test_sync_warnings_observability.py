from contextlib import contextmanager
import asyncio
import logging

from app.ingestion.jobs import sync_warnings


class FakeDB:
    def __init__(self):
        self.providers = set()
        self.is_active = True

    def get(self, model, key):
        if model.__name__ == "Provider":
            return object() if key in self.providers else None
        return None

    def add(self, provider):
        self.providers.add(provider.provider_id)

    def flush(self):
        return None

    def merge(self, _entity):
        return None

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


class FakeEAAdapter:
    provider_id = "ea_england"

    async def fetch_warnings(self):
        return []


def test_sync_warnings_logs_zero_fetch(monkeypatch, caplog):
    db = FakeDB()
    monkeypatch.setattr(sync_warnings, "EAEnglandAdapter", lambda: FakeEAAdapter())
    monkeypatch.setattr(sync_warnings, "tracked_run", fake_tracked_run)

    with caplog.at_level(logging.INFO):
        asyncio.run(sync_warnings.run(db))

    assert "fetched_records=0" in caplog.text
    assert "fetched zero warning records" in caplog.text
