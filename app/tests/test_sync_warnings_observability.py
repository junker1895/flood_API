from contextlib import contextmanager
import asyncio
from datetime import datetime, timezone
import logging

from app.ingestion.jobs import sync_warnings


class FakeDB:
    def __init__(self):
        self.providers = set()
        self.is_active = True
        self.merged = []

    def get(self, model, key):
        if model.__name__ == "Provider":
            return object() if key in self.providers else None
        return None

    def add(self, provider):
        self.providers.add(provider.provider_id)

    def flush(self):
        return None

    def merge(self, entity):
        self.merged.append(entity)
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


class FakeWarningModel:
    def __init__(self, payload):
        self._payload = payload

    def model_dump(self):
        return self._payload


def test_make_json_safe_converts_datetime_values():
    value = {
        "outer": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "items": [{"nested": datetime(2024, 1, 2, tzinfo=timezone.utc)}],
    }

    result = sync_warnings.make_json_safe(value)

    assert result["outer"] == "2024-01-01T00:00:00+00:00"
    assert result["items"][0]["nested"] == "2024-01-02T00:00:00+00:00"


def test_sync_warnings_stores_json_safe_payload(monkeypatch):
    class DatetimeEAAdapter(FakeEAAdapter):
        async def fetch_warnings(self):
            return [{"id": "w1"}]

        def normalize_warning(self, _raw):
            dt = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
            return FakeWarningModel(
                {
                    "warning_id": "w1",
                    "provider_id": self.provider_id,
                    "severity": "high",
                    "title": "Test",
                    "status": "active",
                    "raw_payload": {
                        "description": "desc",
                        "warning_type": "flood",
                        "issued_at": dt,
                        "effective_from": dt,
                        "effective_to": dt,
                        "related_station_ids": ["s1"],
                        "related_reach_ids": ["r1"],
                        "nested": {"at": dt},
                    },
                }
            )

    db = FakeDB()
    monkeypatch.setattr(sync_warnings, "EAEnglandAdapter", lambda: DatetimeEAAdapter())
    monkeypatch.setattr(sync_warnings, "tracked_run", fake_tracked_run)

    asyncio.run(sync_warnings.run(db))

    assert len(db.merged) == 1
    merged = db.merged[0]
    assert merged.raw_payload["issued_at"] == "2024-01-01T12:00:00+00:00"
    assert merged.raw_payload["nested"]["at"] == "2024-01-01T12:00:00+00:00"
