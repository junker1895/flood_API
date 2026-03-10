import asyncio

from app.ingestion import runner
from app.ingestion.jobs import sync_latest, sync_warnings
from app.ingestion.schedule import JobType, build_provider_schedule, get_enabled_provider_jobs


class FakeRunModule:
    def __init__(self):
        self.calls: list[str] = []

    async def run(self, _db, provider_id=None):
        self.calls.append(provider_id)


def test_disabled_provider_not_scheduled(monkeypatch):
    monkeypatch.setenv("PROVIDERS__USGS__ENABLED", "false")

    scheduled = get_enabled_provider_jobs(["usgs"])

    assert scheduled == []


def test_unsupported_jobs_not_dispatched():
    schedule = build_provider_schedule("geoglows")

    assert schedule.jobs[JobType.WARNINGS].enabled is False
    assert schedule.jobs[JobType.THRESHOLDS].enabled is False


def test_interval_parsing_and_legacy_fallback(monkeypatch):
    monkeypatch.setenv("USGS_POLL_MINUTES", "11")
    monkeypatch.setenv("PROVIDERS__USGS__JOBS__LATEST__INTERVAL_MINUTES", "17")

    schedule = build_provider_schedule("usgs")

    assert schedule.jobs[JobType.LATEST].interval_minutes == 17


def test_provider_job_dispatch_calls_correct_job_path(monkeypatch):
    fake_module = FakeRunModule()
    monkeypatch.setitem(runner.JOB_RUNNERS, JobType.METADATA, fake_module)

    runner._run_provider_job_sync("usgs", JobType.METADATA, fake_module)

    assert fake_module.calls == ["usgs"]


def test_sync_latest_provider_filter(monkeypatch):
    seen: list[str] = []

    class FakeAdapter:
        def __init__(self, provider_id):
            self.provider_id = provider_id

        async def fetch_latest_observations(self):
            seen.append(self.provider_id)
            return []

    monkeypatch.setattr(sync_latest, "USGSAdapter", lambda: FakeAdapter("usgs"))
    monkeypatch.setattr(sync_latest, "EAEnglandAdapter", lambda: FakeAdapter("ea_england"))
    monkeypatch.setattr(sync_latest, "GeoglowsAdapter", lambda: FakeAdapter("geoglows"))

    class FakeDB:
        is_active = True

        def rollback(self):
            pass

        def get(self, *_args, **_kwargs):
            return object()

        def add(self, *_args, **_kwargs):
            return None

        def flush(self):
            return None

        def begin_nested(self):
            class Ctx:
                def __enter__(self, *a):
                    return self

                def __exit__(self, *a):
                    return False

            return Ctx()

        def commit(self):
            return None

    class RunState:
        records_failed = 0
        records_seen = 0
        records_inserted = 0
        records_updated = 0
        error_summary = None

    class RunCtx:
        def __enter__(self):
            return RunState()

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(sync_latest, "tracked_run", lambda *_args, **_kwargs: RunCtx())

    asyncio.run(sync_latest.run(FakeDB(), provider_id="usgs"))

    assert seen == ["usgs"]


def test_sync_warnings_unsupported_provider_skips(monkeypatch):
    called = {"count": 0}

    class FakeEA:
        provider_id = "ea_england"

    monkeypatch.setattr(sync_warnings, "EAEnglandAdapter", lambda: FakeEA())
    monkeypatch.setattr(sync_warnings, "_ensure_provider", lambda *_args, **_kwargs: called.__setitem__("count", 1))

    asyncio.run(sync_warnings.run(object(), provider_id="usgs"))

    assert called["count"] == 0
