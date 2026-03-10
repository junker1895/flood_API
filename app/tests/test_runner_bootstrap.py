from app.ingestion import runner
from app.ingestion.schedule import JobSchedule, JobType


class FakeScheduler:
    def __init__(self):
        self.jobs = []
        self.started = False

    def add_job(self, func, trigger, **kwargs):
        self.jobs.append((func, trigger, kwargs))

    def start(self):
        self.started = True


def test_runner_bootstrap_executes_metadata_before_latest(monkeypatch):
    executed: list[tuple[str, JobType]] = []
    fake_scheduler = FakeScheduler()

    monkeypatch.setattr(runner, "configure_logging", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runner, "BlockingScheduler", lambda: fake_scheduler)
    monkeypatch.setattr(
        runner,
        "_run_provider_job_sync",
        lambda provider_id, job_type, _job: executed.append((provider_id, job_type)),
    )
    monkeypatch.setattr(runner, "_wait_for_db_readiness", lambda: True)
    monkeypatch.setattr(runner, "_wait_for_schema_readiness", lambda: True)
    monkeypatch.setenv("INGEST_BOOTSTRAP_ON_START", "true")

    runner.main()

    assert [item[1] for item in executed[:3]] == [
        JobType.METADATA,
        JobType.METADATA,
        JobType.METADATA,
    ]
    assert fake_scheduler.started is True
    assert len(fake_scheduler.jobs) == 8


def test_wait_for_db_readiness_success(monkeypatch):
    calls = {"count": 0}

    def fake_ready():
        calls["count"] += 1
        return True

    monkeypatch.setattr(runner, "_is_db_ready", fake_ready)
    monkeypatch.setenv("INGEST_DB_WAIT_ENABLED", "true")
    monkeypatch.setenv("INGEST_DB_WAIT_TIMEOUT_SECONDS", "5")
    monkeypatch.setenv("INGEST_DB_WAIT_INTERVAL_SECONDS", "0.01")

    assert runner._wait_for_db_readiness() is True
    assert calls["count"] == 1


def test_wait_for_db_readiness_timeout(monkeypatch):
    monkeypatch.setattr(
        runner, "_is_db_ready", lambda: (_ for _ in ()).throw(RuntimeError("db down"))
    )
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)

    ticks = iter([0.0, 0.0, 0.05, 0.1])
    monkeypatch.setattr(runner.time, "monotonic", lambda: next(ticks))

    monkeypatch.setenv("INGEST_DB_WAIT_ENABLED", "true")
    monkeypatch.setenv("INGEST_DB_WAIT_TIMEOUT_SECONDS", "0.1")
    monkeypatch.setenv("INGEST_DB_WAIT_INTERVAL_SECONDS", "0.05")

    assert runner._wait_for_db_readiness() is False


def test_runner_bootstrap_runs_only_when_db_ready(monkeypatch):
    fake_scheduler = FakeScheduler()
    called = {"bootstrap": 0}

    monkeypatch.setattr(runner, "configure_logging", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runner, "BlockingScheduler", lambda: fake_scheduler)
    monkeypatch.setattr(runner, "_register_jobs", lambda _scheduler: None)
    monkeypatch.setattr(runner, "_wait_for_db_readiness", lambda: True)
    monkeypatch.setattr(runner, "_wait_for_schema_readiness", lambda: True)
    monkeypatch.setattr(
        runner,
        "_run_bootstrap",
        lambda: called.__setitem__("bootstrap", called["bootstrap"] + 1),
    )
    monkeypatch.setenv("INGEST_BOOTSTRAP_ON_START", "true")

    runner.main()

    assert called["bootstrap"] == 1
    assert fake_scheduler.started is True


def test_runner_skips_bootstrap_when_db_wait_times_out(monkeypatch):
    fake_scheduler = FakeScheduler()
    called = {"bootstrap": 0}

    monkeypatch.setattr(runner, "configure_logging", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runner, "BlockingScheduler", lambda: fake_scheduler)
    monkeypatch.setattr(runner, "_register_jobs", lambda _scheduler: None)
    monkeypatch.setattr(runner, "_wait_for_db_readiness", lambda: False)
    monkeypatch.setattr(
        runner,
        "_run_bootstrap",
        lambda: called.__setitem__("bootstrap", called["bootstrap"] + 1),
    )
    monkeypatch.setenv("INGEST_BOOTSTRAP_ON_START", "true")

    runner.main()

    assert called["bootstrap"] == 0
    assert fake_scheduler.started is True


def test_wait_for_schema_readiness_success(monkeypatch):
    calls = {"count": 0}

    def fake_schema_ready():
        calls["count"] += 1
        return True

    monkeypatch.setattr(runner, "_is_schema_ready", fake_schema_ready)
    monkeypatch.setenv("INGEST_SCHEMA_WAIT_ENABLED", "true")
    monkeypatch.setenv("INGEST_SCHEMA_WAIT_TIMEOUT_SECONDS", "5")
    monkeypatch.setenv("INGEST_SCHEMA_WAIT_INTERVAL_SECONDS", "0.01")

    assert runner._wait_for_schema_readiness() is True
    assert calls["count"] == 1


def test_wait_for_schema_readiness_timeout(monkeypatch):
    monkeypatch.setattr(runner, "_is_schema_ready", lambda: False)
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)

    ticks = iter([0.0, 0.0, 0.05, 0.1])
    monkeypatch.setattr(runner.time, "monotonic", lambda: next(ticks))

    monkeypatch.setenv("INGEST_SCHEMA_WAIT_ENABLED", "true")
    monkeypatch.setenv("INGEST_SCHEMA_WAIT_TIMEOUT_SECONDS", "0.1")
    monkeypatch.setenv("INGEST_SCHEMA_WAIT_INTERVAL_SECONDS", "0.05")

    assert runner._wait_for_schema_readiness() is False


def test_runner_bootstrap_runs_only_when_db_and_schema_ready(monkeypatch):
    fake_scheduler = FakeScheduler()
    called = {"bootstrap": 0}

    monkeypatch.setattr(runner, "configure_logging", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runner, "BlockingScheduler", lambda: fake_scheduler)
    monkeypatch.setattr(runner, "_register_jobs", lambda _scheduler: None)
    monkeypatch.setattr(runner, "_wait_for_db_readiness", lambda: True)
    monkeypatch.setattr(runner, "_wait_for_schema_readiness", lambda: True)
    monkeypatch.setattr(
        runner,
        "_run_bootstrap",
        lambda: called.__setitem__("bootstrap", called["bootstrap"] + 1),
    )
    monkeypatch.setenv("INGEST_BOOTSTRAP_ON_START", "true")

    runner.main()

    assert called["bootstrap"] == 1
    assert fake_scheduler.started is True


def test_runner_skips_bootstrap_when_schema_wait_times_out(monkeypatch):
    fake_scheduler = FakeScheduler()
    called = {"bootstrap": 0}

    monkeypatch.setattr(runner, "configure_logging", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runner, "BlockingScheduler", lambda: fake_scheduler)
    monkeypatch.setattr(runner, "_register_jobs", lambda _scheduler: None)
    monkeypatch.setattr(runner, "_wait_for_db_readiness", lambda: True)
    monkeypatch.setattr(runner, "_wait_for_schema_readiness", lambda: False)
    monkeypatch.setattr(
        runner,
        "_run_bootstrap",
        lambda: called.__setitem__("bootstrap", called["bootstrap"] + 1),
    )
    monkeypatch.setenv("INGEST_BOOTSTRAP_ON_START", "true")

    runner.main()

    assert called["bootstrap"] == 0
    assert fake_scheduler.started is True


def test_register_provider_job_uses_provider_interval(monkeypatch):
    fake_scheduler = FakeScheduler()
    schedule = JobSchedule(enabled=True, interval_minutes=42)

    monkeypatch.setattr(runner, "_run_provider_job_sync", lambda *_args, **_kwargs: None)

    runner._register_provider_job(fake_scheduler, "usgs", JobType.LATEST, schedule)

    assert fake_scheduler.jobs[0][2]["minutes"] == 42
    assert fake_scheduler.jobs[0][2]["id"] == "usgs:latest"
