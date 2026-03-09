from app.ingestion import runner


class FakeScheduler:
    def __init__(self):
        self.jobs = []
        self.started = False

    def add_job(self, func, trigger, **kwargs):
        self.jobs.append((func, trigger, kwargs))

    def start(self):
        self.started = True


def test_runner_bootstrap_executes_metadata_before_latest(monkeypatch):
    executed: list[str] = []
    fake_scheduler = FakeScheduler()

    monkeypatch.setattr(runner, "configure_logging", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runner, "BlockingScheduler", lambda: fake_scheduler)
    monkeypatch.setattr(runner, "_run_job_sync", lambda job_name, _job: executed.append(job_name))
    monkeypatch.setenv("INGEST_BOOTSTRAP_ON_START", "true")

    runner.main()

    assert executed[:3] == ["sync_metadata", "sync_latest", "sync_warnings"]
    assert fake_scheduler.started is True
    assert len(fake_scheduler.jobs) == 5


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
    monkeypatch.setattr(runner, "_is_db_ready", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
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
    monkeypatch.setattr(runner, "_run_bootstrap", lambda: called.__setitem__("bootstrap", called["bootstrap"] + 1))
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
    monkeypatch.setattr(runner, "_run_bootstrap", lambda: called.__setitem__("bootstrap", called["bootstrap"] + 1))
    monkeypatch.setenv("INGEST_BOOTSTRAP_ON_START", "true")

    runner.main()

    assert called["bootstrap"] == 0
    assert fake_scheduler.started is True
