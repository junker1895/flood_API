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
