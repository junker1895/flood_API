import asyncio
import logging
import os
from collections.abc import Awaitable, Callable

from apscheduler.schedulers.blocking import BlockingScheduler

from app.core.logging import configure_logging
from app.db.session import SessionLocal
from app.ingestion.jobs import sync_history, sync_latest, sync_metadata, sync_thresholds, sync_warnings

logger = logging.getLogger(__name__)

JobRunner = Callable[..., Awaitable[None]]


def _env_bool(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


async def _run_job_once(job_name: str, job: JobRunner) -> None:
    logger.info("job start: %s", job_name)
    with SessionLocal() as db:
        await job.run(db)
    logger.info("job finish: %s", job_name)


def _run_job_sync(job_name: str, job: JobRunner) -> None:
    try:
        asyncio.run(_run_job_once(job_name, job))
    except Exception:
        logger.exception("job failed: %s", job_name)


def _run_bootstrap() -> None:
    bootstrap_jobs: list[tuple[str, JobRunner]] = [
        ("sync_metadata", sync_metadata),
        ("sync_latest", sync_latest),
        ("sync_warnings", sync_warnings),
    ]
    logger.info("bootstrap start")
    for job_name, job in bootstrap_jobs:
        _run_job_sync(job_name, job)
    logger.info("bootstrap finish")


def _register_jobs(scheduler: BlockingScheduler) -> None:
    scheduler.add_job(lambda: _run_job_sync("sync_metadata", sync_metadata), "interval", hours=24)
    scheduler.add_job(lambda: _run_job_sync("sync_latest", sync_latest), "interval", minutes=10)
    scheduler.add_job(lambda: _run_job_sync("sync_history", sync_history), "interval", hours=6)
    scheduler.add_job(lambda: _run_job_sync("sync_thresholds", sync_thresholds), "interval", hours=24)
    scheduler.add_job(lambda: _run_job_sync("sync_warnings", sync_warnings), "interval", minutes=30)


def main() -> None:
    configure_logging(os.getenv("LOG_LEVEL", "INFO"))
    logger.info("worker startup")

    scheduler = BlockingScheduler()
    _register_jobs(scheduler)

    if _env_bool("INGEST_BOOTSTRAP_ON_START", default=True):
        _run_bootstrap()
    else:
        logger.info("bootstrap skipped: INGEST_BOOTSTRAP_ON_START disabled")

    logger.info("scheduler start")
    scheduler.start()


if __name__ == "__main__":
    main()
