import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable

from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import text

from app.core.logging import configure_logging
from app.db.session import SessionLocal
from app.ingestion.jobs import (
    sync_history,
    sync_latest,
    sync_metadata,
    sync_thresholds,
    sync_warnings,
)

logger = logging.getLogger(__name__)

JobRunner = Callable[..., Awaitable[None]]


def _env_bool(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        logger.info("invalid %s=%r; using default=%s", name, value, default)
        return default


def _is_db_ready() -> bool:
    with SessionLocal() as db:
        db.execute(text("SELECT 1"))
    return True


def _is_schema_ready() -> bool:
    required_tables = [
        "providers",
        "stations",
        "reaches",
        "observation_latest",
        "warning_events",
    ]
    with SessionLocal() as db:
        for table_name in required_tables:
            table_exists = db.execute(
                text("SELECT to_regclass(:table_name)"), {"table_name": table_name}
            ).scalar_one()
            if table_exists is None:
                return False
    return True


def _wait_for_schema_readiness() -> bool:
    if not _env_bool("INGEST_SCHEMA_WAIT_ENABLED", default=True):
        logger.info("schema wait skipped: INGEST_SCHEMA_WAIT_ENABLED disabled")
        return True

    timeout_seconds = _env_float("INGEST_SCHEMA_WAIT_TIMEOUT_SECONDS", default=60.0)
    interval_seconds = _env_float("INGEST_SCHEMA_WAIT_INTERVAL_SECONDS", default=2.0)
    start = time.monotonic()
    deadline = start + max(timeout_seconds, 0.0)
    attempt = 0

    logger.info(
        "schema wait start: waiting for schema readiness (timeout=%ss interval=%ss)",
        timeout_seconds,
        interval_seconds,
    )

    while True:
        attempt += 1
        try:
            if _is_schema_ready():
                elapsed = time.monotonic() - start
                logger.info("schema ready: attempts=%s elapsed=%.1fs", attempt, elapsed)
                return True
        except Exception as exc:
            logger.info(
                "schema wait retry count=%s: schema check error=%s", attempt, exc
            )

        now = time.monotonic()
        remaining = deadline - now
        if remaining <= 0:
            logger.info(
                "schema wait timeout: attempts=%s timeout=%ss", attempt, timeout_seconds
            )
            return False

        sleep_seconds = max(0.0, min(interval_seconds, remaining))
        logger.info(
            "schema wait retry count=%s: schema not ready; retry in %.1fs",
            attempt,
            sleep_seconds,
        )
        time.sleep(sleep_seconds)


def _wait_for_db_readiness() -> bool:
    if not _env_bool("INGEST_DB_WAIT_ENABLED", default=True):
        logger.info("db wait skipped: INGEST_DB_WAIT_ENABLED disabled")
        return True

    timeout_seconds = _env_float("INGEST_DB_WAIT_TIMEOUT_SECONDS", default=60.0)
    interval_seconds = _env_float("INGEST_DB_WAIT_INTERVAL_SECONDS", default=2.0)
    start = time.monotonic()
    deadline = start + max(timeout_seconds, 0.0)
    attempt = 0

    logger.info(
        "db wait start: waiting for database readiness (timeout=%ss interval=%ss)",
        timeout_seconds,
        interval_seconds,
    )

    while True:
        attempt += 1
        try:
            _is_db_ready()
            elapsed = time.monotonic() - start
            logger.info("db ready: attempts=%s elapsed=%.1fs", attempt, elapsed)
            return True
        except Exception as exc:
            now = time.monotonic()
            remaining = deadline - now
            if remaining <= 0:
                logger.info(
                    "db wait timeout/failure: attempts=%s timeout=%ss error=%s",
                    attempt,
                    timeout_seconds,
                    exc,
                )
                return False

            sleep_seconds = max(0.0, min(interval_seconds, remaining))
            logger.info(
                "db wait retry count=%s: database not ready yet; retry in %.1fs (%s)",
                attempt,
                sleep_seconds,
                exc,
            )
            time.sleep(sleep_seconds)


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
    scheduler.add_job(
        lambda: _run_job_sync("sync_metadata", sync_metadata), "interval", hours=24
    )
    scheduler.add_job(
        lambda: _run_job_sync("sync_latest", sync_latest), "interval", minutes=10
    )
    scheduler.add_job(
        lambda: _run_job_sync("sync_history", sync_history), "interval", hours=6
    )
    scheduler.add_job(
        lambda: _run_job_sync("sync_thresholds", sync_thresholds), "interval", hours=24
    )
    scheduler.add_job(
        lambda: _run_job_sync("sync_warnings", sync_warnings), "interval", minutes=30
    )


def main() -> None:
    configure_logging(os.getenv("LOG_LEVEL", "INFO"))
    logger.info("worker startup")

    scheduler = BlockingScheduler()
    _register_jobs(scheduler)

    if _env_bool("INGEST_BOOTSTRAP_ON_START", default=True):
        db_ready = _wait_for_db_readiness()
        schema_ready = _wait_for_schema_readiness() if db_ready else False
        if db_ready and schema_ready:
            _run_bootstrap()
        elif not db_ready:
            logger.info("bootstrap skipped: database not ready before timeout")
        else:
            logger.info("bootstrap skipped: schema not ready before timeout")
    else:
        logger.info("bootstrap skipped: INGEST_BOOTSTRAP_ON_START disabled")

    logger.info("scheduler start")
    scheduler.start()


if __name__ == "__main__":
    main()
