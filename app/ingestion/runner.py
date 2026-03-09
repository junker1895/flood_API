import asyncio

from apscheduler.schedulers.blocking import BlockingScheduler

from app.db.session import SessionLocal
from app.ingestion.jobs import sync_history, sync_latest, sync_metadata, sync_thresholds, sync_warnings


async def run_job(job):
    with SessionLocal() as db:
        await job.run(db)


def main() -> None:
    scheduler = BlockingScheduler()
    scheduler.add_job(lambda: asyncio.run(run_job(sync_metadata)), "interval", hours=24)
    scheduler.add_job(lambda: asyncio.run(run_job(sync_latest)), "interval", minutes=10)
    scheduler.add_job(lambda: asyncio.run(run_job(sync_history)), "interval", hours=6)
    scheduler.add_job(lambda: asyncio.run(run_job(sync_thresholds)), "interval", hours=24)
    scheduler.add_job(lambda: asyncio.run(run_job(sync_warnings)), "interval", minutes=30)
    scheduler.start()


if __name__ == "__main__":
    main()
