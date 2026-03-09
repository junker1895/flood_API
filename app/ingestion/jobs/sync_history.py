from sqlalchemy.orm import Session

from app.ingestion.jobs.sync_latest import run as run_latest


async def run(db: Session) -> None:
    # Initial implementation reuses latest ingestion for backfill windows.
    await run_latest(db)
