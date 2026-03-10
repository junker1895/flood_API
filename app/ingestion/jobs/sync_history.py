from sqlalchemy.orm import Session

from app.ingestion.jobs.sync_latest import run as run_latest


async def run(db: Session, provider_id: str | None = None) -> None:
    # Initial implementation reuses latest ingestion for backfill windows.
    await run_latest(db, provider_id=provider_id)
