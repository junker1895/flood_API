from sqlalchemy.orm import Session


async def run(db: Session, provider_id: str | None = None) -> None:
    # TODO: ingest official thresholds per provider.
    _ = provider_id
    db.commit()
