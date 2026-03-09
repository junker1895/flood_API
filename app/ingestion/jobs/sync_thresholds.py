from sqlalchemy.orm import Session


async def run(db: Session) -> None:
    # TODO: ingest official thresholds per provider.
    db.commit()
