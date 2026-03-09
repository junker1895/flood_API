from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.core.time import utcnow
from app.db.models import WarningEvent


async def run(db: Session) -> None:
    ea = EAEnglandAdapter()
    for raw in await ea.fetch_warnings():
        w = ea.normalize_warning(raw)
        db.merge(WarningEvent(**w.model_dump(), ingested_at=utcnow(), raw_payload=w.raw_payload))
    db.commit()
