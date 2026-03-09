from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.core.time import utcnow
from app.db.models import WarningEvent
from app.services.ingestion_service import tracked_run


async def run(db: Session) -> None:
    ea = EAEnglandAdapter()
    with tracked_run(db, ea.provider_id, "sync_warnings") as run_state:
        for raw in await ea.fetch_warnings():
            w = ea.normalize_warning(raw)
            db.merge(WarningEvent(**w.model_dump(), ingested_at=utcnow(), raw_payload=w.raw_payload))
            run_state.records_seen += 1
            run_state.records_updated += 1
    db.commit()
