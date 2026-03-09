import logging

import httpx
from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.core.time import utcnow
from app.db.models import WarningEvent
from app.services.ingestion_service import tracked_run

logger = logging.getLogger(__name__)


async def run(db: Session) -> None:
    ea = EAEnglandAdapter()
    with tracked_run(db, ea.provider_id, "sync_warnings") as run_state:
        try:
            records = await ea.fetch_warnings()
        except (httpx.HTTPError, TimeoutError) as exc:
            run_state.records_failed += 1
            run_state.error_summary = f"fetch_warnings failed: {exc!r}"[:4000]
            logger.warning("warning sync failed for %s: %s", ea.provider_id, exc)
            db.commit()
            return

        for raw in records:
            try:
                w = ea.normalize_warning(raw)
                db.merge(WarningEvent(**w.model_dump(), ingested_at=utcnow()))
                run_state.records_seen += 1
                run_state.records_updated += 1
            except Exception as exc:
                run_state.records_failed += 1
                run_state.error_summary = f"normalize_warning failed: {exc!r}"[:4000]
                logger.warning("warning normalization failed for %s: %s", ea.provider_id, exc)
    db.commit()
