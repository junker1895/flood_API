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
                payload = w.model_dump()
                raw_meta = payload.get("raw_payload", {})
                db.merge(
                    WarningEvent(
                        warning_id=payload["warning_id"],
                        provider_id=payload["provider_id"],
                        severity=payload.get("severity"),
                        title=payload.get("title"),
                        status=payload.get("status"),
                        description=raw_meta.get("description"),
                        warning_type=raw_meta.get("warning_type"),
                        issued_at=raw_meta.get("issued_at"),
                        effective_from=raw_meta.get("effective_from"),
                        effective_to=raw_meta.get("effective_to"),
                        # Keep geometry nullable unless adapter returns DB-ready geometry encoding.
                        geometry=None,
                        related_station_ids=raw_meta.get("related_station_ids"),
                        related_reach_ids=raw_meta.get("related_reach_ids"),
                        raw_payload=payload.get("raw_payload"),
                        ingested_at=utcnow(),
                    )
                )
                run_state.records_seen += 1
                run_state.records_updated += 1
            except Exception as exc:
                run_state.records_failed += 1
                run_state.error_summary = f"normalize_warning failed: {exc!r}"[:4000]
                logger.warning("warning normalization failed for %s: %s", ea.provider_id, exc)
    db.commit()
