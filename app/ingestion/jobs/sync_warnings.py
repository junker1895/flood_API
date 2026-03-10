import logging
from datetime import datetime

import httpx
from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.core.time import utcnow
from app.db.models import Provider, WarningEvent
from app.services.ingestion_service import tracked_run
from app.services.provider_registry import build_provider

logger = logging.getLogger(__name__)


def make_json_safe(obj):
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [make_json_safe(v) for v in obj]

    if isinstance(obj, datetime):
        return obj.isoformat()

    return obj


def _ensure_provider(db: Session, provider_id: str) -> None:
    if db.get(Provider, provider_id) is None:
        db.add(build_provider(provider_id))
        db.flush()


async def run(db: Session, provider_id: str | None = None) -> None:
    ea = EAEnglandAdapter()
    if provider_id is not None and provider_id != ea.provider_id:
        logger.info("sync_warnings skipping unsupported provider=%s", provider_id)
        return
    _ensure_provider(db, ea.provider_id)

    with tracked_run(db, ea.provider_id, "sync_warnings") as run_state:
        try:
            records = await ea.fetch_warnings()
        except (httpx.HTTPError, TimeoutError) as exc:
            run_state.records_failed += 1
            run_state.error_summary = f"fetch_warnings failed: {exc!r}"[:4000]
            logger.warning("warning sync failed for %s: %s", ea.provider_id, exc)
            db.commit()
            return

        raw_count = len(records)
        logger.info(
            "sync_warnings provider=%s fetched_records=%s", ea.provider_id, raw_count
        )
        if raw_count == 0:
            logger.info(
                "sync_warnings provider=%s fetched zero warning records", ea.provider_id
            )

        normalized_count = 0
        inserted_count = 0
        updated_count = 0

        for raw in records:
            try:
                w = ea.normalize_warning(raw)
                normalized_count += 1
                payload = w.model_dump()
                raw_meta = payload.get("raw_payload", {})
                existing = db.get(WarningEvent, payload["warning_id"])
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
                        geometry=None,
                        related_station_ids=make_json_safe(
                            raw_meta.get("related_station_ids")
                        ),
                        related_reach_ids=make_json_safe(
                            raw_meta.get("related_reach_ids")
                        ),
                        raw_payload=make_json_safe(payload.get("raw_payload")),
                        ingested_at=utcnow(),
                    )
                )
                run_state.records_seen += 1
                run_state.records_updated += 1
                if existing is None:
                    inserted_count += 1
                    run_state.records_inserted += 1
                else:
                    updated_count += 1
            except Exception as exc:
                run_state.records_failed += 1
                run_state.error_summary = f"normalize_warning failed: {exc!r}"[:4000]
                logger.warning(
                    "warning normalization failed for %s: %s", ea.provider_id, exc
                )

        if raw_count > 0 and normalized_count == 0:
            logger.warning(
                "sync_warnings provider=%s normalized zero warnings from non-zero fetch",
                ea.provider_id,
            )
        if (
            raw_count > 0
            and inserted_count == 0
            and updated_count == 0
            and run_state.records_failed == 0
        ):
            logger.warning(
                "sync_warnings provider=%s had non-zero fetch but no inserted/updated rows",
                ea.provider_id,
            )

        logger.info(
            "sync_warnings provider=%s raw=%s normalized=%s inserted=%s updated=%s failed=%s",
            ea.provider_id,
            raw_count,
            normalized_count,
            inserted_count,
            updated_count,
            run_state.records_failed,
        )
    db.commit()
    logger.info("sync_warnings committed")
