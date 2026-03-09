from contextlib import contextmanager
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.adapters.base import NormalizedObservation
from app.core.time import utcnow
from app.db.models import IngestionRun, ObservationLatest, ObservationTimeseries


@contextmanager
def tracked_run(db: Session, provider_id: str, job_type: str):
    run = IngestionRun(
        provider_id=provider_id,
        job_type=job_type,
        started_at=utcnow(),
        status="running",
        records_seen=0,
        records_inserted=0,
        records_updated=0,
        records_failed=0,
    )
    db.add(run)
    db.flush()
    try:
        yield run
        run.status = "success"
    except Exception as exc:
        run.status = "failed"
        run.error_summary = str(exc)[:4000]
        raise
    finally:
        run.finished_at = utcnow()


def upsert_latest_and_append_ts(db: Session, obs: NormalizedObservation) -> tuple[int, int]:
    inserted = 0
    updated = 0
    q = select(ObservationLatest).where(
        ObservationLatest.property == obs.property,
        ObservationLatest.station_id == obs.station_id,
        ObservationLatest.reach_id == obs.reach_id,
    )
    existing = db.scalar(q)
    payload = obs.model_dump()
    if existing:
        for k, v in payload.items():
            if hasattr(existing, k):
                setattr(existing, k, v)
        existing.ingested_at = utcnow()
        updated += 1
    else:
        db.add(ObservationLatest(**payload, ingested_at=utcnow()))
        inserted += 1

    ts_exists = db.scalar(
        select(ObservationTimeseries).where(
            ObservationTimeseries.property == obs.property,
            ObservationTimeseries.station_id == obs.station_id,
            ObservationTimeseries.reach_id == obs.reach_id,
            ObservationTimeseries.observed_at == obs.observed_at,
        )
    )
    if not ts_exists:
        db.add(ObservationTimeseries(**payload, ingested_at=utcnow()))
        inserted += 1
    return inserted, updated
