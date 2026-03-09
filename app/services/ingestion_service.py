from sqlalchemy import select
from sqlalchemy.orm import Session

from app.adapters.base import NormalizedObservation
from app.core.time import utcnow
from app.db.models import ObservationLatest, ObservationTimeseries


def upsert_latest_and_append_ts(db: Session, obs: NormalizedObservation) -> None:
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
    else:
        db.add(ObservationLatest(**payload, ingested_at=utcnow()))

    ts_exists = db.scalar(select(ObservationTimeseries).where(
        ObservationTimeseries.property == obs.property,
        ObservationTimeseries.station_id == obs.station_id,
        ObservationTimeseries.reach_id == obs.reach_id,
        ObservationTimeseries.observed_at == obs.observed_at,
    ))
    if not ts_exists:
        db.add(ObservationTimeseries(**payload, ingested_at=utcnow()))
