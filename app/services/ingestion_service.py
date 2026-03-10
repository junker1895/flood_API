from contextlib import contextmanager

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.adapters.base import NormalizedObservation
from app.core.enums import EntityType
from app.core.time import utcnow
from app.db.models import IngestionRun, ObservationLatest, ObservationTimeseries, Reach, Station


@contextmanager
def tracked_run(db: Session, provider_id: str, job_type: str):
    if not db.is_active:
        db.rollback()
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


def _latest_upsert_stmt(payload: dict):
    table = ObservationLatest.__table__
    base = insert(table).values(**payload)

    update_cols = {
        "observed_at": base.excluded.observed_at,
        "value_native": base.excluded.value_native,
        "unit_native": base.excluded.unit_native,
        "value_canonical": base.excluded.value_canonical,
        "unit_canonical": base.excluded.unit_canonical,
        "quality_code": base.excluded.quality_code,
        "quality_score": base.excluded.quality_score,
        "aggregation": base.excluded.aggregation,
        "is_provisional": base.excluded.is_provisional,
        "is_estimated": base.excluded.is_estimated,
        "is_missing": base.excluded.is_missing,
        "is_forecast": base.excluded.is_forecast,
        "is_flagged": base.excluded.is_flagged,
        "provider_observation_id": base.excluded.provider_observation_id,
        "ingested_at": base.excluded.ingested_at,
        "raw_payload": base.excluded.raw_payload,
    }

    if payload["entity_type"] == EntityType.STATION:
        return base.on_conflict_do_update(
            index_elements=["station_id", "property"],
            index_where=table.c.station_id.is_not(None),
            set_=update_cols,
        )

    return base.on_conflict_do_update(
        index_elements=["reach_id", "property"],
        index_where=table.c.reach_id.is_not(None),
        set_=update_cols,
    )


def _timeseries_insert_stmt(payload: dict):
    table = ObservationTimeseries.__table__
    base = insert(table).values(**payload)
    if payload["entity_type"] == EntityType.STATION:
        return base.on_conflict_do_nothing(
            index_elements=["station_id", "property", "observed_at"],
            index_where=table.c.station_id.is_not(None),
        )

    return base.on_conflict_do_nothing(
        index_elements=["reach_id", "property", "observed_at"],
        index_where=table.c.reach_id.is_not(None),
    )


def _entity_exists(db: Session, obs: NormalizedObservation) -> bool:
    if obs.entity_type == EntityType.STATION:
        return bool(obs.station_id and db.get(Station, obs.station_id))
    if obs.entity_type == EntityType.REACH:
        return bool(obs.reach_id and db.get(Reach, obs.reach_id))
    return False


def upsert_latest_and_append_ts(db: Session, obs: NormalizedObservation) -> tuple[int, int]:
    if not _entity_exists(db, obs):
        raise ValueError(f"entity missing for observation: station_id={obs.station_id} reach_id={obs.reach_id}")

    payload = obs.model_dump()
    payload["ingested_at"] = utcnow()

    latest_result = db.execute(_latest_upsert_stmt(payload))
    updated = 1 if latest_result.rowcount else 0

    ts_result = db.execute(_timeseries_insert_stmt(payload))
    inserted = 1 if ts_result.rowcount else 0
    return inserted, updated


def append_timeseries(db: Session, obs: NormalizedObservation) -> int:
    if not _entity_exists(db, obs):
        raise ValueError(f"entity missing for observation: station_id={obs.station_id} reach_id={obs.reach_id}")

    payload = obs.model_dump()
    payload["ingested_at"] = utcnow()
    ts_result = db.execute(_timeseries_insert_stmt(payload))
    return 1 if ts_result.rowcount else 0
