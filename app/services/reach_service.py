from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import ObservationLatest, ObservationTimeseries, Reach, Threshold


def list_reaches(
    db: Session,
    provider_id: str | None = None,
    country_code: str | None = None,
    ids: list[str] | None = None,
    limit: int = 100,
    cursor: str | None = None,
) -> list[Reach]:
    stmt = select(Reach).order_by(Reach.reach_id)
    if provider_id:
        stmt = stmt.where(Reach.provider_id == provider_id)
    if country_code:
        stmt = stmt.where(Reach.country_code == country_code)
    if ids:
        stmt = stmt.where(Reach.reach_id.in_(ids))
    if cursor:
        stmt = stmt.where(Reach.reach_id > cursor)
    return list(db.scalars(stmt.limit(limit)).all())


def get_reach(db: Session, reach_id: str) -> Reach | None:
    return db.get(Reach, reach_id)


def latest_for_reaches(db: Session, property_name: str | None = None, limit: int = 100) -> list[ObservationLatest]:
    stmt = select(ObservationLatest).where(ObservationLatest.reach_id.is_not(None))
    if property_name:
        stmt = stmt.where(ObservationLatest.property == property_name)
    return list(db.scalars(stmt.order_by(ObservationLatest.reach_id).limit(limit)).all())


def reach_timeseries(
    db: Session,
    reach_id: str,
    property_name: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 1000,
) -> list[ObservationTimeseries]:
    stmt = select(ObservationTimeseries).where(ObservationTimeseries.reach_id == reach_id)
    if property_name:
        stmt = stmt.where(ObservationTimeseries.property == property_name)
    if start:
        stmt = stmt.where(ObservationTimeseries.observed_at >= start)
    if end:
        stmt = stmt.where(ObservationTimeseries.observed_at <= end)
    return list(db.scalars(stmt.order_by(ObservationTimeseries.observed_at).limit(limit)).all())


def reach_thresholds(db: Session, reach_id: str, property_name: str | None = None) -> list[Threshold]:
    stmt = select(Threshold).where(Threshold.reach_id == reach_id)
    if property_name:
        stmt = stmt.where(Threshold.property == property_name)
    return list(db.scalars(stmt).all())
