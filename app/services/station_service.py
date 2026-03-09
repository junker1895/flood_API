from datetime import datetime

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.db.models import ObservationLatest, ObservationTimeseries, Station, Threshold


def _apply_station_filters(
    stmt: Select[tuple[Station]],
    provider_id: str | None,
    country_code: str | None,
    ids: list[str] | None,
    updated_since: datetime | None,
    cursor: str | None,
) -> Select[tuple[Station]]:
    if provider_id:
        stmt = stmt.where(Station.provider_id == provider_id)
    if country_code:
        stmt = stmt.where(Station.country_code == country_code)
    if ids:
        stmt = stmt.where(Station.station_id.in_(ids))
    if updated_since:
        stmt = stmt.where(Station.last_metadata_refresh_at >= updated_since)
    if cursor:
        stmt = stmt.where(Station.station_id > cursor)
    return stmt


def list_stations(
    db: Session,
    provider_id: str | None = None,
    country_code: str | None = None,
    ids: list[str] | None = None,
    updated_since: datetime | None = None,
    limit: int = 100,
    cursor: str | None = None,
) -> list[Station]:
    stmt = select(Station).order_by(Station.station_id)
    stmt = _apply_station_filters(stmt, provider_id, country_code, ids, updated_since, cursor)
    return list(db.scalars(stmt.limit(limit)).all())


def get_station(db: Session, station_id: str) -> Station | None:
    return db.get(Station, station_id)


def latest_for_stations(db: Session, property_name: str | None = None, limit: int = 100) -> list[ObservationLatest]:
    stmt = select(ObservationLatest).where(ObservationLatest.station_id.is_not(None))
    if property_name:
        stmt = stmt.where(ObservationLatest.property == property_name)
    return list(db.scalars(stmt.order_by(ObservationLatest.station_id).limit(limit)).all())


def station_timeseries(
    db: Session,
    station_id: str,
    property_name: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 1000,
) -> list[ObservationTimeseries]:
    stmt = select(ObservationTimeseries).where(ObservationTimeseries.station_id == station_id)
    if property_name:
        stmt = stmt.where(ObservationTimeseries.property == property_name)
    if start:
        stmt = stmt.where(ObservationTimeseries.observed_at >= start)
    if end:
        stmt = stmt.where(ObservationTimeseries.observed_at <= end)
    return list(db.scalars(stmt.order_by(ObservationTimeseries.observed_at).limit(limit)).all())


def station_thresholds(db: Session, station_id: str, property_name: str | None = None) -> list[Threshold]:
    stmt = select(Threshold).where(Threshold.station_id == station_id)
    if property_name:
        stmt = stmt.where(Threshold.property == property_name)
    return list(db.scalars(stmt).all())
