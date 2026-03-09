from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import ObservationLatest, ObservationTimeseries, Station, Threshold


def list_stations(db: Session) -> list[Station]:
    return list(db.scalars(select(Station)).all())


def get_station(db: Session, station_id: str) -> Station | None:
    return db.get(Station, station_id)


def latest_for_stations(db: Session) -> list[ObservationLatest]:
    return list(db.scalars(select(ObservationLatest).where(ObservationLatest.station_id.is_not(None))).all())


def station_timeseries(db: Session, station_id: str) -> list[ObservationTimeseries]:
    return list(db.scalars(select(ObservationTimeseries).where(ObservationTimeseries.station_id == station_id)).all())


def station_thresholds(db: Session, station_id: str) -> list[Threshold]:
    return list(db.scalars(select(Threshold).where(Threshold.station_id == station_id)).all())
