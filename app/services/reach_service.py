from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import ObservationLatest, ObservationTimeseries, Reach, Threshold


def list_reaches(db: Session) -> list[Reach]:
    return list(db.scalars(select(Reach)).all())


def get_reach(db: Session, reach_id: str) -> Reach | None:
    return db.get(Reach, reach_id)


def latest_for_reaches(db: Session) -> list[ObservationLatest]:
    return list(db.scalars(select(ObservationLatest).where(ObservationLatest.reach_id.is_not(None))).all())


def reach_timeseries(db: Session, reach_id: str) -> list[ObservationTimeseries]:
    return list(db.scalars(select(ObservationTimeseries).where(ObservationTimeseries.reach_id == reach_id)).all())


def reach_thresholds(db: Session, reach_id: str) -> list[Threshold]:
    return list(db.scalars(select(Threshold).where(Threshold.reach_id == reach_id)).all())
