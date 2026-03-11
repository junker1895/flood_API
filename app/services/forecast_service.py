from __future__ import annotations

from datetime import date

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.db.models.forecast import ForecastReach, ForecastReachDetail, ForecastReachRisk, ForecastRun


def latest_run(db: Session, model: str) -> ForecastRun | None:
    return db.execute(
        select(ForecastRun)
        .where(ForecastRun.model == model)
        .order_by(ForecastRun.forecast_date.desc())
        .limit(1)
    ).scalar_one_or_none()


def pick_run_date(db: Session, model: str, forecast_date: date | None) -> date | None:
    if forecast_date:
        return forecast_date
    run = latest_run(db, model)
    return run.forecast_date if run else None


def reach_risks_in_bbox(db: Session, model: str, forecast_date: date, bbox: tuple[float, float, float, float]):
    minlon, minlat, maxlon, maxlat = bbox
    return db.execute(
        select(ForecastReachRisk)
        .join(ForecastReach, and_(ForecastReach.model == ForecastReachRisk.model, ForecastReach.reach_id == ForecastReachRisk.reach_id))
        .where(
            ForecastReachRisk.model == model,
            ForecastReachRisk.forecast_date == forecast_date,
            ForecastReach.lon >= minlon,
            ForecastReach.lon <= maxlon,
            ForecastReach.lat >= minlat,
            ForecastReach.lat <= maxlat,
        )
    ).scalars().all()


def reach_detail(db: Session, model: str, forecast_date: date, reach_id: int):
    return db.execute(
        select(ForecastReachDetail)
        .where(
            ForecastReachDetail.model == model,
            ForecastReachDetail.forecast_date == forecast_date,
            ForecastReachDetail.reach_id == reach_id,
        )
        .order_by(ForecastReachDetail.timestep_idx.asc())
    ).scalars().all()
