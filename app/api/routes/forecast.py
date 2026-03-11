from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas.forecast import ForecastMetaOut, ForecastReachDetailOut, ForecastReachesOut, ForecastReachRiskOut
from app.core.config import settings
from app.services.api_utils import parse_bbox
from app.services.forecast_service import latest_run, pick_run_date, reach_detail, reach_risks_in_bbox

router = APIRouter(prefix="/forecast", tags=["forecast"])


@router.get("/meta", response_model=ForecastMetaOut)
def forecast_meta(model: str = Query(default=settings.forecast_default_model), db: Session = Depends(get_db)):
    run = latest_run(db, model)
    if not run:
        raise HTTPException(404, "forecast run not found")
    return ForecastMetaOut(
        model=model,
        forecast_date=run.forecast_date,
        timestep_count=run.timestep_count,
        timestep_hours=run.timestep_hours,
        timesteps=[str(v) for v in (run.timesteps_json or [])],
    )


@router.get("/reaches", response_model=ForecastReachesOut)
def forecast_reaches(
    bbox: str,
    model: str = Query(default=settings.forecast_default_model),
    forecast_date: date | None = Query(default=None),
    db: Session = Depends(get_db),
):
    run_date = pick_run_date(db, model, forecast_date)
    if not run_date:
        raise HTTPException(404, "forecast run not found")
    rows = reach_risks_in_bbox(db, model, run_date, parse_bbox(bbox))
    reaches = {str(r.reach_id): ForecastReachRiskOut(risk_class=r.risk_class, peak_time=r.peak_time) for r in rows}
    return ForecastReachesOut(model=model, forecast_date=run_date, reaches=reaches)


@router.get("/reach/{reach_id}", response_model=ForecastReachDetailOut)
def forecast_reach(
    reach_id: int,
    model: str = Query(default=settings.forecast_default_model),
    forecast_date: date | None = Query(default=None),
    db: Session = Depends(get_db),
):
    run_date = pick_run_date(db, model, forecast_date)
    if not run_date:
        raise HTTPException(404, "forecast run not found")
    rows = reach_detail(db, model, run_date, reach_id)
    if not rows:
        return ForecastReachDetailOut(model=model, forecast_date=run_date, reach_id=reach_id, detail_available=False, timesteps=[])
    return ForecastReachDetailOut(
        model=model,
        forecast_date=run_date,
        reach_id=reach_id,
        detail_available=True,
        timesteps=[
            {
                "timestep_idx": row.timestep_idx,
                "valid_time": row.valid_time,
                "flow_median": row.flow_median,
                "prob_exceed_rp2": row.prob_exceed_rp2,
                "prob_exceed_rp5": row.prob_exceed_rp5,
                "prob_exceed_rp10": row.prob_exceed_rp10,
            }
            for row in rows
        ],
    )
