from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas.common import ListEnvelope, Meta, SingleEnvelope
from app.api.schemas.reaches import ReachOut
from app.api.schemas.stations import ObservationOut
from app.core.config import settings
from app.services.reach_service import get_reach, latest_for_reaches, list_reaches, reach_thresholds, reach_timeseries

router = APIRouter(prefix="/reaches", tags=["reaches"])


@router.get("", response_model=ListEnvelope[ReachOut])
def reaches(
    provider_id: str | None = None,
    country_code: str | None = None,
    ids: str | None = None,
    limit: int = Query(settings.default_limit, ge=1, le=settings.max_limit),
    cursor: str | None = None,
    db: Session = Depends(get_db),
):
    id_list = ids.split(",") if ids else None
    items = [ReachOut.model_validate(r, from_attributes=True) for r in list_reaches(db, provider_id, country_code, id_list, limit, cursor)]
    next_cursor = items[-1].reach_id if len(items) == limit else None
    return {"data": items, "meta": Meta(count=len(items), next_cursor=next_cursor)}


@router.get("/latest", response_model=ListEnvelope[ObservationOut])
def reaches_latest(
    property: str | None = None,
    limit: int = Query(settings.default_limit, ge=1, le=settings.max_limit),
    db: Session = Depends(get_db),
):
    items = [ObservationOut.model_validate(o, from_attributes=True) for o in latest_for_reaches(db, property, limit)]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/{reach_id}", response_model=SingleEnvelope[ReachOut])
def reach(reach_id: str, db: Session = Depends(get_db)):
    r = get_reach(db, reach_id)
    if not r:
        raise HTTPException(404, "reach not found")
    return {"data": ReachOut.model_validate(r, from_attributes=True)}


@router.get("/{reach_id}/timeseries", response_model=ListEnvelope[ObservationOut])
def timeseries(
    reach_id: str,
    property: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = Query(1000, ge=1, le=settings.max_limit),
    db: Session = Depends(get_db),
):
    items = [ObservationOut.model_validate(o, from_attributes=True) for o in reach_timeseries(db, reach_id, property, start, end, limit)]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/{reach_id}/thresholds")
def thresholds(reach_id: str, property: str | None = None, db: Session = Depends(get_db)):
    items = [t.threshold_id for t in reach_thresholds(db, reach_id, property)]
    return {"data": items, "meta": {"count": len(items), "next_cursor": None}}
