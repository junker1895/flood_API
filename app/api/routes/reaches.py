from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas.common import ListEnvelope, Meta, SingleEnvelope
from app.api.schemas.reaches import ReachOut
from app.api.schemas.stations import ObservationOut
from app.services.reach_service import get_reach, latest_for_reaches, list_reaches, reach_thresholds, reach_timeseries

router = APIRouter(prefix="/reaches", tags=["reaches"])


@router.get("", response_model=ListEnvelope[ReachOut])
def reaches(db: Session = Depends(get_db)):
    items = [ReachOut.model_validate(r, from_attributes=True) for r in list_reaches(db)]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/latest", response_model=ListEnvelope[ObservationOut])
def reaches_latest(db: Session = Depends(get_db)):
    items = [ObservationOut.model_validate(o, from_attributes=True) for o in latest_for_reaches(db)]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/{reach_id}", response_model=SingleEnvelope[ReachOut])
def reach(reach_id: str, db: Session = Depends(get_db)):
    r = get_reach(db, reach_id)
    if not r:
        raise HTTPException(404, "reach not found")
    return {"data": ReachOut.model_validate(r, from_attributes=True)}


@router.get("/{reach_id}/timeseries", response_model=ListEnvelope[ObservationOut])
def timeseries(reach_id: str, db: Session = Depends(get_db)):
    items = [ObservationOut.model_validate(o, from_attributes=True) for o in reach_timeseries(db, reach_id)]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/{reach_id}/thresholds")
def thresholds(reach_id: str, db: Session = Depends(get_db)):
    items = [t.threshold_id for t in reach_thresholds(db, reach_id)]
    return {"data": items, "meta": {"count": len(items), "next_cursor": None}}
