from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas.common import ListEnvelope, Meta, SingleEnvelope
from app.api.schemas.reaches import ReachMapOut, ReachOut
from app.api.schemas.stations import ObservationOut
from app.api.schemas.thresholds import ThresholdOut
from app.core.config import settings
from app.services.api_utils import parse_bbox, parse_geojson
from app.services.reach_service import (
    get_reach,
    latest_embed_for_reach,
    latest_for_reaches,
    list_reaches,
    reach_map_rows,
    reach_thresholds,
    reach_timeseries,
)

router = APIRouter(prefix="/reaches", tags=["reaches"])


@router.get("", response_model=ListEnvelope[ReachOut])
def reaches(
    bbox: str | None = None,
    provider_id: str | None = None,
    country_code: str | None = None,
    ids: str | None = None,
    include_latest: bool = False,
    latest_property: str | None = None,
    limit: int = Query(settings.default_limit, ge=1, le=settings.max_limit),
    cursor: str | None = None,
    db: Session = Depends(get_db),
):
    id_list = ids.split(",") if ids else None
    rows = list_reaches(db, provider_id, country_code, id_list, limit, cursor, parse_bbox(bbox))
    items = []
    for reach, geometry_geojson in rows:
        payload = ReachOut.model_validate(reach, from_attributes=True).model_dump()
        payload["geometry"] = parse_geojson(geometry_geojson)
        if include_latest:
            payload["latest_observation"] = latest_embed_for_reach(db, reach, latest_property)
        items.append(ReachOut(**payload))
    next_cursor = items[-1].reach_id if len(items) == limit else None
    return {"data": items, "meta": Meta(count=len(items), next_cursor=next_cursor)}


@router.get("/map", response_model=ListEnvelope[ReachMapOut])
def reaches_map(
    bbox: str | None = None,
    property: str | None = None,
    provider_id: str | None = None,
    country_code: str | None = None,
    limit: int = Query(settings.default_limit, ge=1, le=settings.max_limit),
    cursor: str | None = None,
    db: Session = Depends(get_db),
):
    rows = reach_map_rows(db, property, provider_id, country_code, limit, cursor, parse_bbox(bbox))
    items = []
    for row in rows:
        row["geometry"] = parse_geojson(row.get("geometry"))
        items.append(ReachMapOut(**row))
    next_cursor = items[-1].reach_id if len(items) == limit else None
    return {"data": items, "meta": Meta(count=len(items), next_cursor=next_cursor)}


@router.get("/latest", response_model=ListEnvelope[ObservationOut])
def reaches_latest(
    property: str | None = None,
    bbox: str | None = None,
    limit: int = Query(settings.default_limit, ge=1, le=settings.max_limit),
    db: Session = Depends(get_db),
):
    items = [
        ObservationOut.model_validate(o, from_attributes=True)
        for o in latest_for_reaches(db, property, limit, bbox=parse_bbox(bbox))
    ]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/{reach_id}", response_model=SingleEnvelope[ReachOut])
def reach(reach_id: str, db: Session = Depends(get_db)):
    row = get_reach(db, reach_id)
    if not row:
        raise HTTPException(404, "reach not found")
    reach_obj, geometry_geojson = row
    payload = ReachOut.model_validate(reach_obj, from_attributes=True).model_dump()
    payload["geometry"] = parse_geojson(geometry_geojson)
    return {"data": ReachOut(**payload)}


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


@router.get("/{reach_id}/thresholds", response_model=ListEnvelope[ThresholdOut])
def thresholds(reach_id: str, property: str | None = None, db: Session = Depends(get_db)):
    items = [ThresholdOut.model_validate(t, from_attributes=True) for t in reach_thresholds(db, reach_id, property)]
    return {"data": items, "meta": Meta(count=len(items), next_cursor=None)}
