from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas.common import ListEnvelope, Meta, SingleEnvelope
from app.api.schemas.stations import ObservationOut, StationMapOut, StationOut
from app.api.schemas.thresholds import ThresholdOut
from app.core.config import settings
from app.services.api_utils import parse_bbox, parse_geojson
from app.services.station_service import (
    get_station,
    latest_embed_for_station,
    latest_for_stations,
    list_stations,
    station_map_rows,
    station_thresholds,
    station_timeseries,
)

router = APIRouter(prefix="/stations", tags=["stations"])


@router.get("", response_model=ListEnvelope[StationOut])
def stations(
    bbox: str | None = None,
    provider_id: str | None = None,
    country_code: str | None = None,
    ids: str | None = None,
    updated_since: datetime | None = None,
    include_latest: bool = False,
    latest_property: str | None = None,
    limit: int = Query(settings.default_limit, ge=1, le=settings.max_limit),
    cursor: str | None = None,
    db: Session = Depends(get_db),
):
    id_list = ids.split(",") if ids else None
    rows = list_stations(db, provider_id, country_code, id_list, updated_since, limit, cursor, parse_bbox(bbox))
    items = []
    for station, geometry_geojson in rows:
        payload = StationOut.model_validate(station, from_attributes=True).model_dump()
        payload["geometry"] = parse_geojson(geometry_geojson)
        if include_latest:
            payload["latest_observation"] = latest_embed_for_station(db, station, latest_property)
        items.append(StationOut(**payload))
    next_cursor = items[-1].station_id if len(items) == limit else None
    return {"data": items, "meta": Meta(count=len(items), next_cursor=next_cursor)}


@router.get("/map", response_model=ListEnvelope[StationMapOut])
def stations_map(
    bbox: str | None = None,
    property: str | None = None,
    provider_id: str | None = None,
    country_code: str | None = None,
    limit: int = Query(settings.default_limit, ge=1, le=settings.max_limit),
    cursor: str | None = None,
    db: Session = Depends(get_db),
):
    rows = station_map_rows(db, property, provider_id, country_code, limit, cursor, parse_bbox(bbox))
    items = []
    for row in rows:
        row["geometry"] = parse_geojson(row.get("geometry"))
        items.append(StationMapOut(**row))
    next_cursor = items[-1].station_id if len(items) == limit else None
    return {"data": items, "meta": Meta(count=len(items), next_cursor=next_cursor)}


@router.get("/latest", response_model=ListEnvelope[ObservationOut])
def stations_latest(
    property: str | None = None,
    bbox: str | None = None,
    limit: int = Query(settings.default_limit, ge=1, le=settings.max_limit),
    db: Session = Depends(get_db),
):
    items = [
        ObservationOut.model_validate(o, from_attributes=True)
        for o in latest_for_stations(db, property, limit, bbox=parse_bbox(bbox))
    ]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/{station_id}", response_model=SingleEnvelope[StationOut])
def station(station_id: str, db: Session = Depends(get_db)):
    row = get_station(db, station_id)
    if not row:
        raise HTTPException(404, "station not found")
    s, geometry_geojson = row
    payload = StationOut.model_validate(s, from_attributes=True).model_dump()
    payload["geometry"] = parse_geojson(geometry_geojson)
    return {"data": StationOut(**payload)}


@router.get("/{station_id}/timeseries", response_model=ListEnvelope[ObservationOut])
def timeseries(
    station_id: str,
    property: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = Query(1000, ge=1, le=settings.max_limit),
    db: Session = Depends(get_db),
):
    items = [
        ObservationOut.model_validate(o, from_attributes=True)
        for o in station_timeseries(db, station_id, property, start, end, limit)
    ]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/{station_id}/thresholds", response_model=ListEnvelope[ThresholdOut])
def thresholds(station_id: str, property: str | None = None, db: Session = Depends(get_db)):
    items = [ThresholdOut.model_validate(t, from_attributes=True) for t in station_thresholds(db, station_id, property)]
    return {"data": items, "meta": Meta(count=len(items), next_cursor=None)}
