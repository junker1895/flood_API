from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas.common import ListEnvelope, Meta, SingleEnvelope
from app.api.schemas.stations import ObservationOut, StationOut
from app.core.config import settings
from app.services.station_service import (
    get_station,
    latest_for_stations,
    list_stations,
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
    limit: int = Query(settings.default_limit, ge=1, le=settings.max_limit),
    cursor: str | None = None,
    db: Session = Depends(get_db),
):
    if bbox:
        min_lon, min_lat, max_lon, max_lat = map(float, bbox.split(","))
        sql = text(
            """
            SELECT station_id, provider_id, source_type, name, latitude, longitude FROM stations
            WHERE ST_Intersects(geom, ST_MakeEnvelope(:min_lon,:min_lat,:max_lon,:max_lat,4326))
            ORDER BY station_id LIMIT :limit
        """
        )
        rows = db.execute(
            sql,
            {"min_lon": min_lon, "min_lat": min_lat, "max_lon": max_lon, "max_lat": max_lat, "limit": limit},
        ).mappings().all()
        items = [StationOut(**dict(r)) for r in rows]
    else:
        id_list = ids.split(",") if ids else None
        items = [
            StationOut.model_validate(s, from_attributes=True)
            for s in list_stations(db, provider_id, country_code, id_list, updated_since, limit, cursor)
        ]
    next_cursor = items[-1].station_id if len(items) == limit else None
    return {"data": items, "meta": Meta(count=len(items), next_cursor=next_cursor)}


@router.get("/latest", response_model=ListEnvelope[ObservationOut])
def stations_latest(
    property: str | None = None,
    limit: int = Query(settings.default_limit, ge=1, le=settings.max_limit),
    db: Session = Depends(get_db),
):
    items = [ObservationOut.model_validate(o, from_attributes=True) for o in latest_for_stations(db, property, limit)]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/{station_id}", response_model=SingleEnvelope[StationOut])
def station(station_id: str, db: Session = Depends(get_db)):
    s = get_station(db, station_id)
    if not s:
        raise HTTPException(404, "station not found")
    return {"data": StationOut.model_validate(s, from_attributes=True)}


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


@router.get("/{station_id}/thresholds")
def thresholds(station_id: str, property: str | None = None, db: Session = Depends(get_db)):
    items = [t.threshold_id for t in station_thresholds(db, station_id, property)]
    return {"data": items, "meta": {"count": len(items), "next_cursor": None}}
