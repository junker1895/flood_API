from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas.common import ListEnvelope, Meta, SingleEnvelope
from app.api.schemas.stations import ObservationOut, StationOut
from app.services.station_service import (
    get_station,
    latest_for_stations,
    list_stations,
    station_thresholds,
    station_timeseries,
)

router = APIRouter(prefix="/stations", tags=["stations"])


@router.get("", response_model=ListEnvelope[StationOut])
def stations(bbox: str | None = None, db: Session = Depends(get_db)):
    if bbox:
        min_lon, min_lat, max_lon, max_lat = map(float, bbox.split(","))
        sql = text("""
            SELECT * FROM stations
            WHERE ST_Intersects(geom, ST_MakeEnvelope(:min_lon,:min_lat,:max_lon,:max_lat,4326))
        """)
        rows = db.execute(sql, {"min_lon": min_lon, "min_lat": min_lat, "max_lon": max_lon, "max_lat": max_lat}).mappings().all()
        items = [StationOut(**dict(r)) for r in rows]
    else:
        items = [StationOut.model_validate(s, from_attributes=True) for s in list_stations(db)]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/latest", response_model=ListEnvelope[ObservationOut])
def stations_latest(db: Session = Depends(get_db)):
    items = [ObservationOut.model_validate(o, from_attributes=True) for o in latest_for_stations(db)]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/{station_id}", response_model=SingleEnvelope[StationOut])
def station(station_id: str, db: Session = Depends(get_db)):
    s = get_station(db, station_id)
    if not s:
        raise HTTPException(404, "station not found")
    return {"data": StationOut.model_validate(s, from_attributes=True)}


@router.get("/{station_id}/timeseries", response_model=ListEnvelope[ObservationOut])
def timeseries(station_id: str, db: Session = Depends(get_db)):
    items = [ObservationOut.model_validate(o, from_attributes=True) for o in station_timeseries(db, station_id)]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/{station_id}/thresholds")
def thresholds(station_id: str, db: Session = Depends(get_db)):
    items = [t.threshold_id for t in station_thresholds(db, station_id)]
    return {"data": items, "meta": {"count": len(items), "next_cursor": None}}
