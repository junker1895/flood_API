from datetime import datetime, timedelta

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import ObservationLatest, ObservationTimeseries, Station, Threshold, WarningEvent
from app.services.api_utils import freshness_status, max_severity

STATION_PROPERTY_PREFERENCE = ["stage", "water_level", "discharge"]


def _apply_station_filters(
    stmt: Select[tuple[Station]],
    provider_id: str | None,
    country_code: str | None,
    ids: list[str] | None,
    updated_since: datetime | None,
    cursor: str | None,
) -> Select[tuple[Station]]:
    if provider_id:
        stmt = stmt.where(Station.provider_id == provider_id)
    if country_code:
        stmt = stmt.where(Station.country_code == country_code)
    if ids:
        stmt = stmt.where(Station.station_id.in_(ids))
    if updated_since:
        stmt = stmt.where(Station.last_metadata_refresh_at >= updated_since)
    if cursor:
        stmt = stmt.where(Station.station_id > cursor)
    return stmt


def list_stations(
    db: Session,
    provider_id: str | None = None,
    country_code: str | None = None,
    ids: list[str] | None = None,
    updated_since: datetime | None = None,
    limit: int = 100,
    cursor: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
):
    stmt = select(Station, func.ST_AsGeoJSON(Station.geom).label("geometry_geojson")).order_by(Station.station_id)
    stmt = _apply_station_filters(stmt, provider_id, country_code, ids, updated_since, cursor)
    if bbox:
        min_lon, min_lat, max_lon, max_lat = bbox
        stmt = stmt.where(Station.geom.is_not(None)).where(
            func.ST_Intersects(Station.geom, func.ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326))
        )
    return db.execute(stmt.limit(limit)).all()


def get_station(db: Session, station_id: str):
    stmt = select(Station, func.ST_AsGeoJSON(Station.geom).label("geometry_geojson")).where(Station.station_id == station_id)
    return db.execute(stmt).first()


def latest_for_stations(
    db: Session,
    property_name: str | None = None,
    provider_id: str | None = None,
    limit: int = 100,
    bbox: tuple[float, float, float, float] | None = None,
) -> list[ObservationLatest]:
    stmt = select(ObservationLatest).where(ObservationLatest.station_id.is_not(None))
    joined_station = False
    if property_name:
        stmt = stmt.where(ObservationLatest.property == property_name)
    if provider_id:
        stmt = stmt.join(Station, Station.station_id == ObservationLatest.station_id)
        joined_station = True
        stmt = stmt.where(Station.provider_id == provider_id)
    if bbox:
        min_lon, min_lat, max_lon, max_lat = bbox
        if not joined_station:
            stmt = stmt.join(Station, Station.station_id == ObservationLatest.station_id)
        stmt = stmt.where(
            Station.geom.is_not(None),
            func.ST_Intersects(Station.geom, func.ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326)),
        )
    return list(db.scalars(stmt.order_by(ObservationLatest.station_id).limit(limit)).all())


def station_timeseries(
    db: Session,
    station_id: str,
    property_name: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 1000,
) -> list[ObservationTimeseries]:
    stmt = select(ObservationTimeseries).where(ObservationTimeseries.station_id == station_id)
    if property_name:
        stmt = stmt.where(ObservationTimeseries.property == property_name)
    if start:
        stmt = stmt.where(ObservationTimeseries.observed_at >= start)
    if end:
        stmt = stmt.where(ObservationTimeseries.observed_at <= end)
    return list(db.scalars(stmt.order_by(ObservationTimeseries.observed_at).limit(limit)).all())


def station_thresholds(db: Session, station_id: str, property_name: str | None = None) -> list[Threshold]:
    stmt = select(Threshold).where(Threshold.station_id == station_id)
    if property_name:
        stmt = stmt.where(Threshold.property == property_name)
    return list(db.scalars(stmt).all())


def _latest_for_station(db: Session, station: Station, property_name: str | None = None) -> ObservationLatest | None:
    if property_name:
        return db.scalar(
            select(ObservationLatest)
            .where(ObservationLatest.station_id == station.station_id, ObservationLatest.property == property_name)
            .order_by(ObservationLatest.observed_at.desc())
            .limit(1)
        )

    if station.canonical_primary_property:
        preferred = db.scalar(
            select(ObservationLatest)
            .where(
                ObservationLatest.station_id == station.station_id,
                ObservationLatest.property == station.canonical_primary_property,
            )
            .limit(1)
        )
        if preferred:
            return preferred

    for prop in STATION_PROPERTY_PREFERENCE:
        preferred = db.scalar(
            select(ObservationLatest)
            .where(ObservationLatest.station_id == station.station_id, ObservationLatest.property == prop)
            .limit(1)
        )
        if preferred:
            return preferred

    return db.scalar(
        select(ObservationLatest)
        .where(ObservationLatest.station_id == station.station_id)
        .order_by(ObservationLatest.observed_at.desc())
        .limit(1)
    )


def station_threshold_summary(db: Session, station_id: str):
    rows = db.scalars(select(Threshold).where(Threshold.station_id == station_id)).all()
    if not rows:
        return None
    labels = sorted({r.threshold_label for r in rows if r.threshold_label})
    ranks = [r.severity_rank for r in rows if r.severity_rank is not None]
    return {"has_thresholds": True, "max_severity_rank": max(ranks) if ranks else None, "labels": labels}


def station_warning_summary(db: Session, station_id: str):
    rows = db.scalars(select(WarningEvent).where(WarningEvent.related_station_ids.contains([station_id]))).all()
    if not rows:
        return None
    return {
        "has_warning": True,
        "warning_count": len(rows),
        "max_severity": max_severity([r.severity for r in rows]),
    }


def station_map_rows(
    db: Session,
    property_name: str | None,
    provider_id: str | None,
    country_code: str | None,
    limit: int,
    cursor: str | None,
    bbox: tuple[float, float, float, float] | None,
):
    rows = list_stations(
        db,
        provider_id=provider_id,
        country_code=country_code,
        limit=limit,
        cursor=cursor,
        bbox=bbox,
    )
    out = []
    for station, geometry_geojson in rows:
        latest = _latest_for_station(db, station, property_name)
        out.append(
            {
                "station_id": station.station_id,
                "provider_id": station.provider_id,
                "source_type": station.source_type,
                "provider_station_id": station.provider_station_id,
                "name": station.name,
                "river_name": station.river_name,
                "country_code": station.country_code,
                "admin1": station.admin1,
                "admin2": station.admin2,
                "latitude": station.latitude,
                "longitude": station.longitude,
                "geometry": geometry_geojson,
                "canonical_primary_property": station.canonical_primary_property,
                "station_status": station.station_status,
                "observed_at": latest.observed_at if latest else None,
                "value_native": latest.value_native if latest else None,
                "unit_native": latest.unit_native if latest else None,
                "value_canonical": latest.value_canonical if latest else None,
                "unit_canonical": latest.unit_canonical if latest else None,
                "property": latest.property if latest else None,
                "quality_code": latest.quality_code if latest else None,
                "quality_score": latest.quality_score if latest else None,
                "aggregation": latest.aggregation if latest else None,
                "is_forecast": latest.is_forecast if latest else False,
                "is_provisional": latest.is_provisional if latest else False,
                "is_estimated": latest.is_estimated if latest else False,
                "is_missing": latest.is_missing if latest else False,
                "is_flagged": latest.is_flagged if latest else False,
                "ingested_at": latest.ingested_at if latest else None,
                "freshness_status": freshness_status(
                    latest.observed_at if latest else None,
                    fresh_after=timedelta(minutes=settings.station_fresh_minutes),
                    stale_after=timedelta(minutes=settings.station_stale_minutes),
                ),
                "threshold_summary": station_threshold_summary(db, station.station_id),
                "warning_summary": station_warning_summary(db, station.station_id),
            }
        )
    return out


def latest_embed_for_station(db: Session, station: Station, property_name: str | None = None) -> dict | None:
    latest = _latest_for_station(db, station, property_name)
    if not latest:
        return None
    return {
        "property": latest.property,
        "observed_at": latest.observed_at,
        "value_canonical": latest.value_canonical,
        "unit_canonical": latest.unit_canonical,
        "quality_code": latest.quality_code,
        "is_forecast": latest.is_forecast,
        "is_provisional": latest.is_provisional,
        "is_estimated": latest.is_estimated,
        "is_missing": latest.is_missing,
        "is_flagged": latest.is_flagged,
        "ingested_at": latest.ingested_at,
    }
