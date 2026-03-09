from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import ObservationLatest, ObservationTimeseries, Reach, Threshold, WarningEvent
from app.services.api_utils import freshness_status, max_severity


def list_reaches(
    db: Session,
    provider_id: str | None = None,
    country_code: str | None = None,
    ids: list[str] | None = None,
    limit: int = 100,
    cursor: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
):
    stmt = select(Reach, func.ST_AsGeoJSON(Reach.geom).label("geometry_geojson")).order_by(Reach.reach_id)
    if provider_id:
        stmt = stmt.where(Reach.provider_id == provider_id)
    if country_code:
        stmt = stmt.where(Reach.country_code == country_code)
    if ids:
        stmt = stmt.where(Reach.reach_id.in_(ids))
    if cursor:
        stmt = stmt.where(Reach.reach_id > cursor)
    if bbox:
        min_lon, min_lat, max_lon, max_lat = bbox
        stmt = stmt.where(Reach.geom.is_not(None)).where(
            func.ST_Intersects(Reach.geom, func.ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326))
        )
    return db.execute(stmt.limit(limit)).all()


def get_reach(db: Session, reach_id: str):
    return db.execute(
        select(Reach, func.ST_AsGeoJSON(Reach.geom).label("geometry_geojson")).where(Reach.reach_id == reach_id)
    ).first()


def latest_for_reaches(
    db: Session,
    property_name: str | None = None,
    limit: int = 100,
    bbox: tuple[float, float, float, float] | None = None,
) -> list[ObservationLatest]:
    stmt = select(ObservationLatest).where(ObservationLatest.reach_id.is_not(None))
    if property_name:
        stmt = stmt.where(ObservationLatest.property == property_name)
    if bbox:
        min_lon, min_lat, max_lon, max_lat = bbox
        stmt = stmt.join(Reach, Reach.reach_id == ObservationLatest.reach_id).where(
            Reach.geom.is_not(None),
            func.ST_Intersects(Reach.geom, func.ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326)),
        )
    return list(db.scalars(stmt.order_by(ObservationLatest.reach_id).limit(limit)).all())


def reach_timeseries(
    db: Session,
    reach_id: str,
    property_name: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 1000,
) -> list[ObservationTimeseries]:
    stmt = select(ObservationTimeseries).where(ObservationTimeseries.reach_id == reach_id)
    if property_name:
        stmt = stmt.where(ObservationTimeseries.property == property_name)
    if start:
        stmt = stmt.where(ObservationTimeseries.observed_at >= start)
    if end:
        stmt = stmt.where(ObservationTimeseries.observed_at <= end)
    return list(db.scalars(stmt.order_by(ObservationTimeseries.observed_at).limit(limit)).all())


def reach_thresholds(db: Session, reach_id: str, property_name: str | None = None) -> list[Threshold]:
    stmt = select(Threshold).where(Threshold.reach_id == reach_id)
    if property_name:
        stmt = stmt.where(Threshold.property == property_name)
    return list(db.scalars(stmt).all())


def _latest_for_reach(db: Session, reach: Reach, property_name: str | None = None) -> ObservationLatest | None:
    if property_name:
        return db.scalar(
            select(ObservationLatest)
            .where(ObservationLatest.reach_id == reach.reach_id, ObservationLatest.property == property_name)
            .order_by(ObservationLatest.observed_at.desc())
            .limit(1)
        )
    preferred = db.scalar(
        select(ObservationLatest)
        .where(ObservationLatest.reach_id == reach.reach_id, ObservationLatest.property == "discharge")
        .limit(1)
    )
    if preferred:
        return preferred
    return db.scalar(
        select(ObservationLatest)
        .where(ObservationLatest.reach_id == reach.reach_id)
        .order_by(ObservationLatest.observed_at.desc())
        .limit(1)
    )


def reach_threshold_summary(db: Session, reach_id: str):
    rows = db.scalars(select(Threshold).where(Threshold.reach_id == reach_id)).all()
    if not rows:
        return None
    labels = sorted({r.threshold_label for r in rows if r.threshold_label})
    ranks = [r.severity_rank for r in rows if r.severity_rank is not None]
    return {"has_thresholds": True, "max_severity_rank": max(ranks) if ranks else None, "labels": labels}


def reach_warning_summary(db: Session, reach_id: str):
    rows = db.scalars(select(WarningEvent).where(WarningEvent.related_reach_ids.contains([reach_id]))).all()
    if not rows:
        return None
    return {
        "has_warning": True,
        "warning_count": len(rows),
        "max_severity": max_severity([r.severity for r in rows]),
    }


def reach_map_rows(
    db: Session,
    property_name: str | None,
    provider_id: str | None,
    country_code: str | None,
    limit: int,
    cursor: str | None,
    bbox: tuple[float, float, float, float] | None,
):
    rows = list_reaches(
        db,
        provider_id=provider_id,
        country_code=country_code,
        limit=limit,
        cursor=cursor,
        bbox=bbox,
    )
    out = []
    for reach, geometry_geojson in rows:
        latest = _latest_for_reach(db, reach, property_name)
        out.append(
            {
                "reach_id": reach.reach_id,
                "provider_id": reach.provider_id,
                "source_type": reach.source_type,
                "provider_reach_id": reach.provider_reach_id,
                "name": reach.name,
                "river_name": reach.river_name,
                "country_code": reach.country_code,
                "network_name": reach.network_name,
                "latitude": reach.latitude,
                "longitude": reach.longitude,
                "geometry": geometry_geojson,
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
                    fresh_after=timedelta(minutes=settings.reach_fresh_minutes),
                    stale_after=timedelta(minutes=settings.reach_stale_minutes),
                ),
                "threshold_summary": reach_threshold_summary(db, reach.reach_id),
                "warning_summary": reach_warning_summary(db, reach.reach_id),
            }
        )
    return out


def latest_embed_for_reach(db: Session, reach: Reach, property_name: str | None = None) -> dict | None:
    latest = _latest_for_reach(db, reach, property_name)
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
