from datetime import UTC, datetime

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.db.models import WarningEvent


INACTIVE_STATUSES = {"inactive", "cancelled", "canceled", "expired", "withdrawn"}


def list_warnings(db: Session, bbox: tuple[float, float, float, float] | None = None):
    stmt = select(WarningEvent, func.ST_AsGeoJSON(WarningEvent.geometry).label("geometry_geojson"))
    if bbox:
        min_lon, min_lat, max_lon, max_lat = bbox
        stmt = stmt.where(WarningEvent.geometry.is_not(None)).where(
            func.ST_Intersects(WarningEvent.geometry, func.ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326))
        )
    return db.execute(stmt).all()


def active_warnings(db: Session, bbox: tuple[float, float, float, float] | None = None):
    now = datetime.now(UTC)
    stmt = select(WarningEvent, func.ST_AsGeoJSON(WarningEvent.geometry).label("geometry_geojson"))
    stmt = stmt.where(
        or_(WarningEvent.status.is_(None), func.lower(WarningEvent.status).not_in(INACTIVE_STATUSES))
    )
    # conservative window logic: keep warnings when windows are missing;
    # only exclude if an explicit window definitively places warning outside "active now".
    stmt = stmt.where(
        or_(WarningEvent.effective_from.is_(None), WarningEvent.effective_from <= now),
        or_(WarningEvent.effective_to.is_(None), WarningEvent.effective_to >= now),
    )
    if bbox:
        min_lon, min_lat, max_lon, max_lat = bbox
        stmt = stmt.where(WarningEvent.geometry.is_not(None)).where(
            func.ST_Intersects(WarningEvent.geometry, func.ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326))
        )
    return db.execute(stmt).all()
