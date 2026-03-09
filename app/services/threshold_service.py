from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.models import ObservationTimeseries, Reach, Station, Threshold


def percentile_summary(values: list[float]) -> dict[str, float]:
    if not values:
        return {}
    values = sorted(values)

    def pct(p: float) -> float:
        idx = int((len(values) - 1) * p)
        return values[idx]

    return {
        "derived_p50": pct(0.50),
        "derived_p75": pct(0.75),
        "derived_p90": pct(0.90),
        "derived_p95": pct(0.95),
        "derived_p99": pct(0.99),
    }


def derived_percentiles(db: Session, station_id: str, prop: str) -> dict[str, float]:
    vals = db.scalars(
        select(ObservationTimeseries.value_canonical).where(
            ObservationTimeseries.station_id == station_id,
            ObservationTimeseries.property == prop,
            ObservationTimeseries.is_missing.is_(False),
            ObservationTimeseries.is_flagged.is_(False),
            ObservationTimeseries.is_forecast.is_(False),
            ObservationTimeseries.value_canonical.is_not(None),
        )
    ).all()
    return percentile_summary([float(v) for v in vals])


def list_thresholds(
    db: Session,
    station_id: str | None = None,
    reach_id: str | None = None,
    property_name: str | None = None,
    provider_id: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    limit: int = 100,
    cursor: str | None = None,
) -> list[Threshold]:
    stmt = select(Threshold).order_by(Threshold.threshold_id)
    if station_id:
        stmt = stmt.where(Threshold.station_id == station_id)
    if reach_id:
        stmt = stmt.where(Threshold.reach_id == reach_id)
    if property_name:
        stmt = stmt.where(Threshold.property == property_name)
    if provider_id:
        stmt = stmt.outerjoin(Station, Station.station_id == Threshold.station_id).outerjoin(
            Reach, Reach.reach_id == Threshold.reach_id
        ).where((Station.provider_id == provider_id) | (Reach.provider_id == provider_id))
    if bbox:
        min_lon, min_lat, max_lon, max_lat = bbox
        stmt = stmt.outerjoin(Station, Station.station_id == Threshold.station_id).outerjoin(
            Reach, Reach.reach_id == Threshold.reach_id
        ).where(
            (Station.geom.is_not(None) & func.ST_Intersects(Station.geom, func.ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326)))
            | (Reach.geom.is_not(None) & func.ST_Intersects(Reach.geom, func.ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326)))
        )
    if cursor:
        stmt = stmt.where(Threshold.threshold_id > cursor)
    return list(db.scalars(stmt.limit(limit)).all())
