from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import ObservationTimeseries


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
