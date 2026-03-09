from datetime import datetime

from pydantic import BaseModel


class ThresholdOut(BaseModel):
    threshold_id: str
    entity_type: str | None = None
    station_id: str | None = None
    reach_id: str | None = None
    property: str | None = None
    threshold_type: str | None = None
    threshold_label: str | None = None
    severity_rank: int | None = None
    value_native: float | None = None
    unit_native: str | None = None
    value_canonical: float | None = None
    unit_canonical: str | None = None
    effective_from: datetime | None = None
    effective_to: datetime | None = None
    source: str | None = None
    method: str | None = None
