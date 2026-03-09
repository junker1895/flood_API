from datetime import datetime

from pydantic import BaseModel


class StationOut(BaseModel):
    station_id: str
    provider_id: str
    source_type: str
    name: str
    latitude: float
    longitude: float


class ObservationOut(BaseModel):
    entity_type: str
    station_id: str | None = None
    reach_id: str | None = None
    property: str
    observed_at: datetime
    value_native: float | None = None
    unit_native: str | None = None
    value_canonical: float | None = None
    unit_canonical: str | None = None
    quality_code: str | None = None
    provider_observation_id: str | None = None
