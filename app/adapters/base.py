from abc import ABC
from datetime import datetime
from typing import Any

from pydantic import BaseModel, model_validator

from app.core.enums import EntityType


class NormalizedStation(BaseModel):
    station_id: str
    provider_id: str
    provider_station_id: str
    name: str
    latitude: float
    longitude: float
    source_type: str = "observed"
    raw_metadata: dict[str, Any]


class NormalizedReach(BaseModel):
    reach_id: str
    provider_id: str
    provider_reach_id: str
    source_type: str = "modeled"
    latitude: float | None = None
    longitude: float | None = None
    raw_metadata: dict[str, Any]


class NormalizedObservation(BaseModel):
    entity_type: str
    station_id: str | None = None
    reach_id: str | None = None
    property: str
    observed_at: datetime
    value_native: float | None = None
    unit_native: str | None = None
    value_canonical: float | None = None
    unit_canonical: str | None = None
    quality_code: str
    aggregation: str = "instantaneous"
    is_provisional: bool = False
    is_estimated: bool = False
    is_missing: bool = False
    is_forecast: bool = False
    is_flagged: bool = False
    provider_observation_id: str | None = None
    raw_payload: dict[str, Any]

    @model_validator(mode="after")
    def validate_entity(self):
        if self.entity_type == EntityType.STATION and not self.station_id:
            raise ValueError("station observation requires station_id")
        if self.entity_type == EntityType.REACH and not self.reach_id:
            raise ValueError("reach observation requires reach_id")
        if bool(self.station_id) == bool(self.reach_id):
            raise ValueError("exactly one of station_id/reach_id must be set")
        return self


class NormalizedThreshold(BaseModel):
    threshold_id: str
    entity_type: str
    station_id: str | None = None
    reach_id: str | None = None
    property: str
    threshold_type: str
    source: str
    method: str | None = None
    value_native: float | None = None
    unit_native: str | None = None
    value_canonical: float | None = None
    unit_canonical: str | None = None
    raw_payload: dict[str, Any]


class NormalizedWarning(BaseModel):
    warning_id: str
    provider_id: str
    severity: str | None = None
    title: str | None = None
    status: str | None = None
    raw_payload: dict[str, Any]


class BaseAdapter(ABC):
    provider_id: str
    supports_stations: bool = False
    supports_reaches: bool = False
    supports_warnings: bool = False
    supports_thresholds: bool = False
    supports_history: bool = False

    async def fetch_station_catalog(self) -> list[dict]:
        return []

    async def fetch_reach_catalog(self) -> list[dict]:
        return []

    async def fetch_latest_observations(self) -> list[dict]:
        return []

    async def fetch_historical_timeseries(self, *args, **kwargs) -> list[dict]:
        return []

    async def fetch_thresholds(self) -> list[dict]:
        return []

    async def fetch_warnings(self) -> list[dict]:
        return []
