from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class LatestEmbedOut(BaseModel):
    property: str
    observed_at: datetime
    value_canonical: float | None = None
    unit_canonical: str | None = None
    quality_code: str | None = None
    is_forecast: bool = False
    is_provisional: bool = False
    is_estimated: bool = False
    is_missing: bool = False
    is_flagged: bool = False
    ingested_at: datetime | None = None


class StationOut(BaseModel):
    station_id: str
    provider_id: str
    source_type: str
    name: str
    latitude: float
    longitude: float
    geometry: dict[str, Any] | None = None
    latest_observation: LatestEmbedOut | None = None


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
    quality_score: float | None = None
    aggregation: str | None = None
    is_forecast: bool = False
    is_provisional: bool = False
    is_estimated: bool = False
    is_missing: bool = False
    is_flagged: bool = False
    provider_observation_id: str | None = None
    ingested_at: datetime | None = None


class ThresholdSummaryOut(BaseModel):
    has_thresholds: bool
    max_severity_rank: int | None = None
    labels: list[str] = Field(default_factory=list)


class WarningSummaryOut(BaseModel):
    has_warning: bool
    warning_count: int
    max_severity: str | None = None


class StationMapOut(BaseModel):
    station_id: str
    provider_id: str
    source_type: str
    provider_station_id: str | None = None
    name: str | None = None
    river_name: str | None = None
    country_code: str | None = None
    admin1: str | None = None
    admin2: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    geometry: dict[str, Any] | None = None
    canonical_primary_property: str | None = None
    station_status: str | None = None
    observed_at: datetime | None = None
    value_native: float | None = None
    unit_native: str | None = None
    value_canonical: float | None = None
    unit_canonical: str | None = None
    property: str | None = None
    quality_code: str | None = None
    quality_score: float | None = None
    aggregation: str | None = None
    is_forecast: bool = False
    is_provisional: bool = False
    is_estimated: bool = False
    is_missing: bool = False
    is_flagged: bool = False
    ingested_at: datetime | None = None
    freshness_status: str
    threshold_summary: ThresholdSummaryOut | None = None
    warning_summary: WarningSummaryOut | None = None
