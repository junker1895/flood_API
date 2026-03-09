from datetime import datetime
from typing import Any

from pydantic import BaseModel

from app.api.schemas.stations import LatestEmbedOut, ThresholdSummaryOut, WarningSummaryOut


class ReachOut(BaseModel):
    reach_id: str
    provider_id: str
    source_type: str
    latitude: float | None = None
    longitude: float | None = None
    geometry: dict[str, Any] | None = None
    latest_observation: LatestEmbedOut | None = None


class ReachMapOut(BaseModel):
    reach_id: str
    provider_id: str
    source_type: str
    provider_reach_id: str | None = None
    name: str | None = None
    river_name: str | None = None
    country_code: str | None = None
    network_name: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    geometry: dict[str, Any] | None = None
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
