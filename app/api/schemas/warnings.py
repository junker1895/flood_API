from datetime import datetime
from typing import Any

from pydantic import BaseModel


class WarningOut(BaseModel):
    warning_id: str
    provider_id: str
    country_code: str | None = None
    warning_type: str | None = None
    severity: str | None = None
    title: str | None = None
    description: str | None = None
    issued_at: datetime | None = None
    effective_from: datetime | None = None
    effective_to: datetime | None = None
    status: str | None = None
    related_station_ids: list[str] | None = None
    related_reach_ids: list[str] | None = None
    geometry: dict[str, Any] | None = None
    ingested_at: datetime | None = None
