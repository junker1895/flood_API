import json
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import HTTPException


def parse_bbox(bbox: str | None) -> tuple[float, float, float, float] | None:
    if not bbox:
        return None
    parts = bbox.split(",")
    if len(parts) != 4:
        raise HTTPException(status_code=400, detail="bbox must be minLon,minLat,maxLon,maxLat")
    try:
        min_lon, min_lat, max_lon, max_lat = map(float, parts)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="bbox values must be numeric") from exc
    if min_lon > max_lon or min_lat > max_lat:
        raise HTTPException(status_code=400, detail="bbox min values must be <= max values")
    return min_lon, min_lat, max_lon, max_lat


def parse_geojson(value: str | dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def utc_now() -> datetime:
    return datetime.now(UTC)


def freshness_status(
    observed_at: datetime | None,
    fresh_after: timedelta,
    stale_after: timedelta,
    now: datetime | None = None,
) -> str:
    if observed_at is None:
        return "unknown"
    ref = now or utc_now()
    age = ref - observed_at.astimezone(UTC)
    if age <= fresh_after:
        return "fresh"
    if age <= stale_after:
        return "stale"
    return "old"


def max_severity(items: list[str | None]) -> str | None:
    ranks = {"minor": 1, "moderate": 2, "severe": 3, "warning": 4, "flood": 5, "danger": 6}
    vals = [i for i in items if i]
    if not vals:
        return None
    return sorted(vals, key=lambda x: ranks.get(x.lower(), 0))[-1]
