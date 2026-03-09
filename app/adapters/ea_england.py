from datetime import UTC, datetime

import httpx

from app.adapters.base import BaseAdapter, NormalizedObservation, NormalizedStation, NormalizedWarning
from app.core.ids import station_id
from app.core.quality import normalize_quality
from app.core.units import to_canonical


class EAEnglandAdapter(BaseAdapter):
    provider_id = "ea_england"
    supports_stations = True
    supports_warnings = True

    async def fetch_station_catalog(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get("https://environment.data.gov.uk/flood-monitoring/id/stations?parameter=level")
            r.raise_for_status()
            return r.json().get("items", [])[:100]

    def normalize_station(self, raw: dict) -> NormalizedStation:
        sid = raw.get("notation", raw.get("stationReference", "unknown"))
        return NormalizedStation(
            station_id=station_id(self.provider_id, sid),
            provider_id=self.provider_id,
            provider_station_id=sid,
            name=raw.get("label", sid),
            latitude=raw.get("lat", 0),
            longitude=raw.get("long", 0),
            raw_metadata=raw,
        )

    async def fetch_latest_observations(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get("https://environment.data.gov.uk/flood-monitoring/id/measures?parameter=level")
            r.raise_for_status()
            return r.json().get("items", [])[:30]

    async def fetch_station_by_reference(self, station_reference: str) -> dict | None:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(f"https://environment.data.gov.uk/flood-monitoring/id/stations/{station_reference}")
            r.raise_for_status()
            return r.json().get("items")

    def normalize_observation(self, raw: dict) -> NormalizedObservation:
        prop = "stage"
        unit = raw.get("unitName", "m")
        latest = raw.get("latestReading", {})
        value = latest.get("value")
        q = normalize_quality(raw.get("qualifier"))
        value_c, unit_c = to_canonical(float(value), unit, prop) if value is not None else (None, None)
        return NormalizedObservation(
            entity_type="station",
            station_id=station_id(self.provider_id, raw.get("stationReference", "unknown")),
            property=prop,
            observed_at=datetime.fromisoformat(latest.get("dateTime", datetime.now(UTC).isoformat()).replace("Z", "+00:00")),
            value_native=value,
            unit_native=unit,
            value_canonical=value_c,
            unit_canonical=unit_c,
            quality_code=q["quality_code"],
            raw_payload=raw,
        )

    async def fetch_warnings(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get("https://environment.data.gov.uk/flood-monitoring/id/floods")
            r.raise_for_status()
            return r.json().get("items", [])

    def normalize_warning(self, raw: dict) -> NormalizedWarning:
        return NormalizedWarning(
            warning_id=raw.get("floodAreaID", raw.get("id", "ea-warning")),
            provider_id=self.provider_id,
            severity=raw.get("severity"),
            title=raw.get("description"),
            status=raw.get("message"),
            raw_payload=raw,
        )
