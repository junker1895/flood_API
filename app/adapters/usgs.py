from datetime import UTC, datetime

import httpx

from app.adapters.base import BaseAdapter, NormalizedObservation, NormalizedStation
from app.core.ids import station_id
from app.core.quality import normalize_quality
from app.core.units import to_canonical


class USGSAdapter(BaseAdapter):
    provider_id = "usgs"

    async def fetch_station_catalog(self) -> list[dict]:
        url = "https://waterservices.usgs.gov/nwis/site/?format=rdb&stateCd=co&siteType=ST"
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url)
            r.raise_for_status()
        lines = [ln for ln in r.text.splitlines() if ln and not ln.startswith("#")]
        return [{"line": ln} for ln in lines[:50]]

    def normalize_station(self, raw: dict) -> NormalizedStation:
        parts = raw["line"].split("\t")
        provider_station_id = parts[1]
        return NormalizedStation(
            station_id=station_id(self.provider_id, provider_station_id),
            provider_id=self.provider_id,
            provider_station_id=provider_station_id,
            name=parts[2] if len(parts) > 2 else provider_station_id,
            latitude=float(parts[4]) if len(parts) > 4 else 0,
            longitude=float(parts[5]) if len(parts) > 5 else 0,
            raw_metadata=raw,
        )

    async def fetch_latest_observations(self) -> list[dict]:
        url = "https://waterservices.usgs.gov/nwis/iv/?format=json&parameterCd=00060,00065&sites=01646500"
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url)
            r.raise_for_status()
            return r.json().get("value", {}).get("timeSeries", [])

    def normalize_observation(self, series: dict) -> list[NormalizedObservation]:
        var = series.get("variable", {}).get("variableCode", [{}])[0].get("value")
        prop = "discharge" if var == "00060" else "stage"
        unit = series.get("variable", {}).get("unit", {}).get("unitCode", "")
        observations: list[NormalizedObservation] = []
        for p in series.get("values", [{}])[0].get("value", [])[-5:]:
            value = float(p["value"])
            qual = normalize_quality(p.get("qualifiers", [""])[0])
            value_c, unit_c = to_canonical(value, unit, prop)
            observations.append(NormalizedObservation(
                entity_type="station",
                station_id=station_id(self.provider_id, series["sourceInfo"]["siteCode"][0]["value"]),
                property=prop,
                observed_at=datetime.fromisoformat(p["dateTime"].replace("Z", "+00:00")).astimezone(UTC),
                value_native=value,
                unit_native=unit,
                value_canonical=value_c,
                unit_canonical=unit_c,
                quality_code=qual["quality_code"],
                is_provisional=bool(qual["is_provisional"]),
                is_estimated=bool(qual["is_estimated"]),
                is_missing=bool(qual["is_missing"]),
                is_forecast=bool(qual["is_forecast"]),
                is_flagged=bool(qual["is_flagged"]),
                raw_payload=p,
            ))
        return observations
