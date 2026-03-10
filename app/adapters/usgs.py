from datetime import UTC, datetime, timedelta
import os

import httpx

from app.adapters.base import BaseAdapter, NormalizedObservation, NormalizedStation
from app.core.ids import station_id
from app.core.quality import normalize_quality
from app.core.units import to_canonical

USGS_IV_URL = "https://waterservices.usgs.gov/nwis/iv/"
USGS_SITE_URL = "https://waterservices.usgs.gov/nwis/site/"

USGS_PARAMETER_MAP: dict[str, str] = {
    "00060": "discharge",
    "00065": "stage",
}

US_STATE_FIPS_TO_POSTAL: dict[str, str] = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO", "09": "CT",
    "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI", "16": "ID", "17": "IL",
    "18": "IN", "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD",
    "25": "MA", "26": "MI", "27": "MN", "28": "MS", "29": "MO", "30": "MT", "31": "NE",
    "32": "NV", "33": "NH", "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
    "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA", "54": "WV",
    "55": "WI", "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR", "78": "VI",
}


class USGSAdapter(BaseAdapter):
    provider_id = "usgs"
    supports_stations = True
    supports_history = True

    def __init__(self) -> None:
        self.http_timeout_seconds = float(os.getenv("USGS_TIMEOUT_SECONDS", "20"))
        self.site_ids = self._parse_csv(os.getenv("USGS_SITE_LIST"))
        self.state_codes = self._parse_state_codes(os.getenv("USGS_STATE_CODES"))
        self.parameter_codes = self._parse_csv(os.getenv("USGS_PARAMETER_CODES")) or ["00060", "00065"]
        self.bbox = self._parse_bbox(os.getenv("USGS_BBOX"))
        self.history_lookback_days = self._parse_int(os.getenv("USGS_HISTORY_LOOKBACK_DAYS"), default=7)
        self.history_start = self._parse_date(os.getenv("USGS_HISTORY_START"))
        self.history_end = self._parse_date(os.getenv("USGS_HISTORY_END"))

    @staticmethod
    def _parse_csv(value: str | None) -> list[str]:
        if not value:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]

    @classmethod
    def _parse_state_codes(cls, value: str | None) -> list[str]:
        normalized: list[str] = []
        for token in cls._parse_csv(value):
            cleaned = token.strip().upper()
            if not cleaned:
                continue
            if cleaned.isdigit():
                cleaned = cleaned.zfill(2)
                mapped = US_STATE_FIPS_TO_POSTAL.get(cleaned)
                if mapped:
                    cleaned = mapped
            normalized.append(cleaned)
        return normalized

    @staticmethod
    def _parse_int(value: str | None, default: int) -> int:
        if value is None:
            return default
        try:
            parsed = int(value)
        except ValueError:
            return default
        return parsed if parsed > 0 else default

    @staticmethod
    def _parse_date(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @staticmethod
    def _parse_bbox(value: str | None) -> tuple[float, float, float, float] | None:
        if not value:
            return None
        try:
            west, south, east, north = [float(part.strip()) for part in value.split(",")]
            return (west, south, east, north)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_float(value: str | None, default: float | None = 0.0) -> float | None:
        try:
            return float(value) if value is not None else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _clean_unit(unit: str | None) -> str:
        if not unit:
            return ""
        return unit.strip()

    @staticmethod
    def _chunk(items: list[str], size: int) -> list[list[str]]:
        return [items[i : i + size] for i in range(0, len(items), size)]

    @staticmethod
    def _parse_usgs_rdb(text: str) -> list[dict]:
        header: list[str] | None = None
        rows: list[dict] = []
        for line in text.splitlines():
            if not line or line.startswith("#"):
                continue
            cols = line.split("\t")
            if header is None:
                header = cols
                continue
            if header and len(cols) == len(header) and all(col.endswith("s") for col in cols if col):
                continue
            if header is None:
                continue
            padded = cols + [""] * (len(header) - len(cols))
            row = dict(zip(header, padded, strict=False))
            if row.get("agency_cd") != "USGS":
                continue
            rows.append(row)
        return rows

    @staticmethod
    def map_parameter_code(parameter_code: str | None) -> str | None:
        return USGS_PARAMETER_MAP.get(parameter_code or "")

    def _station_query_params(self) -> dict[str, str]:
        params: dict[str, str] = {
            "format": "rdb",
            "siteStatus": "active",
            "siteType": "ST",
            "hasDataTypeCd": "iv",
        }
        if self.site_ids:
            params["sites"] = ",".join(self.site_ids)
        if self.state_codes:
            params["stateCd"] = ",".join(self.state_codes)
        if self.bbox is not None:
            west, south, east, north = self.bbox
            params["bBox"] = f"{west},{south},{east},{north}"
        return params

    def _iv_params(self, site_ids: list[str], start: datetime | None = None, end: datetime | None = None) -> dict[str, str]:
        params = {
            "format": "json",
            "sites": ",".join(site_ids),
            "parameterCd": ",".join(self.parameter_codes),
        }
        if start is not None:
            params["startDT"] = start.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        if end is not None:
            params["endDT"] = end.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        return params

    async def fetch_station_catalog(self) -> list[dict]:
        async with httpx.AsyncClient(timeout=self.http_timeout_seconds) as client:
            r = await client.get(USGS_SITE_URL, params=self._station_query_params())
            r.raise_for_status()
        return self._parse_usgs_rdb(r.text)

    def normalize_station(self, raw: dict) -> NormalizedStation:
        provider_station_id = raw.get("site_no", "")
        station_name = raw.get("station_nm") or provider_station_id
        river_name = raw.get("dec_coord_datum_cd")

        observed_properties = {"discharge": True, "stage": True}
        available_parameters = self._parse_csv(raw.get("parm_cd"))
        if available_parameters:
            observed_properties = {"discharge": False, "stage": False}
            for parameter_code in available_parameters:
                prop = self.map_parameter_code(parameter_code)
                if prop:
                    observed_properties[prop] = True

        drainage_sqmi = self._safe_float(raw.get("drain_area_va"), default=None)
        drainage_km2 = drainage_sqmi * 2.58999 if drainage_sqmi is not None else None
        metadata = {
            **raw,
            "parameter_codes": available_parameters,
            "mapped_properties": [self.map_parameter_code(code) for code in available_parameters if self.map_parameter_code(code)],
            "canonical_primary_property": "discharge" if observed_properties.get("discharge") else "stage",
            "flow_unit_native": "ft3/s",
            "flow_unit_canonical": "m3/s",
            "stage_unit_native": "ft",
            "stage_unit_canonical": "m",
            "drainage_area_km2": drainage_km2,
            "datum_vertical_reference": raw.get("dec_coord_datum_cd"),
        }

        return NormalizedStation(
            station_id=station_id(self.provider_id, provider_station_id),
            provider_id=self.provider_id,
            provider_station_id=provider_station_id,
            name=station_name,
            latitude=self._safe_float(raw.get("dec_lat_va")),
            longitude=self._safe_float(raw.get("dec_long_va")),
            raw_metadata={
                **metadata,
                "river_name": river_name,
                "country_code": raw.get("country_cd") or "US",
                "admin1": raw.get("state_cd"),
                "timezone": raw.get("tz_cd"),
                "drainage_area_native": raw.get("drain_area_va"),
                "datum_name": raw.get("alt_datum_cd"),
                "source_type": "observed",
                "observed_properties": observed_properties,
            },
        )

    async def fetch_latest_observations(self) -> list[dict]:
        station_records = await self.fetch_station_catalog()
        site_ids = [row["site_no"] for row in station_records if row.get("site_no")]
        if not site_ids:
            return []

        collected: list[dict] = []
        async with httpx.AsyncClient(timeout=self.http_timeout_seconds) as client:
            for chunk in self._chunk(site_ids, 100):
                r = await client.get(USGS_IV_URL, params=self._iv_params(chunk))
                r.raise_for_status()
                series = r.json().get("value", {}).get("timeSeries", [])
                collected.extend(series)
        return collected

    async def fetch_historical_timeseries(self) -> list[dict]:
        station_records = await self.fetch_station_catalog()
        site_ids = [row["site_no"] for row in station_records if row.get("site_no")]
        if not site_ids:
            return []

        end = self.history_end or datetime.now(UTC)
        start = self.history_start or (end - timedelta(days=self.history_lookback_days))
        if start > end:
            start, end = end, start

        collected: list[dict] = []
        async with httpx.AsyncClient(timeout=self.http_timeout_seconds) as client:
            for chunk in self._chunk(site_ids, 50):
                r = await client.get(USGS_IV_URL, params=self._iv_params(chunk, start=start, end=end))
                r.raise_for_status()
                series = r.json().get("value", {}).get("timeSeries", [])
                collected.extend(series)
        return collected

    def normalize_observation(self, series: dict) -> list[NormalizedObservation]:
        var_code = series.get("variable", {}).get("variableCode", [{}])[0].get("value")
        prop = self.map_parameter_code(var_code)
        if not prop:
            return []

        unit = self._clean_unit(series.get("variable", {}).get("unit", {}).get("unitCode"))
        source = series.get("sourceInfo", {})
        site_code = source.get("siteCode", [{}])[0].get("value", "")

        observations: list[NormalizedObservation] = []
        values_collections = series.get("values", [])
        for values in values_collections:
            for point in values.get("value", []):
                numeric_value = self._safe_float(point.get("value"), default=None)
                if numeric_value is None:
                    quality = normalize_quality("missing")
                else:
                    quality = normalize_quality((point.get("qualifiers") or [""])[0])
                value_c, unit_c = (None, None)
                if numeric_value is not None:
                    value_c, unit_c = to_canonical(numeric_value, unit, prop)

                raw_payload = {
                    "series": {
                        "siteCode": site_code,
                        "parameterCd": var_code,
                        "unitCode": unit,
                    },
                    "point": point,
                }
                observations.append(
                    NormalizedObservation(
                        entity_type="station",
                        station_id=station_id(self.provider_id, site_code),
                        property=prop,
                        observed_at=datetime.fromisoformat(point["dateTime"].replace("Z", "+00:00")).astimezone(UTC),
                        value_native=numeric_value,
                        unit_native=unit or None,
                        value_canonical=value_c,
                        unit_canonical=unit_c,
                        quality_code=quality["quality_code"],
                        is_provisional=bool(quality["is_provisional"]),
                        is_estimated=bool(quality["is_estimated"]),
                        is_missing=bool(quality["is_missing"]),
                        is_forecast=bool(quality["is_forecast"]),
                        is_flagged=bool(quality["is_flagged"]),
                        raw_payload=raw_payload,
                    )
                )
        return observations
