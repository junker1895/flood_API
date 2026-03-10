from datetime import UTC, datetime, timedelta
import logging
import os
from typing import Any

import httpx

from app.adapters.base import BaseAdapter, NormalizedObservation, NormalizedReach
from app.core.ids import reach_id
from app.core.quality import normalize_quality


logger = logging.getLogger(__name__)


class GeoglowsAdapter(BaseAdapter):
    provider_id = "geoglows"
    supports_reaches = True
    supports_history = True

    _KNOWN_PLACEHOLDER_RIVER_IDS = {"000000000", "111111111", "123456789", "987654321"}

    def __init__(self) -> None:
        self.api_base_url = os.getenv("GEOGLOWS_API_BASE_URL", "https://geoglows.ecmwf.int").rstrip("/")
        self.api_key = os.getenv("GEOGLOWS_API_KEY") or os.getenv("GEOGLOWS_TOKEN")
        self.timeout_seconds = self._parse_float(os.getenv("GEOGLOWS_TIMEOUT_SECONDS"), default=30.0)
        self.trust_env = self._parse_bool(os.getenv("GEOGLOWS_TRUST_ENV"), default=True)
        self.max_reaches = self._parse_int(os.getenv("GEOGLOWS_MAX_REACHES"), default=200)
        self.reach_ids = self._parse_csv(os.getenv("GEOGLOWS_REACH_IDS"))
        self.region_filter = os.getenv("GEOGLOWS_REGION")
        self.history_lookback_days = self._parse_int(os.getenv("GEOGLOWS_HISTORY_LOOKBACK_DAYS"), default=7)

        self.reach_catalog_endpoint = os.getenv("GEOGLOWS_CATALOG_ENDPOINT", "/api/AvailableData/")
        self.reach_metadata_endpoint = os.getenv("GEOGLOWS_REACH_METADATA_ENDPOINT", "/api/GetReachInfo/")
        self.latest_endpoint = os.getenv("GEOGLOWS_LATEST_ENDPOINT", "/api/ForecastStats/")
        fallback_endpoints = self._parse_csv(os.getenv("GEOGLOWS_LATEST_FALLBACK_ENDPOINTS"))
        self.latest_endpoints = [self.latest_endpoint, *[ep for ep in fallback_endpoints if ep != self.latest_endpoint]]
        self.history_endpoint = os.getenv("GEOGLOWS_HISTORY_ENDPOINT", "/api/HistoricSimulation/")

    @classmethod
    def _valid_river_id(cls, value: str) -> bool:
        return value.isdigit() and len(value) == 9 and value not in cls._KNOWN_PLACEHOLDER_RIVER_IDS

    @staticmethod
    def _valid_river_id(value: str) -> bool:
        return value.isdigit() and len(value) == 9

    @staticmethod
    def _parse_csv(value: str | None) -> list[str]:
        if not value:
            return []
        return [item.strip() for item in value.split(",") if item.strip()]

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
    def _parse_float(value: str | None, default: float) -> float:
        if value is None:
            return default
        try:
            parsed = float(value)
        except ValueError:
            return default
        return parsed if parsed > 0 else default

    @staticmethod
    def _parse_bool(value: str | None, default: bool) -> bool:
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=UTC)
        if not isinstance(value, str):
            return None
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
            return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            return None

    @staticmethod
    def _geometry_wkt(raw: dict) -> str | None:
        geometry = raw.get("geometry")
        if not isinstance(geometry, dict) or geometry.get("type") != "LineString":
            return None

        coords = geometry.get("coordinates")
        if not isinstance(coords, list) or len(coords) < 2:
            return None

        pairs: list[str] = []
        for coord in coords:
            if not isinstance(coord, (list, tuple)) or len(coord) < 2:
                return None
            lon = GeoglowsAdapter._safe_float(coord[0])
            lat = GeoglowsAdapter._safe_float(coord[1])
            if lon is None or lat is None:
                return None
            pairs.append(f"{lon} {lat}")

        return f"LINESTRING({', '.join(pairs)})"

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["x-api-key"] = self.api_key
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def _request_json(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.api_base_url}{endpoint}"
        async with httpx.AsyncClient(timeout=self.timeout_seconds, trust_env=self.trust_env) as client:
            response = await client.get(url, params=params, headers=self._headers())
            response.raise_for_status()
            return response.json()

    def _extract_reach_ids(self, payload: Any) -> list[str]:
        if isinstance(payload, list):
            values = payload
        elif isinstance(payload, dict):
            values = payload.get("reach_ids") or payload.get("reaches") or payload.get("data") or []
        else:
            values = []

        output: list[str] = []
        for item in values:
            if isinstance(item, dict):
                candidate = item.get("river_id") or item.get("reach_id") or item.get("id") or item.get("comid") or item.get("link_no")
            else:
                candidate = item
            if candidate is None:
                continue
            output.append(str(candidate))
        return output

    def _configured_river_ids(self) -> list[str]:
        valid_ids: list[str] = []
        invalid_ids: list[str] = []
        for raw in self.reach_ids:
            rid = str(raw).strip()
            if self._valid_river_id(rid):
                valid_ids.append(rid)
            else:
                invalid_ids.append(rid)

        if invalid_ids:
            placeholder_ids = [rid for rid in invalid_ids if rid in self._KNOWN_PLACEHOLDER_RIVER_IDS]
            logger.warning(
                "geoglows configured river IDs contain invalid entries (must be real 9-digit COMID/Link Number, placeholders rejected): invalid_ids=%s placeholder_ids=%s",
                invalid_ids,
                placeholder_ids,
            )
        if valid_ids:
            logger.info("geoglows using configured river IDs: river_ids=%s", valid_ids)
        return valid_ids

    def _series_points(self, payload: Any, value_keys: tuple[str, ...]) -> list[tuple[datetime, float, dict[str, Any]]]:
        points: list[tuple[datetime, float, dict[str, Any]]] = []

        if isinstance(payload, dict):
            # dict[datetime] = value
            if all(isinstance(k, str) for k in payload.keys()) and any(self._parse_datetime(k) for k in payload.keys()):
                for ts, val in payload.items():
                    dt = self._parse_datetime(ts)
                    flow = self._safe_float(val)
                    if dt is None or flow is None:
                        continue
                    points.append((dt, flow, {"datetime": ts, "value": val}))
                return points

            collections = payload.get("data") or payload.get("values") or payload.get("series") or payload.get("points")
            if collections is None:
                collections = payload.get("forecast") or payload.get("historical") or payload.get("records")
        else:
            collections = payload

        if not isinstance(collections, list):
            return points

        for item in collections:
            if not isinstance(item, dict):
                continue
            dt = self._parse_datetime(item.get("datetime") or item.get("time") or item.get("date") or item.get("timestamp"))
            value = None
            for key in value_keys:
                value = self._safe_float(item.get(key))
                if value is not None:
                    break
            if dt is None or value is None:
                continue
            points.append((dt, value, item))
        return points

    async def fetch_reach_by_id(self, provider_reach_id: str) -> dict | None:
        params = {"river_id": provider_reach_id}
        try:
            payload = await self._request_json(self.reach_metadata_endpoint, params=params)
        except (httpx.HTTPError, TimeoutError) as exc:
            logger.info(
                "geoglows metadata best-effort failed for river_id=%s endpoint=%s: %s",
                provider_reach_id,
                self.reach_metadata_endpoint,
                exc,
            )
            return None

        if isinstance(payload, dict):
            if payload.get("river_id") or payload.get("reach_id") or payload.get("id") or payload.get("comid") or payload.get("link_no"):
                return payload
            data = payload.get("data")
            if isinstance(data, dict):
                return data
            if isinstance(data, list) and data:
                first = data[0]
                if isinstance(first, dict):
                    return first
        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
            return payload[0]
        return None

    async def fetch_reach_catalog(self) -> list[dict]:
        reach_ids = list(dict.fromkeys(self._configured_river_ids()))
        if not reach_ids:
            params: dict[str, Any] = {}
            if self.region_filter:
                params["region"] = self.region_filter
            try:
                payload = await self._request_json(self.reach_catalog_endpoint, params=params or None)
            except (httpx.HTTPError, TimeoutError) as exc:
                logger.warning(
                    "geoglows reach catalog fetch failed endpoint=%s: %s; returning empty catalog",
                    self.reach_catalog_endpoint,
                    exc,
                )
                return []
            discovered_ids = self._extract_reach_ids(payload)
            reach_ids = [rid for rid in discovered_ids if self._valid_river_id(rid)]
            invalid_discovered = [rid for rid in discovered_ids if not self._valid_river_id(rid)]
            if invalid_discovered:
                placeholder_discovered = [rid for rid in invalid_discovered if rid in self._KNOWN_PLACEHOLDER_RIVER_IDS]
                logger.warning(
                    "geoglows catalog returned invalid river IDs (must be real 9-digit COMID/Link Number, placeholders rejected): invalid_ids=%s placeholder_ids=%s",
                    invalid_discovered,
                    placeholder_discovered,
                )
            logger.info(
                "geoglows discovered river IDs from catalog endpoint=%s valid_count=%s invalid_count=%s",
                self.reach_catalog_endpoint,
                len(reach_ids),
                len(invalid_discovered),
            )

        records: list[dict] = []
        for rid in reach_ids[: self.max_reaches]:
            metadata = await self.fetch_reach_by_id(rid)
            if metadata is None:
                metadata = {"reach_id": rid, "river_id": rid}
            else:
                metadata.setdefault("reach_id", rid)
                metadata.setdefault("river_id", rid)
            records.append(metadata)
        return records

    def normalize_reach(self, raw: dict) -> NormalizedReach:
        provider_reach = str(raw.get("river_id") or raw.get("reach_id") or raw.get("id") or raw.get("comid") or raw.get("link_no"))
        if not provider_reach:
            raise ValueError("missing GEOGLOWS reach identifier")

        lat = self._safe_float(raw.get("lat") or raw.get("latitude") or raw.get("y"))
        lon = self._safe_float(raw.get("lon") or raw.get("lng") or raw.get("longitude") or raw.get("x"))
        normalized_raw = {
            **raw,
            "modeled_source_type": "geoglows_streamflow",
        }

        return NormalizedReach(
            reach_id=reach_id(self.provider_id, provider_reach),
            provider_id=self.provider_id,
            provider_reach_id=provider_reach,
            source_type="modeled",
            latitude=lat,
            longitude=lon,
            geometry_wkt=self._geometry_wkt(raw),
            raw_metadata=normalized_raw,
        )

    async def fetch_latest_observations(self) -> list[dict]:
        reaches = await self.fetch_reach_catalog()
        collected: list[dict] = []
        if not reaches:
            logger.info("geoglows latest skipped: no valid configured/discovered river IDs available")
        for reach in reaches:
            rid = str(reach.get("river_id") or reach.get("reach_id") or reach.get("id") or reach.get("comid") or reach.get("link_no"))
            if not rid:
                continue

            points: list[tuple[datetime, float, dict[str, Any]]] = []
            selected_endpoint: str | None = None
            endpoint_failed = False
            for endpoint in self.latest_endpoints:
                logger.info("geoglows latest requesting endpoint=%s river_id=%s", endpoint, rid)
                try:
                    payload = await self._request_json(endpoint, params={"river_id": rid})
                except (httpx.HTTPError, TimeoutError) as exc:
                    endpoint_failed = True
                    logger.warning("geoglows latest endpoint failure river_id=%s endpoint=%s: %s", rid, endpoint, exc)
                    continue

                points = self._series_points(payload, value_keys=("flow", "streamflow", "discharge", "qout", "value", "mean"))
                logger.info(
                    "geoglows latest parsed points river_id=%s endpoint=%s count=%s",
                    rid,
                    endpoint,
                    len(points),
                )
                if points:
                    selected_endpoint = endpoint
                    break

            if not points:
                reason = "endpoint failures" if endpoint_failed else "empty/unparseable payload or invalid river_id"
                logger.info(
                    "geoglows latest yielded zero parseable points for river_id=%s reason=%s",
                    rid,
                    reason,
                )
                continue

            dt, flow, point_raw = max(points, key=lambda item: item[0])
            collected.append(
                {
                    "reach_id": rid,
                    "datetime": dt.isoformat(),
                    "flow": flow,
                    "series_type": "forecast",
                    "meta": {"endpoint": selected_endpoint or self.latest_endpoint, "point": point_raw, "reach": reach},
                }
            )
        return collected

    async def fetch_historical_timeseries(self) -> list[dict]:
        reaches = await self.fetch_reach_catalog()
        start = datetime.now(UTC) - timedelta(days=self.history_lookback_days)
        records: list[dict] = []
        if not reaches:
            logger.info("geoglows history skipped: no valid configured/discovered river IDs available")
        for reach in reaches:
            rid = str(reach.get("river_id") or reach.get("reach_id") or reach.get("id") or reach.get("comid") or reach.get("link_no"))
            if not rid:
                continue
            endpoint_failed = False
            logger.info("geoglows history requesting endpoint=%s river_id=%s", self.history_endpoint, rid)
            try:
                payload = await self._request_json(
                    self.history_endpoint,
                    params={"river_id": rid, "start_date": start.date().isoformat()},
                )
            except (httpx.HTTPError, TimeoutError) as exc:
                endpoint_failed = True
                logger.warning("geoglows history endpoint failure river_id=%s endpoint=%s: %s", rid, self.history_endpoint, exc)
                continue

            points = self._series_points(payload, value_keys=("flow", "streamflow", "discharge", "qout", "value", "simulated"))
            logger.info(
                "geoglows history parsed points river_id=%s endpoint=%s count=%s",
                rid,
                self.history_endpoint,
                len(points),
            )
            if not points:
                reason = "endpoint failures" if endpoint_failed else "empty/unparseable payload or invalid river_id"
                logger.info(
                    "geoglows history yielded zero parseable points for river_id=%s reason=%s",
                    rid,
                    reason,
                )
                continue

            for dt, flow, point_raw in points:
                records.append(
                    {
                        "reach_id": rid,
                        "datetime": dt.isoformat(),
                        "flow": flow,
                        "series_type": "reanalysis",
                        "meta": {"endpoint": self.history_endpoint, "point": point_raw, "reach": reach},
                    }
                )
        return records

    def normalize_observation(self, raw: dict) -> NormalizedObservation:
        observed_at = self._parse_datetime(raw.get("datetime"))
        if observed_at is None:
            raise ValueError("GEOGLOWS observation missing datetime")

        flow = self._safe_float(raw.get("flow"))
        series_type = str(raw.get("series_type") or "forecast").lower()
        is_forecast = series_type != "reanalysis"

        quality_input = "forecast" if is_forecast else "raw"
        if flow is None:
            quality_input = "missing"

        q = normalize_quality(quality_input, forecast=is_forecast)

        return NormalizedObservation(
            entity_type="reach",
            reach_id=reach_id(self.provider_id, str(raw.get("reach_id"))),
            property="discharge",
            observed_at=observed_at,
            value_native=flow,
            unit_native="m3/s",
            value_canonical=flow,
            unit_canonical="m3/s",
            quality_code=q["quality_code"],
            is_missing=flow is None,
            is_forecast=is_forecast,
            raw_payload=raw,
        )
