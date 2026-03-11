from __future__ import annotations

from datetime import UTC, date, datetime
import json
import logging
from pathlib import Path
from typing import Any, Iterable

import httpx

from app.core.config import settings
from app.forecast_models.base import ForecastModelProvider, ForecastRunDescriptor

logger = logging.getLogger(__name__)


class GeoglowsForecastProvider(ForecastModelProvider):
    model_name = "geoglows"

    def __init__(self) -> None:
        self.metadata_url = settings.geoglows_forecast_reach_metadata_url
        self.run_manifest_url = settings.geoglows_forecast_run_manifest_url
        self.run_data_url_template = settings.geoglows_forecast_run_data_url_template
        self.timeout_seconds = settings.geoglows_timeout_seconds

    def _load_json(self, source: str) -> Any:
        if source.startswith("http://") or source.startswith("https://"):
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.get(source)
                response.raise_for_status()
                return response.json()
        return json.loads(Path(source).read_text())

    @staticmethod
    def _unwrap_rows(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            data = payload.get("data")
            rows = data if isinstance(data, list) else []
        else:
            rows = []
        return [row for row in rows if isinstance(row, dict)]

    @staticmethod
    def _extract_reach_id(row: dict[str, Any]) -> int | None:
        candidate = row.get("reach_id") or row.get("river_id") or row.get("link_no") or row.get("LINKNO") or row.get("comid")
        try:
            return int(candidate)
        except (TypeError, ValueError):
            return None

    def _validate_metadata_schema(self, row: dict[str, Any]) -> None:
        if self._extract_reach_id(row) is None:
            raise ValueError("GEOGLOWS metadata missing reach identifier; expected LINKNO/comid/reach_id/river_id")

    def _validate_run_schema(self, row: dict[str, Any]) -> None:
        if self._extract_reach_id(row) is None:
            raise ValueError("GEOGLOWS run row missing reach identifier")
        timesteps = row.get("timesteps")
        if not isinstance(timesteps, list):
            raise ValueError("GEOGLOWS run row missing timesteps[] payload")

    def iter_reach_metadata_chunks(self, chunk_size: int = 5000) -> Iterable[list[dict[str, Any]]]:
        if not self.metadata_url:
            raise ValueError("GEOGLOWS_FORECAST_REACH_METADATA_URL is required")

        payload = self._load_json(self.metadata_url)
        rows = self._unwrap_rows(payload)
        if not rows:
            raise ValueError("GEOGLOWS metadata payload has no rows")
        self._validate_metadata_schema(rows[0])

        logger.info("geoglows reach metadata identifier: GEOGLOWS v2 river network uses 9-digit LINKNO/COMID identifiers")
        for idx in range(0, len(rows), chunk_size):
            chunk = []
            for raw in rows[idx : idx + chunk_size]:
                reach_id = self._extract_reach_id(raw)
                if reach_id is None:
                    continue
                chunk.append(
                    {
                        "model": self.model_name,
                        "reach_id": reach_id,
                        "lon": raw.get("lon") or raw.get("longitude") or raw.get("x"),
                        "lat": raw.get("lat") or raw.get("latitude") or raw.get("y"),
                        "uparea": raw.get("uparea") or raw.get("upstream_area") or raw.get("drainage_area"),
                        "rp2": raw.get("rp2"),
                        "rp5": raw.get("rp5"),
                        "rp10": raw.get("rp10"),
                        "rp25": raw.get("rp25"),
                        "rp50": raw.get("rp50"),
                        "rp100": raw.get("rp100"),
                        "source_metadata": raw,
                    }
                )
            yield chunk

    def discover_run(self, forecast_date: date | None = None) -> ForecastRunDescriptor:
        if not self.run_manifest_url:
            raise ValueError("GEOGLOWS_FORECAST_RUN_MANIFEST_URL is required")
        payload = self._load_json(self.run_manifest_url)
        raw = payload["data"] if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else payload
        if not isinstance(raw, dict):
            raise ValueError("GEOGLOWS run manifest payload must be an object")

        date_value = raw.get("forecast_date")
        if forecast_date is None:
            if not date_value:
                raise ValueError("run manifest missing forecast_date")
            forecast_date = date.fromisoformat(str(date_value))

        timesteps = raw.get("timesteps") or []
        timestep_count = raw.get("timestep_count") or (len(timesteps) if isinstance(timesteps, list) else None)
        issued_at = raw.get("run_issued_at")
        issued = datetime.fromisoformat(str(issued_at).replace("Z", "+00:00")).astimezone(UTC) if issued_at else None

        return ForecastRunDescriptor(
            forecast_date=forecast_date,
            run_issued_at=issued,
            timestep_count=timestep_count,
            timestep_hours=raw.get("timestep_hours"),
            timesteps=[str(t) for t in timesteps] if isinstance(timesteps, list) else [],
            source_path=self.run_data_url_template,
            source_metadata={"manifest_url": self.run_manifest_url},
        )

    def iter_run_forecast_chunks(self, run: ForecastRunDescriptor, chunk_size: int = 2500) -> Iterable[list[dict[str, Any]]]:
        if not self.run_data_url_template:
            raise ValueError("GEOGLOWS_FORECAST_RUN_DATA_URL_TEMPLATE is required")
        source = self.run_data_url_template.format(forecast_date=run.forecast_date.isoformat())
        rows = self._unwrap_rows(self._load_json(source))
        if not rows:
            raise ValueError("GEOGLOWS forecast run payload has no rows")
        self._validate_run_schema(rows[0])

        for idx in range(0, len(rows), chunk_size):
            yield rows[idx : idx + chunk_size]
