from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable


@dataclass
class ForecastRunDescriptor:
    forecast_date: date
    run_issued_at: datetime | None
    timestep_count: int | None
    timestep_hours: int | None
    timesteps: list[str]
    source_path: str | None
    source_metadata: dict[str, Any] | None


class ForecastModelProvider(ABC):
    model_name: str

    @abstractmethod
    def iter_reach_metadata_chunks(self, chunk_size: int = 5000) -> Iterable[list[dict[str, Any]]]:
        raise NotImplementedError

    @abstractmethod
    def discover_run(self, forecast_date: date | None = None) -> ForecastRunDescriptor:
        raise NotImplementedError

    @abstractmethod
    def iter_run_forecast_chunks(
        self,
        run: ForecastRunDescriptor,
        chunk_size: int = 2500,
    ) -> Iterable[list[dict[str, Any]]]:
        raise NotImplementedError
