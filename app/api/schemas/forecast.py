from datetime import date, datetime

from pydantic import BaseModel


class ForecastMetaOut(BaseModel):
    model: str
    forecast_date: date
    timestep_count: int | None = None
    timestep_hours: int | None = None
    timesteps: list[str]


class ForecastReachRiskOut(BaseModel):
    risk_class: int
    peak_time: datetime | None = None


class ForecastReachesOut(BaseModel):
    model: str
    forecast_date: date
    reaches: dict[str, ForecastReachRiskOut]


class ForecastReachDetailOut(BaseModel):
    model: str
    forecast_date: date
    reach_id: int
    detail_available: bool
    timesteps: list[dict] | None = None
