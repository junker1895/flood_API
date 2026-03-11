from app.forecast_models.base import ForecastModelProvider
from app.forecast_models.geoglows import GeoglowsForecastProvider


def get_forecast_provider(model: str) -> ForecastModelProvider:
    normalized = model.strip().lower()
    if normalized == "geoglows":
        return GeoglowsForecastProvider()
    raise ValueError(f"unsupported forecast model: {model}")
