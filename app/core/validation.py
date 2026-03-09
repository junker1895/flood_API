from datetime import datetime


def valid_latlon(lat: float, lon: float) -> bool:
    return -90 <= lat <= 90 and -180 <= lon <= 180


def valid_timestamp(ts: datetime, is_forecast: bool = False) -> bool:
    if is_forecast:
        return True
    return ts <= datetime.now(ts.tzinfo)
