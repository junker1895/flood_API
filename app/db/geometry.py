from __future__ import annotations

from geoalchemy2.elements import WKTElement


def point_geom_from_latlon(latitude: float | None, longitude: float | None) -> WKTElement | None:
    if latitude is None or longitude is None:
        return None

    try:
        lat = float(latitude)
        lon = float(longitude)
    except (TypeError, ValueError):
        return None

    if lat == 0 and lon == 0:
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    return WKTElement(f"POINT({lon} {lat})", srid=4326)
