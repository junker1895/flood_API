from __future__ import annotations

from geoalchemy2.elements import WKTElement


def point_geom_from_latlon(latitude: float | None, longitude: float | None) -> WKTElement | None:
    if latitude is None or longitude is None:
        return None
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return None
    return WKTElement(f"POINT({longitude} {latitude})", srid=4326)
