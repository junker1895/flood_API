from app.db.geometry import point_geom_from_latlon


def test_point_geom_from_latlon_uses_lon_lat_order_and_srid():
    geom = point_geom_from_latlon(51.874767, -1.740083)

    assert geom is not None
    assert geom.srid == 4326
    assert geom.data == "POINT(-1.740083 51.874767)"


def test_point_geom_from_latlon_rejects_zero_zero_placeholder():
    assert point_geom_from_latlon(0, 0) is None
