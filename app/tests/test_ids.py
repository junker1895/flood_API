from app.core.ids import reach_id, station_id


def test_deterministic_ids():
    assert station_id("usgs", "123") == "usgs-123"
    assert reach_id("geoglows", "456") == "geoglows-456"
