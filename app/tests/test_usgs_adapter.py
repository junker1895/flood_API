from app.adapters.usgs import USGSAdapter


def test_usgs_data_line_filter_skips_header_and_format_rows():
    adapter = USGSAdapter()
    assert adapter._is_data_line("USGS\t06752260\tTEST\t...")
    assert not adapter._is_data_line("agency_cd\tsite_no\tstation_nm")
    assert not adapter._is_data_line("5s\t15s\t50s")


def test_usgs_normalize_station_tolerates_non_numeric_lat_lon():
    adapter = USGSAdapter()
    # header-like tokens in lat/lon positions should not crash normalization
    raw = {"line": "USGS\t123\tDemo\tfoo\tdec_lat_va\tdec_long_va"}
    s = adapter.normalize_station(raw)
    assert s.latitude == 0.0
    assert s.longitude == 0.0
