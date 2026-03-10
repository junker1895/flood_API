import pytest
import asyncio

from app.adapters.usgs import USGSAdapter


def test_usgs_rdb_parser_extracts_data_rows():
    adapter = USGSAdapter()
    text = """# comment\nagency_cd\tsite_no\tstation_nm\tdec_lat_va\tdec_long_va\n5s\t15s\t50s\t16s\t16s\nUSGS\t01646500\tPOTOMAC RIVER\t38.949\t-77.127\n"""
    rows = adapter._parse_usgs_rdb(text)
    assert len(rows) == 1
    assert rows[0]["site_no"] == "01646500"


def test_usgs_normalize_station_maps_metadata():
    adapter = USGSAdapter()
    raw = {
        "agency_cd": "USGS",
        "site_no": "01646500",
        "station_nm": "Potomac River at Point of Rocks",
        "dec_lat_va": "39.274",
        "dec_long_va": "-77.543",
        "state_cd": "24",
        "country_cd": "US",
        "tz_cd": "EST",
        "drain_area_va": "100",
        "alt_datum_cd": "NAVD88",
        "dec_coord_datum_cd": "NAD83",
        "parm_cd": "00060,00065",
    }
    station = adapter.normalize_station(raw)
    assert station.station_id == "usgs-01646500"
    assert station.latitude == 39.274
    assert station.raw_metadata["observed_properties"]["discharge"] is True
    assert station.raw_metadata["drainage_area_km2"] == pytest.approx(258.999)


def test_map_parameter_code():
    assert USGSAdapter.map_parameter_code("00060") == "discharge"
    assert USGSAdapter.map_parameter_code("00065") == "stage"
    assert USGSAdapter.map_parameter_code("99999") is None


def test_normalize_observation_handles_missing_value_and_units():
    adapter = USGSAdapter()
    series = {
        "sourceInfo": {"siteCode": [{"value": "01646500"}]},
        "variable": {
            "variableCode": [{"value": "00060"}],
            "unit": {"unitCode": "ft3/s"},
        },
        "values": [{"value": [{"value": "123", "dateTime": "2024-01-01T00:00:00Z", "qualifiers": ["P"]}, {"value": "Ice", "dateTime": "2024-01-01T01:00:00Z", "qualifiers": [""]}]}],
    }
    obs = adapter.normalize_observation(series)
    assert len(obs) == 2
    assert obs[0].property == "discharge"
    assert obs[0].unit_canonical == "m3/s"
    assert obs[1].value_native is None
    assert obs[1].is_missing is True


def test_fetch_station_catalog_uses_state_filter(monkeypatch):
    monkeypatch.setenv("USGS_STATE_CODES", "24,51")
    captured = {}

    class FakeResponse:
        text = "agency_cd\tsite_no\tstation_nm\n5s\t15s\t50s\nUSGS\t1\tA"

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, _url, params=None):
            captured["params"] = params
            return FakeResponse()

    monkeypatch.setattr("app.adapters.usgs.httpx.AsyncClient", FakeClient)
    adapter = USGSAdapter()
    rows = asyncio.run(adapter.fetch_station_catalog())
    assert rows[0]["site_no"] == "1"
    assert captured["params"]["stateCd"] == "MD,VA"


def test_fetch_station_catalog_accepts_postal_state_filter(monkeypatch):
    monkeypatch.setenv("USGS_STATE_CODES", "md,va")
    captured = {}

    class FakeResponse:
        text = "agency_cd\tsite_no\tstation_nm\n5s\t15s\t50s\nUSGS\t1\tA"

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, _url, params=None):
            captured["params"] = params
            return FakeResponse()

    monkeypatch.setattr("app.adapters.usgs.httpx.AsyncClient", FakeClient)
    adapter = USGSAdapter()
    rows = asyncio.run(adapter.fetch_station_catalog())
    assert rows[0]["site_no"] == "1"
    assert captured["params"]["stateCd"] == "MD,VA"


def test_fetch_historical_timeseries_uses_time_window(monkeypatch):
    monkeypatch.setenv("USGS_SITE_LIST", "01646500")
    monkeypatch.setenv("USGS_HISTORY_START", "2024-01-01T00:00:00+00:00")
    monkeypatch.setenv("USGS_HISTORY_END", "2024-01-02T00:00:00+00:00")
    calls = []

    class FakeResponse:
        def __init__(self, text="", payload=None):
            self.text = text
            self._payload = payload or {}

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, *args, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url, params=None):
            calls.append((url, params))
            if "site" in url:
                return FakeResponse(text="agency_cd\tsite_no\tstation_nm\n5s\t15s\t50s\nUSGS\t01646500\tA")
            return FakeResponse(payload={"value": {"timeSeries": [{"id": "x"}]}})

    monkeypatch.setattr("app.adapters.usgs.httpx.AsyncClient", FakeClient)
    adapter = USGSAdapter()
    out = asyncio.run(adapter.fetch_historical_timeseries())
    assert len(out) == 1
    iv_call = calls[-1][1]
    assert iv_call["startDT"] == "2024-01-01T00:00:00Z"
    assert iv_call["endDT"] == "2024-01-02T00:00:00Z"


def test_fetch_latest_observations_multiple_sites(monkeypatch):
    monkeypatch.setenv("USGS_SITE_LIST", "01646500,01651000")

    class FakeResponse:
        def __init__(self, text="", payload=None):
            self.text = text
            self._payload = payload or {}

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, *args, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url, params=None):
            if "site" in url:
                return FakeResponse(text="""agency_cd	site_no	station_nm
5s	15s	50s
USGS	01646500	A
USGS	01651000	B""")
            payload = {"value": {"timeSeries": [{"sourceInfo": {"siteCode": [{"value": "01646500"}]}, "variable": {"variableCode": [{"value": "00060"}], "unit": {"unitCode": "ft3/s"}}, "values": [{"value": []}]}, {"sourceInfo": {"siteCode": [{"value": "01651000"}]}, "variable": {"variableCode": [{"value": "00065"}], "unit": {"unitCode": "ft"}}, "values": [{"value": []}]}]}}
            return FakeResponse(payload=payload)

    monkeypatch.setattr("app.adapters.usgs.httpx.AsyncClient", FakeClient)
    adapter = USGSAdapter()
    series = asyncio.run(adapter.fetch_latest_observations())
    assert len(series) == 2


def test_http_client_defaults_to_not_trusting_env_proxy(monkeypatch):
    monkeypatch.delenv("USGS_TRUST_ENV", raising=False)
    adapter = USGSAdapter()
    assert adapter.http_trust_env is False

        async def get(self, url, params=None):
            if "site" in url:
                return FakeResponse(text="""agency_cd	site_no	station_nm
5s	15s	50s
USGS	01646500	A
USGS	01651000	B""")
            payload = {"value": {"timeSeries": [{"sourceInfo": {"siteCode": [{"value": "01646500"}]}, "variable": {"variableCode": [{"value": "00060"}], "unit": {"unitCode": "ft3/s"}}, "values": [{"value": []}]}, {"sourceInfo": {"siteCode": [{"value": "01651000"}]}, "variable": {"variableCode": [{"value": "00065"}], "unit": {"unitCode": "ft"}}, "values": [{"value": []}]}]}}
            return FakeResponse(payload=payload)

def test_http_client_can_trust_env_proxy_when_enabled(monkeypatch):
    monkeypatch.setenv("USGS_TRUST_ENV", "true")
    adapter = USGSAdapter()
    assert adapter.http_trust_env is True
