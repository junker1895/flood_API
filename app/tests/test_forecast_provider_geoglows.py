from datetime import date

import pytest

from app.forecast_models.geoglows import GeoglowsForecastProvider


class _Table:
    def __init__(self, rows):
        self._rows = rows
        self.columns = list(rows[0].keys()) if rows else []

    def to_dict(self, orient="records"):
        assert orient == "records"
        return list(self._rows)


class _Array:
    def __init__(self, values):
        self.values = values


class _Dataset:
    def __init__(self, coords, variables, attrs=None):
        self.coords = coords
        self.variables = variables
        self.attrs = attrs or {}
        self._values = {**coords, **variables}

    def __getitem__(self, key):
        return _Array(self._values[key])

    def isel(self, _indexers):
        return self


def test_storage_options_are_unsigned_public(monkeypatch):
    monkeypatch.setattr("app.forecast_models.geoglows.settings.geoglows_aws_region", "us-west-2")
    provider = GeoglowsForecastProvider()
    options = provider._storage_options()
    assert options["anon"] is True
    assert options["client_kwargs"]["region_name"] == "us-west-2"


def test_metadata_ingest_from_tables_and_return_periods(monkeypatch):
    provider = GeoglowsForecastProvider()
    monkeypatch.setattr(provider, "_discover_metadata_table_uris", lambda: ["s3://geoglows-v2/tables/reaches.parquet"])
    monkeypatch.setattr(
        provider,
        "_read_table",
        lambda _uri: _Table(
            [
                {"LINKNO": 101, "lat": 1.0, "lon": 2.0, "uparea": 3.0},
                {"comid": 102, "latitude": 4.0, "longitude": 5.0, "upstream_area": 6.0},
            ]
        ),
    )
    monkeypatch.setattr(
        provider,
        "_read_return_periods",
        lambda: {
            101: {"rp2": 10.0, "rp5": 20.0, "rp10": 30.0, "rp25": 40.0, "rp50": 50.0, "rp100": 60.0},
            102: {"rp2": 11.0, "rp5": 21.0, "rp10": 31.0, "rp25": 41.0, "rp50": 51.0, "rp100": 61.0},
        },
    )

    chunks = list(provider.iter_reach_metadata_chunks(chunk_size=1))
    assert len(chunks) == 2
    assert chunks[0][0]["reach_id"] == 101
    assert chunks[0][0]["rp10"] == 30.0


def test_metadata_discovery_fails_when_no_tables(monkeypatch):
    provider = GeoglowsForecastProvider()

    class _FS:
        def find(self, _prefix):
            return []

    monkeypatch.setattr(provider, "_s3_filesystem", lambda: _FS())
    with pytest.raises(ValueError, match="No usable GEOGLOWS metadata tables"):
        provider._discover_metadata_table_uris()


def test_discover_latest_and_explicit_run(monkeypatch):
    provider = GeoglowsForecastProvider()
    monkeypatch.setattr(
        provider,
        "_discover_forecast_zarr_paths",
        lambda: [
            "s3://geoglows-v2-forecasts/archive/2026-03-10/forecast.zarr",
            "s3://geoglows-v2-forecasts/archive/2026-03-11/forecast.zarr",
        ],
    )
    monkeypatch.setattr(
        provider,
        "_open_forecast_dataset",
        lambda _path: _Dataset(
            coords={"time": ["2026-03-11T00:00:00+00:00", "2026-03-11T03:00:00+00:00"]},
            variables={"reach_id": [101, 102], "flow_median": [[1, 2], [3, 4]]},
            attrs={"production_time": "2026-03-11T00:00:00Z"},
        ),
    )

    latest = provider.discover_run()
    assert latest.forecast_date == date(2026, 3, 11)

    explicit = provider.discover_run(date(2026, 3, 10))
    assert explicit.forecast_date == date(2026, 3, 10)


def test_iter_run_forecast_chunks_fails_when_required_variable_missing(monkeypatch):
    provider = GeoglowsForecastProvider()
    monkeypatch.setattr(
        provider,
        "_open_forecast_dataset",
        lambda _path: _Dataset(coords={"time": ["2026-03-11T00:00:00+00:00"], "reach_id": [101]}, variables={}),
    )

    run = type("Run", (), {"source_path": "s3://geoglows-v2-forecasts/archive/2026-03-11/forecast.zarr"})()
    with pytest.raises(ValueError, match="median flow variable"):
        list(provider.iter_run_forecast_chunks(run, chunk_size=1))


def test_read_return_periods_from_curve_dataset(monkeypatch):
    provider = GeoglowsForecastProvider()

    class _Arr:
        def __init__(self, values):
            self.values = values

    class _Ds:
        coords = {"river_id": None, "return_period": None}
        variables = {"river_id": None, "return_period": None, "logpearson3": None}
        data_vars = {"logpearson3": None}

        def __getitem__(self, key):
            if key == "river_id":
                return _Arr([101, 102])
            if key == "return_period":
                return _Arr([2, 5, 10, 25, 50, 100])
            if key == "logpearson3":
                return _Arr([
                    [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                    [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
                ])
            raise KeyError(key)

    import types, sys
    fake_xr = types.SimpleNamespace(open_zarr=lambda *args, **kwargs: _Ds())
    monkeypatch.setitem(sys.modules, "xarray", fake_xr)
    out = provider._read_return_periods()
    assert out[101]["rp10"] == 3.0
    assert out[102]["rp100"] == 60.0


def test_read_return_periods_from_transposed_curve_dataset(monkeypatch):
    provider = GeoglowsForecastProvider()

    class _Arr:
        def __init__(self, values):
            self.values = values

    class _Ds:
        coords = {"river_id": None, "return_period": None}
        variables = {"river_id": None, "return_period": None, "logpearson3": None}
        data_vars = {"logpearson3": None}

        def __getitem__(self, key):
            if key == "river_id":
                return _Arr([101, 102])
            if key == "return_period":
                return _Arr([2, 5, 10, 25, 50, 100])
            if key == "logpearson3":
                # shape: [return_period, river_id]
                return _Arr([
                    [1.0, 10.0],
                    [2.0, 20.0],
                    [3.0, 30.0],
                    [4.0, 40.0],
                    [5.0, 50.0],
                    [6.0, 60.0],
                ])
            raise KeyError(key)

    import types, sys
    fake_xr = types.SimpleNamespace(open_zarr=lambda *args, **kwargs: _Ds())
    monkeypatch.setitem(sys.modules, "xarray", fake_xr)
    out = provider._read_return_periods()
    assert out[101]["rp10"] == 3.0
    assert out[102]["rp100"] == 60.0
