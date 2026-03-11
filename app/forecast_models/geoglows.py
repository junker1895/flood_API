from __future__ import annotations

from datetime import UTC, date, datetime
import logging
import re
from typing import Any, Iterable

from app.core.config import settings
from app.forecast_models.base import ForecastModelProvider, ForecastRunDescriptor

logger = logging.getLogger(__name__)


class GeoglowsForecastProvider(ForecastModelProvider):
    model_name = "geoglows"

    def __init__(self) -> None:
        self.forecast_bucket = settings.geoglows_forecast_bucket
        self.forecast_prefix = settings.geoglows_forecast_prefix.strip("/")
        self.metadata_bucket = settings.geoglows_metadata_bucket
        self.metadata_tables_prefix = settings.geoglows_metadata_tables_prefix.strip("/")
        self.return_periods_zarr_path = settings.geoglows_return_periods_zarr_path
        self.aws_region = settings.geoglows_aws_region

    def _storage_options(self) -> dict[str, Any]:
        return {"anon": True, "client_kwargs": {"region_name": self.aws_region}}

    def _s3_filesystem(self):
        import fsspec

        return fsspec.filesystem("s3", **self._storage_options())

    @staticmethod
    def _extract_reach_id(row: dict[str, Any]) -> int | None:
        candidate = row.get("reach_id") or row.get("river_id") or row.get("link_no") or row.get("LINKNO") or row.get("comid")
        try:
            return int(candidate)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _find_column(columns: list[str], aliases: list[str]) -> str | None:
        lowered = {c.lower(): c for c in columns}
        for alias in aliases:
            if alias.lower() in lowered:
                return lowered[alias.lower()]
        return None


    @staticmethod
    def _normalize_return_period(value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            iv = int(round(float(value)))
            return iv if iv in {2, 5, 10, 25, 50, 100} else None
        raw = str(value).strip().lower()
        digits = ''.join(ch for ch in raw if ch.isdigit())
        if not digits:
            return None
        iv = int(digits)
        return iv if iv in {2, 5, 10, 25, 50, 100} else None

    def _read_table(self, uri: str):
        import pandas as pd

        if uri.endswith(".parquet"):
            return pd.read_parquet(uri, storage_options=self._storage_options())
        if uri.endswith(".csv"):
            return pd.read_csv(uri, storage_options=self._storage_options())
        raise ValueError(f"unsupported table format for {uri}")

    def _discover_metadata_table_uris(self) -> list[str]:
        fs = self._s3_filesystem()
        prefix = f"{self.metadata_bucket}/{self.metadata_tables_prefix}".rstrip("/")
        candidates = fs.find(prefix)
        uris = []
        for key in candidates:
            lowered = key.lower()
            if not (lowered.endswith(".parquet") or lowered.endswith(".csv")):
                continue
            if any(token in lowered for token in ["reach", "river", "network", "comid", "table"]):
                uris.append(f"s3://{key}")
        if not uris:
            raise ValueError(f"No usable GEOGLOWS metadata tables discovered under s3://{prefix}")
        return uris

    def _read_return_periods(self) -> dict[int, dict[str, float | None]]:
        import xarray as xr

        ds = xr.open_zarr(self.return_periods_zarr_path, storage_options=self._storage_options())
        reach_dim = self._find_column(list(ds.coords) + list(ds.variables), ["reach_id", "river_id", "comid", "link_no", "LINKNO"])
        if not reach_dim:
            raise ValueError("GEOGLOWS return-period dataset missing reach identifier coordinate")

        rp_vars = {
            "rp2": self._find_column(list(ds.variables), ["rp2", "return_period_2"]),
            "rp5": self._find_column(list(ds.variables), ["rp5", "return_period_5"]),
            "rp10": self._find_column(list(ds.variables), ["rp10", "return_period_10"]),
            "rp25": self._find_column(list(ds.variables), ["rp25", "return_period_25"]),
            "rp50": self._find_column(list(ds.variables), ["rp50", "return_period_50"]),
            "rp100": self._find_column(list(ds.variables), ["rp100", "return_period_100"]),
        }

        reach_values = ds[reach_dim].values
        output: dict[int, dict[str, float | None]] = {
            int(rv): {"rp2": None, "rp5": None, "rp10": None, "rp25": None, "rp50": None, "rp100": None}
            for rv in reach_values
        }

        if any(rp_vars.values()):
            for idx, rv in enumerate(reach_values):
                rid = int(rv)
                for key, var_name in rp_vars.items():
                    output[rid][key] = float(ds[var_name].values[idx]) if var_name else None
            return output

        rp_dim = self._find_column(list(ds.coords) + list(ds.variables), ["return_period", "rp", "rp_dim"])
        rp_curve_var = self._find_column(list(ds.data_vars), ["logpearson3", "gumbel", "return_period_flow"])
        if not rp_dim or not rp_curve_var:
            raise ValueError("GEOGLOWS return-period dataset missing RP variables")

        rp_index_map: dict[int, int] = {}
        for idx, rp_raw in enumerate(ds[rp_dim].values):
            parsed = self._normalize_return_period(rp_raw)
            if parsed is not None:
                rp_index_map[parsed] = idx

        required = {2, 5, 10, 25, 50, 100}
        missing = sorted(required - set(rp_index_map))
        if missing:
            raise ValueError(f"GEOGLOWS return-period coordinate missing expected values: {missing}")

        values = ds[rp_curve_var].values
        rp_values = ds[rp_dim].values

        def _value_at(reach_idx: int, rp_idx: int) -> float:
            # Some GEOGLOWS return-period datasets are stored as
            # [river_id, return_period] while others are [return_period, river_id].
            if len(values) == len(reach_values):
                return float(values[reach_idx][rp_idx])
            if len(values) == len(rp_values):
                return float(values[rp_idx][reach_idx])
            raise ValueError(
                "GEOGLOWS return-period curve shape does not match reach/return_period coordinates"
            )

        for idx, rv in enumerate(reach_values):
            rid = int(rv)
            output[rid]["rp2"] = _value_at(idx, rp_index_map[2])
            output[rid]["rp5"] = _value_at(idx, rp_index_map[5])
            output[rid]["rp10"] = _value_at(idx, rp_index_map[10])
            output[rid]["rp25"] = _value_at(idx, rp_index_map[25])
            output[rid]["rp50"] = _value_at(idx, rp_index_map[50])
            output[rid]["rp100"] = _value_at(idx, rp_index_map[100])
        return output

    def iter_reach_metadata_chunks(self, chunk_size: int = 5000) -> Iterable[list[dict[str, Any]]]:
        table_uris = self._discover_metadata_table_uris()
        rp_map = self._read_return_periods()

        assembled: list[dict[str, Any]] = []
        for uri in table_uris:
            table = self._read_table(uri)
            id_col = self._find_column(list(table.columns), ["reach_id", "river_id", "comid", "link_no", "LINKNO"])
            if not id_col:
                continue
            lon_col = self._find_column(list(table.columns), ["lon", "longitude", "x"])
            lat_col = self._find_column(list(table.columns), ["lat", "latitude", "y"])
            uparea_col = self._find_column(list(table.columns), ["uparea", "upstream_area", "drainage_area"])

            for row in table.to_dict(orient="records"):
                rid = self._extract_reach_id(row)
                if rid is None:
                    continue
                rp = rp_map.get(rid, {})
                assembled.append(
                    {
                        "model": self.model_name,
                        "reach_id": rid,
                        "lon": row.get(lon_col) if lon_col else None,
                        "lat": row.get(lat_col) if lat_col else None,
                        "uparea": row.get(uparea_col) if uparea_col else None,
                        "rp2": rp.get("rp2"),
                        "rp5": rp.get("rp5"),
                        "rp10": rp.get("rp10"),
                        "rp25": rp.get("rp25"),
                        "rp50": rp.get("rp50"),
                        "rp100": rp.get("rp100"),
                        "source_metadata": {"table_uri": uri},
                    }
                )
            if assembled:
                break

        if not assembled:
            raise ValueError("Unable to discover usable GEOGLOWS reach metadata rows from AWS Open Data tables")

        logger.info("geoglows reach metadata identifier: GEOGLOWS v2 river network uses 9-digit LINKNO/COMID identifiers")
        for idx in range(0, len(assembled), chunk_size):
            yield assembled[idx : idx + chunk_size]

    @staticmethod
    def _extract_date_from_key(key: str) -> date | None:
        for pattern in [r"(\d{4}-\d{2}-\d{2})", r"(\d{8})"]:
            m = re.search(pattern, key)
            if not m:
                continue
            raw = m.group(1)
            try:
                return date.fromisoformat(raw) if "-" in raw else datetime.strptime(raw, "%Y%m%d").date()
            except ValueError:
                continue
        return None

    def _discover_forecast_zarr_paths(self) -> list[str]:
        fs = self._s3_filesystem()
        base = f"{self.forecast_bucket}/{self.forecast_prefix}".rstrip("/")
        keys = fs.find(base)
        zarr_dirs = sorted({k.split(".zarr/")[0] + ".zarr" for k in keys if ".zarr/" in k or k.endswith(".zarr")})
        if not zarr_dirs:
            raise ValueError(f"No GEOGLOWS forecast Zarr datasets discovered under s3://{base}")
        return [f"s3://{key}" for key in zarr_dirs]

    def _open_forecast_dataset(self, source_path: str):
        import xarray as xr

        return xr.open_zarr(source_path, storage_options=self._storage_options())

    def _discover_time_values(self, ds) -> list[str]:
        time_var = self._find_column(list(ds.coords) + list(ds.variables), ["time", "valid_time", "datetime"])
        if not time_var:
            raise ValueError("GEOGLOWS forecast dataset missing time coordinate")
        return [str(v) for v in ds[time_var].values]

    def _discover_reach_coord(self, ds) -> str:
        reach_var = self._find_column(list(ds.coords) + list(ds.variables), ["reach_id", "river_id", "comid", "link_no", "LINKNO"])
        if not reach_var:
            raise ValueError("GEOGLOWS forecast dataset missing reach coordinate")
        return reach_var

    def discover_run(self, forecast_date: date | None = None) -> ForecastRunDescriptor:
        paths = self._discover_forecast_zarr_paths()
        dated_paths = [(self._extract_date_from_key(path), path) for path in paths]
        dated_paths = [(d, p) for d, p in dated_paths if d is not None]
        if not dated_paths:
            raise ValueError("Unable to derive forecast dates from GEOGLOWS forecast bucket keys")

        if forecast_date is None:
            picked_date, source_path = max(dated_paths, key=lambda item: item[0])
        else:
            matching = [p for d, p in dated_paths if d == forecast_date]
            if not matching:
                raise ValueError(f"No GEOGLOWS forecast dataset found for forecast_date={forecast_date}")
            picked_date, source_path = forecast_date, matching[0]

        ds = self._open_forecast_dataset(source_path)
        timesteps = self._discover_time_values(ds)
        issued_at = None
        issued_raw = ds.attrs.get("run_issued_at") or ds.attrs.get("forecast_issued_at") or ds.attrs.get("production_time")
        if issued_raw:
            issued_at = datetime.fromisoformat(str(issued_raw).replace("Z", "+00:00")).astimezone(UTC)

        timestep_hours = None
        if len(timesteps) >= 2:
            t0 = datetime.fromisoformat(timesteps[0].replace("Z", "+00:00"))
            t1 = datetime.fromisoformat(timesteps[1].replace("Z", "+00:00"))
            timestep_hours = int((t1 - t0).total_seconds() // 3600)

        return ForecastRunDescriptor(
            forecast_date=picked_date,
            run_issued_at=issued_at,
            timestep_count=len(timesteps),
            timestep_hours=timestep_hours,
            timesteps=timesteps,
            source_path=source_path,
            source_metadata={"dataset_attrs": {k: str(v) for k, v in ds.attrs.items()}},
        )

    def iter_run_forecast_chunks(self, run: ForecastRunDescriptor, chunk_size: int = 2500) -> Iterable[list[dict[str, Any]]]:
        ds = self._open_forecast_dataset(run.source_path or "")
        reach_coord = self._discover_reach_coord(ds)
        time_coord = self._find_column(list(ds.coords) + list(ds.variables), ["time", "valid_time", "datetime"])
        if not time_coord:
            raise ValueError("GEOGLOWS forecast dataset missing time coordinate")

        median_flow_var = self._find_column(list(ds.variables), ["flow_median", "median", "q50", "ensemble_median", "streamflow_median"])
        if not median_flow_var:
            raise ValueError("GEOGLOWS forecast dataset missing median flow variable")

        rp2_prob_var = self._find_column(list(ds.variables), ["prob_exceed_rp2", "probability_rp2"])
        rp5_prob_var = self._find_column(list(ds.variables), ["prob_exceed_rp5", "probability_rp5"])
        rp10_prob_var = self._find_column(list(ds.variables), ["prob_exceed_rp10", "probability_rp10"])
        ensemble_var = self._find_column(list(ds.variables), ["ensemble", "ensemble_flow", "streamflow_ensemble"])

        reach_values = ds[reach_coord].values
        time_values = ds[time_coord].values

        for start in range(0, len(reach_values), chunk_size):
            end = min(start + chunk_size, len(reach_values))
            sub = ds.isel({reach_coord: slice(start, end)})
            chunk_rows: list[dict[str, Any]] = []
            for ridx, rid in enumerate(reach_values[start:end]):
                timesteps = []
                for tidx, tval in enumerate(time_values):
                    ts = {
                        "valid_time": str(tval),
                        "flow_median": float(sub[median_flow_var].values[ridx, tidx]),
                        "prob_exceed_rp2": float(sub[rp2_prob_var].values[ridx, tidx]) if rp2_prob_var else None,
                        "prob_exceed_rp5": float(sub[rp5_prob_var].values[ridx, tidx]) if rp5_prob_var else None,
                        "prob_exceed_rp10": float(sub[rp10_prob_var].values[ridx, tidx]) if rp10_prob_var else None,
                    }
                    if ensemble_var:
                        ts["ensemble_members"] = [float(v) for v in sub[ensemble_var].values[ridx, :, tidx]]
                    timesteps.append(ts)
                chunk_rows.append({"reach_id": int(rid), "timesteps": timesteps})
            yield chunk_rows
