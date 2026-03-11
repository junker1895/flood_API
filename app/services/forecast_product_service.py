from __future__ import annotations

from datetime import datetime
from typing import Any


def compute_exceedance_probability(ensemble_members: list[float] | None, threshold: float | None) -> float | None:
    if not ensemble_members or threshold is None:
        return None
    valid = [float(v) for v in ensemble_members if v is not None]
    if not valid:
        return None
    exceed = sum(1 for member in valid if member > float(threshold))
    return float(exceed / len(valid))


def _prob_value(timestep: dict[str, Any], prob_key: str, rp_key: str) -> float | None:
    direct = timestep.get(prob_key)
    if direct is not None:
        return float(direct)
    return compute_exceedance_probability(timestep.get("ensemble_members"), timestep.get(rp_key))


def _max_prob(timesteps: list[dict[str, Any]], prob_key: str, rp_key: str, end_idx: int) -> float | None:
    values = [
        _prob_value(ts, prob_key, rp_key)
        for ts in timesteps[:end_idx]
    ]
    valid = [v for v in values if v is not None]
    return float(max(valid)) if valid else None


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def classify_risk(max_prob_rp2_72h: float | None, max_prob_rp5_72h: float | None, max_prob_rp10_72h: float | None) -> int:
    if (max_prob_rp10_72h or 0.0) >= 0.5:
        return 3
    if (max_prob_rp5_72h or 0.0) >= 0.5:
        return 2
    if (max_prob_rp2_72h or 0.0) >= 0.5:
        return 1
    return 0


def build_risk_row(model: str, forecast_date, reach_id: int, timesteps: list[dict[str, Any]]) -> dict[str, Any]:
    max_prob_rp2_24h = _max_prob(timesteps, "prob_exceed_rp2", "rp2", 8)
    max_prob_rp5_24h = _max_prob(timesteps, "prob_exceed_rp5", "rp5", 8)
    max_prob_rp10_24h = _max_prob(timesteps, "prob_exceed_rp10", "rp10", 8)

    max_prob_rp2_72h = _max_prob(timesteps, "prob_exceed_rp2", "rp2", 24)
    max_prob_rp5_72h = _max_prob(timesteps, "prob_exceed_rp5", "rp5", 24)
    max_prob_rp10_72h = _max_prob(timesteps, "prob_exceed_rp10", "rp10", 24)

    max_prob_rp2_7d = _max_prob(timesteps, "prob_exceed_rp2", "rp2", 56)
    max_prob_rp5_7d = _max_prob(timesteps, "prob_exceed_rp5", "rp5", 56)
    max_prob_rp10_7d = _max_prob(timesteps, "prob_exceed_rp10", "rp10", 56)

    peak_ts = max(timesteps, key=lambda t: t.get("flow_median") or float("-inf"), default={})
    risk_class = classify_risk(max_prob_rp2_72h, max_prob_rp5_72h, max_prob_rp10_72h)

    return {
        "model": model,
        "forecast_date": forecast_date,
        "reach_id": reach_id,
        "risk_class": risk_class,
        "max_prob_rp2_24h": max_prob_rp2_24h,
        "max_prob_rp5_24h": max_prob_rp5_24h,
        "max_prob_rp10_24h": max_prob_rp10_24h,
        "max_prob_rp2_72h": max_prob_rp2_72h,
        "max_prob_rp5_72h": max_prob_rp5_72h,
        "max_prob_rp10_72h": max_prob_rp10_72h,
        "max_prob_rp2_7d": max_prob_rp2_7d,
        "max_prob_rp5_7d": max_prob_rp5_7d,
        "max_prob_rp10_7d": max_prob_rp10_7d,
        "peak_median_flow": peak_ts.get("flow_median"),
        "peak_time": _parse_time(peak_ts.get("valid_time")),
        "source_metadata": None,
    }
