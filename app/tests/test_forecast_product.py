from datetime import date

from app.services.forecast_product_service import build_risk_row, compute_exceedance_probability


def test_build_risk_row_classification():
    row = build_risk_row(
        model="geoglows",
        forecast_date=date(2026, 3, 11),
        reach_id=100,
        timesteps=[
            {"valid_time": "2026-03-11T00:00:00Z", "flow_median": 10.0, "prob_exceed_rp2": 0.2, "prob_exceed_rp5": 0.3, "prob_exceed_rp10": 0.4},
            {"valid_time": "2026-03-12T00:00:00Z", "flow_median": 20.0, "prob_exceed_rp2": 0.7, "prob_exceed_rp5": 0.6, "prob_exceed_rp10": 0.2},
        ],
    )
    assert row["risk_class"] == 2
    assert row["peak_median_flow"] == 20.0


def test_compute_exceedance_probability_from_ensemble_members():
    assert compute_exceedance_probability([10, 20, 30, 40], 25) == 0.5


def test_build_risk_row_uses_ensemble_members_when_probability_missing():
    row = build_risk_row(
        model="geoglows",
        forecast_date=date(2026, 3, 11),
        reach_id=101,
        timesteps=[
            {
                "valid_time": "2026-03-11T00:00:00Z",
                "flow_median": 100.0,
                "ensemble_members": [5, 15, 30, 40],
                "rp2": 10,
                "rp5": 20,
                "rp10": 35,
            }
        ],
    )
    assert row["max_prob_rp2_24h"] == 0.75
    assert row["max_prob_rp5_24h"] == 0.5
    assert row["max_prob_rp10_24h"] == 0.25
