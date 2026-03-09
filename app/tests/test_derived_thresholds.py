from app.services.threshold_service import percentile_summary


def test_percentile_summary():
    out = percentile_summary([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    assert out["derived_p50"] == 5
    assert out["derived_p99"] == 9
