from app.core.quality import normalize_quality


def test_quality_provisional():
    q = normalize_quality("Provisional")
    assert q["quality_code"] == "provisional"
    assert q["is_provisional"]
