from app.core.units import to_canonical


def test_ft3s_to_m3s():
    value, unit = to_canonical(100.0, "ft3/s", "discharge")
    assert unit == "m3/s"
    assert round(value or 0, 3) == 2.832
