import pytest

from app.adapters.ea_england import EAEnglandAdapter


def test_ea_normalize_station_requires_coordinates():
    adapter = EAEnglandAdapter()
    with pytest.raises(ValueError, match="station missing coordinates"):
        adapter.normalize_station({"notation": "A1", "label": "Station A"})


def test_ea_normalize_station_rejects_out_of_range_coordinates():
    adapter = EAEnglandAdapter()
    with pytest.raises(ValueError, match="out-of-range"):
        adapter.normalize_station({"notation": "A1", "lat": 95, "long": -1})
