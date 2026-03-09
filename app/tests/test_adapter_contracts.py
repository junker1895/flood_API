from app.adapters.geoglows import GeoglowsAdapter
from app.adapters.usgs import USGSAdapter


def test_geoglows_modeled_only_flags():
    adapter = GeoglowsAdapter()
    assert adapter.supports_reaches is True
    assert adapter.supports_stations is False


def test_usgs_observed_support_flags():
    adapter = USGSAdapter()
    assert adapter.supports_stations is True
