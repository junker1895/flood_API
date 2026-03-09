from app.services.provider_registry import PROVIDER_DEFINITIONS


def test_provider_registry_contains_core_providers():
    assert {"usgs", "ea_england", "geoglows", "whos"}.issubset(set(PROVIDER_DEFINITIONS))
