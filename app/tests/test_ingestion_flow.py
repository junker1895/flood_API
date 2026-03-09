from datetime import UTC, datetime

import pytest

from app.adapters.base import NormalizedObservation


def test_normalized_observation_entity_shape():
    o = NormalizedObservation(
        entity_type="station",
        station_id="usgs-1",
        property="discharge",
        observed_at=datetime.now(UTC),
        quality_code="raw",
        raw_payload={"a": 1},
    )
    assert o.station_id == "usgs-1"
    assert o.reach_id is None


def test_normalized_observation_requires_exactly_one_entity_id():
    with pytest.raises(ValueError):
        NormalizedObservation(
            entity_type="station",
            property="discharge",
            observed_at=datetime.now(UTC),
            quality_code="raw",
            raw_payload={"a": 1},
        )
