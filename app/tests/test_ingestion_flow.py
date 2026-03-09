from datetime import UTC, datetime

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
