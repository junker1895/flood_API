from datetime import UTC, datetime

from app.adapters.base import BaseAdapter, NormalizedObservation, NormalizedReach
from app.core.ids import reach_id
from app.core.quality import normalize_quality


class GeoglowsAdapter(BaseAdapter):
    provider_id = "geoglows"
    supports_reaches = True

    async def fetch_reach_catalog(self) -> list[dict]:
        return [{"reach_id": "1001", "lat": 0.0, "lon": 0.0, "river": "Demo"}]

    def normalize_reach(self, raw: dict) -> NormalizedReach:
        rid = str(raw["reach_id"])
        return NormalizedReach(
            reach_id=reach_id(self.provider_id, rid),
            provider_id=self.provider_id,
            provider_reach_id=rid,
            latitude=raw.get("lat"),
            longitude=raw.get("lon"),
            raw_metadata=raw,
        )

    async def fetch_latest_observations(self) -> list[dict]:
        return [{"reach_id": "1001", "datetime": datetime.now(UTC).isoformat(), "flow": 12.0}]

    def normalize_observation(self, raw: dict) -> NormalizedObservation:
        q = normalize_quality("forecast", forecast=True)
        return NormalizedObservation(
            entity_type="reach",
            reach_id=reach_id(self.provider_id, str(raw["reach_id"])),
            property="discharge",
            observed_at=datetime.fromisoformat(raw["datetime"]),
            value_native=raw["flow"],
            unit_native="m3/s",
            value_canonical=raw["flow"],
            unit_canonical="m3/s",
            quality_code=q["quality_code"],
            is_forecast=True,
            raw_payload=raw,
        )
