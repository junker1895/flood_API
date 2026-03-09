from app.adapters.base import BaseAdapter


class WhosAdapter(BaseAdapter):
    provider_id = "whos"

    async def fetch_station_catalog(self) -> list[dict]:
        # TODO: Expand this to support WHOS metadata discovery and provider enrichment.
        return []
