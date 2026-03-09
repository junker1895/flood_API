from pydantic import BaseModel


class ReachOut(BaseModel):
    reach_id: str
    provider_id: str
    source_type: str
    latitude: float | None = None
    longitude: float | None = None
