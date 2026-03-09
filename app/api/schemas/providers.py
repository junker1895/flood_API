from pydantic import BaseModel


class ProviderOut(BaseModel):
    provider_id: str
    name: str
    provider_type: str
    status: str
