from pydantic import BaseModel


class WarningOut(BaseModel):
    warning_id: str
    provider_id: str
    severity: str | None = None
    title: str | None = None
    status: str | None = None
