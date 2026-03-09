from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import DateTime, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class WarningEvent(Base):
    __tablename__ = "warning_events"
    warning_id: Mapped[str] = mapped_column(String, primary_key=True)
    provider_id: Mapped[str] = mapped_column(ForeignKey("providers.provider_id"))
    country_code: Mapped[str | None] = mapped_column(String)
    warning_type: Mapped[str | None] = mapped_column(String)
    severity: Mapped[str | None] = mapped_column(String)
    title: Mapped[str | None] = mapped_column(String)
    description: Mapped[str | None] = mapped_column(String)
    issued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    effective_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    effective_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    geometry = mapped_column(Geometry("GEOMETRY", srid=4326), nullable=True)
    related_station_ids: Mapped[list | None] = mapped_column(JSONB)
    related_reach_ids: Mapped[list | None] = mapped_column(JSONB)
    status: Mapped[str | None] = mapped_column(String)
    raw_payload: Mapped[dict] = mapped_column(JSONB)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
