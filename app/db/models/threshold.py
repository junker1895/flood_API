from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Threshold(Base):
    __tablename__ = "thresholds"
    threshold_id: Mapped[str] = mapped_column(String, primary_key=True)
    entity_type: Mapped[str] = mapped_column(String)
    station_id: Mapped[str | None] = mapped_column(ForeignKey("stations.station_id"))
    reach_id: Mapped[str | None] = mapped_column(ForeignKey("reaches.reach_id"))
    property: Mapped[str] = mapped_column(String)
    threshold_type: Mapped[str] = mapped_column(String)
    threshold_label: Mapped[str | None] = mapped_column(String)
    severity_rank: Mapped[int | None] = mapped_column()
    value_native: Mapped[float | None] = mapped_column(Float)
    unit_native: Mapped[str | None] = mapped_column(String)
    value_canonical: Mapped[float | None] = mapped_column(Float)
    unit_canonical: Mapped[str | None] = mapped_column(String)
    effective_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    effective_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source: Mapped[str] = mapped_column(String)
    method: Mapped[str | None] = mapped_column(String)
    raw_payload: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
