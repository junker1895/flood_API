import uuid
from datetime import datetime

from sqlalchemy import Boolean, CheckConstraint, DateTime, Float, ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ObservationLatest(Base):
    __tablename__ = "observation_latest"
    __table_args__ = (
        CheckConstraint("(station_id IS NULL) != (reach_id IS NULL)", name="ck_one_entity_latest"),
        Index("uq_latest_station_property", "station_id", "property", unique=True, postgresql_where="station_id IS NOT NULL"),
        Index("uq_latest_reach_property", "reach_id", "property", unique=True, postgresql_where="reach_id IS NOT NULL"),
    )

    latest_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_type: Mapped[str] = mapped_column(String, nullable=False)
    station_id: Mapped[str | None] = mapped_column(ForeignKey("stations.station_id"))
    reach_id: Mapped[str | None] = mapped_column(ForeignKey("reaches.reach_id"))
    property: Mapped[str] = mapped_column(String, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    value_native: Mapped[float | None] = mapped_column(Float)
    unit_native: Mapped[str | None] = mapped_column(String)
    value_canonical: Mapped[float | None] = mapped_column(Float)
    unit_canonical: Mapped[str | None] = mapped_column(String)
    quality_code: Mapped[str | None] = mapped_column(String)
    quality_score: Mapped[float | None] = mapped_column(Float)
    aggregation: Mapped[str | None] = mapped_column(String)
    is_provisional: Mapped[bool] = mapped_column(Boolean, default=False)
    is_estimated: Mapped[bool] = mapped_column(Boolean, default=False)
    is_missing: Mapped[bool] = mapped_column(Boolean, default=False)
    is_forecast: Mapped[bool] = mapped_column(Boolean, default=False)
    is_flagged: Mapped[bool] = mapped_column(Boolean, default=False)
    provider_observation_id: Mapped[str | None] = mapped_column(String)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    raw_payload: Mapped[dict] = mapped_column(JSONB)
