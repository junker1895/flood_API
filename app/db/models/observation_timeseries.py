from datetime import datetime

from sqlalchemy import Boolean, CheckConstraint, DateTime, Float, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ObservationTimeseries(Base):
    __tablename__ = "observation_timeseries"
    __table_args__ = (CheckConstraint("(station_id IS NULL) != (reach_id IS NULL)", name="ck_one_entity_ts"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
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
    aggregation: Mapped[str | None] = mapped_column(String)
    is_provisional: Mapped[bool] = mapped_column(Boolean, default=False)
    is_estimated: Mapped[bool] = mapped_column(Boolean, default=False)
    is_missing: Mapped[bool] = mapped_column(Boolean, default=False)
    is_forecast: Mapped[bool] = mapped_column(Boolean, default=False)
    is_flagged: Mapped[bool] = mapped_column(Boolean, default=False)
    provider_observation_id: Mapped[str | None] = mapped_column(String)
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    raw_payload: Mapped[dict] = mapped_column(JSONB)
