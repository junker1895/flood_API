from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import DateTime, Float, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Reach(Base):
    __tablename__ = "reaches"
    __table_args__ = (UniqueConstraint("provider_id", "provider_reach_id", name="uq_reach_provider"),)

    reach_id: Mapped[str] = mapped_column(String, primary_key=True)
    provider_id: Mapped[str] = mapped_column(ForeignKey("providers.provider_id"), nullable=False)
    source_type: Mapped[str] = mapped_column(String, nullable=False)
    provider_reach_id: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    river_name: Mapped[str | None] = mapped_column(String)
    country_code: Mapped[str | None] = mapped_column(String)
    latitude: Mapped[float | None] = mapped_column(Float)
    longitude: Mapped[float | None] = mapped_column(Float)
    network_name: Mapped[str | None] = mapped_column(String)
    geometry_type: Mapped[str | None] = mapped_column(String)
    geom = mapped_column(Geometry("GEOMETRY", srid=4326, spatial_index=True), nullable=True)
    raw_metadata: Mapped[dict] = mapped_column(JSONB)
    normalization_version: Mapped[str] = mapped_column(String)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_metadata_refresh_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
