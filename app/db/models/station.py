from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import DateTime, Float, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Station(Base):
    __tablename__ = "stations"
    __table_args__ = (UniqueConstraint("provider_id", "provider_station_id", name="uq_station_provider"),)

    station_id: Mapped[str] = mapped_column(String, primary_key=True)
    provider_id: Mapped[str] = mapped_column(ForeignKey("providers.provider_id"), nullable=False)
    source_type: Mapped[str] = mapped_column(String, nullable=False)
    provider_station_id: Mapped[str] = mapped_column(String, nullable=False)
    provider_station_code: Mapped[str | None] = mapped_column(String)
    name: Mapped[str] = mapped_column(String)
    river_name: Mapped[str | None] = mapped_column(String)
    waterbody_type: Mapped[str] = mapped_column(String, default="unknown")
    country_code: Mapped[str | None] = mapped_column(String)
    admin1: Mapped[str | None] = mapped_column(String)
    admin2: Mapped[str | None] = mapped_column(String)
    latitude: Mapped[float] = mapped_column(Float)
    longitude: Mapped[float] = mapped_column(Float)
    elevation_m: Mapped[float | None] = mapped_column(Float)
    timezone: Mapped[str | None] = mapped_column(String)
    station_status: Mapped[str | None] = mapped_column(String)
    observed_properties: Mapped[dict | None] = mapped_column(JSONB)
    canonical_primary_property: Mapped[str | None] = mapped_column(String)
    flow_unit_native: Mapped[str | None] = mapped_column(String)
    stage_unit_native: Mapped[str | None] = mapped_column(String)
    flow_unit_canonical: Mapped[str | None] = mapped_column(String)
    stage_unit_canonical: Mapped[str | None] = mapped_column(String)
    drainage_area_km2: Mapped[float | None] = mapped_column(Float)
    datum_name: Mapped[str | None] = mapped_column(String)
    datum_vertical_reference: Mapped[str | None] = mapped_column(String)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_metadata_refresh_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    geom = mapped_column(Geometry("POINT", srid=4326, spatial_index=True))
    raw_metadata: Mapped[dict] = mapped_column(JSONB)
    normalization_version: Mapped[str] = mapped_column(String)
