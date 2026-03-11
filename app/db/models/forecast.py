from datetime import date, datetime

from sqlalchemy import BigInteger, Date, DateTime, Float, Index, PrimaryKeyConstraint, SmallInteger, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ForecastReach(Base):
    __tablename__ = "forecast_reaches"
    __table_args__ = (
        PrimaryKeyConstraint("model", "reach_id", name="pk_forecast_reaches"),
        Index("forecast_reaches_model_lat_lon_idx", "model", "lat", "lon"),
    )

    model: Mapped[str] = mapped_column(String, nullable=False)
    reach_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    lon: Mapped[float | None] = mapped_column(Float)
    lat: Mapped[float | None] = mapped_column(Float)
    uparea: Mapped[float | None] = mapped_column(Float)
    rp2: Mapped[float | None] = mapped_column(Float)
    rp5: Mapped[float | None] = mapped_column(Float)
    rp10: Mapped[float | None] = mapped_column(Float)
    rp25: Mapped[float | None] = mapped_column(Float)
    rp50: Mapped[float | None] = mapped_column(Float)
    rp100: Mapped[float | None] = mapped_column(Float)
    source_metadata: Mapped[dict | None] = mapped_column(JSONB)


class ForecastRun(Base):
    __tablename__ = "forecast_runs"
    __table_args__ = (PrimaryKeyConstraint("model", "forecast_date", name="pk_forecast_runs"),)

    model: Mapped[str] = mapped_column(String, nullable=False)
    forecast_date: Mapped[date] = mapped_column(Date, nullable=False)
    run_issued_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    timestep_count: Mapped[int | None] = mapped_column(SmallInteger)
    timestep_hours: Mapped[int | None] = mapped_column(SmallInteger)
    timesteps_json: Mapped[dict | list | None] = mapped_column(JSONB)
    source_path: Mapped[str | None] = mapped_column(Text)
    source_metadata: Mapped[dict | None] = mapped_column(JSONB)


class ForecastReachRisk(Base):
    __tablename__ = "forecast_reach_risk"
    __table_args__ = (
        PrimaryKeyConstraint("model", "forecast_date", "reach_id", name="pk_forecast_reach_risk"),
        Index("forecast_reach_risk_lookup_idx", "model", "forecast_date", "risk_class"),
    )

    model: Mapped[str] = mapped_column(String, nullable=False)
    forecast_date: Mapped[date] = mapped_column(Date, nullable=False)
    reach_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    risk_class: Mapped[int] = mapped_column(SmallInteger, nullable=False)

    max_prob_rp2_24h: Mapped[float | None] = mapped_column(Float)
    max_prob_rp5_24h: Mapped[float | None] = mapped_column(Float)
    max_prob_rp10_24h: Mapped[float | None] = mapped_column(Float)

    max_prob_rp2_72h: Mapped[float | None] = mapped_column(Float)
    max_prob_rp5_72h: Mapped[float | None] = mapped_column(Float)
    max_prob_rp10_72h: Mapped[float | None] = mapped_column(Float)

    max_prob_rp2_7d: Mapped[float | None] = mapped_column(Float)
    max_prob_rp5_7d: Mapped[float | None] = mapped_column(Float)
    max_prob_rp10_7d: Mapped[float | None] = mapped_column(Float)

    peak_median_flow: Mapped[float | None] = mapped_column(Float)
    peak_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    source_metadata: Mapped[dict | None] = mapped_column(JSONB)


class ForecastReachDetail(Base):
    __tablename__ = "forecast_reach_detail"
    __table_args__ = (
        PrimaryKeyConstraint(
            "model",
            "forecast_date",
            "reach_id",
            "timestep_idx",
            name="pk_forecast_reach_detail",
        ),
        Index("forecast_reach_detail_lookup_idx", "model", "forecast_date", "reach_id"),
    )

    model: Mapped[str] = mapped_column(String, nullable=False)
    forecast_date: Mapped[date] = mapped_column(Date, nullable=False)
    reach_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    timestep_idx: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    valid_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    flow_median: Mapped[float | None] = mapped_column(Float)
    prob_exceed_rp2: Mapped[float | None] = mapped_column(Float)
    prob_exceed_rp5: Mapped[float | None] = mapped_column(Float)
    prob_exceed_rp10: Mapped[float | None] = mapped_column(Float)
    source_metadata: Mapped[dict | None] = mapped_column(JSONB)
