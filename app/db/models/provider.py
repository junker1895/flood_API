from datetime import datetime

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Provider(Base):
    __tablename__ = "providers"

    provider_id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    provider_type: Mapped[str] = mapped_column(String, nullable=False)
    home_url: Mapped[str | None] = mapped_column(String)
    api_base_url: Mapped[str | None] = mapped_column(String)
    license_name: Mapped[str | None] = mapped_column(String)
    license_url: Mapped[str | None] = mapped_column(String)
    attribution_text: Mapped[str | None] = mapped_column(String)
    default_poll_interval_minutes: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String, default="active")
    auth_type: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
