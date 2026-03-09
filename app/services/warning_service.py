from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import WarningEvent


def list_warnings(db: Session) -> list[WarningEvent]:
    return list(db.scalars(select(WarningEvent)).all())


def active_warnings(db: Session) -> list[WarningEvent]:
    return list(db.scalars(select(WarningEvent).where(WarningEvent.status != "inactive")).all())
