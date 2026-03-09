from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import Provider


def list_providers(db: Session) -> list[Provider]:
    return list(db.scalars(select(Provider)).all())


def get_provider(db: Session, provider_id: str) -> Provider | None:
    return db.get(Provider, provider_id)
