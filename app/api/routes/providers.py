from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas.common import ListEnvelope, Meta, SingleEnvelope
from app.api.schemas.providers import ProviderOut
from app.services.provider_service import get_provider, list_providers

router = APIRouter(prefix="/providers", tags=["providers"])


@router.get("", response_model=ListEnvelope[ProviderOut])
def providers(db: Session = Depends(get_db)):
    items = [ProviderOut.model_validate(p, from_attributes=True) for p in list_providers(db)]
    return {"data": items, "meta": Meta(count=len(items), next_cursor=None)}


@router.get("/{provider_id}", response_model=SingleEnvelope[ProviderOut])
def provider(provider_id: str, db: Session = Depends(get_db)):
    item = get_provider(db, provider_id)
    if not item:
        raise HTTPException(404, "provider not found")
    return {"data": ProviderOut.model_validate(item, from_attributes=True)}
