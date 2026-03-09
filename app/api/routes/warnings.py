from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas.common import ListEnvelope, Meta
from app.api.schemas.warnings import WarningOut
from app.services.warning_service import active_warnings, list_warnings

router = APIRouter(prefix="/warnings", tags=["warnings"])


@router.get("", response_model=ListEnvelope[WarningOut])
def warnings(db: Session = Depends(get_db)):
    items = [WarningOut.model_validate(w, from_attributes=True) for w in list_warnings(db)]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/active", response_model=ListEnvelope[WarningOut])
def warnings_active(db: Session = Depends(get_db)):
    items = [WarningOut.model_validate(w, from_attributes=True) for w in active_warnings(db)]
    return {"data": items, "meta": Meta(count=len(items))}
