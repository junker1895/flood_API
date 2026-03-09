from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas.common import ListEnvelope, Meta
from app.api.schemas.warnings import WarningOut
from app.services.api_utils import parse_bbox, parse_geojson
from app.services.warning_service import active_warnings, list_warnings

router = APIRouter(prefix="/warnings", tags=["warnings"])


def _to_out(row) -> WarningOut:
    warning, geometry_geojson = row
    payload = WarningOut.model_validate(warning, from_attributes=True).model_dump()
    payload["geometry"] = parse_geojson(geometry_geojson)
    return WarningOut(**payload)


@router.get("", response_model=ListEnvelope[WarningOut])
def warnings(bbox: str | None = None, db: Session = Depends(get_db)):
    items = [_to_out(w) for w in list_warnings(db, bbox=parse_bbox(bbox))]
    return {"data": items, "meta": Meta(count=len(items))}


@router.get("/active", response_model=ListEnvelope[WarningOut])
def warnings_active(bbox: str | None = None, db: Session = Depends(get_db)):
    items = [_to_out(w) for w in active_warnings(db, bbox=parse_bbox(bbox))]
    return {"data": items, "meta": Meta(count=len(items))}
