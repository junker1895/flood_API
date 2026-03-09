from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.schemas.common import ListEnvelope, Meta
from app.api.schemas.thresholds import ThresholdOut
from app.core.config import settings
from app.services.api_utils import parse_bbox
from app.services.threshold_service import list_thresholds

router = APIRouter(prefix="/thresholds", tags=["thresholds"])


@router.get("", response_model=ListEnvelope[ThresholdOut])
def thresholds(
    station_id: str | None = None,
    reach_id: str | None = None,
    property: str | None = None,
    provider_id: str | None = None,
    bbox: str | None = None,
    limit: int = Query(settings.default_limit, ge=1, le=settings.max_limit),
    cursor: str | None = None,
    db: Session = Depends(get_db),
):
    rows = list_thresholds(
        db,
        station_id=station_id,
        reach_id=reach_id,
        property_name=property,
        provider_id=provider_id,
        bbox=parse_bbox(bbox),
        limit=limit,
        cursor=cursor,
    )
    items = [ThresholdOut.model_validate(r, from_attributes=True) for r in rows]
    next_cursor = items[-1].threshold_id if len(items) == limit else None
    return {"data": items, "meta": Meta(count=len(items), next_cursor=next_cursor)}
