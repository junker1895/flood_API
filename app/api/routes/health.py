from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.db.models import IngestionRun, Provider

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live")
def live():
    return {"status": "ok"}


@router.get("/ready")
def ready(db: Session = Depends(get_db)):
    db.execute(select(1))
    return {"status": "ready"}


@router.get("/providers")
def providers_health(db: Session = Depends(get_db)):
    providers = db.scalars(select(Provider)).all()
    out = []
    for p in providers:
        last_run = db.scalar(
            select(IngestionRun).where(IngestionRun.provider_id == p.provider_id).order_by(IngestionRun.started_at.desc()).limit(1)
        )
        out.append(
            {
                "provider_id": p.provider_id,
                "status": p.status,
                "last_job_type": last_run.job_type if last_run else None,
                "last_status": last_run.status if last_run else None,
                "last_started_at": last_run.started_at if last_run else None,
            }
        )
    return {"status": "ok", "providers": out, "count": len(out)}
