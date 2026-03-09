from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.adapters.geoglows import GeoglowsAdapter
from app.adapters.usgs import USGSAdapter
from app.db.models import Provider, Reach, Station
from app.services.ingestion_service import tracked_run
from app.services.provider_registry import build_provider




def _ensure_providers(db: Session) -> None:
    for provider_id in ["usgs", "ea_england", "geoglows", "whos"]:
        existing = db.get(Provider, provider_id)
        if existing is None:
            db.add(build_provider(provider_id))

async def run(db: Session) -> None:
    _ensure_providers(db)
    for adapter in [USGSAdapter(), EAEnglandAdapter()]:
        with tracked_run(db, adapter.provider_id, "sync_metadata") as run_state:
            for raw in await adapter.fetch_station_catalog():
                st = adapter.normalize_station(raw)
                db.merge(Station(**st.model_dump(), normalization_version="v1"))
                run_state.records_seen += 1
                run_state.records_updated += 1

    g = GeoglowsAdapter()
    with tracked_run(db, g.provider_id, "sync_metadata") as run_state:
        for raw in await g.fetch_reach_catalog():
            reach = g.normalize_reach(raw)
            db.merge(Reach(**reach.model_dump(), normalization_version="v1"))
            run_state.records_seen += 1
            run_state.records_updated += 1

    db.commit()
