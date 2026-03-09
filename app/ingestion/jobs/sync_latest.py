from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.adapters.geoglows import GeoglowsAdapter
from app.adapters.usgs import USGSAdapter
from app.services.ingestion_service import tracked_run, upsert_latest_and_append_ts


async def run(db: Session) -> None:
    total_seen = total_inserted = total_updated = 0

    usgs = USGSAdapter()
    with tracked_run(db, usgs.provider_id, "sync_latest") as run_state:
        for s in await usgs.fetch_latest_observations():
            for obs in usgs.normalize_observation(s):
                total_seen += 1
                ins, upd = upsert_latest_and_append_ts(db, obs)
                total_inserted += ins
                total_updated += upd
        run_state.records_seen = total_seen
        run_state.records_inserted = total_inserted
        run_state.records_updated = total_updated

    ea = EAEnglandAdapter()
    with tracked_run(db, ea.provider_id, "sync_latest") as run_state:
        for raw in await ea.fetch_latest_observations():
            total_seen += 1
            ins, upd = upsert_latest_and_append_ts(db, ea.normalize_observation(raw))
            total_inserted += ins
            total_updated += upd
        run_state.records_seen = total_seen
        run_state.records_inserted = total_inserted
        run_state.records_updated = total_updated

    g = GeoglowsAdapter()
    with tracked_run(db, g.provider_id, "sync_latest") as run_state:
        for raw in await g.fetch_latest_observations():
            total_seen += 1
            ins, upd = upsert_latest_and_append_ts(db, g.normalize_observation(raw))
            total_inserted += ins
            total_updated += upd
        run_state.records_seen = total_seen
        run_state.records_inserted = total_inserted
        run_state.records_updated = total_updated

    db.commit()
