from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.adapters.geoglows import GeoglowsAdapter
from app.adapters.usgs import USGSAdapter
from app.services.ingestion_service import upsert_latest_and_append_ts


async def run(db: Session) -> None:
    usgs = USGSAdapter()
    for s in await usgs.fetch_latest_observations():
        for obs in usgs.normalize_observation(s):
            upsert_latest_and_append_ts(db, obs)

    ea = EAEnglandAdapter()
    for raw in await ea.fetch_latest_observations():
        upsert_latest_and_append_ts(db, ea.normalize_observation(raw))

    g = GeoglowsAdapter()
    for raw in await g.fetch_latest_observations():
        upsert_latest_and_append_ts(db, g.normalize_observation(raw))

    db.commit()
