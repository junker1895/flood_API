from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.adapters.geoglows import GeoglowsAdapter
from app.adapters.usgs import USGSAdapter
from app.db.models import Reach, Station


async def run(db: Session) -> None:
    for adapter in [USGSAdapter(), EAEnglandAdapter()]:
        for raw in await adapter.fetch_station_catalog():
            st = adapter.normalize_station(raw)
            db.merge(Station(**st.model_dump(), normalization_version="v1"))
    g = GeoglowsAdapter()
    for raw in await g.fetch_reach_catalog():
        reach = g.normalize_reach(raw)
        db.merge(Reach(**reach.model_dump(), normalization_version="v1"))
    db.commit()
