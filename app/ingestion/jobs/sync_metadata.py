import asyncio
import logging

import httpx
from geoalchemy2 import WKTElement
from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.adapters.geoglows import GeoglowsAdapter
from app.adapters.usgs import USGSAdapter
from app.core.time import utcnow
from app.db.geometry import point_geom_from_latlon
from app.db.models import Provider, Reach, Station
from app.db.session import SessionLocal
from app.services.ingestion_service import tracked_run
from app.services.provider_registry import build_provider

logger = logging.getLogger(__name__)


def _ensure_providers(db: Session) -> int:
    created = 0
    for provider_id in ["usgs", "ea_england", "geoglows", "whos"]:
        existing = db.get(Provider, provider_id)
        if existing is None:
            db.add(build_provider(provider_id))
            created += 1
    if created:
        db.flush()
    return created


async def run(db: Session) -> None:
    created = _ensure_providers(db)
    logger.info("sync_metadata providers ensured: created=%s", created)

    for adapter in [USGSAdapter(), EAEnglandAdapter()]:
        with tracked_run(db, adapter.provider_id, "sync_metadata") as run_state:
            try:
                records = await adapter.fetch_station_catalog()
                logger.info("sync_metadata provider=%s fetched_station_records=%s", adapter.provider_id, len(records))
            except (httpx.HTTPError, TimeoutError) as exc:
                run_state.records_failed += 1
                run_state.error_summary = f"fetch_station_catalog failed: {exc!r}"[:4000]
                logger.warning("metadata sync failed for %s: %s", adapter.provider_id, exc)
                continue

            if not records:
                logger.info("sync_metadata provider=%s fetched zero station records", adapter.provider_id)

            for raw in records:
                try:
                    st = adapter.normalize_station(raw)
                    payload = st.model_dump()
                    now = utcnow()
                    station_geom = point_geom_from_latlon(payload.get("latitude"), payload.get("longitude"))
                    db.merge(
                        Station(
                            **payload,
                            geom=station_geom,
                            first_seen_at=now,
                            last_seen_at=now,
                            last_metadata_refresh_at=now,
                            normalization_version="v1",
                        )
                    )
                    run_state.records_seen += 1
                    run_state.records_updated += 1
                except Exception as exc:  # keep run resilient to malformed records
                    run_state.records_failed += 1
                    run_state.error_summary = f"normalize_station failed: {exc!r}"[:4000]
                    logger.warning("station normalization failed for %s: %s", adapter.provider_id, exc)

            logger.info(
                "sync_metadata provider=%s seen=%s inserted=%s updated=%s failed=%s",
                adapter.provider_id,
                run_state.records_seen,
                run_state.records_inserted,
                run_state.records_updated,
                run_state.records_failed,
            )

    g = GeoglowsAdapter()
    with tracked_run(db, g.provider_id, "sync_metadata") as run_state:
        try:
            records = await g.fetch_reach_catalog()
            logger.info("sync_metadata provider=%s fetched_reach_records=%s", g.provider_id, len(records))
        except (httpx.HTTPError, TimeoutError) as exc:
            run_state.records_failed += 1
            run_state.error_summary = f"fetch_reach_catalog failed: {exc!r}"[:4000]
            logger.warning("metadata sync failed for %s: %s", g.provider_id, exc)
            db.commit()
            return

        if not records:
            logger.info("sync_metadata provider=%s fetched zero reach records", g.provider_id)

        for raw in records:
            try:
                reach = g.normalize_reach(raw)
                payload = reach.model_dump()
                reach_geometry_wkt = payload.pop("geometry_wkt", None)
                reach_geom = WKTElement(reach_geometry_wkt, srid=4326) if reach_geometry_wkt else point_geom_from_latlon(
                    payload.get("latitude"), payload.get("longitude")
                )
                now = utcnow()
                db.merge(
                    Reach(
                        **payload,
                        geom=reach_geom,
                        first_seen_at=now,
                        last_metadata_refresh_at=now,
                        normalization_version="v1",
                    )
                )
                run_state.records_seen += 1
                run_state.records_updated += 1
            except Exception as exc:
                run_state.records_failed += 1
                run_state.error_summary = f"normalize_reach failed: {exc!r}"[:4000]
                logger.warning("reach normalization failed for %s: %s", g.provider_id, exc)

        logger.info(
            "sync_metadata provider=%s seen=%s inserted=%s updated=%s failed=%s",
            g.provider_id,
            run_state.records_seen,
            run_state.records_inserted,
            run_state.records_updated,
            run_state.records_failed,
        )

    db.commit()
    logger.info("sync_metadata committed")


def main() -> None:
    with SessionLocal() as db:
        asyncio.run(run(db))


if __name__ == "__main__":
    main()
