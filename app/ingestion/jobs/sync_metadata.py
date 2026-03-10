import asyncio
import logging

import httpx
from geoalchemy2 import WKTElement
from sqlalchemy.exc import IntegrityError
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
        if existing is not None:
            continue

        db.add(build_provider(provider_id))
        try:
            db.flush()
            created += 1
        except IntegrityError:
            if hasattr(db, "rollback"):
                db.rollback()
            logger.info("provider already exists during ensure_providers: provider_id=%s", provider_id)
    return created


def _adapter_jobs(provider_id: str | None):
    adapters = [USGSAdapter(), EAEnglandAdapter()]
    if provider_id is None:
        return adapters
    return [adapter for adapter in adapters if adapter.provider_id == provider_id]


async def run(db: Session, provider_id: str | None = None) -> None:
    created = _ensure_providers(db)
    logger.info("sync_metadata providers ensured: created=%s", created)

    for adapter in _adapter_jobs(provider_id):
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
                    raw_meta = payload.get("raw_metadata") or {}
                    db.merge(
                        Station(
                            **payload,
                            provider_station_code=raw_meta.get("site_no") or payload.get("provider_station_id"),
                            river_name=raw_meta.get("river_name"),
                            country_code=raw_meta.get("country_code"),
                            admin1=raw_meta.get("admin1"),
                            timezone=raw_meta.get("timezone"),
                            observed_properties=raw_meta.get("observed_properties"),
                            canonical_primary_property=raw_meta.get("canonical_primary_property"),
                            flow_unit_native=raw_meta.get("flow_unit_native"),
                            stage_unit_native=raw_meta.get("stage_unit_native"),
                            flow_unit_canonical=raw_meta.get("flow_unit_canonical"),
                            stage_unit_canonical=raw_meta.get("stage_unit_canonical"),
                            drainage_area_km2=raw_meta.get("drainage_area_km2"),
                            datum_name=raw_meta.get("datum_name"),
                            datum_vertical_reference=raw_meta.get("datum_vertical_reference"),
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
                    station_ref = raw.get("notation") or raw.get("stationReference") or raw.get("id") or "unknown"
                    logger.warning(
                        "station normalization skipped for provider=%s station_ref=%s reason=%s",
                        adapter.provider_id,
                        station_ref,
                        exc,
                    )

            logger.info(
                "sync_metadata provider=%s seen=%s inserted=%s updated=%s failed=%s",
                adapter.provider_id,
                run_state.records_seen,
                run_state.records_inserted,
                run_state.records_updated,
                run_state.records_failed,
            )

    g = GeoglowsAdapter()
    if provider_id is not None and provider_id != g.provider_id:
        db.commit()
        logger.info("sync_metadata committed")
        return
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
