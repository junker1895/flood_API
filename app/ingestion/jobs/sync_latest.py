import logging

import httpx
from geoalchemy2 import WKTElement
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.adapters.geoglows import GeoglowsAdapter
from app.adapters.usgs import USGSAdapter
from app.adapters.base import NormalizedObservation
from app.core.ids import station_id
from app.core.time import utcnow
from app.db.geometry import point_geom_from_latlon
from app.db.models import Provider, Reach, Station
from app.services.ingestion_service import tracked_run, upsert_latest_and_append_ts
from app.services.provider_registry import build_provider

logger = logging.getLogger(__name__)


def _ensure_provider(db: Session, provider_id: str) -> None:
    if db.get(Provider, provider_id) is None:
        db.add(build_provider(provider_id))
        db.flush()


async def _enrich_ea_station_if_missing(db: Session, adapter: EAEnglandAdapter, raw: dict) -> bool:
    station_ref = raw.get("stationReference")
    if not station_ref:
        return False

    sid = station_id(adapter.provider_id, station_ref)
    if db.get(Station, sid):
        return True

    try:
        station_raw = await adapter.fetch_station_by_reference(station_ref)
    except (httpx.HTTPError, TimeoutError) as exc:
        logger.warning("ea station enrichment fetch failed for %s: %s", station_ref, exc)
        return False

    if not station_raw:
        return False

    station = adapter.normalize_station(station_raw)
    now = utcnow()
    station_payload = station.model_dump()
    station_payload["station_id"] = sid
    station_payload["provider_station_id"] = station_ref
    db.merge(
        Station(
            **station_payload,
            geom=point_geom_from_latlon(station_payload.get("latitude"), station_payload.get("longitude")),
            observed_properties={"stage": True},
            canonical_primary_property="stage",
            stage_unit_native=raw.get("unitName"),
            stage_unit_canonical="m",
            first_seen_at=now,
            last_seen_at=now,
            last_metadata_refresh_at=now,
            normalization_version="v1",
        )
    )
    db.flush()
    return True


async def _enrich_usgs_station_if_missing(db: Session, adapter: USGSAdapter, obs: NormalizedObservation) -> bool:
    if not obs.station_id:
        return False

    if db.get(Station, obs.station_id):
        return True

    provider_station = obs.station_id.removeprefix(f"{adapter.provider_id}-")
    try:
        records = await adapter.fetch_station_catalog()
    except (httpx.HTTPError, TimeoutError) as exc:
        logger.warning("usgs station enrichment fetch failed for %s: %s", provider_station, exc)
        return False

    for raw in records:
        st = adapter.normalize_station(raw)
        if st.provider_station_id != provider_station:
            continue

        now = utcnow()
        payload = st.model_dump()
        payload["station_id"] = obs.station_id
        payload["provider_station_id"] = provider_station
        db.merge(
            Station(
                **payload,
                geom=point_geom_from_latlon(payload.get("latitude"), payload.get("longitude")),
                first_seen_at=now,
                last_seen_at=now,
                last_metadata_refresh_at=now,
                normalization_version="v1",
            )
        )
        db.flush()
        logger.info("usgs station enrichment created station_id=%s", obs.station_id)
        return True

    logger.info("usgs station enrichment found no matching station for station_id=%s", obs.station_id)
    return False


async def _enrich_geoglows_reach_if_missing(db: Session, adapter: GeoglowsAdapter, raw: dict, obs: NormalizedObservation) -> bool:
    if not obs.reach_id:
        return False
    if db.get(Reach, obs.reach_id):
        return True

    provider_reach = obs.reach_id.removeprefix(f"{adapter.provider_id}-")
    try:
        records = await adapter.fetch_reach_catalog()
    except (httpx.HTTPError, TimeoutError) as exc:
        logger.warning("geoglows reach enrichment fetch failed for %s: %s", provider_reach, exc)
        return False

    selected = None
    for record in records:
        if str(record.get("reach_id")) == provider_reach:
            selected = record
            break

    if selected is None and raw.get("reach_id") is not None:
        selected = {
            "reach_id": str(raw["reach_id"]),
            "lat": raw.get("lat"),
            "lon": raw.get("lon"),
            "river": raw.get("river", "Unknown"),
        }

    if selected is None:
        logger.info("geoglows reach enrichment found no match for reach_id=%s", obs.reach_id)
        return False

    reach = adapter.normalize_reach(selected)
    now = utcnow()
    payload = reach.model_dump()
    reach_geometry_wkt = payload.pop("geometry_wkt", None)
    reach_geom = WKTElement(reach_geometry_wkt, srid=4326) if reach_geometry_wkt else point_geom_from_latlon(
        payload.get("latitude"), payload.get("longitude")
    )
    db.merge(
        Reach(
            **payload,
            geom=reach_geom,
            first_seen_at=now,
            last_metadata_refresh_at=now,
            normalization_version="v1",
        )
    )
    db.flush()
    logger.info("geoglows reach enrichment created reach_id=%s", obs.reach_id)
    return True


def _adapters(provider_id: str | None):
    adapters = [USGSAdapter(), EAEnglandAdapter(), GeoglowsAdapter()]
    if provider_id is None:
        return adapters
    return [adapter for adapter in adapters if adapter.provider_id == provider_id]


async def run(db: Session, provider_id: str | None = None) -> None:
    for adapter in _adapters(provider_id):
        if not db.is_active:
            db.rollback()

        _ensure_provider(db, adapter.provider_id)

        with tracked_run(db, adapter.provider_id, "sync_latest") as run_state:
            try:
                records = await adapter.fetch_latest_observations()
                logger.info("sync_latest provider=%s fetched_records=%s", adapter.provider_id, len(records))
            except (httpx.HTTPError, TimeoutError) as exc:
                run_state.records_failed += 1
                run_state.error_summary = f"fetch_latest_observations failed: {exc!r}"[:4000]
                logger.warning("latest sync fetch failed for %s: %s", adapter.provider_id, exc)
                continue

            if not records:
                logger.info("sync_latest provider=%s fetched zero records", adapter.provider_id)

            for raw in records:
                try:
                    normalized = adapter.normalize_observation(raw)
                    observations = normalized if isinstance(normalized, list) else [normalized]
                    for obs in observations:
                        try:
                            with db.begin_nested():
                                ins, upd = upsert_latest_and_append_ts(db, obs)
                            run_state.records_seen += 1
                            run_state.records_inserted += ins
                            run_state.records_updated += upd
                        except ValueError as exc:
                            recovered = False
                            if adapter.provider_id == "ea_england" and "entity missing for observation" in str(exc):
                                recovered = await _enrich_ea_station_if_missing(db, adapter, raw)
                            elif adapter.provider_id == "usgs" and "entity missing for observation" in str(exc):
                                recovered = await _enrich_usgs_station_if_missing(db, adapter, obs)
                            elif adapter.provider_id == "geoglows" and "entity missing for observation" in str(exc):
                                recovered = await _enrich_geoglows_reach_if_missing(db, adapter, raw, obs)

                            if recovered:
                                try:
                                    with db.begin_nested():
                                        ins, upd = upsert_latest_and_append_ts(db, obs)
                                    run_state.records_seen += 1
                                    run_state.records_inserted += ins
                                    run_state.records_updated += upd
                                    continue
                                except (ValueError, SQLAlchemyError) as retry_exc:
                                    run_state.records_failed += 1
                                    run_state.error_summary = f"upsert_latest_and_append_ts retry failed: {retry_exc!r}"[:4000]
                                    logger.warning("latest upsert retry failed for %s: %s", adapter.provider_id, retry_exc)
                                    continue

                            run_state.records_failed += 1
                            run_state.error_summary = f"upsert_latest_and_append_ts failed: {exc!r}"[:4000]
                            logger.warning("latest upsert failed for %s: %s", adapter.provider_id, exc)
                            continue
                        except SQLAlchemyError as exc:
                            run_state.records_failed += 1
                            run_state.error_summary = f"upsert_latest_and_append_ts failed: {exc!r}"[:4000]
                            logger.warning("latest upsert failed for %s: %s", adapter.provider_id, exc)
                            continue
                except Exception as exc:
                    run_state.records_failed += 1
                    run_state.error_summary = f"normalize_observation failed: {exc!r}"[:4000]
                    logger.warning("latest normalization failed for %s: %s", adapter.provider_id, exc)

            logger.info(
                "sync_latest provider=%s seen=%s inserted=%s updated=%s failed=%s",
                adapter.provider_id,
                run_state.records_seen,
                run_state.records_inserted,
                run_state.records_updated,
                run_state.records_failed,
            )

    db.commit()
    logger.info("sync_latest committed")
