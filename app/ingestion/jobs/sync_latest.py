import logging

import httpx
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.adapters.geoglows import GeoglowsAdapter
from app.adapters.usgs import USGSAdapter
from app.core.ids import station_id
from app.core.time import utcnow
from app.db.models import Provider, Station
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
    # EA station detail can use a different notation than measure.stationReference.
    # Keep IDs aligned to the observation reference to satisfy FK upsert retry.
    station_payload["station_id"] = sid
    station_payload["provider_station_id"] = station_ref
    db.merge(
        Station(
            **station_payload,
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


async def run(db: Session) -> None:
    for adapter in [USGSAdapter(), EAEnglandAdapter(), GeoglowsAdapter()]:
        if not db.is_active:
            db.rollback()

        _ensure_provider(db, adapter.provider_id)

        with tracked_run(db, adapter.provider_id, "sync_latest") as run_state:
            try:
                records = await adapter.fetch_latest_observations()
            except (httpx.HTTPError, TimeoutError) as exc:
                run_state.records_failed += 1
                run_state.error_summary = f"fetch_latest_observations failed: {exc!r}"[:4000]
                logger.warning("latest sync fetch failed for %s: %s", adapter.provider_id, exc)
                continue

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
                            if (
                                adapter.provider_id == "ea_england"
                                and "entity missing for observation" in str(exc)
                                and await _enrich_ea_station_if_missing(db, adapter, raw)
                            ):
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

    db.commit()
