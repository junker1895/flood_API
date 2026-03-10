import logging

import httpx
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.adapters.geoglows import GeoglowsAdapter
from app.adapters.usgs import USGSAdapter
from app.ingestion.jobs.sync_latest import _enrich_usgs_station_if_missing
from app.services.ingestion_service import append_timeseries, tracked_run

logger = logging.getLogger(__name__)


def _adapters(provider_id: str | None):
    adapters = [USGSAdapter(), EAEnglandAdapter(), GeoglowsAdapter()]
    if provider_id is None:
        return adapters
    return [adapter for adapter in adapters if adapter.provider_id == provider_id]


async def run(db: Session, provider_id: str | None = None) -> None:
    for adapter in _adapters(provider_id):
        if not adapter.supports_history:
            continue

        with tracked_run(db, adapter.provider_id, "sync_history") as run_state:
            try:
                records = await adapter.fetch_historical_timeseries()
                logger.info("sync_history provider=%s fetched_records=%s", adapter.provider_id, len(records))
            except (httpx.HTTPError, TimeoutError) as exc:
                run_state.records_failed += 1
                run_state.error_summary = f"fetch_historical_timeseries failed: {exc!r}"[:4000]
                logger.warning("history sync fetch failed for %s: %s", adapter.provider_id, exc)
                continue

            for raw in records:
                try:
                    observations = adapter.normalize_observation(raw)
                    for obs in observations:
                        try:
                            inserted = append_timeseries(db, obs)
                            run_state.records_seen += 1
                            run_state.records_inserted += inserted
                        except ValueError as exc:
                            recovered = False
                            if adapter.provider_id == "usgs" and "entity missing for observation" in str(exc):
                                recovered = await _enrich_usgs_station_if_missing(db, adapter, obs)
                            if recovered:
                                try:
                                    inserted = append_timeseries(db, obs)
                                    run_state.records_seen += 1
                                    run_state.records_inserted += inserted
                                    continue
                                except (ValueError, SQLAlchemyError) as retry_exc:
                                    run_state.records_failed += 1
                                    run_state.error_summary = f"append_timeseries retry failed: {retry_exc!r}"[:4000]
                                    logger.warning("history append retry failed for %s: %s", adapter.provider_id, retry_exc)
                                    continue

                            run_state.records_failed += 1
                            run_state.error_summary = f"append_timeseries failed: {exc!r}"[:4000]
                            logger.warning("history append failed for %s: %s", adapter.provider_id, exc)
                        except SQLAlchemyError as exc:
                            run_state.records_failed += 1
                            run_state.error_summary = f"append_timeseries failed: {exc!r}"[:4000]
                            logger.warning("history append failed for %s: %s", adapter.provider_id, exc)
                except Exception as exc:
                    run_state.records_failed += 1
                    run_state.error_summary = f"normalize_observation failed: {exc!r}"[:4000]
                    logger.warning("history normalization failed for %s: %s", adapter.provider_id, exc)

            logger.info(
                "sync_history provider=%s seen=%s inserted=%s updated=%s failed=%s",
                adapter.provider_id,
                run_state.records_seen,
                run_state.records_inserted,
                run_state.records_updated,
                run_state.records_failed,
            )

    db.commit()
    logger.info("sync_history committed")
