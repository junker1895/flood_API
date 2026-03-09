import logging

import httpx
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.adapters.ea_england import EAEnglandAdapter
from app.adapters.geoglows import GeoglowsAdapter
from app.adapters.usgs import USGSAdapter
from app.services.ingestion_service import tracked_run, upsert_latest_and_append_ts

logger = logging.getLogger(__name__)


async def run(db: Session) -> None:
    for adapter in [USGSAdapter(), EAEnglandAdapter(), GeoglowsAdapter()]:
        if not db.is_active:
            db.rollback()

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
                        except (ValueError, SQLAlchemyError) as exc:
                            run_state.records_failed += 1
                            run_state.error_summary = f"upsert_latest_and_append_ts failed: {exc!r}"[:4000]
                            logger.warning("latest upsert failed for %s: %s", adapter.provider_id, exc)
                            continue
                except Exception as exc:
                    run_state.records_failed += 1
                    run_state.error_summary = f"normalize_observation failed: {exc!r}"[:4000]
                    logger.warning("latest normalization failed for %s: %s", adapter.provider_id, exc)

    db.commit()
