from __future__ import annotations

import argparse
import logging

from sqlalchemy.dialects.postgresql import insert

from app.db.models.forecast import ForecastReach
from app.db.session import SessionLocal
from app.forecast_models import get_forecast_provider

logger = logging.getLogger(__name__)


def run(model: str) -> None:
    provider = get_forecast_provider(model)
    seen = 0
    with SessionLocal() as db:
        for chunk_idx, chunk in enumerate(provider.iter_reach_metadata_chunks(), start=1):
            if not chunk:
                continue
            stmt = insert(ForecastReach).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=[ForecastReach.model, ForecastReach.reach_id],
                set_={
                    "lon": stmt.excluded.lon,
                    "lat": stmt.excluded.lat,
                    "uparea": stmt.excluded.uparea,
                    "rp2": stmt.excluded.rp2,
                    "rp5": stmt.excluded.rp5,
                    "rp10": stmt.excluded.rp10,
                    "rp25": stmt.excluded.rp25,
                    "rp50": stmt.excluded.rp50,
                    "rp100": stmt.excluded.rp100,
                    "source_metadata": stmt.excluded.source_metadata,
                },
            )
            db.execute(stmt)
            db.commit()
            seen += len(chunk)
            logger.info("forecast reach ingest model=%s chunk=%s rows=%s total=%s", model, chunk_idx, len(chunk), seen)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    args = parser.parse_args()
    run(args.model)


if __name__ == "__main__":
    main()
