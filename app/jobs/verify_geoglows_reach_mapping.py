from __future__ import annotations

import argparse

from sqlalchemy import select

from app.db.models.forecast import ForecastReach
from app.db.models.reach import Reach
from app.db.session import SessionLocal


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify GEOGLOWS reach IDs against existing frontend reach IDs.")
    parser.add_argument("--sample-size", type=int, default=100)
    args = parser.parse_args()

    with SessionLocal() as db:
        geoglows_ids = db.execute(
            select(ForecastReach.reach_id).where(ForecastReach.model == "geoglows").limit(args.sample_size)
        ).scalars().all()
        frontend_ids = set(db.execute(select(Reach.reach_id)).scalars().all())

    overlap = sum(1 for rid in geoglows_ids if str(rid) in frontend_ids)
    print("GEOGLOWS v2 identifier is LINKNO/COMID-style 9-digit river ID.")
    print(f"sample_size={len(geoglows_ids)} overlap_with_frontend_reach_ids={overlap}")
    if geoglows_ids:
        print("example_geoglows_ids=", ",".join(str(v) for v in geoglows_ids[:10]))


if __name__ == "__main__":
    main()
