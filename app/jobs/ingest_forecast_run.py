from __future__ import annotations

import argparse
from datetime import date, datetime
import logging

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.core.config import settings
from app.db.models.forecast import ForecastReach, ForecastReachDetail, ForecastReachRisk, ForecastRun
from app.db.session import SessionLocal
from app.forecast_models import get_forecast_provider
from app.services.forecast_product_service import build_risk_row

logger = logging.getLogger(__name__)


def _csv_set(value: str) -> set[str]:
    return {v.strip() for v in value.split(",") if v.strip()}


def should_store_risk(*, risk_class: int, uparea: float, region_id: str, major_threshold: float, priority_regions: set[str]) -> bool:
    return risk_class > 0 or uparea >= major_threshold or region_id in priority_regions


def should_store_detail(*, risk_class: int, uparea: float, region_id: str, detail_threshold: float, detail_regions: set[str]) -> bool:
    return risk_class > 0 or uparea >= detail_threshold or region_id in detail_regions


def run(model: str, forecast_date: date | None) -> None:
    provider = get_forecast_provider(model)
    run_descriptor = provider.discover_run(forecast_date)

    major_regions = _csv_set(settings.forecast_priority_region_ids)
    detail_regions = _csv_set(settings.forecast_detail_region_ids)

    with SessionLocal() as db:
        run_stmt = insert(ForecastRun).values(
            {
                "model": model,
                "forecast_date": run_descriptor.forecast_date,
                "run_issued_at": run_descriptor.run_issued_at,
                "timestep_count": run_descriptor.timestep_count,
                "timestep_hours": run_descriptor.timestep_hours,
                "timesteps_json": run_descriptor.timesteps,
                "source_path": run_descriptor.source_path,
                "source_metadata": run_descriptor.source_metadata,
            }
        )
        run_stmt = run_stmt.on_conflict_do_update(
            index_elements=[ForecastRun.model, ForecastRun.forecast_date],
            set_={
                "run_issued_at": run_stmt.excluded.run_issued_at,
                "timestep_count": run_stmt.excluded.timestep_count,
                "timestep_hours": run_stmt.excluded.timestep_hours,
                "timesteps_json": run_stmt.excluded.timesteps_json,
                "source_path": run_stmt.excluded.source_path,
                "source_metadata": run_stmt.excluded.source_metadata,
            },
        )
        db.execute(run_stmt)
        db.commit()

        for chunk_idx, chunk in enumerate(provider.iter_run_forecast_chunks(run_descriptor), start=1):
            reach_ids = [int(row.get("reach_id") or row.get("river_id") or row.get("link_no") or row.get("LINKNO") or row.get("comid")) for row in chunk]
            meta_rows = db.execute(
                select(ForecastReach.reach_id, ForecastReach.uparea, ForecastReach.rp2, ForecastReach.rp5, ForecastReach.rp10, ForecastReach.source_metadata).where(
                    ForecastReach.model == model, ForecastReach.reach_id.in_(reach_ids)
                )
            ).all()
            meta_map = {int(r.reach_id): {"uparea": r.uparea, "rp2": r.rp2, "rp5": r.rp5, "rp10": r.rp10, "source_metadata": r.source_metadata or {}} for r in meta_rows}

            risk_rows = []
            detail_rows = []
            for row in chunk:
                reach_id = int(row.get("reach_id") or row.get("river_id") or row.get("link_no") or row.get("LINKNO") or row.get("comid"))
                timesteps = row.get("timesteps") or []
                meta = meta_map.get(reach_id, {})
                for ts in timesteps:
                    ts.setdefault("rp2", meta.get("rp2"))
                    ts.setdefault("rp5", meta.get("rp5"))
                    ts.setdefault("rp10", meta.get("rp10"))
                risk = build_risk_row(model, run_descriptor.forecast_date, reach_id, timesteps)
                uparea = meta.get("uparea") or 0
                region_id = str((meta.get("source_metadata") or {}).get("region_id") or "")

                store_risk = should_store_risk(
                    risk_class=risk["risk_class"],
                    uparea=uparea,
                    region_id=region_id,
                    major_threshold=settings.forecast_major_river_threshold,
                    priority_regions=major_regions,
                )
                if store_risk:
                    risk_rows.append(risk)

                store_detail = should_store_detail(
                    risk_class=risk["risk_class"],
                    uparea=uparea,
                    region_id=region_id,
                    detail_threshold=settings.forecast_detail_river_threshold,
                    detail_regions=detail_regions,
                )
                if store_detail:
                    for idx, ts in enumerate(timesteps):
                        detail_rows.append(
                            {
                                "model": model,
                                "forecast_date": run_descriptor.forecast_date,
                                "reach_id": reach_id,
                                "timestep_idx": idx,
                                "valid_time": ts.get("valid_time"),
                                "flow_median": ts.get("flow_median"),
                                "prob_exceed_rp2": ts.get("prob_exceed_rp2"),
                                "prob_exceed_rp5": ts.get("prob_exceed_rp5"),
                                "prob_exceed_rp10": ts.get("prob_exceed_rp10"),
                                "source_metadata": None,
                            }
                        )

            if risk_rows:
                risk_stmt = insert(ForecastReachRisk).values(risk_rows)
                risk_stmt = risk_stmt.on_conflict_do_update(
                    index_elements=[ForecastReachRisk.model, ForecastReachRisk.forecast_date, ForecastReachRisk.reach_id],
                    set_={c.name: getattr(risk_stmt.excluded, c.name) for c in ForecastReachRisk.__table__.columns if c.name not in {"model", "forecast_date", "reach_id"}},
                )
                db.execute(risk_stmt)

            if detail_rows:
                detail_stmt = insert(ForecastReachDetail).values(detail_rows)
                detail_stmt = detail_stmt.on_conflict_do_update(
                    index_elements=[
                        ForecastReachDetail.model,
                        ForecastReachDetail.forecast_date,
                        ForecastReachDetail.reach_id,
                        ForecastReachDetail.timestep_idx,
                    ],
                    set_={c.name: getattr(detail_stmt.excluded, c.name) for c in ForecastReachDetail.__table__.columns if c.name not in {"model", "forecast_date", "reach_id", "timestep_idx"}},
                )
                db.execute(detail_stmt)

            db.commit()
            logger.info("forecast run ingest model=%s forecast_date=%s chunk=%s risk_rows=%s detail_rows=%s", model, run_descriptor.forecast_date, chunk_idx, len(risk_rows), len(detail_rows))


def _parse_forecast_date(value: str | None) -> date | None:
    if not value:
        return None
    raw = value.strip()
    if len(raw) == 8 and raw.isdigit():
        return datetime.strptime(raw, "%Y%m%d").date()
    return date.fromisoformat(raw)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--forecast-date")
    args = parser.parse_args()
    date_value = _parse_forecast_date(args.forecast_date)
    run(args.model, date_value)


if __name__ == "__main__":
    main()
