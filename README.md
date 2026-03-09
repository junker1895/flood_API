# global-hydrology-feed

Production-minded backend platform for ingesting global hydrology data from multiple providers, normalizing it into a canonical schema, storing in PostgreSQL/PostGIS, and serving a FastAPI REST API.

## Scope and rules
- Backend-only (no dashboard/map frontend).
- Strict separation between:
  - **Observed stations** (`stations`, `entity_type=station`, `source_type=observed`)
  - **Modeled reaches** (`reaches`, `entity_type=reach`, `source_type=modeled`)
- Native and canonical values/units are both retained.
- Raw upstream payload is always preserved.

## Architecture
- `app/adapters/*`: provider-specific fetch + mapping logic.
- `app/core/*`: enums, IDs, units, quality normalization, validation, config.
- `app/db/models/*`: SQLAlchemy ORM models.
- `app/ingestion/jobs/*`: metadata/latest/history/threshold/warning jobs.
- `app/services/*`: DB query and ingestion helper services.
- `app/api/*`: FastAPI routes and schemas.
- `alembic/*`: migrations.

## Providers (initial)
- USGS: observed stations, latest/historical patterns.
- Environment Agency England: observed stations + warnings.
- GEOGLOWS: modeled reaches and modeled discharge observations.
- WHOS: metadata-discovery scaffold for future enrichment.

## Local setup
1. Copy env:
   ```bash
   cp .env.example .env
   ```
2. Build and start stack:
   ```bash
   docker compose up --build -d
   ```
3. Run migrations:
   ```bash
   docker compose exec api alembic upgrade head
   ```
4. API docs:
   - http://localhost:8000/docs

## Run API / worker manually (without docker)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[test]
alembic upgrade head
uvicorn app.api.main:app --reload
# and in another shell
python -m app.ingestion.runner
```

## Testing
```bash
pytest
python -m compileall app
```

## Key API routes
- Providers: `GET /v1/providers`, `GET /v1/providers/{provider_id}`
- Stations: `GET /v1/stations`, `GET /v1/stations/latest`, `GET /v1/stations/{station_id}/timeseries`, `GET /v1/stations/{station_id}/thresholds`
- Reaches: `GET /v1/reaches`, `GET /v1/reaches/latest`, `GET /v1/reaches/{reach_id}/timeseries`, `GET /v1/reaches/{reach_id}/thresholds`
- Warnings: `GET /v1/warnings`, `GET /v1/warnings/active`
- Health: `GET /health/live`, `GET /health/ready`, `GET /health/providers`

## Useful query params
- `bbox=minLon,minLat,maxLon,maxLat` (stations)
- `provider_id`, `country_code`, `ids=id1,id2`
- `property`, `updated_since`, `start`, `end`
- `limit`, `cursor`

## Extending with new adapters
1. Add adapter module implementing `BaseAdapter` contract.
2. Return normalized Pydantic models (`NormalizedStation`, `NormalizedReach`, etc.).
3. Keep provider-specific mappings inside adapter.
4. Register adapter in ingestion jobs.
5. Add tests for mapping, units, and quality normalization.
