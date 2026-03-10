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

## Roadmap
- See `PROJECT_ROADMAP.md` for the phased implementation plan, conflict/dependency notes, and execution checklist.

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
3. Verify services:
   - `migrate` runs automatically before `api`/`worker` start.
   - API docs: http://localhost:8000/docs

4. (Optional) re-run migrations manually:
   ```bash
   docker compose exec api alembic upgrade head
   ```

## Run API / worker manually (without docker)
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[test]
cp .env.example .env
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


## USGS observed ingestion
- Station discovery now uses USGS Water Services `site` endpoint (`format=rdb`) with configurable selection filters.
- Latest and historical observed ingestion use USGS Water Services `iv` endpoint (`format=json`) across discovered station sets (not a single demo station).
- Supported normalized variables: `discharge` (USGS `00060`) and `stage` (USGS `00065`).
- Latest sync upserts `observation_latest` and appends to `observation_timeseries`; history sync appends true historical timeseries rows with idempotent conflict handling.
- Station metadata includes provider station code, state/country, timezone, observed properties, drainage area (km²), datum metadata, and raw payload traceability.

USGS configuration env vars:

```bash
# station selection
USGS_SITE_LIST=01646500,01651000     # optional explicit site list
USGS_STATE_CODES=24,51               # optional state filter
USGS_BBOX=-78.0,38.0,-76.0,40.0      # optional bbox filter (west,south,east,north)
USGS_DEFAULT_BBOX=-125,24,-66,50     # used when site/state/bbox are all unset

# variable selection
USGS_PARAMETER_CODES=00060,00065

# history window
USGS_HISTORY_LOOKBACK_DAYS=7
USGS_HISTORY_START=2024-01-01T00:00:00+00:00  # optional explicit start
USGS_HISTORY_END=2024-01-08T00:00:00+00:00    # optional explicit end

# transport
USGS_TIMEOUT_SECONDS=20
USGS_TRUST_ENV=false  # set true only if your runtime must use HTTP(S)_PROXY env vars
```

Notes/limitations:
- If no station selection filters are provided, the adapter falls back to `USGS_DEFAULT_BBOX` to satisfy NWIS site-query requirements and avoid a 400 response.
- Parameter mappings are currently limited to `00060` and `00065`; other USGS parameters are preserved in raw payload but skipped for normalized observations.

## Provider-level ingestion scheduling
- Scheduler dispatch is provider-scoped (`provider_id + job_type`) instead of one global job per job type.
- A provider must be enabled and the job must be both supported and enabled to be scheduled.
- Supported job types are explicit: `metadata`, `latest`, `history`, `thresholds`, `warnings`.
- Intervals are configured per provider per job.

Environment variable pattern:

```bash
PROVIDERS__USGS__ENABLED=true
PROVIDERS__USGS__JOBS__METADATA__INTERVAL_MINUTES=1440
PROVIDERS__USGS__JOBS__LATEST__INTERVAL_MINUTES=15
PROVIDERS__USGS__JOBS__HISTORY__INTERVAL_MINUTES=360
PROVIDERS__USGS__JOBS__WARNINGS__ENABLED=false

PROVIDERS__EA_ENGLAND__ENABLED=true
PROVIDERS__EA_ENGLAND__JOBS__LATEST__INTERVAL_MINUTES=15
PROVIDERS__EA_ENGLAND__JOBS__WARNINGS__INTERVAL_MINUTES=15
```

Optional policy hooks are available per provider/job:
- `PROVIDERS__<PROVIDER>__JOBS__<JOB>__TIMEOUT_SECONDS`
- `PROVIDERS__<PROVIDER>__JOBS__<JOB>__MAX_RETRIES`

Backwards compatibility is preserved for existing provider toggles and latest polling env vars:
- `ENABLE_PROVIDER_USGS`, `ENABLE_PROVIDER_EA`, `ENABLE_PROVIDER_GEOGLOWS`, `ENABLE_PROVIDER_WHOS`
- `USGS_POLL_MINUTES`, `EA_POLL_MINUTES`, `GEOGLOWS_POLL_MINUTES`

### Testing when running in Docker (recommended for this repo)
If you're running the stack with `docker compose`, run checks inside the `api` container so dependencies (including `pytest-asyncio`) match the app runtime.

1. Build and start containers:
   ```bash
   docker compose up --build -d
   ```
2. Apply migrations:
   ```bash
   docker compose exec api alembic upgrade head
   ```
3. Run the focused upgrade tests:
   ```bash
   docker compose exec api pytest -q app/tests/test_api_upgrades.py
   ```
4. Run the full suite:
   ```bash
   docker compose exec api pytest -q
   ```
5. Optional sanity compile check:
   ```bash
   docker compose exec api python -m compileall app
   ```

### Quick API smoke checks in Docker
After `docker compose up` and migrations:

```bash
curl "http://localhost:8000/v1/stations/map?bbox=-180,-90,180,90&limit=5"
curl "http://localhost:8000/v1/reaches/map?bbox=-180,-90,180,90&limit=5"
curl "http://localhost:8000/v1/warnings/active?bbox=-180,-90,180,90"
curl "http://localhost:8000/v1/thresholds?limit=5"
```

## Key API routes
- Providers: `GET /v1/providers`, `GET /v1/providers/{provider_id}`
- Stations: `GET /v1/stations`, `GET /v1/stations/latest`, `GET /v1/stations/{station_id}/timeseries`, `GET /v1/stations/{station_id}/thresholds`
- Reaches: `GET /v1/reaches`, `GET /v1/reaches/latest`, `GET /v1/reaches/{reach_id}/timeseries`, `GET /v1/reaches/{reach_id}/thresholds`
- Warnings: `GET /v1/warnings`, `GET /v1/warnings/active`
- Health: `GET /health/live`, `GET /health/ready`, `GET /health/providers`

## Useful query params
- `bbox=minLon,minLat,maxLon,maxLat` (stations, reaches, warnings, thresholds, and latest/map variants)
- `provider_id`, `country_code`, `ids=id1,id2`
- `property`, `updated_since`, `start`, `end`, `include_latest`, `latest_property`
- `limit`, `cursor`


## Troubleshooting
- If `docker compose exec api pytest` fails with `"pytest": executable file not found in $PATH`, rebuild the API image after this change (it now installs test extras):
  ```bash
  docker compose build api --no-cache
  docker compose up -d
  docker compose exec api pytest
  ```

- If `docker compose exec api alembic upgrade head` fails with `ModuleNotFoundError: No module named "app"`, rebuild images after pulling latest changes and retry:
  ```bash
  docker compose down
  docker compose up --build -d
  docker compose exec api alembic upgrade head
  ```

## Extending with new adapters
1. Add adapter module implementing `BaseAdapter` contract.
2. Return normalized Pydantic models (`NormalizedStation`, `NormalizedReach`, etc.).
3. Keep provider-specific mappings inside adapter.
4. Register adapter in ingestion jobs.
5. Add tests for mapping, units, and quality normalization.
