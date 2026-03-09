# global-hydrology-feed

Backend-only hydrology feed platform that ingests observed station data and modeled reach data, normalizes it, stores raw + canonical values, and serves a FastAPI API.

## Features
- Strict observed (`stations`) vs modeled (`reaches`) separation
- Adapter architecture (USGS, EA England, GEOGLOWS, WHOS scaffold)
- Normalized observations with native and canonical units
- Latest + timeseries ingestion path
- PostgreSQL/PostGIS SQLAlchemy models and Alembic migration
- FastAPI REST API under `/v1`
- APScheduler worker for ingestion jobs
- Docker compose for local stack

## Quickstart
1. Copy `.env.example` to `.env`
2. `docker compose up --build`
3. Run migrations: `alembic upgrade head`
4. Open docs at `http://localhost:8000/docs`

## API
- Providers: `/v1/providers`
- Stations: `/v1/stations`, `/v1/stations/latest`
- Reaches: `/v1/reaches`, `/v1/reaches/latest`
- Warnings: `/v1/warnings`, `/v1/warnings/active`
- Health: `/health/live`, `/health/ready`, `/health/providers`

## Extending adapters
Implement a new adapter subclassing `BaseAdapter`, keep provider-specific mapping inside adapter methods, return normalized Pydantic models, then wire into ingestion jobs.
