# FloodPulse Backend Integration Specification

This specification is extracted from the repository implementation (models, migrations, routes, services, adapters, and ingestion jobs). It is intentionally implementation-accurate and calls out unimplemented or absent features explicitly.

## SECTION 1 — DATABASE SCHEMA

Source of truth: `alembic/versions/0001_init.py` plus ORM models under `app/db/models`.

### Global schema characteristics
- PostGIS extension is enabled in migration: `CREATE EXTENSION IF NOT EXISTS postgis`.
- Spatial SRID used in all geometry columns: **4326**.
- Spatial indexes:
  - `ix_stations_geom` (GIST on `stations.geom`)
  - `ix_reaches_geom` (GIST on `reaches.geom`)
  - `ix_warning_geometry` (GIST on `warning_events.geometry`)

---

### `providers`
- `provider_id` `String` PK, nullable: no
- `name` `String`, nullable: no
- `provider_type` `String`, nullable: no
- `home_url` `String`, nullable: yes
- `api_base_url` `String`, nullable: yes
- `license_name` `String`, nullable: yes
- `license_url` `String`, nullable: yes
- `attribution_text` `String`, nullable: yes
- `default_poll_interval_minutes` `Integer`, nullable: yes
- `status` `String`, nullable: no
- `auth_type` `String`, nullable: yes
- `created_at` `DateTime(timezone=True)`, nullable: yes
- `updated_at` `DateTime(timezone=True)`, nullable: yes

Indexes/constraints/FKs:
- Primary key: `provider_id`
- No additional indexes/unique constraints/FKs.

---

### `stations`
- `station_id` `String` PK, nullable: no
- `provider_id` `String`, FK -> `providers.provider_id`, nullable: no
- `source_type` `String`, nullable: no
- `provider_station_id` `String`, nullable: no
- `provider_station_code` `String`, nullable: yes
- `name` `String`, nullable: yes
- `river_name` `String`, nullable: yes
- `waterbody_type` `String`, nullable: yes
- `country_code` `String`, nullable: yes
- `admin1` `String`, nullable: yes
- `admin2` `String`, nullable: yes
- `latitude` `Float`, nullable: yes
- `longitude` `Float`, nullable: yes
- `elevation_m` `Float`, nullable: yes
- `timezone` `String`, nullable: yes
- `station_status` `String`, nullable: yes
- `observed_properties` `JSONB`, nullable: yes
- `canonical_primary_property` `String`, nullable: yes
- `flow_unit_native` `String`, nullable: yes
- `stage_unit_native` `String`, nullable: yes
- `flow_unit_canonical` `String`, nullable: yes
- `stage_unit_canonical` `String`, nullable: yes
- `drainage_area_km2` `Float`, nullable: yes
- `datum_name` `String`, nullable: yes
- `datum_vertical_reference` `String`, nullable: yes
- `first_seen_at` `DateTime(timezone=True)`, nullable: yes
- `last_metadata_refresh_at` `DateTime(timezone=True)`, nullable: yes
- `last_seen_at` `DateTime(timezone=True)`, nullable: yes
- `geom` `Geometry(POINT, 4326)`, nullable: yes
- `raw_metadata` `JSONB`, nullable: yes
- `normalization_version` `String`, nullable: yes

Indexes/constraints/FKs:
- PK: `station_id`
- FK: `provider_id -> providers.provider_id`
- Unique constraint: `uq_station_provider(provider_id, provider_station_id)`
- Spatial index: `ix_stations_geom` (GIST on `geom`)

---

### `reaches`
- `reach_id` `String` PK, nullable: no
- `provider_id` `String`, FK -> `providers.provider_id`, nullable: no
- `source_type` `String`, nullable: no
- `provider_reach_id` `String`, nullable: no
- `name` `String`, nullable: yes
- `river_name` `String`, nullable: yes
- `country_code` `String`, nullable: yes
- `latitude` `Float`, nullable: yes
- `longitude` `Float`, nullable: yes
- `network_name` `String`, nullable: yes
- `geometry_type` `String`, nullable: yes
- `geom` `Geometry(GEOMETRY, 4326)`, nullable: yes
- `raw_metadata` `JSONB`, nullable: yes
- `normalization_version` `String`, nullable: yes
- `first_seen_at` `DateTime(timezone=True)`, nullable: yes
- `last_metadata_refresh_at` `DateTime(timezone=True)`, nullable: yes

Indexes/constraints/FKs:
- PK: `reach_id`
- FK: `provider_id -> providers.provider_id`
- Unique constraint: `uq_reach_provider(provider_id, provider_reach_id)`
- Spatial index: `ix_reaches_geom` (GIST on `geom`)

---

### `observation_latest`
- `latest_id` `UUID(as_uuid=True)` PK, nullable: no
- `entity_type` `String`, nullable: no
- `station_id` `String`, FK -> `stations.station_id`, nullable: yes
- `reach_id` `String`, FK -> `reaches.reach_id`, nullable: yes
- `property` `String`, nullable: no
- `observed_at` `DateTime(timezone=True)`, nullable: yes
- `value_native` `Float`, nullable: yes
- `unit_native` `String`, nullable: yes
- `value_canonical` `Float`, nullable: yes
- `unit_canonical` `String`, nullable: yes
- `quality_code` `String`, nullable: yes
- `quality_score` `Float`, nullable: yes
- `aggregation` `String`, nullable: yes
- `is_provisional` `Boolean`, nullable: yes, server default `false`
- `is_estimated` `Boolean`, nullable: yes, server default `false`
- `is_missing` `Boolean`, nullable: yes, server default `false`
- `is_forecast` `Boolean`, nullable: yes, server default `false`
- `is_flagged` `Boolean`, nullable: yes, server default `false`
- `provider_observation_id` `String`, nullable: yes
- `ingested_at` `DateTime(timezone=True)`, nullable: yes
- `raw_payload` `JSONB`, nullable: yes

Indexes/constraints/FKs:
- PK: `latest_id`
- FKs: `station_id -> stations.station_id`, `reach_id -> reaches.reach_id`
- Check constraint: `ck_one_entity_latest` with expression `(station_id IS NULL) != (reach_id IS NULL)`
- Partial unique index: `uq_latest_station_property(station_id, property)` where `station_id IS NOT NULL`
- Partial unique index: `uq_latest_reach_property(reach_id, property)` where `reach_id IS NOT NULL`

---

### `observation_timeseries`
- `id` `Integer` PK autoincrement, nullable: no
- `entity_type` `String`, nullable: no
- `station_id` `String`, FK -> `stations.station_id`, nullable: yes
- `reach_id` `String`, FK -> `reaches.reach_id`, nullable: yes
- `property` `String`, nullable: no
- `observed_at` `DateTime(timezone=True)`, nullable: yes
- `value_native` `Float`, nullable: yes
- `unit_native` `String`, nullable: yes
- `value_canonical` `Float`, nullable: yes
- `unit_canonical` `String`, nullable: yes
- `quality_code` `String`, nullable: yes
- `aggregation` `String`, nullable: yes
- `is_provisional` `Boolean`, nullable: yes, server default `false`
- `is_estimated` `Boolean`, nullable: yes, server default `false`
- `is_missing` `Boolean`, nullable: yes, server default `false`
- `is_forecast` `Boolean`, nullable: yes, server default `false`
- `is_flagged` `Boolean`, nullable: yes, server default `false`
- `provider_observation_id` `String`, nullable: yes
- `ingested_at` `DateTime(timezone=True)`, nullable: yes
- `raw_payload` `JSONB`, nullable: yes

Indexes/constraints/FKs:
- PK: `id`
- FKs: `station_id -> stations.station_id`, `reach_id -> reaches.reach_id`
- Check constraint: `ck_one_entity_ts` with expression `(station_id IS NULL) != (reach_id IS NULL)`
- Partial unique index: `uq_ts_station_prop_time(station_id, property, observed_at)` where `station_id IS NOT NULL`
- Partial unique index: `uq_ts_reach_prop_time(reach_id, property, observed_at)` where `reach_id IS NOT NULL`

---

### `thresholds`
- `threshold_id` `String` PK, nullable: no
- `entity_type` `String`, nullable: yes
- `station_id` `String`, FK -> `stations.station_id`, nullable: yes
- `reach_id` `String`, FK -> `reaches.reach_id`, nullable: yes
- `property` `String`, nullable: yes
- `threshold_type` `String`, nullable: yes
- `threshold_label` `String`, nullable: yes
- `severity_rank` `Integer`, nullable: yes
- `value_native` `Float`, nullable: yes
- `unit_native` `String`, nullable: yes
- `value_canonical` `Float`, nullable: yes
- `unit_canonical` `String`, nullable: yes
- `effective_from` `DateTime(timezone=True)`, nullable: yes
- `effective_to` `DateTime(timezone=True)`, nullable: yes
- `source` `String`, nullable: yes
- `method` `String`, nullable: yes
- `raw_payload` `JSONB`, nullable: yes
- `created_at` `DateTime(timezone=True)`, nullable: yes

Indexes/constraints/FKs:
- PK: `threshold_id`
- FKs: `station_id -> stations.station_id`, `reach_id -> reaches.reach_id`
- No additional explicit indexes or unique constraints.

---

### `warning_events`
- `warning_id` `String` PK, nullable: no
- `provider_id` `String`, FK -> `providers.provider_id`, nullable: yes
- `country_code` `String`, nullable: yes
- `warning_type` `String`, nullable: yes
- `severity` `String`, nullable: yes
- `title` `String`, nullable: yes
- `description` `String`, nullable: yes
- `issued_at` `DateTime(timezone=True)`, nullable: yes
- `effective_from` `DateTime(timezone=True)`, nullable: yes
- `effective_to` `DateTime(timezone=True)`, nullable: yes
- `geometry` `Geometry(GEOMETRY, 4326)`, nullable: yes
- `related_station_ids` `JSONB`, nullable: yes
- `related_reach_ids` `JSONB`, nullable: yes
- `status` `String`, nullable: yes
- `raw_payload` `JSONB`, nullable: yes
- `ingested_at` `DateTime(timezone=True)`, nullable: yes

Indexes/constraints/FKs:
- PK: `warning_id`
- FK: `provider_id -> providers.provider_id`
- Spatial index: `ix_warning_geometry` (GIST on `geometry`)

---

### `ingestion_runs`
- `run_id` `UUID(as_uuid=True)` PK, nullable: no
- `provider_id` `String`, nullable: yes
- `job_type` `String`, nullable: yes
- `started_at` `DateTime(timezone=True)`, nullable: yes
- `finished_at` `DateTime(timezone=True)`, nullable: yes
- `status` `String`, nullable: yes
- `records_seen` `Integer`, nullable: yes
- `records_inserted` `Integer`, nullable: yes
- `records_updated` `Integer`, nullable: yes
- `records_failed` `Integer`, nullable: yes
- `error_summary` `String`, nullable: yes
- `metadata` `JSONB`, nullable: yes

Indexes/constraints/FKs:
- PK: `run_id`
- No explicit FK/index beyond PK.

---

### `raw_ingest_archive`
- `archive_id` `Integer` PK autoincrement, nullable: no
- `provider_id` `String`, nullable: yes
- `job_type` `String`, nullable: yes
- `fetched_at` `DateTime(timezone=True)`, nullable: yes
- `source_url` `String`, nullable: yes
- `payload` `JSONB`, nullable: yes
- `payload_hash` `String`, nullable: yes

Indexes/constraints/FKs:
- PK: `archive_id`
- No explicit FK/index beyond PK.


## SECTION 2 — PROPERTY VOCABULARY

Canonical vocabulary and units are defined in `app/core/enums.py` and `app/core/units.py`; provider mappings are defined in adapters.

### Canonical properties present in code
- `discharge`
- `stage`
- `water_level`
- `storage`
- `velocity`
- `rainfall`
- `temperature`
- `snow_water_equivalent`
- `reservoir_storage`
- `reservoir_elevation`

### Property/unit contract table

| Property | Canonical Unit | Supported Entity Types in current ingestion | Example Providers currently emitting data |
|---|---|---|---|
| discharge | m3/s | station + reach | USGS (station), GEOGLOWS (reach) |
| stage | m | station | USGS, EA England |
| water_level | m | not currently emitted by adapters | none in current adapters |
| storage | m3 | not currently emitted by adapters | none in current adapters |
| velocity | m/s | not currently emitted by adapters | none in current adapters |
| rainfall | mm | not currently emitted by adapters | none in current adapters |
| temperature | degC | not currently emitted by adapters | none in current adapters |
| snow_water_equivalent | no canonical unit mapping implemented | not currently emitted by adapters | none in current adapters |
| reservoir_storage | m3 | not currently emitted by adapters | none in current adapters |
| reservoir_elevation | m | not currently emitted by adapters | none in current adapters |

### Native unit handling and conversion rules
- Canonical conversion map currently supports:
  - `ft3/s -> m3/s`
  - `kcfs -> m3/s`
  - `ft -> m`
  - `cm -> m`
  - `acre-ft -> m3`
  - `ML -> m3`
- If native unit equals canonical unit, value is passed through.
- If property has no canonical unit entry or no conversion pair, canonical value/unit become `null`.
- Provider-specific usage in current adapters:
  - USGS reads unit from USGS JSON variable unit code and maps property from parameter code (`00060` => `discharge`, else `stage`).
  - EA uses `unitName` for stage (defaults to `m` if missing).
  - GEOGLOWS emits `discharge` in `m3/s` directly.


## SECTION 3 — API ENDPOINTS

API prefix is `/v1` for providers/stations/reaches/warnings routes. Health routes are **not** under `/v1`.
Authentication is not implemented on any route.

### Provider endpoints

#### `GET /v1/providers`
- Query params: none
- Path params: none
- Auth: none
- Pagination: no cursor/limit support in handler
- Example request: `GET /v1/providers`
- Example response shape:
```json
{
  "data": [
    {
      "provider_id": "usgs",
      "name": "USGS Water Services",
      "provider_type": "government",
      "status": "active"
    }
  ],
  "meta": {"count": 1, "next_cursor": null}
}
```

#### `GET /v1/providers/{provider_id}`
- Query params: none
- Path params: `provider_id`
- Auth: none
- 404 if provider missing

### Station endpoints

#### `GET /v1/stations`
- Query params:
  - `bbox` (string `minLon,minLat,maxLon,maxLat`)
  - `provider_id` (string)
  - `country_code` (string)
  - `ids` (comma-separated station IDs)
  - `updated_since` (datetime)
  - `limit` (int, default `settings.default_limit`, min 1, max `settings.max_limit`)
  - `cursor` (string)
- Path params: none
- Auth: none
- Filtering support:
  - bbox: yes (uses `ST_Intersects` on station `geom`)
  - provider: yes
  - property: no
  - time range: `updated_since` against station metadata refresh timestamp
  - pagination: cursor + limit
- Note: bbox execution path only returns limited station fields from raw SQL select.

#### `GET /v1/stations/latest`
- Query params:
  - `property` (string)
  - `limit` (int default `default_limit`, min 1, max `max_limit`)
- Path params: none
- Auth: none
- Filtering support:
  - property: yes
  - bbox/provider/time-range: no
  - pagination: limit only (no cursor)

#### `GET /v1/stations/{station_id}`
- Query params: none
- Path params: `station_id`
- Auth: none
- 404 if missing

#### `GET /v1/stations/{station_id}/timeseries`
- Query params:
  - `property` (string)
  - `start` (datetime, inclusive)
  - `end` (datetime, inclusive)
  - `limit` (int default 1000, min 1, max `max_limit`)
- Path params: `station_id`
- Auth: none
- Filtering support: property + time range + limit

#### `GET /v1/stations/{station_id}/thresholds`
- Query params: `property` (string)
- Path params: `station_id`
- Auth: none
- Response currently returns threshold IDs only.

### Reach endpoints

#### `GET /v1/reaches`
- Query params:
  - `provider_id` (string)
  - `country_code` (string)
  - `ids` (comma-separated reach IDs)
  - `limit` (int default `default_limit`, min 1, max `max_limit`)
  - `cursor` (string)
- Path params: none
- Auth: none
- Filtering support:
  - provider: yes
  - bbox: no
  - property: no
  - time range: no
  - pagination: cursor + limit

#### `GET /v1/reaches/latest`
- Query params:
  - `property` (string)
  - `limit` (int default `default_limit`, min 1, max `max_limit`)
- Path params: none
- Auth: none
- Filtering support: property + limit only

#### `GET /v1/reaches/{reach_id}`
- Query params: none
- Path params: `reach_id`
- Auth: none
- 404 if missing

#### `GET /v1/reaches/{reach_id}/timeseries`
- Query params:
  - `property` (string)
  - `start` (datetime, inclusive)
  - `end` (datetime, inclusive)
  - `limit` (int default 1000, min 1, max `max_limit`)
- Path params: `reach_id`
- Auth: none

#### `GET /v1/reaches/{reach_id}/thresholds`
- Query params: `property` (string)
- Path params: `reach_id`
- Auth: none
- Response currently returns threshold IDs only.

### Warning endpoints

#### `GET /v1/warnings`
- Query params: none
- Path params: none
- Auth: none
- Filtering/pagination: none implemented

#### `GET /v1/warnings/active`
- Query params: none
- Path params: none
- Auth: none
- Active criterion: `status != "inactive"`

### Health endpoints

#### `GET /health/live`
- Returns `{"status": "ok"}`.

#### `GET /health/ready`
- Executes DB `select(1)` and returns `{"status": "ready"}` if DB call succeeds.

#### `GET /health/providers`
- Returns provider status plus latest ingestion run summary (`last_job_type`, `last_status`, `last_started_at`).

### Endpoints requested but not implemented
- `/v1/thresholds`: **not implemented**.
- `/v1/stations/timeseries` (without `{station_id}`): **not implemented**.
- `/v1/reaches/timeseries` (without `{reach_id}`): **not implemented**.


## SECTION 4 — RESPONSE STRUCTURE

Response envelopes use:
- List: `{ data: [...], meta: { count, next_cursor? } }`
- Single: `{ data: {...} }`

### `ProviderOut` (`/v1/providers*`)
- `provider_id: string`
- `name: string`
- `provider_type: string`
- `status: string`

### `StationOut` (`/v1/stations*`, `/v1/stations/{id}`)
- `station_id: string`
- `provider_id: string`
- `source_type: string`
- `name: string`
- `latitude: number`
- `longitude: number`

### `ReachOut` (`/v1/reaches*`, `/v1/reaches/{id}`)
- `reach_id: string`
- `provider_id: string`
- `source_type: string`
- `latitude: number | null`
- `longitude: number | null`

### `ObservationOut` (`/v1/stations/latest`, `/{station_id}/timeseries`, `/v1/reaches/latest`, `/{reach_id}/timeseries`)
- `entity_type: string` (`station` or `reach` in current ingestion)
- `station_id: string | null`
- `reach_id: string | null`
- `property: string`
- `observed_at: datetime`
- `value_native: number | null`
- `unit_native: string | null`
- `value_canonical: number | null`
- `unit_canonical: string | null`
- `quality_code: string | null`
- `provider_observation_id: string | null`

### `WarningOut` (`/v1/warnings*`)
- `warning_id: string`
- `provider_id: string`
- `severity: string | null`
- `title: string | null`
- `status: string | null`

### Geometry behavior in API responses
- Geometry is **not returned** in current response schemas for stations/reaches/warnings.
- No GeoJSON field is exposed by the current API schemas.
- Geometry therefore is not optional in API payloads; it is simply absent.
- Station metadata is **not embedded** in latest/timeseries observation responses beyond IDs and observation fields.


## SECTION 5 — THRESHOLD MODEL

### Table model support
`thresholds` table supports:
- entity linkage (`station_id` or `reach_id`)
- `property`
- `threshold_type`
- optional `threshold_label`
- optional numeric `severity_rank`
- native/canonical value+unit
- validity window (`effective_from`, `effective_to`)
- source/method/raw payload

### Ingestion status
- `sync_thresholds` job currently contains only TODO stub and commits with no ingestion.
- Therefore there is currently no implemented provider threshold normalization pipeline.

### Severity/label normalization
- No severity ranking or label normalization logic is implemented in adapters/services for thresholds.
- Any threshold label/rank would currently have to be preloaded externally into DB.


## SECTION 6 — WARNING EVENTS

### Warning fields in DB
`warning_events` supports:
- identity/provider: `warning_id`, `provider_id`
- metadata: `country_code`, `warning_type`, `severity`, `title`, `description`, `status`
- validity/timing: `issued_at`, `effective_from`, `effective_to`, `ingested_at`
- geometry: `geometry` (`GEOMETRY`, SRID 4326)
- relationships: `related_station_ids` JSONB, `related_reach_ids` JSONB
- passthrough raw: `raw_payload`

### Current normalization implementation
- Only EA England warnings are fetched (`/id/floods`).
- Normalized warning currently maps only:
  - `warning_id` from `floodAreaID` fallback `id` fallback literal
  - `provider_id`
  - `severity` from source `severity`
  - `title` from source `description`
  - `status` from source `message`
  - `raw_payload`
- Fields like `geometry`, `effective_from`, `effective_to`, related IDs are not populated by current adapter normalization.

### Active warnings logic
- `/v1/warnings/active` returns warnings with `status != "inactive"` only.
- No date-window-based activeness check is implemented.


## SECTION 7 — DATA FRESHNESS MODEL

### Scheduled ingestion frequencies (worker)
- `sync_metadata`: every 24 hours
- `sync_latest`: every 10 minutes
- `sync_history`: every 6 hours (currently delegates to `sync_latest`)
- `sync_thresholds`: every 24 hours (currently no-op)
- `sync_warnings`: every 30 minutes

### Practical freshness implications
- Station observations: expected update cadence tied to `sync_latest` (10 min), subject to provider data and failures.
- Reach observations: same (GEOGLOWS included in `sync_latest`, 10 min scheduler cadence).
- Threshold updates: no effective freshness because threshold ingestion is unimplemented.
- Warnings: expected update cadence 30 minutes.

### Timestamp guidance for clients
- Use `observed_at` as the measurement/event time for charting and time axes.
- Use `ingested_at` for backend freshness/latency indicators (available in DB, not exposed in `ObservationOut` or `WarningOut`).
- A separate `provider_timestamp` field does not exist in schema; provider times are normalized into `observed_at`.


## SECTION 8 — INGESTION PIPELINE

### Common ingestion run tracking
All sync jobs use `tracked_run` context manager, which:
- creates `ingestion_runs` row with `status="running"` and counters initialized
- sets `status="success"` on normal completion
- sets `status="failed"` and `error_summary` on exception
- always sets `finished_at`

Per-record counters are mutated by jobs:
- `records_seen`
- `records_inserted`
- `records_updated`
- `records_failed`

### Provider-specific implementation

#### USGS
- Entity type: station
- Properties ingested: `discharge`, `stage`
- Metadata source: NWIS site endpoint (single hard-coded site sample in current code)
- Latest observations source: NWIS IV endpoint (`parameterCd=00060,00065`, hard-coded site)
- Unit normalization: uses `to_canonical` with USGS unit codes
- Quality normalization: `normalize_quality` from qualifiers
- History support flag: true (`supports_history=True`), but history job currently reuses latest logic

#### EA England
- Entity type: station; warnings
- Properties ingested: `stage`
- Metadata source: `/id/stations?parameter=level`
- Latest observations source: `/id/measures?parameter=level`
- Warning source: `/id/floods`
- Unit normalization: uses `unitName` -> canonical via `to_canonical`
- Quality normalization: `normalize_quality(raw.get("qualifier"))`
- Special behavior: if latest observation references unknown station, sync attempts station enrichment via `/id/stations/{station_reference}` then retries upsert

#### GEOGLOWS
- Entity type: reach
- Properties ingested: `discharge`
- Metadata source: currently stubbed static sample list (`reach_id=1001`)
- Latest observations source: currently stubbed static sample record
- Unit normalization: native and canonical both `m3/s`
- Quality normalization: forced forecast (`normalize_quality("forecast", forecast=True)`), sets `is_forecast=True`

#### WHOS
- Entity type: station metadata scaffold
- Current implementation: no data returned (`fetch_station_catalog` returns empty list)
- No metadata/latest/threshold/warning/history ingestion wired in jobs.


## SECTION 9 — SPATIAL BEHAVIOR

### Geometry storage types and SRID
- Stations: `geom` as `POINT`, SRID 4326
- Reaches: `geom` as generic `GEOMETRY`, SRID 4326
- Warnings: `geometry` as generic `GEOMETRY`, SRID 4326

### Spatial indexing
- GIST indexes on station geom, reach geom, warning geometry as listed in Section 1.

### Spatial query behavior
- `GET /v1/stations` supports bbox via `ST_Intersects(geom, ST_MakeEnvelope(...,4326))`.
- No bbox filtering for reaches/warnings endpoints.

### Reach geometry relationship to global rivers layer
- No explicit global rivers layer table or join logic exists in repository.
- Reach catalog for GEOGLOWS is currently stubbed and does not perform external spatial joins.

### Snapping/spatial joins
- No snapping logic found.
- No server-side spatial join logic found beyond station bbox intersect filter.


## SECTION 10 — FRONTEND INTEGRATION CONTRACT

### Map markers
- Station markers: use `GET /v1/stations` (supports bbox, provider/country/ids filters, cursor pagination).
- Reach markers/centroids: use `GET /v1/reaches` (no bbox filter; use provider/country/ids).
- For marker values: pair marker list with `/v1/stations/latest` and `/v1/reaches/latest` filtered by `property` as needed.

### Charts
- Station chart series: `GET /v1/stations/{station_id}/timeseries?property=...&start=...&end=...`
- Reach chart series: `GET /v1/reaches/{reach_id}/timeseries?property=...&start=...&end=...`

### Station/reach status determination
- Observation status fields available to frontend are limited to `quality_code` in `ObservationOut`.
- Booleans like `is_forecast`, `is_provisional`, `is_estimated`, `is_flagged`, `is_missing` exist in DB but are not included in API response schema.
- Provider/platform status can be supplemented by `/health/providers` (`status`, `last_status`, `last_started_at`).

### Warning overlays
- Use `GET /v1/warnings/active` for currently active warnings according to backend rule (`status != inactive`).
- Geometry is not exposed in `WarningOut`; overlay polygons cannot be rendered from current warnings API without extending response schema.

### Data freshness in UI
- For observations, freshness at API layer can only be inferred from `observed_at` returned by latest endpoints.
- Ingestion-time freshness (`ingested_at`) is stored but not exposed in observation/warning API schemas.
- Use `/health/providers` to show last ingestion run timestamps/status at provider level.

---

## Implementation gaps relevant to FloodPulse integration
- No `/v1/thresholds` aggregate endpoint.
- Threshold ingestion (`sync_thresholds`) is unimplemented.
- Warning geometry and warning validity window fields exist in DB but are not currently normalized/populated by EA adapter nor returned by `WarningOut`.
- Observation API does not expose DB quality booleans or ingestion timestamp.
- Reach and station API outputs omit geometry fields (only lat/lon for station/reach list/detail schemas).
