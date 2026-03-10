# Flood API Implementation Roadmap (Aligned)

This roadmap translates the proposed workstreams into repository-ready phases and calls out conflicts with the current implementation so execution can proceed without churn.

## Current-state alignment summary

The proposed plan is directionally correct and matches the architecture in this repository:
- observed station providers (`stations`) and modeled reach providers (`reaches`) are already separated
- warning and threshold models already exist
- provider adapters and ingestion jobs already follow an extensible pattern

## Conflict and dependency notes

### 1) Provider completion should account for actual current support
- **USGS** is currently hard-coded to one station (`01646500`) in both metadata and latest ingestion; scaling to bbox/region/config list is required.
- **EA England** currently ingests levels and warnings; warning geometry/timing/relationships are not yet persisted to first-class warning columns.
- **GEOGLOWS** is still a static demo response and must be replaced with real API integration.

**Execution impact:** no conflict with architecture; this is straightforward hardening work.

### 2) Reliability work has one scheduling conflict to resolve early
Current scheduler intervals are global hard-coded jobs (metadata/latest/history/thresholds/warnings) rather than per-provider polling intervals.

**Conflict:** adding provider-level intervals later without first introducing provider-aware schedule config risks rework.

**Resolution:** in Phase A, add provider-level schedule config and migrate jobs to provider-scoped dispatch.

### 3) Forecast model expansion should avoid premature table split
The schema already has `is_forecast` plus raw payload storage and can support an MVP for GloFAS/GEOGLOWS.

**Conflict:** introducing dedicated forecast tables too early would duplicate ingestion logic before forecast access patterns are validated.

**Resolution:** ship forecast metadata fields first; defer dedicated forecast tables until query/performance requirements prove necessary.

### 4) Threshold and warning workstreams overlap (good) but need one shared normalization contract
Threshold severity/status and warning severity/status should use a shared canonical mapping to avoid provider-specific drift.

**Conflict:** implementing these independently can create inconsistent frontend semantics.

**Resolution:** define one normalization matrix for severity/status in Phase B and reuse across both jobs.

### 5) Flood-products subsystem is correct, but must remain decoupled from station/reach APIs
GFMS/Copernicus products do not fit point/reach timeseries cleanly.

**Conflict:** forcing flood-product assets into existing observation tables would complicate current APIs and degrade model clarity.

**Resolution:** add a separate flood-products schema and routes in Phase D.

### 6) Discovery feeds (WHOS/GSIM/GRDC) should remain non-blocking
Using these as initial live ingestion sources would slow delivery of core capabilities.

**Conflict:** prioritizing discovery before provider hardening reduces near-term user value.

**Resolution:** keep discovery as Phase E metadata enrichment and provider-discovery support.

## Agreed phase order

## Phase A — Core hardening (highest priority)
1. USGS completion
   - station catalog via bbox/region/configured list
   - latest stage/discharge from real station sets
   - true historical ingestion (not latest replay)
   - improved station metadata normalization and unit handling
2. EA England completion
   - persist warning geometry and warning timing
   - complete station history ingestion
   - normalize warning severity/status and link warnings to related entities where possible
   - support both level and flow observations where available
3. GEOGLOWS completion
   - replace demo responses with real integration
   - ingest reach metadata + geometry + stable provider reach IDs
   - ingest forecast discharge and available historical/reanalysis series
4. Cross-provider reliability
   - provider-scoped timeout/retry policies
   - clearer per-provider logs and ingestion-run summaries
   - provider-level failure isolation
   - idempotent upserts retained and verified
   - provider-level polling intervals
5. Normalization tightening
   - strict provider ID conventions
   - UTC timestamp normalization guarantees
   - observed vs modeled and forecast vs analysis distinctions
   - preserve raw payloads for traceability

## Phase B — Gauge expansion and warning/threshold quality
1. BoM Australia provider
   - station catalog
   - latest level and discharge where available
   - threshold/flood class ingestion where available
2. Threshold hardening
   - threshold type normalization and severity ordering
   - station/reach/provider mapping consistency
   - support multi-band thresholds
3. Warning hardening completion
   - polygon persistence and effective start/end support
   - normalized severity/status contract

## Phase C — Modeled forecast expansion
1. Add GloFAS provider
   - modeled reach/point mapping to internal reaches
   - forecast discharge ingestion and forecast metadata
2. Forecast-aware schema upgrades
   - add lead-time, forecast reference-time, and timeseries-kind fields
   - keep ensemble detail in raw payload initially
3. Reach API enhancements for forecast use cases

## Phase D — Flood products subsystem
1. Add new models/routes
   - `flood_products`
   - `flood_product_assets`
   - `flood_product_areas`
2. Integrate first feeds
   - GFMS
   - Copernicus GFM
3. Later candidate
   - Google Flood Forecasting (subject to access)

## Phase E — Discovery and metadata expansion
1. WHOS-driven provider discovery workflow
2. GSIM/GRDC-style station metadata enrichment
3. Additional national provider rollout using the hardened adapter template

## Repository update checklist

- [ ] Add provider-level schedule configuration and dispatch
- [ ] Upgrade adapters: USGS, EA England, GEOGLOWS
- [ ] Implement true history ingestion path
- [ ] Implement threshold ingestion pipeline
- [ ] Add warning geometry/timing normalization persistence
- [ ] Add BoM provider
- [ ] Add GloFAS provider + forecast metadata fields
- [ ] Add flood-products subsystem (models + routes)
- [ ] Add capability metadata endpoint for frontend integration

