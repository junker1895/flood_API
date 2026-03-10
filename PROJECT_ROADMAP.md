FloodWatch / Flood API — Updated Implementation Roadmap
System Goal

Build a global flood situational awareness platform that combines:

river observations
river forecasts
rainfall
flood thresholds
basin context

to explain:

what is happening
why it is happening
what will happen next
Phase A — Core ingestion hardening

(highest priority)

Goal: ensure ingestion is reliable, scalable, and provider-agnostic.

1. USGS provider completion

Current state:

single station fallback

partial history

Required work:

station discovery across all states
multi-state request handling
deduplication of station results
configurable station selectors
true historical ingestion
improved metadata normalization
unit normalization

Expected output:

~10,000 USGS gauges
stage + discharge
5–15 min updates
2. EA England completion

Current state:

levels ingesting

warnings ingesting

geometry partially handled

Required work:

persist warning geometry
persist warning start/end times
normalize warning severity/status
support both flow and level observations
link warnings to stations/reaches
complete history ingestion
3. GEOGLOWS completion

Current state:

demo placeholder

Required work:

real API integration
reach metadata ingestion
reach geometry persistence
stable reach IDs
forecast discharge ingestion
reanalysis time series ingestion
4. Provider-level scheduling

Current state:

global ingestion schedule

Required work:

provider-specific polling intervals
provider-scoped retries
provider timeout configuration
provider-level failure isolation
structured ingestion logs
5. Data normalization tightening

Ensure consistency across providers.

strict provider ID rules
station/reach ID conventions
UTC timestamp guarantees
observed vs modeled distinction
forecast vs analysis distinction
unit normalization
raw payload preservation
Phase A.5 — Hydrograph usefulness

(new phase)

Goal: make gauges operationally useful.

1. Historical backfill improvements
minimum 3–7 days of observations
efficient history ingestion
incremental timeseries updates
2. Hydrograph query optimization
indexed timeseries queries
efficient station history API
Phase B — Flood intelligence layer

(critical)

Goal: convert observations into flood signals.

1. Flood threshold ingestion

Required fields:

station_id
threshold_value
threshold_type
severity_level

Normalize severity levels:

bankfull
minor flood
moderate flood
major flood

Providers:

USGS
EA England
BoM
other national agencies
2. Warning normalization

Persist and normalize warnings:

warning polygons
effective start/end
severity/status normalization
station/reach relationships
Phase C — Forecast capability

Goal: predict flood evolution.

1. GEOGLOWS forecast integration
reach forecasts
forecast reference time
lead times
forecast metadata
2. Add GloFAS provider
reach mapping
forecast ingestion
lead-time metadata
3. Forecast-aware schema upgrades

Add fields:

forecast_reference_time
lead_time
timeseries_kind
ensemble metadata

Ensemble detail can remain in raw payload initially.

Phase C.5 — Rainfall layer

(new phase)

Goal: explain flood drivers.

Required data
rain_rate
24h rainfall accumulation
timestamp
grid location
Candidate feeds
IMERG satellite rainfall
GFS precipitation forecasts
national radar mosaics

Rainfall enables:

upstream rainfall analysis
flood cause identification
Phase D — Flood products subsystem

Flood extent products do not fit station/reach models.

Create new models:

flood_products
flood_product_assets
flood_product_areas
Initial feeds
GFMS
Copernicus Global Flood Monitoring

Possible future feed:

Google Flood Forecasting

(access permitting)

Phase E — Global gauge expansion

Once intelligence layers are working, expand coverage.

Priority providers
BoM Australia
Canada HYDAT
additional European agencies
Brazil ANA
Discovery feeds

Used for metadata enrichment:

WHOS
GRDC
GSIM

These should not block core functionality.

Phase F — Basin intelligence

Goal: provide hydrologic context.

Datasets:

HydroBASINS
HydroRIVERS

Capabilities:

upstream basin delineation
basin rainfall aggregation
river network analysis
Phase G — Flood severity indicators

Create derived signals combining layers:

stage
threshold exceedance
rainfall
forecast discharge

Example output:

Flood Severity Index
0–5 scale

This dramatically improves map usability.

Repository update checklist

Core infrastructure

[ ] provider-level scheduling
[ ] ingestion reliability improvements
[ ] adapter normalization tightening

Providers

[ ] USGS completion
[ ] EA England completion
[ ] GEOGLOWS completion
[ ] BoM provider
[ ] GloFAS provider

Hydrologic intelligence

[ ] threshold ingestion pipeline
[ ] warning geometry persistence
[ ] rainfall ingestion layer

Data systems

[ ] hydrograph history improvements
[ ] forecast metadata fields
[ ] basin datasets integration

Flood products

[ ] flood_products schema
[ ] GFMS ingestion
[ ] Copernicus GFM ingestion

Discovery

[ ] WHOS integration
[ ] GSIM / GRDC metadata enrichment
Long-term platform outcome

When the roadmap is complete the system will provide:

global river observations
global river forecasts
global rainfall monitoring
flood thresholds
warning polygons
basin context
satellite flood detection

Which enables a map that answers:

Is flooding happening?
Why is it happening?
Where will it spread next?
