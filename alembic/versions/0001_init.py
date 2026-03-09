"""init full schema"""

from alembic import op
import sqlalchemy as sa
from geoalchemy2 import Geometry
from sqlalchemy.dialects import postgresql

revision = "0001_init"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    op.create_table(
        "providers",
        sa.Column("provider_id", sa.String(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("provider_type", sa.String(), nullable=False),
        sa.Column("home_url", sa.String()),
        sa.Column("api_base_url", sa.String()),
        sa.Column("license_name", sa.String()),
        sa.Column("license_url", sa.String()),
        sa.Column("attribution_text", sa.String()),
        sa.Column("default_poll_interval_minutes", sa.Integer()),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("auth_type", sa.String()),
        sa.Column("created_at", sa.DateTime(timezone=True)),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
    )

    op.create_table(
        "stations",
        sa.Column("station_id", sa.String(), primary_key=True),
        sa.Column("provider_id", sa.String(), sa.ForeignKey("providers.provider_id"), nullable=False),
        sa.Column("source_type", sa.String(), nullable=False),
        sa.Column("provider_station_id", sa.String(), nullable=False),
        sa.Column("provider_station_code", sa.String()),
        sa.Column("name", sa.String()),
        sa.Column("river_name", sa.String()),
        sa.Column("waterbody_type", sa.String()),
        sa.Column("country_code", sa.String()),
        sa.Column("admin1", sa.String()),
        sa.Column("admin2", sa.String()),
        sa.Column("latitude", sa.Float()),
        sa.Column("longitude", sa.Float()),
        sa.Column("elevation_m", sa.Float()),
        sa.Column("timezone", sa.String()),
        sa.Column("station_status", sa.String()),
        sa.Column("observed_properties", postgresql.JSONB()),
        sa.Column("canonical_primary_property", sa.String()),
        sa.Column("flow_unit_native", sa.String()),
        sa.Column("stage_unit_native", sa.String()),
        sa.Column("flow_unit_canonical", sa.String()),
        sa.Column("stage_unit_canonical", sa.String()),
        sa.Column("drainage_area_km2", sa.Float()),
        sa.Column("datum_name", sa.String()),
        sa.Column("datum_vertical_reference", sa.String()),
        sa.Column("first_seen_at", sa.DateTime(timezone=True)),
        sa.Column("last_metadata_refresh_at", sa.DateTime(timezone=True)),
        sa.Column("last_seen_at", sa.DateTime(timezone=True)),
        sa.Column("geom", Geometry("POINT", srid=4326)),
        sa.Column("raw_metadata", postgresql.JSONB()),
        sa.Column("normalization_version", sa.String()),
        sa.UniqueConstraint("provider_id", "provider_station_id", name="uq_station_provider"),
    )
    op.create_index("ix_stations_geom", "stations", ["geom"], postgresql_using="gist")

    op.create_table(
        "reaches",
        sa.Column("reach_id", sa.String(), primary_key=True),
        sa.Column("provider_id", sa.String(), sa.ForeignKey("providers.provider_id"), nullable=False),
        sa.Column("source_type", sa.String(), nullable=False),
        sa.Column("provider_reach_id", sa.String(), nullable=False),
        sa.Column("name", sa.String()),
        sa.Column("river_name", sa.String()),
        sa.Column("country_code", sa.String()),
        sa.Column("latitude", sa.Float()),
        sa.Column("longitude", sa.Float()),
        sa.Column("network_name", sa.String()),
        sa.Column("geometry_type", sa.String()),
        sa.Column("geom", Geometry("GEOMETRY", srid=4326)),
        sa.Column("raw_metadata", postgresql.JSONB()),
        sa.Column("normalization_version", sa.String()),
        sa.Column("first_seen_at", sa.DateTime(timezone=True)),
        sa.Column("last_metadata_refresh_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("provider_id", "provider_reach_id", name="uq_reach_provider"),
    )
    op.create_index("ix_reaches_geom", "reaches", ["geom"], postgresql_using="gist")

    op.create_table(
        "observation_latest",
        sa.Column("latest_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("entity_type", sa.String(), nullable=False),
        sa.Column("station_id", sa.String(), sa.ForeignKey("stations.station_id")),
        sa.Column("reach_id", sa.String(), sa.ForeignKey("reaches.reach_id")),
        sa.Column("property", sa.String(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True)),
        sa.Column("value_native", sa.Float()),
        sa.Column("unit_native", sa.String()),
        sa.Column("value_canonical", sa.Float()),
        sa.Column("unit_canonical", sa.String()),
        sa.Column("quality_code", sa.String()),
        sa.Column("quality_score", sa.Float()),
        sa.Column("aggregation", sa.String()),
        sa.Column("is_provisional", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("is_estimated", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("is_missing", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("is_forecast", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("is_flagged", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("provider_observation_id", sa.String()),
        sa.Column("ingested_at", sa.DateTime(timezone=True)),
        sa.Column("raw_payload", postgresql.JSONB()),
        sa.CheckConstraint("(station_id IS NULL) != (reach_id IS NULL)", name="ck_one_entity_latest"),
    )
    op.create_index("uq_latest_station_property", "observation_latest", ["station_id", "property"], unique=True, postgresql_where=sa.text("station_id IS NOT NULL"))
    op.create_index("uq_latest_reach_property", "observation_latest", ["reach_id", "property"], unique=True, postgresql_where=sa.text("reach_id IS NOT NULL"))

    op.create_table(
        "observation_timeseries",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("entity_type", sa.String(), nullable=False),
        sa.Column("station_id", sa.String(), sa.ForeignKey("stations.station_id")),
        sa.Column("reach_id", sa.String(), sa.ForeignKey("reaches.reach_id")),
        sa.Column("property", sa.String(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True)),
        sa.Column("value_native", sa.Float()),
        sa.Column("unit_native", sa.String()),
        sa.Column("value_canonical", sa.Float()),
        sa.Column("unit_canonical", sa.String()),
        sa.Column("quality_code", sa.String()),
        sa.Column("aggregation", sa.String()),
        sa.Column("is_provisional", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("is_estimated", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("is_missing", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("is_forecast", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("is_flagged", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("provider_observation_id", sa.String()),
        sa.Column("ingested_at", sa.DateTime(timezone=True)),
        sa.Column("raw_payload", postgresql.JSONB()),
        sa.CheckConstraint("(station_id IS NULL) != (reach_id IS NULL)", name="ck_one_entity_ts"),
    )
    op.create_index("uq_ts_station_prop_time", "observation_timeseries", ["station_id", "property", "observed_at"], unique=True, postgresql_where=sa.text("station_id IS NOT NULL"))
    op.create_index("uq_ts_reach_prop_time", "observation_timeseries", ["reach_id", "property", "observed_at"], unique=True, postgresql_where=sa.text("reach_id IS NOT NULL"))

    op.create_table(
        "thresholds",
        sa.Column("threshold_id", sa.String(), primary_key=True),
        sa.Column("entity_type", sa.String()),
        sa.Column("station_id", sa.String(), sa.ForeignKey("stations.station_id")),
        sa.Column("reach_id", sa.String(), sa.ForeignKey("reaches.reach_id")),
        sa.Column("property", sa.String()),
        sa.Column("threshold_type", sa.String()),
        sa.Column("threshold_label", sa.String()),
        sa.Column("severity_rank", sa.Integer()),
        sa.Column("value_native", sa.Float()),
        sa.Column("unit_native", sa.String()),
        sa.Column("value_canonical", sa.Float()),
        sa.Column("unit_canonical", sa.String()),
        sa.Column("effective_from", sa.DateTime(timezone=True)),
        sa.Column("effective_to", sa.DateTime(timezone=True)),
        sa.Column("source", sa.String()),
        sa.Column("method", sa.String()),
        sa.Column("raw_payload", postgresql.JSONB()),
        sa.Column("created_at", sa.DateTime(timezone=True)),
    )

    op.create_table(
        "warning_events",
        sa.Column("warning_id", sa.String(), primary_key=True),
        sa.Column("provider_id", sa.String(), sa.ForeignKey("providers.provider_id")),
        sa.Column("country_code", sa.String()),
        sa.Column("warning_type", sa.String()),
        sa.Column("severity", sa.String()),
        sa.Column("title", sa.String()),
        sa.Column("description", sa.String()),
        sa.Column("issued_at", sa.DateTime(timezone=True)),
        sa.Column("effective_from", sa.DateTime(timezone=True)),
        sa.Column("effective_to", sa.DateTime(timezone=True)),
        sa.Column("geometry", Geometry("GEOMETRY", srid=4326)),
        sa.Column("related_station_ids", postgresql.JSONB()),
        sa.Column("related_reach_ids", postgresql.JSONB()),
        sa.Column("status", sa.String()),
        sa.Column("raw_payload", postgresql.JSONB()),
        sa.Column("ingested_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_warning_geometry", "warning_events", ["geometry"], postgresql_using="gist")

    op.create_table(
        "ingestion_runs",
        sa.Column("run_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("provider_id", sa.String()),
        sa.Column("job_type", sa.String()),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("status", sa.String()),
        sa.Column("records_seen", sa.Integer()),
        sa.Column("records_inserted", sa.Integer()),
        sa.Column("records_updated", sa.Integer()),
        sa.Column("records_failed", sa.Integer()),
        sa.Column("error_summary", sa.String()),
        sa.Column("metadata", postgresql.JSONB()),
    )

    op.create_table(
        "raw_ingest_archive",
        sa.Column("archive_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("provider_id", sa.String()),
        sa.Column("job_type", sa.String()),
        sa.Column("fetched_at", sa.DateTime(timezone=True)),
        sa.Column("source_url", sa.String()),
        sa.Column("payload", postgresql.JSONB()),
        sa.Column("payload_hash", sa.String()),
    )


def downgrade() -> None:
    op.drop_table("raw_ingest_archive")
    op.drop_table("ingestion_runs")
    op.drop_index("ix_warning_geometry", table_name="warning_events")
    op.drop_table("warning_events")
    op.drop_table("thresholds")
    op.drop_index("uq_ts_reach_prop_time", table_name="observation_timeseries")
    op.drop_index("uq_ts_station_prop_time", table_name="observation_timeseries")
    op.drop_table("observation_timeseries")
    op.drop_index("uq_latest_reach_property", table_name="observation_latest")
    op.drop_index("uq_latest_station_property", table_name="observation_latest")
    op.drop_table("observation_latest")
    op.drop_index("ix_reaches_geom", table_name="reaches")
    op.drop_table("reaches")
    op.drop_index("ix_stations_geom", table_name="stations")
    op.drop_table("stations")
    op.drop_table("providers")
