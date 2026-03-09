"""init"""

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
    op.create_table("providers", sa.Column("provider_id", sa.String(), primary_key=True), sa.Column("name", sa.String(), nullable=False), sa.Column("provider_type", sa.String(), nullable=False), sa.Column("home_url", sa.String()), sa.Column("api_base_url", sa.String()), sa.Column("license_name", sa.String()), sa.Column("license_url", sa.String()), sa.Column("attribution_text", sa.String()), sa.Column("default_poll_interval_minutes", sa.Integer()), sa.Column("status", sa.String(), nullable=False), sa.Column("auth_type", sa.String()), sa.Column("created_at", sa.DateTime(timezone=True)), sa.Column("updated_at", sa.DateTime(timezone=True)))
    op.create_table("stations", sa.Column("station_id", sa.String(), primary_key=True), sa.Column("provider_id", sa.String(), sa.ForeignKey("providers.provider_id")), sa.Column("source_type", sa.String(), nullable=False), sa.Column("provider_station_id", sa.String(), nullable=False), sa.Column("name", sa.String()), sa.Column("latitude", sa.Float()), sa.Column("longitude", sa.Float()), sa.Column("geom", Geometry("POINT", srid=4326)), sa.Column("raw_metadata", postgresql.JSONB()), sa.Column("normalization_version", sa.String()), sa.UniqueConstraint("provider_id", "provider_station_id", name="uq_station_provider"))
    op.create_table("reaches", sa.Column("reach_id", sa.String(), primary_key=True), sa.Column("provider_id", sa.String(), sa.ForeignKey("providers.provider_id")), sa.Column("source_type", sa.String(), nullable=False), sa.Column("provider_reach_id", sa.String(), nullable=False), sa.Column("latitude", sa.Float()), sa.Column("longitude", sa.Float()), sa.Column("geom", Geometry("GEOMETRY", srid=4326)), sa.Column("raw_metadata", postgresql.JSONB()), sa.Column("normalization_version", sa.String()), sa.UniqueConstraint("provider_id", "provider_reach_id", name="uq_reach_provider"))


def downgrade() -> None:
    op.drop_table("reaches")
    op.drop_table("stations")
    op.drop_table("providers")
