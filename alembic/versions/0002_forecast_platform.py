"""add forecast platform tables"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0002_forecast_platform"
down_revision = "0001_init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "forecast_reaches",
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("reach_id", sa.BigInteger(), nullable=False),
        sa.Column("lon", sa.Float()),
        sa.Column("lat", sa.Float()),
        sa.Column("uparea", sa.Float()),
        sa.Column("rp2", sa.Float()),
        sa.Column("rp5", sa.Float()),
        sa.Column("rp10", sa.Float()),
        sa.Column("rp25", sa.Float()),
        sa.Column("rp50", sa.Float()),
        sa.Column("rp100", sa.Float()),
        sa.Column("source_metadata", postgresql.JSONB()),
        sa.PrimaryKeyConstraint("model", "reach_id", name="pk_forecast_reaches"),
    )
    op.create_index("forecast_reaches_model_lat_lon_idx", "forecast_reaches", ["model", "lat", "lon"])

    op.create_table(
        "forecast_runs",
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("forecast_date", sa.Date(), nullable=False),
        sa.Column("run_issued_at", sa.DateTime(timezone=True)),
        sa.Column("timestep_count", sa.SmallInteger()),
        sa.Column("timestep_hours", sa.SmallInteger()),
        sa.Column("timesteps_json", postgresql.JSONB()),
        sa.Column("source_path", sa.Text()),
        sa.Column("source_metadata", postgresql.JSONB()),
        sa.PrimaryKeyConstraint("model", "forecast_date", name="pk_forecast_runs"),
    )

    op.create_table(
        "forecast_reach_risk",
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("forecast_date", sa.Date(), nullable=False),
        sa.Column("reach_id", sa.BigInteger(), nullable=False),
        sa.Column("risk_class", sa.SmallInteger(), nullable=False),
        sa.Column("max_prob_rp2_24h", sa.Float()),
        sa.Column("max_prob_rp5_24h", sa.Float()),
        sa.Column("max_prob_rp10_24h", sa.Float()),
        sa.Column("max_prob_rp2_72h", sa.Float()),
        sa.Column("max_prob_rp5_72h", sa.Float()),
        sa.Column("max_prob_rp10_72h", sa.Float()),
        sa.Column("max_prob_rp2_7d", sa.Float()),
        sa.Column("max_prob_rp5_7d", sa.Float()),
        sa.Column("max_prob_rp10_7d", sa.Float()),
        sa.Column("peak_median_flow", sa.Float()),
        sa.Column("peak_time", sa.DateTime(timezone=True)),
        sa.Column("source_metadata", postgresql.JSONB()),
        sa.PrimaryKeyConstraint("model", "forecast_date", "reach_id", name="pk_forecast_reach_risk"),
    )
    op.create_index(
        "forecast_reach_risk_lookup_idx",
        "forecast_reach_risk",
        ["model", "forecast_date", "risk_class"],
    )

    op.create_table(
        "forecast_reach_detail",
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("forecast_date", sa.Date(), nullable=False),
        sa.Column("reach_id", sa.BigInteger(), nullable=False),
        sa.Column("timestep_idx", sa.SmallInteger(), nullable=False),
        sa.Column("valid_time", sa.DateTime(timezone=True)),
        sa.Column("flow_median", sa.Float()),
        sa.Column("prob_exceed_rp2", sa.Float()),
        sa.Column("prob_exceed_rp5", sa.Float()),
        sa.Column("prob_exceed_rp10", sa.Float()),
        sa.Column("source_metadata", postgresql.JSONB()),
        sa.PrimaryKeyConstraint(
            "model",
            "forecast_date",
            "reach_id",
            "timestep_idx",
            name="pk_forecast_reach_detail",
        ),
    )
    op.create_index(
        "forecast_reach_detail_lookup_idx",
        "forecast_reach_detail",
        ["model", "forecast_date", "reach_id"],
    )


def downgrade() -> None:
    op.drop_index("forecast_reach_detail_lookup_idx", table_name="forecast_reach_detail")
    op.drop_table("forecast_reach_detail")
    op.drop_index("forecast_reach_risk_lookup_idx", table_name="forecast_reach_risk")
    op.drop_table("forecast_reach_risk")
    op.drop_table("forecast_runs")
    op.drop_index("forecast_reaches_model_lat_lon_idx", table_name="forecast_reaches")
    op.drop_table("forecast_reaches")
