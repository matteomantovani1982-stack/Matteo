"""cv_builder: quality scoring, build mode, templates

Adds:
  - cv_templates table (5 default templates seeded)
  - quality_score, quality_feedback, build_mode, template_slug to cvs
  - Indexes for build_mode queries

Revision ID: 004
Revises: 003
Create Date: 2025-01-04 00:00:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None

# ── Template seed data ────────────────────────────────────────────────────────
TEMPLATES = [
    {
        "slug": "classic",
        "display_name": "Classic",
        "description": "Traditional chronological format. Conservative, widely accepted.",
        "sort_order": 0,
        "preview_metadata": '{"accent_color": "#1a1a2e", "font_name": "Times New Roman"}',
    },
    {
        "slug": "modern",
        "display_name": "Modern",
        "description": "Clean sans-serif, subtle accents. ATS-friendly, contemporary.",
        "sort_order": 1,
        "preview_metadata": '{"accent_color": "#2d6cdf", "font_name": "Calibri"}',
    },
    {
        "slug": "executive",
        "display_name": "Executive",
        "description": "Wide margins, strong hierarchy. Suited to director and C-suite roles.",
        "sort_order": 2,
        "preview_metadata": '{"accent_color": "#1c1c1c", "font_name": "Garamond"}',
    },
    {
        "slug": "compact",
        "display_name": "Compact",
        "description": "One-page optimised. Tight spacing for early-career professionals.",
        "sort_order": 3,
        "preview_metadata": '{"accent_color": "#2c6e49", "font_name": "Arial"}',
    },
    {
        "slug": "minimal",
        "display_name": "Minimal",
        "description": "Text only, maximum ATS compatibility.",
        "sort_order": 4,
        "preview_metadata": '{"accent_color": "#000000", "font_name": "Arial"}',
    },
]


def upgrade() -> None:
    # ── cv_templates ───────────────────────────────────────────────────────
    op.create_table(
        "cv_templates",
        sa.Column("id", sa.dialects.postgresql.UUID(as_uuid=True), primary_key=True,
                  server_default=sa.text("gen_random_uuid()")),
        sa.Column("slug", sa.String(50), nullable=False, unique=True),
        sa.Column("display_name", sa.String(100), nullable=False),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("s3_key", sa.String(1000), nullable=False, server_default=""),
        sa.Column("s3_bucket", sa.String(255), nullable=False, server_default=""),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("sort_order", sa.Integer, nullable=False, server_default="0"),
        sa.Column("preview_metadata", JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_cv_templates_id", "cv_templates", ["id"])
    op.create_index("ix_cv_templates_slug", "cv_templates", ["slug"])

    # ── Seed default templates ─────────────────────────────────────────────
    conn = op.get_bind()
    for t in TEMPLATES:
        conn.execute(
            sa.text(
                "INSERT INTO cv_templates (slug, display_name, description, sort_order, preview_metadata) "
                "VALUES (:slug, :display_name, :description, :sort_order, CAST(:preview_metadata AS jsonb)) "
                "ON CONFLICT (slug) DO NOTHING"
            ),
            {
                "slug": t["slug"],
                "display_name": t["display_name"],
                "description": t["description"],
                "sort_order": t["sort_order"],
                "preview_metadata": t["preview_metadata"],
            }
        )

    # ── Add columns to cvs ─────────────────────────────────────────────────
    op.add_column("cvs", sa.Column(
        "quality_score", sa.Integer, nullable=True,
        comment="LLM quality score 0-100. <40=poor, 40-70=weak, 70-85=good, 85+=strong",
    ))
    op.add_column("cvs", sa.Column(
        "quality_feedback", JSONB, nullable=True,
        comment="Full quality assessment: {score, verdict, top_issues, recommendation, rebuild_recommended}",
    ))
    op.add_column("cvs", sa.Column(
        "build_mode", sa.String(20), nullable=False, server_default="edit",
        comment="edit | rebuild | from_scratch",
    ))
    op.add_column("cvs", sa.Column(
        "template_slug", sa.String(50), nullable=True,
        comment="Template slug for rebuild/from_scratch modes",
    ))

    op.create_index("ix_cvs_build_mode", "cvs", ["build_mode"])
    op.create_index("ix_cvs_template_slug", "cvs", ["template_slug"])


def downgrade() -> None:
    op.drop_index("ix_cvs_template_slug", "cvs")
    op.drop_index("ix_cvs_build_mode", "cvs")
    op.drop_column("cvs", "template_slug")
    op.drop_column("cvs", "build_mode")
    op.drop_column("cvs", "quality_feedback")
    op.drop_column("cvs", "quality_score")
    op.drop_table("cv_templates")
