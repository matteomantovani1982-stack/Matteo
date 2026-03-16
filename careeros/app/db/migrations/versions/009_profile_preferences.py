"""add preferences to candidate_profiles

Revision ID: 008_profile_preferences
Revises: 007_keyword_match_score
Create Date: 2026-03-05
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = '009_profile_preferences'
down_revision = '008_pipeline_tracker'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column('candidate_profiles', sa.Column('preferences', JSONB(), nullable=True))

def downgrade() -> None:
    op.drop_column('candidate_profiles', 'preferences')
