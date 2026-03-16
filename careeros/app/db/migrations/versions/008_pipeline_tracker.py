"""add pipeline tracker fields to job_runs

Revision ID: 008_pipeline_tracker
Revises: 007_keyword_match_score
Create Date: 2026-03-06
"""
from alembic import op
import sqlalchemy as sa

revision = '008_pipeline_tracker'
down_revision = '007_keyword_match_score'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column('job_runs', sa.Column('pipeline_stage', sa.String(), nullable=True, server_default='watching'))
    op.add_column('job_runs', sa.Column('pipeline_notes', sa.Text(), nullable=True))
    op.add_column('job_runs', sa.Column('applied_at', sa.DateTime(timezone=True), nullable=True))

def downgrade() -> None:
    op.drop_column('job_runs', 'pipeline_stage')
    op.drop_column('job_runs', 'pipeline_notes')
    op.drop_column('job_runs', 'applied_at')
