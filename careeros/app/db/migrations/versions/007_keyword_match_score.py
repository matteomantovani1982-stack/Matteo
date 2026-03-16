"""add keyword_match_score to job_runs

Revision ID: 007_keyword_match_score
Revises: 006_sprint_l_email
Create Date: 2026-03-05
"""
from alembic import op
import sqlalchemy as sa

revision = '007_keyword_match_score'
down_revision = '006'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.add_column('job_runs', sa.Column('keyword_match_score', sa.Integer(), nullable=True))

def downgrade() -> None:
    op.drop_column('job_runs', 'keyword_match_score')
