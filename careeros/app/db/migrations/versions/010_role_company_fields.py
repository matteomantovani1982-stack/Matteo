"""Add role_title, company_name, apply_url to job_runs

Revision ID: 010
Revises: 009
Create Date: 2026-03-14
"""
from alembic import op
import sqlalchemy as sa

revision = '010_role_company_fields'
down_revision = '009_profile_preferences'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('job_runs', sa.Column('role_title', sa.String(255), nullable=True))
    op.add_column('job_runs', sa.Column('company_name', sa.String(255), nullable=True))
    op.add_column('job_runs', sa.Column('apply_url', sa.String(2000), nullable=True))


def downgrade() -> None:
    op.drop_column('job_runs', 'apply_url')
    op.drop_column('job_runs', 'company_name')
    op.drop_column('job_runs', 'role_title')
