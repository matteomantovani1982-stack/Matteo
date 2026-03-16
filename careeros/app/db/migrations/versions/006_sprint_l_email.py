"""
006_sprint_l_email

Email verification and password reset support.
No schema changes needed — is_verified column already exists on users table
(added in 002_users_profiles).

This migration is a no-op but establishes the migration chain for Sprint L.
"""

from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # is_verified already exists — just ensure index exists for lookup speed
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_users_email_verified "
        "ON users (email, is_verified)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_users_email_verified")
