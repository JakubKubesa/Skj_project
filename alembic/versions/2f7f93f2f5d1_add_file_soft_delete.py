"""add_file_soft_delete

Revision ID: 2f7f93f2f5d1
Revises: 037a04d32d9e
Create Date: 2026-04-13 22:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "2f7f93f2f5d1"
down_revision: Union[str, Sequence[str], None] = "037a04d32d9e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table("files", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("is_deleted", sa.Boolean(), nullable=False, server_default=sa.text("0"))
        )

    op.execute(sa.text("UPDATE files SET is_deleted = 0 WHERE is_deleted IS NULL"))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("files", schema=None) as batch_op:
        batch_op.drop_column("is_deleted")
