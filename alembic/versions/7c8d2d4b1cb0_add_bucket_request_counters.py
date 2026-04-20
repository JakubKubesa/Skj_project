"""add_bucket_request_counters

Revision ID: 7c8d2d4b1cb0
Revises: 2f7f93f2f5d1
Create Date: 2026-04-13 22:31:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "7c8d2d4b1cb0"
down_revision: Union[str, Sequence[str], None] = "2f7f93f2f5d1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table("buckets", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("count_write_requests", sa.Integer(), nullable=False, server_default="0")
        )
        batch_op.add_column(
            sa.Column("count_read_requests", sa.Integer(), nullable=False, server_default="0")
        )

    op.execute(
        sa.text(
            "UPDATE buckets "
            "SET count_write_requests = 0, count_read_requests = 0 "
            "WHERE count_write_requests IS NULL OR count_read_requests IS NULL"
        )
    )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("buckets", schema=None) as batch_op:
        batch_op.drop_column("count_read_requests")
        batch_op.drop_column("count_write_requests")
