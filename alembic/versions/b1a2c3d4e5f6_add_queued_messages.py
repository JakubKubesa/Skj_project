"""add_queued_messages

Revision ID: b1a2c3d4e5f6
Revises: 7c8d2d4b1cb0
Create Date: 2026-04-20 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b1a2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "7c8d2d4b1cb0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "queued_messages" not in inspector.get_table_names():
        op.create_table(
            "queued_messages",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("topic", sa.String(), nullable=False),
            sa.Column("payload", sa.LargeBinary(), nullable=False),
            sa.Column("payload_format", sa.String(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.Column("is_delivered", sa.Boolean(), nullable=False, server_default=sa.text("0")),
            sa.PrimaryKeyConstraint("id"),
        )

    inspector = sa.inspect(bind)
    indexes = {index["name"] for index in inspector.get_indexes("queued_messages")}
    if op.f("ix_queued_messages_topic") not in indexes:
        op.create_index(op.f("ix_queued_messages_topic"), "queued_messages", ["topic"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if "queued_messages" in inspector.get_table_names():
        indexes = {index["name"] for index in inspector.get_indexes("queued_messages")}
        if op.f("ix_queued_messages_topic") in indexes:
            op.drop_index(op.f("ix_queued_messages_topic"), table_name="queued_messages")
        op.drop_table("queued_messages")
