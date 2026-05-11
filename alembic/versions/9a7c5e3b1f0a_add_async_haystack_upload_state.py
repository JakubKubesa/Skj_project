"""add_async_haystack_upload_state

Revision ID: 9a7c5e3b1f0a
Revises: e5f7a9c1d2b3
Create Date: 2026-05-11 16:45:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "9a7c5e3b1f0a"
down_revision: Union[str, Sequence[str], None] = "e5f7a9c1d2b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


OBJECTS_TABLE = "objects"
STORAGE_OBJECT_INDEX = "ix_objects_storage_object_id"


def _table_names(bind) -> set[str]:
    return set(sa.inspect(bind).get_table_names())


def _column_names(bind, table_name: str) -> set[str]:
    return {column["name"] for column in sa.inspect(bind).get_columns(table_name)}


def _index_names(bind, table_name: str) -> set[str]:
    return {index["name"] for index in sa.inspect(bind).get_indexes(table_name)}


def upgrade() -> None:
    """Add object state needed for async Haystack-backed writes."""
    bind = op.get_bind()
    if OBJECTS_TABLE not in _table_names(bind):
        return

    column_names = _column_names(bind, OBJECTS_TABLE)
    with op.batch_alter_table(OBJECTS_TABLE, schema=None) as batch_op:
        if "status" not in column_names:
            batch_op.add_column(sa.Column("status", sa.String(), nullable=False, server_default="ready"))
        if "storage_object_id" not in column_names:
            batch_op.add_column(sa.Column("storage_object_id", sa.String(), nullable=True))
        if "volume_id" not in column_names:
            batch_op.add_column(sa.Column("volume_id", sa.Integer(), nullable=True))
        if "offset" not in column_names:
            batch_op.add_column(sa.Column("offset", sa.Integer(), nullable=True))
        if "pending_previous_size" not in column_names:
            batch_op.add_column(sa.Column("pending_previous_size", sa.Integer(), nullable=False, server_default="0"))
        if "pending_is_internal" not in column_names:
            batch_op.add_column(sa.Column("pending_is_internal", sa.Boolean(), nullable=False, server_default=sa.text("0")))

    op.execute(sa.text("UPDATE objects SET status = COALESCE(status, 'ready')"))
    op.execute(sa.text("UPDATE objects SET pending_previous_size = COALESCE(pending_previous_size, 0)"))
    op.execute(sa.text("UPDATE objects SET pending_is_internal = COALESCE(pending_is_internal, 0)"))

    index_names = _index_names(bind, OBJECTS_TABLE)
    with op.batch_alter_table(OBJECTS_TABLE, schema=None) as batch_op:
        if STORAGE_OBJECT_INDEX not in index_names:
            batch_op.create_index(STORAGE_OBJECT_INDEX, ["storage_object_id"], unique=False)


def downgrade() -> None:
    """Remove async Haystack upload state columns."""
    bind = op.get_bind()
    if OBJECTS_TABLE not in _table_names(bind):
        return

    index_names = _index_names(bind, OBJECTS_TABLE)
    column_names = _column_names(bind, OBJECTS_TABLE)

    with op.batch_alter_table(OBJECTS_TABLE, schema=None) as batch_op:
        if STORAGE_OBJECT_INDEX in index_names:
            batch_op.drop_index(STORAGE_OBJECT_INDEX)
        if "pending_is_internal" in column_names:
            batch_op.drop_column("pending_is_internal")
        if "pending_previous_size" in column_names:
            batch_op.drop_column("pending_previous_size")
        if "offset" in column_names:
            batch_op.drop_column("offset")
        if "volume_id" in column_names:
            batch_op.drop_column("volume_id")
        if "storage_object_id" in column_names:
            batch_op.drop_column("storage_object_id")
        if "status" in column_names:
            batch_op.drop_column("status")
