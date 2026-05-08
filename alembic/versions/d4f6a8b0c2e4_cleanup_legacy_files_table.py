"""cleanup_legacy_files_table

Revision ID: d4f6a8b0c2e4
Revises: c2d4e6f8a0b1
Create Date: 2026-04-24 00:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d4f6a8b0c2e4"
down_revision: Union[str, Sequence[str], None] = "c2d4e6f8a0b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


FILES_TABLE = "files"
OBJECTS_TABLE = "objects"
OLD_BUCKET_INDEX = "ix_files_bucket_id"
OLD_USER_INDEX = "ix_files_user_id"
NEW_BUCKET_INDEX = "ix_objects_bucket_id"
NEW_USER_INDEX = "ix_objects_user_id"


def _table_names(bind) -> set[str]:
    return set(sa.inspect(bind).get_table_names())


def _column_names(bind, table_name: str) -> set[str]:
    return {column["name"] for column in sa.inspect(bind).get_columns(table_name)}


def _index_names(bind, table_name: str) -> set[str]:
    return {index["name"] for index in sa.inspect(bind).get_indexes(table_name)}


def upgrade() -> None:
    """Remove the legacy files table and keep only the objects schema."""
    bind = op.get_bind()
    table_names = _table_names(bind)

    if FILES_TABLE in table_names and OBJECTS_TABLE in table_names:
        file_columns = _column_names(bind, FILES_TABLE)
        object_columns = _column_names(bind, OBJECTS_TABLE)
        if "filename" in file_columns and "object_key" in object_columns:
            op.execute(
                sa.text(
                    "INSERT INTO objects (id, user_id, object_key, path, size, created_at, is_deleted, bucket_id) "
                    "SELECT f.id, f.user_id, f.filename, f.path, f.size, f.created_at, COALESCE(f.is_deleted, 0), f.bucket_id "
                    "FROM files f "
                    "LEFT JOIN objects o ON o.id = f.id "
                    "WHERE o.id IS NULL"
                )
            )
        op.drop_table(FILES_TABLE)

    table_names = _table_names(bind)
    if FILES_TABLE in table_names and OBJECTS_TABLE not in table_names:
        op.rename_table(FILES_TABLE, OBJECTS_TABLE)
        with op.batch_alter_table(OBJECTS_TABLE, schema=None) as batch_op:
            batch_op.alter_column(
                "filename",
                new_column_name="object_key",
                existing_type=sa.String(),
                existing_nullable=False,
            )

    if OBJECTS_TABLE not in _table_names(bind):
        return

    index_names = _index_names(bind, OBJECTS_TABLE)
    with op.batch_alter_table(OBJECTS_TABLE, schema=None) as batch_op:
        if NEW_BUCKET_INDEX not in index_names:
            batch_op.create_index(NEW_BUCKET_INDEX, ["bucket_id"], unique=False)
        if NEW_USER_INDEX not in index_names:
            batch_op.create_index(NEW_USER_INDEX, ["user_id"], unique=False)


def downgrade() -> None:
    """Recreate the legacy files table for a rollback path."""
    bind = op.get_bind()
    table_names = _table_names(bind)
    if FILES_TABLE in table_names:
        return

    op.create_table(
        FILES_TABLE,
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("filename", sa.String(), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("bucket_id", sa.String(), nullable=True),
        sa.Column("is_deleted", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.ForeignKeyConstraint(["bucket_id"], ["buckets.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table(FILES_TABLE, schema=None) as batch_op:
        batch_op.create_index(OLD_BUCKET_INDEX, ["bucket_id"], unique=False)
        batch_op.create_index(OLD_USER_INDEX, ["user_id"], unique=False)

    if OBJECTS_TABLE in _table_names(bind):
        object_columns = _column_names(bind, OBJECTS_TABLE)
        if "object_key" in object_columns:
            op.execute(
                sa.text(
                    "INSERT INTO files (id, user_id, filename, path, size, created_at, is_deleted, bucket_id) "
                    "SELECT id, user_id, object_key, path, size, created_at, is_deleted, bucket_id FROM objects"
                )
            )
