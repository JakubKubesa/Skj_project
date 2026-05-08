"""rename_files_to_objects

Revision ID: c2d4e6f8a0b1
Revises: b1a2c3d4e5f6
Create Date: 2026-04-24 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c2d4e6f8a0b1"
down_revision: Union[str, Sequence[str], None] = "b1a2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


OLD_TABLE = "files"
NEW_TABLE = "objects"
OLD_COLUMN = "filename"
NEW_COLUMN = "object_key"
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
    """Upgrade schema."""
    bind = op.get_bind()
    table_names = _table_names(bind)

    if OLD_TABLE in table_names and NEW_TABLE not in table_names:
        op.rename_table(OLD_TABLE, NEW_TABLE)

    table_names = _table_names(bind)
    if NEW_TABLE not in table_names:
        return

    column_names = _column_names(bind, NEW_TABLE)
    if OLD_COLUMN in column_names and NEW_COLUMN not in column_names:
        with op.batch_alter_table(NEW_TABLE, schema=None) as batch_op:
            batch_op.alter_column(
                OLD_COLUMN,
                new_column_name=NEW_COLUMN,
                existing_type=sa.String(),
                existing_nullable=False,
            )

    index_names = _index_names(bind, NEW_TABLE)
    with op.batch_alter_table(NEW_TABLE, schema=None) as batch_op:
        if OLD_BUCKET_INDEX in index_names:
            batch_op.drop_index(OLD_BUCKET_INDEX)
        if OLD_USER_INDEX in index_names:
            batch_op.drop_index(OLD_USER_INDEX)

    index_names = _index_names(bind, NEW_TABLE)
    with op.batch_alter_table(NEW_TABLE, schema=None) as batch_op:
        if NEW_BUCKET_INDEX not in index_names:
            batch_op.create_index(NEW_BUCKET_INDEX, ["bucket_id"], unique=False)
        if NEW_USER_INDEX not in index_names:
            batch_op.create_index(NEW_USER_INDEX, ["user_id"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    bind = op.get_bind()
    table_names = _table_names(bind)
    if NEW_TABLE not in table_names:
        return

    index_names = _index_names(bind, NEW_TABLE)
    with op.batch_alter_table(NEW_TABLE, schema=None) as batch_op:
        if NEW_BUCKET_INDEX in index_names:
            batch_op.drop_index(NEW_BUCKET_INDEX)
        if NEW_USER_INDEX in index_names:
            batch_op.drop_index(NEW_USER_INDEX)

    column_names = _column_names(bind, NEW_TABLE)
    if NEW_COLUMN in column_names and OLD_COLUMN not in column_names:
        with op.batch_alter_table(NEW_TABLE, schema=None) as batch_op:
            batch_op.alter_column(
                NEW_COLUMN,
                new_column_name=OLD_COLUMN,
                existing_type=sa.String(),
                existing_nullable=False,
            )

    index_names = _index_names(bind, NEW_TABLE)
    with op.batch_alter_table(NEW_TABLE, schema=None) as batch_op:
        if OLD_BUCKET_INDEX not in index_names:
            batch_op.create_index(OLD_BUCKET_INDEX, ["bucket_id"], unique=False)
        if OLD_USER_INDEX not in index_names:
            batch_op.create_index(OLD_USER_INDEX, ["user_id"], unique=False)

    table_names = _table_names(bind)
    if NEW_TABLE in table_names and OLD_TABLE not in table_names:
        op.rename_table(NEW_TABLE, OLD_TABLE)
