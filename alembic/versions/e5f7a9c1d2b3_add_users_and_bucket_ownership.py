"""add_users_and_bucket_ownership

Revision ID: e5f7a9c1d2b3
Revises: d4f6a8b0c2e4
Create Date: 2026-05-08 13:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e5f7a9c1d2b3"
down_revision: Union[str, Sequence[str], None] = "d4f6a8b0c2e4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_names(bind) -> set[str]:
    return set(sa.inspect(bind).get_table_names())


def _column_names(bind, table_name: str) -> set[str]:
    return {column["name"] for column in sa.inspect(bind).get_columns(table_name)}


def _index_names(bind, table_name: str) -> set[str]:
    return {index["name"] for index in sa.inspect(bind).get_indexes(table_name)}


def upgrade() -> None:
    """Create auth tables and link buckets to users."""
    bind = op.get_bind()
    table_names = _table_names(bind)

    if "users" not in table_names:
        op.create_table(
            "users",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("username", sa.String(), nullable=False),
            sa.Column("password_hash", sa.String(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index("ix_users_username", "users", ["username"], unique=True)

    table_names = _table_names(bind)
    if "auth_sessions" not in table_names:
        op.create_table(
            "auth_sessions",
            sa.Column("token", sa.String(), nullable=False),
            sa.Column("user_id", sa.String(), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
            sa.PrimaryKeyConstraint("token"),
        )
        op.create_index("ix_auth_sessions_user_id", "auth_sessions", ["user_id"], unique=False)

    if "buckets" in _table_names(bind) and "user_id" not in _column_names(bind, "buckets"):
        with op.batch_alter_table("buckets", schema=None) as batch_op:
            batch_op.add_column(sa.Column("user_id", sa.String(), nullable=True))
            batch_op.create_foreign_key("fk_buckets_user_id_users", "users", ["user_id"], ["id"])

    if "buckets" in _table_names(bind) and "ix_buckets_user_id" not in _index_names(bind, "buckets"):
        with op.batch_alter_table("buckets", schema=None) as batch_op:
            batch_op.create_index("ix_buckets_user_id", ["user_id"], unique=False)


def downgrade() -> None:
    """Remove user auth tables and bucket ownership."""
    bind = op.get_bind()
    if "buckets" in _table_names(bind) and "user_id" in _column_names(bind, "buckets"):
        index_names = _index_names(bind, "buckets")
        with op.batch_alter_table("buckets", schema=None) as batch_op:
            if "ix_buckets_user_id" in index_names:
                batch_op.drop_index("ix_buckets_user_id")
            batch_op.drop_constraint("fk_buckets_user_id_users", type_="foreignkey")
            batch_op.drop_column("user_id")

    if "auth_sessions" in _table_names(bind):
        index_names = _index_names(bind, "auth_sessions")
        if "ix_auth_sessions_user_id" in index_names:
            op.drop_index("ix_auth_sessions_user_id", table_name="auth_sessions")
        op.drop_table("auth_sessions")

    if "users" in _table_names(bind):
        index_names = _index_names(bind, "users")
        if "ix_users_username" in index_names:
            op.drop_index("ix_users_username", table_name="users")
        op.drop_table("users")
