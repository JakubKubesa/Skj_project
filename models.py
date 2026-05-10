"""SQLAlchemy ORM models for users, buckets, stored objects, and broker messages."""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, LargeBinary, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base


class Bucket(Base):
    """Bucket metadata plus billing counters for the personal cloud API."""

    __tablename__ = "buckets"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    user_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("users.id"), index=True, nullable=True)

    current_storage_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    ingress_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    egress_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    internal_transfer_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    count_write_requests: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    count_read_requests: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)

    # Relationship used by the bucket/object API. Object rows stay in the table
    # even after soft delete; API queries filter out deleted rows explicitly.
    objects = relationship("ObjectModel", back_populates="bucket")
    owner = relationship("User", back_populates="buckets")


class User(Base):
    """Application user who owns one or more personal buckets."""

    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    username: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False)

    buckets = relationship("Bucket", back_populates="owner")
    sessions = relationship("AuthSession", back_populates="user")


class AuthSession(Base):
    """Bearer token session used by the web client."""

    __tablename__ = "auth_sessions"

    token: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[str] = mapped_column(String, ForeignKey("users.id"), index=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False)

    user = relationship("User", back_populates="sessions")


class ObjectModel(Base):
    """Stored object metadata tracked in the bucket/object API."""

    __tablename__ = "objects"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(String, index=True)
    object_key: Mapped[str] = mapped_column(String)
    path: Mapped[str] = mapped_column(String)
    size: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0", nullable=False)

    bucket_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("buckets.id"), index=True, nullable=True)
    bucket = relationship("Bucket", back_populates="objects")
    # New fields for Haystack integration: store where the object was written
    volume_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    offset: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Upload status used by the Gateway: 'uploading' | 'ready'
    status: Mapped[str] = mapped_column(String, default="ready", server_default="ready", nullable=False)


class QueuedMessage(Base):
    """Durable message persisted by the broker until acknowledged."""

    __tablename__ = "queued_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String, index=True, nullable=False)
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    payload_format: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False)
    is_delivered: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0", nullable=False)
