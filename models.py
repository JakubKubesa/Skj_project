import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, LargeBinary, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base


class Bucket(Base):
    __tablename__ = "buckets"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    current_storage_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    ingress_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    egress_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    internal_transfer_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    count_write_requests: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    count_read_requests: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)

    files = relationship("FileModel", back_populates="bucket")


class FileModel(Base):
    __tablename__ = "files"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(String, index=True)
    filename: Mapped[str] = mapped_column(String)
    path: Mapped[str] = mapped_column(String)
    size: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0", nullable=False)

    bucket_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("buckets.id"), index=True, nullable=True)
    bucket = relationship("Bucket", back_populates="files")


class QueuedMessage(Base):
    __tablename__ = "queued_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String, index=True, nullable=False)
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    payload_format: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False)
    is_delivered: Mapped[bool] = mapped_column(Boolean, default=False, server_default="0", nullable=False)
