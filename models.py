import uuid
from sqlalchemy import String, DateTime, Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database import Base
from datetime import datetime
from typing import Optional


class Bucket(Base):
    __tablename__ = "buckets"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # Billing counters
    current_storage_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    ingress_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    egress_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    internal_transfer_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)

    # relationship placeholder for convenience
    files = relationship("FileModel", back_populates="bucket")


class FileModel(Base):
    __tablename__ = "files"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(String, index=True)
    filename: Mapped[str] = mapped_column(String)
    path: Mapped[str] = mapped_column(String)
    size: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)

    # Optional link to bucket
    bucket_id: Mapped[Optional[str]] = mapped_column(String, ForeignKey("buckets.id"), index=True, nullable=True)
    bucket = relationship("Bucket", back_populates="files")
