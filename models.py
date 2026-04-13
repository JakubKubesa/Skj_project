import uuid
from sqlalchemy import String, DateTime, Integer  # Přidán Integer
from sqlalchemy.orm import Mapped, mapped_column
from database import Base
from datetime import datetime

class FileModel(Base):
    __tablename__ = "files"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(String, index=True)
    filename: Mapped[str] = mapped_column(String)
    path: Mapped[str] = mapped_column(String)
    size: Mapped[int] = mapped_column(Integer)  # Opraveno z (int) na (Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now)