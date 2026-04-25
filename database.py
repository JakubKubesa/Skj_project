"""Database configuration shared by the REST API, worker, and tests.

The project uses a local SQLite database for metadata, bucket accounting, and
queued broker messages. SQLAlchemy sessions are created through SessionLocal.
"""

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

BASE_DIR = Path(__file__).resolve().parent
DATABASE_PATH = BASE_DIR / "metadata.db"
SQLALCHEMY_DATABASE_URL = f"sqlite:///{DATABASE_PATH.as_posix()}"

engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    """Declarative base for all ORM models in the project."""



def get_db():
    """Yield one SQLAlchemy session for a FastAPI request lifecycle."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
