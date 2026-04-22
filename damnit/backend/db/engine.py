"""SQLAlchemy engine + session machinery for the DAMNIT Postgres backend."""
import os
from contextlib import contextmanager
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

DEFAULT_URL = "postgresql+psycopg://damnit@localhost/damnit"
ENV_VAR = "DAMNIT_DATABASE_URL"

_engine: Optional[Engine] = None
_Session: Optional[sessionmaker] = None


def database_url() -> str:
    return os.environ.get(ENV_VAR, DEFAULT_URL)


def get_engine() -> Engine:
    global _engine, _Session
    if _engine is None:
        _engine = create_engine(database_url(), future=True, pool_pre_ping=True)
        _Session = sessionmaker(bind=_engine, expire_on_commit=False, future=True)
    return _engine


def get_sessionmaker() -> sessionmaker:
    if _Session is None:
        get_engine()
    return _Session


def reset_engine() -> None:
    """Dispose of the cached engine. Used by tests that swap the URL."""
    global _engine, _Session
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _Session = None


@contextmanager
def session_scope() -> Session:
    sm = get_sessionmaker()
    session = sm()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def pg_advisory_xact_lock(session: Session, key1: int, key2: int) -> None:
    """Acquire a transaction-scoped Postgres advisory lock."""
    session.execute(text("SELECT pg_advisory_xact_lock(:k1, :k2)"),
                    {"k1": key1, "k2": key2})
