import functools
import random
import time
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

from models import Base

import os
from dotenv import load_dotenv

load_dotenv()

DEFAULT_URL = os.getenv("DATABASE_URL")
_engine = None
SessionLocal: sessionmaker[Session] | None = None


def init_engine(url: str = DEFAULT_URL, echo: bool = False, isolation_level: str = "READ COMMITTED", **pool_options):
    """ Initialize the SQLAlchemy engine and session factory. """
    global _engine, SessionLocal
    engine_options = {"echo": echo, "future": True, "pool_pre_ping": True, **pool_options}
    # SQLite does not accept PostgreSQL's READ COMMITTED isolation-level name.
    # Keeping its default makes the local performance fixture runnable while
    # PostgreSQL continues to use READ COMMITTED by default.
    if not url.startswith("sqlite"):
        engine_options["isolation_level"] = isolation_level
    _engine = create_engine(url, **engine_options)
    SessionLocal = sessionmaker(bind=_engine, expire_on_commit=False, future=True)
    return _engine


def create_tables(drop_first: bool = False):
    """(1) Create every table of the ER diagram."""
    if drop_first:
        Base.metadata.drop_all(_engine)
    Base.metadata.create_all(_engine)


@contextmanager
def tx():
    """One unit of work = one transaction. Commit on success, rollback on anything."""
    s = SessionLocal()
    try:
        with s.begin():
            yield s
    finally:
        s.close()


class BusinessError(Exception):
    """Rule violation — caller's fault, never retried."""


def lock(stmt):
    """FOR UPDATE on PostgreSQL; harmless no-op on SQLite (BEGIN IMMEDIATE already serializes)."""
    return stmt.with_for_update()


RETRYABLE_SQLSTATES = {"40001", "40P01"}  # serialization_failure, deadlock_detected


def retry_on_conflict(attempts: int = 5, base_delay: float = 0.02):
    """Serialization failures/deadlocks are expected, not bugs — retry with backoff."""
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for i in range(attempts):
                try:
                    return fn(*args, **kwargs)
                except (DBAPIError, OperationalError) as exc:
                    orig = getattr(exc, "orig", None)
                    code = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
                    msg = str(orig).lower()
                    transient = code in RETRYABLE_SQLSTATES or "deadlock" in msg or "database is locked" in msg
                    if not transient or i == attempts - 1:
                        raise
                    time.sleep(base_delay * (2 ** i) + random.uniform(0, 0.01))
            return None
        return wrapper
    return deco
