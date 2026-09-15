import functools
import random
import time
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

from models import Base

DEFAULT_URL = "postgresql+psycopg://app:app@localhost:5432/oltp_demo"
_engine = None
SessionLocal: sessionmaker[Session] | None = None


def init_engine(url: str = DEFAULT_URL, echo: bool = False):
    """REPEATABLE READ + explicit row locks is our baseline isolation."""
    global _engine, SessionLocal
    if url.startswith("sqlite"):
        _engine = create_engine(url, echo=echo, future=True)

        @event.listens_for(_engine, "connect")
        def _fk_on(dbapi_conn, _):
            dbapi_conn.execute("PRAGMA foreign_keys=ON")
            dbapi_conn.isolation_level = None  # we drive transactions ourselves

        @event.listens_for(_engine, "begin")
        def _begin_immediate(conn):
            conn.exec_driver_sql("BEGIN IMMEDIATE")  # write lock from statement #1
    else:
        _engine = create_engine(url, echo=echo, future=True, pool_pre_ping=True,
                                isolation_level="REPEATABLE READ")
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
