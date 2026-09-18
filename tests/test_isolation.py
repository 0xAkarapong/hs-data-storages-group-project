import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db import DEFAULT_URL, create_tables, init_engine, tx
from models import User


@pytest.mark.parametrize(
    ("isolation_level", "expected_second_read"),
    [
        pytest.param("READ COMMITTED", "updated", id="read-committed"),
        pytest.param("REPEATABLE READ", "initial", id="repeatable-read"),
    ],
)
def test_visibility_of_committed_update(isolation_level, expected_second_read):
    """Compare snapshot visibility across two real PostgreSQL transactions."""
    if not DEFAULT_URL:
        pytest.skip("DATABASE_URL is not configured")

    try:
        engine = init_engine(DEFAULT_URL, isolation_level=isolation_level)
    except (SQLAlchemyError, ValueError) as exc:
        pytest.skip(f"database configuration is unavailable: {exc}")

    if engine.dialect.name != "postgresql":
        engine.dispose()
        pytest.skip("transaction isolation test requires PostgreSQL")

    try:
        with engine.connect():
            pass
        create_tables()
    except SQLAlchemyError as exc:
        engine.dispose()
        pytest.skip(f"PostgreSQL is unavailable: {exc}")

    token = uuid4().hex
    email = f"isolation-{token}@example.com"
    first_read_done = Event()
    writer_committed = Event()

    with tx() as session:
        user = User(
            email=email,
            password_hash="test-only",
            display_name="initial",
        )
        session.add(user)
        session.flush()
        user_id = user.user_id

    def read_twice():
        with tx() as session:
            first = session.scalar(
                select(User.display_name).where(User.user_id == user_id)
            )
            first_read_done.set()
            if not writer_committed.wait(timeout=5):
                raise TimeoutError("writer did not commit within 5 seconds")
            second = session.scalar(
                select(User.display_name).where(User.user_id == user_id)
            )
            return first, second

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            reader = executor.submit(read_twice)
            if not first_read_done.wait(timeout=5):
                writer_committed.set()
                raise TimeoutError("reader did not start within 5 seconds")

            try:
                with tx() as session:
                    session.execute(
                        update(User)
                        .where(User.user_id == user_id)
                        .values(display_name="updated")
                    )
            finally:
                writer_committed.set()

            first, second = reader.result(timeout=5)

        assert first == "initial"
        assert second == expected_second_read
    finally:
        with tx() as session:
            session.execute(delete(User).where(User.user_id == user_id))
        engine.dispose()
