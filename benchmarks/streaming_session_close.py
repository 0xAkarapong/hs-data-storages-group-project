"""Simple performance test for closing 10,000 streaming sessions.

Run from the project root: python benchmarks/streaming_session_close.py
"""
import datetime as dt
import sys
import tempfile
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import insert

from db import create_tables, init_engine, tx
from models import EventStatus, Plan, SportsEvent, StreamingSession, SubStatus, Subscription, User
from operations.streaming import end_streaming_session, end_streaming_sessions_bulk

SESSION_COUNT = 10_000


def seed_sessions(count=SESSION_COUNT):
    """Create open sessions and return their ids."""
    now = dt.datetime.now(dt.timezone.utc)
    with tx() as s:
        user = User(email="benchmark@example.com", password_hash="x", display_name="Benchmark")
        plan = Plan(plan_name="Benchmark", monthly_price=Decimal("1.00"),
                    max_concurrent_streams=count)
        event = SportsEvent(title="Benchmark", sport_type="test", start_time=now,
                            status=EventStatus.live)
        s.add_all([user, plan, event])
        s.flush()
        subscription = Subscription(user_id=user.user_id, plan_id=plan.plan_id,
                                    status=SubStatus.active, start_date=now.date(),
                                    end_date=now.date() + dt.timedelta(days=1))
        s.add(subscription)
        s.flush()
        s.execute(insert(StreamingSession), [
            {"user_id": user.user_id, "sports_event_id": event.sports_event_id,
             "subscription_id": subscription.subscription_id, "started_at": now}
            for _ in range(count)
        ])
    return list(range(1, count + 1))


def measure(name, close_sessions, session_ids):
    started = time.perf_counter()
    closed = close_sessions(session_ids)
    seconds = time.perf_counter() - started
    assert closed == SESSION_COUNT
    rps = SESSION_COUNT / seconds
    print(f"{name}: {seconds:.3f}s, {rps:,.0f} sessions/s")
    return rps


def main():
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as database:
        database_path = database.name
    try:
        init_engine(f"sqlite+pysqlite:///{database_path}")
        create_tables(drop_first=True)
        session_ids = seed_sessions()
        one_by_one = measure("one at a time", lambda ids: sum(
            end_streaming_session(session_id) for session_id in ids), session_ids)

        create_tables(drop_first=True)
        session_ids = seed_sessions()
        batch = measure("batch", end_streaming_sessions_bulk, session_ids)
        print(f"speedup: {batch / one_by_one:.1f}x")
    finally:
        Path(database_path).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
