"""Demonstrate and fix stream-limit double-booking at READ COMMITTED.

Run the broken version with ``python tests/test_isolation.py unsafe`` and the
locked production version with ``python tests/test_isolation.py fixed``.
"""

import argparse
import datetime as dt
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, select

from db import BusinessError, create_tables, init_engine, tx
from models import EventStatus, Plan, SportsEvent, StreamingSession, SubStatus, Subscription
from operations.streaming import start_streaming_session
from seed import seed_demo_flow


WORKERS = 5


def start_streaming_session_without_lock(
    user_id: int,
    sports_event_id: int,
    all_counts_read: Barrier,
) -> int:
    """The intentionally broken, pre-fix operation used only by this demo."""
    with tx() as session:
        subscription = session.scalar(select(Subscription).where(
            Subscription.user_id == user_id,
            Subscription.status == SubStatus.active,
        ))
        if not subscription:
            raise BusinessError("no active subscription")

        plan = session.get(Plan, subscription.plan_id)
        event = session.get(SportsEvent, sports_event_id)

        if not event or event.status not in (EventStatus.scheduled, EventStatus.live):
            raise BusinessError("event not streamable")

        open_streams = session.scalar(
            select(func.count()).select_from(StreamingSession).where(
                StreamingSession.subscription_id == subscription.subscription_id,
                StreamingSession.ended_at.is_(None),
            )
        )

        # Force every transaction to make its decision from the same stale count.
        all_counts_read.wait(timeout=10)
        if open_streams >= plan.max_concurrent_streams:
            raise BusinessError("concurrent stream limit reached")

        # Start join the stream. This is the point where the race condition can cause double-booking.
        streaming_session = StreamingSession(
            user_id=user_id,
            sports_event_id=sports_event_id,
            subscription_id=subscription.subscription_id,
            started_at=dt.datetime.now(dt.timezone.utc),
        )

        session.add(streaming_session)
        session.flush()
        return streaming_session.session_id


def run_concurrently(mode: str, user_id: int, event_id: int):
    barrier = Barrier(WORKERS)

    def invoke(_):
        try:
            if mode == "unsafe":
                return start_streaming_session_without_lock(
                    user_id, event_id, barrier
                )

            barrier.wait(timeout=10)
            return start_streaming_session(user_id, event_id)
        except BusinessError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        return list(executor.map(invoke, range(WORKERS)))


def _prepare_database(database_url: str) -> dict:
    init_engine(database_url, isolation_level="READ COMMITTED")
    create_tables(drop_first=True)
    return seed_demo_flow()  # Standard plan: limit 2, with 1 session open.


def _load_open_sessions(user_id: int):
    """Plan limit plus every still-open streaming session for that user."""
    with tx() as session:
        subscription = session.scalar(select(Subscription).where(
            Subscription.user_id == user_id,
            Subscription.status == SubStatus.active,
        ))

        plan = session.get(Plan, subscription.plan_id)
        rows = session.scalars(select(StreamingSession).where(
            StreamingSession.subscription_id == subscription.subscription_id,
            StreamingSession.ended_at.is_(None),
        ).order_by(StreamingSession.session_id)).all()

        return plan, rows


def _report(mode: str, plan: Plan, rows: list[StreamingSession], results: list):
    over_limit = len(rows) > plan.max_concurrent_streams
    print(f"mode: {mode}")
    print(f"plan limit: {plan.max_concurrent_streams}")
    print(f"open sessions: {len(rows)}")
    print(f"worker results: {results}")
    for row in rows:
        label = "corrupted row:" if over_limit else "session row:"
        print(label, {
            "session_id": row.session_id,
            "subscription_id": row.subscription_id,
            "ended_at": row.ended_at,
        })


def assert_stream_limit(mode: str):
    context = _prepare_database(os.getenv("DATABASE_URL"))

    results = run_concurrently(mode, context["user_id"], context
    ["event_id"])
    plan, rows = _load_open_sessions(context["user_id"])

    _report(mode, plan, rows, results)

    assert len(rows) <= plan.max_concurrent_streams, (
        f"double-booking: plan permits {plan.max_concurrent_streams} open "
        f"sessions but database contains {len(rows)}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("unsafe", "fixed"))
    arguments = parser.parse_args()

    assert_stream_limit(arguments.mode)
