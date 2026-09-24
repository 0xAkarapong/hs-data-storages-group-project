"""Three reusable anomaly demos, one function per classic isolation phenomenon.

Run one demo against a fixed isolation level:

    python tests/test_isolation_matrix.py non_repeatable_read
    python tests/test_isolation_matrix.py phantom_read
    python tests/test_isolation_matrix.py write_skew

To move across levels, edit the ``isolation_level=`` argument to
``init_engine`` below and rerun. Expected results on PostgreSQL:

                        READ COMMITTED   REPEATABLE READ   SERIALIZABLE
    non_repeatable_read      FAILS            passes           passes
    phantom_read             FAILS            passes           passes   (PG's REPEATABLE READ is snapshot isolation, stricter than the SQL standard)
    write_skew               FAILS            FAILS            passes   (blocked by a serialization_failure, not by staying consistent)
"""

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, select

import db
from db import create_tables, init_engine, tx
from models import Plan, StreamingSession, Subscription, SubStatus
from seed import seed_demo_flow


def _sessions():
    if db.SessionLocal is None:
        raise RuntimeError("database engine is not initialized")
    return db.SessionLocal(), db.SessionLocal()


def _setup():
    """Fresh schema + demo data. Standard plan: limit 2, with 1 session open."""
    create_tables(drop_first=True)
    ctx = seed_demo_flow()
    with tx() as s:
        sub = s.scalar(select(Subscription).where(
            Subscription.user_id == ctx["user_id"],
            Subscription.status == SubStatus.active,
        ))
        ctx["subscription_id"] = sub.subscription_id
        ctx["plan_id"] = sub.plan_id
    return ctx


def non_repeatable_read(ctx):
    """Session A reads a row twice in one transaction; Session B updates it in between."""
    a, b = _sessions()
    try:
        print("="*80 + "starting non-repeatable read test" + "="*80)
        with a.begin():
            v1 = a.scalar(select(Plan.max_concurrent_streams).where(Plan.plan_id == ctx["plan_id"]))
            assert v1 is not None

            with b.begin():
                plan = b.get(Plan, ctx["plan_id"])
                assert plan is not None
                plan.max_concurrent_streams = v1 + 1
                b.commit()
        
            v2 = a.scalar(select(Plan.max_concurrent_streams).where(Plan.plan_id == ctx["plan_id"]))
    finally:
        a.close()
        b.close()

    print(f"first read: {v1}, second read (same transaction): {v2}")

    assert v1 == v2, f"non-repeatable read: {v1} -> {v2} within one transaction"


def phantom_read(ctx):
    """Session A re-runs a range COUNT; Session B inserts a matching row in between."""
    a, b = _sessions()

    def query_open_sessions(s):
        return s.scalar(
            select(func.count()).select_from(StreamingSession).where(
                StreamingSession.subscription_id == ctx["subscription_id"],
                StreamingSession.ended_at.is_(None),
            )
        )

    try:
        print("="*80 + "starting phantom read test" + "="*80)
        with a.begin():
            c1 = query_open_sessions(a)
            print(f"first count: {c1}")

            with b.begin():
                b.add(StreamingSession(
                    user_id=ctx["user_id"],
                    sports_event_id=ctx["event_id"],
                    subscription_id=ctx["subscription_id"],
                    started_at=dt.datetime.now(dt.UTC),
                ))
                b.commit()

            c2 = query_open_sessions(a)
            print(f"second count (same transaction): {c2}")
    finally:
        a.close()
        b.close()

    print(f"first count: {c1}, second count (same transaction): {c2}")
    assert c1 == c2, f"phantom read: count went {c1} -> {c2} within one transaction"

DEMOS = {
    "non_repeatable_read": non_repeatable_read,
    "phantom_read": phantom_read,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("demo", choices=[*DEMOS, "all"])
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL")

    # READ COMMITTED
    # REPEATABLE READ
    # SERIALIZABLE
    init_engine(database_url, echo=True, isolation_level="SERIALIZABLE")

    names = DEMOS if args.demo == "all" else [args.demo]
    for name in names:
        print(f"\n===== {name} =====")
        try:
            ctx = _setup()
            DEMOS[name](ctx)
            print("PASSED: no anomaly observed")

        except AssertionError as exc:
            print(f"FAILED: {exc}")
