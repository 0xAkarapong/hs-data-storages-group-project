"""Compare 300 concurrent record_ping calls and show Redis's failure cost.

Uses SCHEMA_NAME from .env (the pre-created schema, e.g. drake888) — this
role has no CREATE SCHEMA privilege on the real RDS (checked, permission
denied), so unlike an earlier version of this script it never creates or
drops its own schema. Instead it seeds one fresh SportsEvent/StreamingSession
via seed_event_with_session() and deletes only those rows afterward, never
touching teammates' existing data in the shared schema.

Run: python benchmarks/redis_ping_boost.py
"""
import os
import statistics
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import delete, select
from sqlalchemy.engine import URL

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from db import create_tables, init_engine, tx
from models import (
    SCHEMA_NAME,
    Bet,
    OddsSnapshot,
    Outcome,
    Payment,
    Plan,
    SportsEvent,
    StreamingSession,
    Subscription,
    User,
)
from operations.engagement import ping_key, record_ping, record_ping_redis
from redis_client import init_redis
from seed import seed_event_with_session, seed_plans

WORKERS = 300
CALLS = 300


def _existing_or_seeded_plan_id(s) -> int:
    """Reuse a plan already in the shared schema; only seed one if the schema is
    genuinely empty (local dev), never seed_plans() unconditionally — that would
    duplicate teammates' Basic/Standard/Premium rows in the shared drake888 schema."""
    plan_id = s.scalar(select(Plan.plan_id).limit(1))
    if plan_id is not None:
        return plan_id
    return seed_plans(s)["Basic"]


def run(fn, session_id):
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = list(pool.map(fn, [session_id] * CALLS))
    seconds = time.perf_counter() - started
    assert all(result["latest_price"] is not None for result in results)
    assert sorted(result["ping_count"] for result in results) == list(range(1, CALLS + 1))
    return seconds


def demonstrate_failed_ping(session_id, event_id, redis, key):
    """The same failed request rolls back in SQL but survives in Redis."""
    with tx() as s:
        s.get(SportsEvent, event_id).ping_count = 0
    redis.delete(key)

    for fn in (record_ping, record_ping_redis):
        try:
            fn(session_id, fail_after_increment=True)
        except RuntimeError as exc:
            print(f"{fn.__name__}: {exc}")
        else:
            raise AssertionError(f"{fn.__name__} did not fail")

        with tx() as s:
            sql_count = s.get(SportsEvent, event_id).ping_count
        redis_count = int(redis.get(key) or 0)
        expected = (0, 0) if fn is record_ping else (0, 1)
        assert (sql_count, redis_count) == expected, (sql_count, redis_count)
        print(f"after failed {fn.__name__}: SQL={sql_count}, Redis={redis_count}")


def cleanup(event_id: int, user_id: int) -> None:
    """Delete only the rows this run created, in FK-safe (child-first) order —
    never DROP SCHEMA on the shared drake888 schema."""
    with tx() as s:
        s.execute(delete(Payment).where(Payment.user_id == user_id))
        s.execute(delete(Bet).where(Bet.user_id == user_id))
        s.execute(delete(StreamingSession).where(StreamingSession.user_id == user_id))
        s.execute(delete(Subscription).where(Subscription.user_id == user_id))
        s.execute(delete(User).where(User.user_id == user_id))
        outcome_ids = s.scalars(select(Outcome.outcome_id).where(Outcome.sports_event_id == event_id)).all()
        if outcome_ids:
            s.execute(delete(OddsSnapshot).where(OddsSnapshot.outcome_id.in_(outcome_ids)))
        s.execute(delete(Outcome).where(Outcome.sports_event_id == event_id))
        s.execute(delete(SportsEvent).where(SportsEvent.sports_event_id == event_id))


def main():
    url = URL.create(
        "postgresql+psycopg",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ["POSTGRES_DB"],
    )
    # Pool sizes are matched to redis_client.py's MAX_CONNECTIONS so this is a fair
    # comparison of per-call cost at equal concurrency, not pool-size vs pool-size —
    # a wide SQL pool against a classroom-safe-capped Redis pool would make the SQL
    # path look faster purely because it has 9x the concurrent connections.
    from redis_client import MAX_CONNECTIONS
    engine = init_engine(url.render_as_string(hide_password=False),
                         pool_size=MAX_CONNECTIONS, max_overflow=0, pool_timeout=60)
    redis = init_redis()
    redis.ping()
    create_tables()  # additive only — creates missing tables, never drops/alters existing ones

    with tx() as s:
        plan_id = _existing_or_seeded_plan_id(s)
    tag = f"bench_ping_{uuid.uuid4().hex[:12]}"
    event_id, session_id = seed_event_with_session(plan_id, tag)
    key = ping_key(event_id)

    try:
        times = {"sql": [], "redis": []}

        for name in ("sql", "redis", "redis", "sql", "sql", "redis"):
            with tx() as s:
                s.get(SportsEvent, event_id).ping_count = 0
            redis.delete(key)

            seconds = run(record_ping if name == "sql" else record_ping_redis, session_id)
            times[name].append(seconds)
            with tx() as s:
                sql_count = s.get(SportsEvent, event_id).ping_count
            redis_count = int(redis.get(key) or 0)
            assert (sql_count, redis_count) == ((CALLS, 0) if name == "sql" else (0, CALLS))
            print(f"{name}: {seconds:.3f}s, {CALLS / seconds:,.0f} req/s")

        sql_rps = CALLS / statistics.median(times["sql"])
        redis_rps = CALLS / statistics.median(times["redis"])
        print(f"median: SQL {sql_rps:,.0f} req/s, Redis {redis_rps:,.0f} req/s, {redis_rps / sql_rps:.2f}x")
        demonstrate_failed_ping(session_id, event_id, redis, key)
    finally:
        with tx() as s:
            user_id = s.scalar(select(StreamingSession.user_id).where(StreamingSession.session_id == session_id))
        try:
            redis.delete(key)
        finally:  # a Redis error here must not leave this run's rows behind
            cleanup(event_id, user_id)


if __name__ == "__main__":
    main()
