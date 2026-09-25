"""Compare 300 concurrent record_ping calls and show Redis's failure cost.

Uses an isolated schema on the PostgreSQL instance in .env and removes it afterward.
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
from sqlalchemy.engine import URL

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

SCHEMA = f"bench_ping_{uuid.uuid4().hex[:12]}"
os.environ["SCHEMA_NAME"] = SCHEMA

from db import create_tables, init_engine, tx
from models import SportsEvent
from operations.engagement import record_ping, record_ping_redis
from redis_client import init_redis
from seed import seed_demo_flow

WORKERS = 300
CALLS = 300


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


def main():
    url = URL.create(
        "postgresql+psycopg",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ["POSTGRES_DB"],
    )
    engine = init_engine(url.render_as_string(hide_password=False), pool_size=20, max_overflow=70, pool_timeout=60)
    redis = init_redis()
    redis.ping()
    key = f"event:{SCHEMA}:1:pings"

    with engine.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA "{SCHEMA}"')
    try:
        create_tables()
        ctx = seed_demo_flow()
        key = f"event:{SCHEMA}:{ctx['event_id']}:pings"
        times = {"sql": [], "redis": []}

        for name in ("sql", "redis", "redis", "sql", "sql", "redis"):
            with tx() as s:
                s.get(SportsEvent, ctx["event_id"]).ping_count = 0
            redis.delete(key)

            seconds = run(record_ping if name == "sql" else record_ping_redis, ctx["session_id"])
            times[name].append(seconds)
            with tx() as s:
                sql_count = s.get(SportsEvent, ctx["event_id"]).ping_count
            redis_count = int(redis.get(key) or 0)
            assert (sql_count, redis_count) == ((CALLS, 0) if name == "sql" else (0, CALLS))
            print(f"{name}: {seconds:.3f}s, {CALLS / seconds:,.0f} req/s")

        sql_rps = CALLS / statistics.median(times["sql"])
        redis_rps = CALLS / statistics.median(times["redis"])
        print(f"median: SQL {sql_rps:,.0f} req/s, Redis {redis_rps:,.0f} req/s, {redis_rps / sql_rps:.2f}x")
        demonstrate_failed_ping(ctx["session_id"], ctx["event_id"], redis, key)
    finally:
        redis.delete(key)
        with engine.begin() as conn:
            conn.exec_driver_sql(f'DROP SCHEMA "{SCHEMA}" CASCADE')


if __name__ == "__main__":
    main()
