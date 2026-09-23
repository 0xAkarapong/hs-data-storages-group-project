"""Compare 300 concurrent record_ping calls with SQL and Redis counters.

Uses an isolated schema on local PostgreSQL and removes it afterward.
Run from the project root: python benchmarks/redis_ping_boost.py
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
load_dotenv()

SCHEMA = f"bench_ping_{uuid.uuid4().hex[:12]}"
os.environ["SCHEMA_NAME"] = SCHEMA

from db import create_tables, init_engine, tx  # noqa: E402
from models import SportsEvent  # noqa: E402
from operations.engagement import record_ping, record_ping_redis  # noqa: E402
from redis_client import init_redis  # noqa: E402
from seed import seed_demo_flow  # noqa: E402

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


def main():
    url = URL.create(
        "postgresql+psycopg",
        username=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host="localhost",
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ["POSTGRES_DB"],
    )
    engine = init_engine(url.render_as_string(hide_password=False), pool_size=20, max_overflow=70, pool_timeout=60)
    redis = init_redis("redis://localhost:6379/0")
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
    finally:
        redis.delete(key)
        with engine.begin() as conn:
            conn.exec_driver_sql(f'DROP SCHEMA "{SCHEMA}" CASCADE')


if __name__ == "__main__":
    main()
