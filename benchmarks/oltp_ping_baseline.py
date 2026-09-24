"""OLTP baseline for record_ping under one hot sports_event_id — HW3 task 2.

Concurrent worker threads all ping the SAME streaming session, so every
UPDATE lands on the same sports_events row and serializes on its row lock.
See docs/adr/302-oltp-ping-baseline.md for the pre-run
prediction and post-run post-mortem.

Uses a temporary schema on local Postgres (make db-up) and removes it afterward.

Run from the project root: python benchmarks/oltp_ping_baseline.py
"""
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from sqlalchemy.engine import URL

load_dotenv()
SCHEMA = f"bench_ping_{uuid.uuid4().hex[:12]}"
os.environ["SCHEMA_NAME"] = SCHEMA

from db import create_tables, init_engine, tx
from models import SportsEvent
from operations.engagement import record_ping
from seed import seed_demo_flow

WORKERS = 300
TOTAL_CALLS = 300

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
    with engine.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA "{SCHEMA}"')
    try:
        create_tables()
        ctx = seed_demo_flow()  # one open streaming session on one hot event

        with tx() as s:
            s.get(SportsEvent, ctx["event_id"])  # warm the pool/connection before timing

        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            results = list(ex.map(record_ping, [ctx["session_id"]] * TOTAL_CALLS))
        seconds = time.perf_counter() - started

        assert all(result["latest_price"] is not None for result in results)
        assert sorted(result["ping_count"] for result in results) == list(range(1, TOTAL_CALLS + 1))

        with tx() as s:
            final_count = s.get(SportsEvent, ctx["event_id"]).ping_count

        assert final_count == TOTAL_CALLS

        print(f"oltp record_ping, {WORKERS} workers, 1 hot sports_event_id: {seconds:.3f}s, {TOTAL_CALLS / seconds:,.0f} req/s")
        print(f"ping_count after run: {final_count} (== successful calls: {len(results)})")
    finally:
        with engine.begin() as conn:
            conn.exec_driver_sql(f'DROP SCHEMA "{SCHEMA}" CASCADE')


if __name__ == "__main__":
    main()
