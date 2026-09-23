"""OLTP baseline for record_ping under one hot sports_event_id — HW3 task 2.

Concurrent worker threads all ping the SAME streaming session, so every
UPDATE lands on the same sports_events row and serializes on its row lock.
See docs/adr/0002-oltp-ping-baseline-postgres-only.md for the pre-run
prediction and post-run post-mortem.

Local Postgres only (make db-up) — refuses to run against a non-local host
since it calls create_tables(drop_first=True).

Run from the project root: python benchmarks/oltp_ping_baseline.py
"""
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

from db import create_tables, init_engine, tx
from models import SportsEvent
from operations.engagement import record_ping
from seed import seed_demo_flow

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
WORKERS = 300
TOTAL_CALLS = 300

def main():
    init_engine(DATABASE_URL, pool_size=20, max_overflow=70, pool_timeout=60)
    create_tables(drop_first=True)
    ctx = seed_demo_flow()  # one open streaming session on one hot event

    with tx() as s:
        s.get(SportsEvent, ctx["event_id"])  # warm the pool/connection before timing

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:

        # passing `recoding_ping` function
        futs = [ex.submit(record_ping, ctx["session_id"]) for _ in range(TOTAL_CALLS)]
        results = [f.result() for f in futs]
    seconds = time.perf_counter() - started

    rps = TOTAL_CALLS / seconds
    print(f"oltp record_ping, {WORKERS} workers, 1 hot sports_event_id: {seconds:.3f}s, {rps:,.0f} req/s")

    with tx() as s:
        final_count = s.get(SportsEvent, ctx["event_id"]).ping_count
    print(f"ping_count after run: {final_count} (== successful calls: {len(results)})")
    

if __name__ == "__main__":
    main()
