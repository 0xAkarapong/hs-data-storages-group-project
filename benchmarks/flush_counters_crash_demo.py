"""Show a lost Redis-backed ping counter after a real crash mid-flush.

Pings several distinct events through the Redis-boosted `record_ping_redis`,
then reconciles them. Half the events get an actual crash: `_crash_worker.py`
runs as a real subprocess, pops that event's Redis counter — the point of no
return — announces it, and blocks; this script SIGKILLs it right there,
before it can ever reach the Postgres write. That's a genuine OS-level
process kill, not a `random.random() < p` roll. The other half reconcile
normally through `flush_counters(p=0.0)`. The mismatch on the killed rows IS
the deliverable ("show me the corrupted row").

Uses an isolated schema on local PostgreSQL and removes it afterward.
Run from the project root: python benchmarks/flush_counters_crash_demo.py
"""
import os
import random
import signal
import subprocess
import sys
import uuid
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import select

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
load_dotenv()

SCHEMA = f"demo_flush_{uuid.uuid4().hex[:12]}"
os.environ["SCHEMA_NAME"] = SCHEMA

from db import create_tables, init_engine, tx
from models import SportsEvent
from operations.engagement import flush_counters, ping_key, record_ping_redis
from redis_client import init_redis
from seed import seed_event_with_session, seed_plans

EVENTS = 8
PINGS_PER_EVENT = 3
CRASH_WORKER = Path(__file__).with_name("_crash_worker.py")


def kill_mid_reconcile(sports_event_id: int) -> None:
    """Really crash a process right after it pops Redis, before it can write Postgres."""
    proc = subprocess.Popen(
        [sys.executable, str(CRASH_WORKER), str(sports_event_id)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    )
    assert proc.stdout is not None
    proc.stdout.readline()  # "popped <delta>" — the pop already happened, irreversibly
    proc.kill()             # SIGKILL: no chance left to reach the Postgres write
    proc.wait()
    assert proc.returncode == -signal.SIGKILL, f"expected a real SIGKILL, got {proc.returncode}"


def main():
    engine = init_engine()
    redis = init_redis()
    redis.ping()

    with engine.begin() as conn:
        conn.exec_driver_sql(f'CREATE SCHEMA "{SCHEMA}"')
    try:
        create_tables()

        with tx() as s:
            plan_ids = seed_plans(s)

        sessions = [seed_event_with_session(plan_ids["Standard"], f"demo{n}") for n in range(EVENTS)]
        event_ids = [event_id for event_id, _ in sessions]

        true_counts = {}
        for event_id, session_id in sessions:
            for _ in range(PINGS_PER_EVENT):
                record_ping_redis(session_id)
            true_counts[event_id] = int(redis.get(ping_key(event_id)) or 0)

        victims = set(random.sample(event_ids, k=EVENTS // 2))
        for event_id in victims:
            kill_mid_reconcile(event_id)  # real crash: pop happened, write never will

        flush_counters()

        with tx() as s:
            persisted = {row.sports_event_id: row.ping_count for row in s.scalars(select(SportsEvent))}

        # report part
        print(f"{'event_id':>10}  {'redis (true)':>13}  {'postgres':>9}  match")
        mismatches = 0
        for event_id in event_ids:
            redis_count = true_counts[event_id]
            pg_count = persisted[event_id]
            ok = redis_count == pg_count
            mismatches += not ok
            match = "yes" if ok else "no  <-- lost write"
            cause = "  (SIGKILLed mid-flush)" if event_id in victims else ""
            print(f"{event_id:>10}  {redis_count:>13}  {pg_count:>9}  {match}{cause}")
        print(f"\n{mismatches}/{EVENTS} events lost their ping count to a real crash mid-flush.")
    finally:
        with engine.begin() as conn:
            conn.exec_driver_sql(f'DROP SCHEMA "{SCHEMA}" CASCADE')


if __name__ == "__main__":
    main()
