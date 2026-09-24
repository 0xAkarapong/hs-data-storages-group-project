"""Real crash victim for flush_counters_crash_demo.py.

Pops exactly one event's Redis ping counter (the point of no return: once
popped, that count exists nowhere else), announces it on stdout, then blocks
waiting for a go-ahead on stdin that never comes — the parent SIGKILLs this
process the instant it sees the announcement. Not a `random.random() < p`
roll: this process actually dies before it can reach the Postgres write
below.

Internal to flush_counters_crash_demo.py — not meant to be run by hand.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import update

from db import init_engine, tx
from models import SportsEvent
from operations.engagement import ping_key
from redis_client import init_redis

if __name__ == "__main__":
    sports_event_id = int(sys.argv[1])

    init_engine()
    r = init_redis()

    delta = int(r.getdel(ping_key(sports_event_id)) or 0)
    print(f"popped {delta}", flush=True)

    sys.stdin.readline()  # the parent's go-ahead — it never sends one

    with tx() as s:
        s.execute(
            update(SportsEvent)
            .where(SportsEvent.sports_event_id == sports_event_id)
            .values(ping_count=SportsEvent.ping_count + delta)
        )
    print("committed", flush=True)
