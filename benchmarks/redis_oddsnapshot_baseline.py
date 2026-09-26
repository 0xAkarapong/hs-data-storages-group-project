"""Single-client Redis new-record SET baseline — HW3-alternative task 1.

A fresh key each call, so each SET is genuinely a new record, not a repeated
write to one hot key — that contention story is task 2-3's, not this one.
This number is a floor, not Redis's actual ceiling: one client, one
connection, no pipelining — concurrency would move it up. Mirrors the shape
of redis_ping_baseline.py (HW3 task 1's original), just SET instead of INCR
since an odds tick isn't a counter.

Keys live under {SCHEMA_NAME}:bench:oddsnap1:*, deleted before AND after the
timed section so this never pollutes the shared Redis instance's keyspace
(which already carries tens of thousands of leftover keys from past runs
across the class — counted here by the loop's own tally, never DBSIZE) and
never collides with another teammate/student running this same script
concurrently against the same shared instance.

Run from the project root: python benchmarks/redis_oddsnapshot_baseline.py
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import SCHEMA_NAME
from redis_client import init_redis

RECORD_COUNT = 100


def main():
    keys = [f"{SCHEMA_NAME or 'public'}:bench:oddsnap1:{n}" for n in range(RECORD_COUNT)]
    payload = json.dumps({"outcome_id": 1, "captured_at": "2026-09-26T12:00:00+00:00", "price": "2.000"})

    r = init_redis()
    r.ping()  # warm the connection before timing
    r.delete(*keys)  # one round trip; SCAN over a large remote keyspace takes minutes

    started = time.perf_counter()
    for key in keys:
        assert r.set(key, payload, nx=True)  # nx=True: fail loudly if the key wasn't actually fresh
    seconds = time.perf_counter() - started

    r.delete(*keys)  # leave no trace on the shared instance

    rps = RECORD_COUNT / seconds
    print(f"redis set, new key per call: {seconds:.3f}s, {rps:,.0f} records/s")


if __name__ == "__main__":
    main()
