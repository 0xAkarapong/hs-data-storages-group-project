"""Single-client Redis new-key INCR baseline — HW3 task 1.

A fresh key each call, so each INCR is genuinely a new record, not a
repeated increment on one hot key — that contention story is task 2-3's,
not this one. This number is a floor, not Redis's actual ceiling: one
client, one connection, no pipelining — concurrency would move it up.

Keys live under bench:event:*:pings, a namespace distinct from the
event:{schema}:{sports_event_id}:pings keys record_ping_redis() uses, so
this never collides with (or gets confused for) real ping counts.

Run from the project root: python benchmarks/redis_ping_baseline.py
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from redis_client import init_redis

PING_COUNT = 100


def main():
    keys = [f"bench:event:{event_id}:pings" for event_id in range(PING_COUNT)]
    print("PING PONG")
    r = init_redis()
    r.ping()  # warm the connection before timing
    r.delete(*keys)  # one round trip; SCAN over a large remote keyspace takes minutes

    started = time.perf_counter()
    for key in keys:
        assert r.incr(key) == 1
    seconds = time.perf_counter() - started

    rps = PING_COUNT / seconds
    print(f"redis incr, new key per call: {seconds:.3f}s, {rps:,.0f} incr/s")


if __name__ == "__main__":
    main()
