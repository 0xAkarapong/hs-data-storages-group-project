"""Redis new-record SET baseline — HW3-alternative task 1.

A fresh key each call, so each SET is genuinely a new record, not a repeated
write to one hot key — that contention story is task 2-3's, not this one.
Mirrors the shape of redis_ping_baseline.py (HW3 task 1's original), just SET
instead of INCR since an odds tick isn't a counter.

Two measurements, same records, same single connection:
  - sequential: one SET per round trip. Bound by network latency, not Redis —
    ~30 records/s against Redis Cloud (~34ms RTT) no matter how fast Redis is.
  - pipelined:  PIPELINE_BATCH SETs per round trip. This is how a real ingest
    path talks to Redis, and it is the number that answers hw-3's ">10 000
    requests per second" — the round trip is paid once per batch, not per record.

Keys live under {SCHEMA_NAME}:bench:oddsnap1:*, deleted before AND after the
timed section so this never pollutes the shared Redis instance's keyspace
(which already carries tens of thousands of leftover keys from past runs
across the class — counted here by the loop's own tally, never DBSIZE) and
never collides with another teammate/student running this same script
concurrently against the same shared instance. RECORD_COUNT is kept modest
for the same reason (shared memory); pass a bigger one locally:

Run from the project root: python benchmarks/redis_oddsnapshot_baseline.py [record_count]
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from models import SCHEMA_NAME
from redis_client import init_redis

SEQUENTIAL_COUNT = 100  # one round trip each — more than this takes minutes against Redis Cloud
RECORD_COUNT = int(sys.argv[1]) if len(sys.argv) > 1 else 20_000
PIPELINE_BATCH = 1_000


def make_keys(prefix: str, count: int) -> list[str]:
    return [f"{SCHEMA_NAME or 'public'}:bench:oddsnap1:{prefix}:{n}" for n in range(count)]


def delete_keys(r, keys: list[str]) -> None:
    """Chunked so a 100k-key delete isn't one giant command; SCAN over a large remote keyspace takes minutes."""
    for i in range(0, len(keys), PIPELINE_BATCH):
        r.delete(*keys[i:i + PIPELINE_BATCH])


def main():
    payload = json.dumps({"outcome_id": 1, "captured_at": "2026-09-26T12:00:00+00:00", "price": "2.000"})
    seq_keys = make_keys("seq", SEQUENTIAL_COUNT)
    pipe_keys = make_keys("pipe", RECORD_COUNT)

    r = init_redis()
    r.ping()  # warm the connection before timing
    delete_keys(r, seq_keys + pipe_keys)

    try:
        started = time.perf_counter()
        for key in seq_keys:
            assert r.set(key, payload, nx=True)  # nx=True: fail loudly if the key wasn't actually fresh
        seq_seconds = time.perf_counter() - started

        started = time.perf_counter()
        for i in range(0, RECORD_COUNT, PIPELINE_BATCH):
            pipe = r.pipeline(transaction=False)
            for key in pipe_keys[i:i + PIPELINE_BATCH]:
                pipe.set(key, payload, nx=True)
            assert all(pipe.execute())
        pipe_seconds = time.perf_counter() - started
    finally:
        delete_keys(r, seq_keys + pipe_keys)  # leave no trace on the shared instance

    seq_rps = SEQUENTIAL_COUNT / seq_seconds
    pipe_rps = RECORD_COUNT / pipe_seconds
    print(f"redis set, new key per call, 1 connection:")
    print(f"  sequential: {SEQUENTIAL_COUNT:>7,} records in {seq_seconds:.3f}s -> {seq_rps:>10,.0f} records/s "
          f"({seq_seconds / SEQUENTIAL_COUNT * 1000:.2f} ms/record = one round trip)")
    print(f"  pipelined:  {RECORD_COUNT:>7,} records in {pipe_seconds:.3f}s -> {pipe_rps:>10,.0f} records/s "
          f"(batches of {PIPELINE_BATCH:,})")
    print(f"  pipelining speed-up: {pipe_rps / seq_rps:.1f}x")


if __name__ == "__main__":
    main()
