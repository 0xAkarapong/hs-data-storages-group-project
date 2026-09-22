# OLTP baseline for `record_ping`: predict, then measure, on local Postgres only

## Prediction (written before the benchmark runs)

`record_ping()` does three statements inside one `tx()`:

1. `SELECT` the streaming session (no lock).
2. `UPDATE sports_events SET ping_count = ping_count + 1 ... RETURNING ping_count` — takes the row lock.
3. `SELECT` the latest odds snapshot (no lock, unrelated table).
4. `COMMIT` (releases the row lock).

The row lock from step 2 is held until `COMMIT`, i.e. across the third
`SELECT` and the commit round-trip too, not just across the `UPDATE`
itself. Every worker in this benchmark pings the *same* `sports_event_id`,
so step 2 forces every writer onto one row: whoever gets the lock first
runs steps 2-4 to completion while everyone else queues; only READ
COMMITTED, so a queued writer re-reads the row and proceeds once the lock
frees, instead of raising a serialization error.

That makes the row lock, not connection count or thread count, the
throughput ceiling. Adding workers should **not** multiply aggregate RPS
past a small multiple of the single-writer number — the extra concurrency
mostly buys a longer wait queue, not more parallel work. Expect a
roughly flat curve across 100 → 500 threads, not a linear one.

Estimating the single-writer transaction latency on local Postgres
(loopback, `synchronous_commit=on`, no network hop): each round trip is
sub-millisecond, but `COMMIT`'s WAL fsync typically dominates at ~1-2ms on
non-tmpfs storage. Three round trips + commit ≈ 1.5-3ms per transaction.

**Prediction: aggregate throughput of roughly 300-800 req/s, essentially flat
across 100/300/500 concurrent workers**, because the workload is bound by
how fast Postgres can serialize commits through one row lock, not by how
many clients are waiting. We also predict `retry_on_conflict` stays
effectively inert for this workload — READ COMMITTED blocks and re-reads
instead of throwing `40001`, so the decorator's retry path is written for
`SERIALIZABLE`/deadlock scenarios this benchmark won't exercise.

A secondary, non-bottleneck factor worth naming ahead of time: the
benchmark's connection pool is deliberately sized under Postgres'
`max_connections` (100 on the local `docker-compose` image), so at
300-500 threads most workers queue on pool checkout as well as on the row
lock. This shouldn't change the aggregate number (the row lock is already
the tighter constraint) but it's a second reason the curve should stay
flat rather than degrade sharply.

Compare against `hw-3.md`'s ">10,000 RPS" target: this single hot-row
OLTP path is expected to land roughly 15-30x under that, which is the
motivation for task 3 (moving the counter to Redis, see
[0001](0001-ping-counter-redis-write-behind.md)).

## Measured results

_TODO: filled in after `make benchmark-ping` runs against local Postgres._

## Post-mortem: how the prediction held up

_TODO: written after the measured results are in._
