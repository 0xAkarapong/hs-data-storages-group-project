# ADR 302-alternative: RPS by row count, on OddsSnapshot

## Why

The teacher flagged ADR 302's approach (N threads, timed wall-clock) as
measuring burst contention, not real end-user RPS. Fix: read RPS as a
database row-count delta over a fixed duration. This redoes hw-3's four
tasks against `OddsSnapshot` (10M-50M rows/day, `docs/team-buisness.md`) —
unlike `ping_count`, one request = one row, so row-count-over-time *is*
throughput (see `CONTEXT.md`'s one caveat: a buffered path needs its own
count).

## Constraints discovered

- No `CREATE SCHEMA` privilege on the real RDS — only the pre-created
  `drake888` schema. Scripts use additive `create_tables()` + an idempotent
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, scoping counts to one fresh
  `outcome_id`, never a whole table.
- Redis `maxclients=30`, shared class-wide (`INFO clients`) — worker counts
  kept conservative (8).

## Task 1 — NoSQL base performance

Prediction: single-client, unpipelined `SET`, bound by one round trip —
tens of records/s. Measured: **30 records/s** (3.362s/100 calls). Matches:
~34ms/call = one round trip.

That number is the network, not Redis — so it can't answer hw-3's ">10 000
RPS". The script now also sends the same kind of records **pipelined**
(1 000 `SET`s per round trip, 20 000 records by default, `argv[1]` to
override). Local Docker Redis, one connection: sequential 4 529/s,
pipelined **159 067/s** (35x); **1 000 000 records in 7.0s** (142 735/s).
Prediction for Redis Cloud: one ~34ms round trip per 1 000 records ≈
25-30k records/s — still >10k. _RDS/Redis Cloud rerun: TODO._

## Task 2 — OLTP baseline (`record_odds_update`)

Three ops, one transaction, one hot outcome (row lock, like `record_ping`).
Anchor: 20 `SELECT 1` round trips, p50=164ms. Prediction: 15-40 req/s
(dominated by the RDS round trip, not lock cost). Measured (15s): **28.1
RPS**, 428 accepted = 428 landed, 0 conflicts. Inside range; one-request-
one-row holds.

### Bottleneck: the hot row, not the round trip alone

Round trips per call: pool pre-ping, `BEGIN`, `SELECT` outcome, `INSERT`
snapshot, `UPDATE outcomes.latest_price`, `COMMIT` — ~6. The `UPDATE`
takes the outcome's row lock and holds it until `COMMIT` arrives, i.e. for
~1 network round trip. Every worker updates the *same* row, so commits on
one outcome are serialized: **ceiling ≈ 1 / RTT ≈ 1 / 34ms ≈ 29 writes/s**,
however many workers. Measured 28.1 and 29.1 RPS — the ceiling, not a
coincidence. Our original "dominated by the round trip, not lock cost" was
half right: it *is* the round trip, but paid *while holding the lock*.

Evidence (local Docker Postgres, RTT ≈ 0.1ms, so the ceiling is far higher
and Python's GIL also shows up — `oltp_oddsnapshot_baseline.py [workers]`):

| workers | RPS | p50 | p99 |
| ---: | ---: | ---: | ---: |
| 1 | 810 | 1.1 ms | 2.9 ms |
| 8 | 1 277 | 5.6 ms | 16.9 ms |
| 16 | 1 070 | 11.2 ms | 58.9 ms |
| 8, hot-row `UPDATE` removed | 1 819 | 4.2 ms | 8.4 ms |

8x the workers buys 1.6x the throughput; 16 is *worse* than 8 — workers
queue on the lock. Removing just the `UPDATE` gives +42% RPS and halves
p99. _RDS rerun with 1 and 16 workers: TODO — the prediction is ~29 RPS
for both 8 and 16._

### Improvements (without leaving SQL)

- Drop the hot-row `UPDATE`: read the latest price from `ix_snapshot_latest`
  (`ORDER BY captured_at DESC LIMIT 1`) instead of caching it on `outcomes`.
  Removes the lock entirely; costs a slightly more expensive read.
- Batch: one multi-row `INSERT` per N ticks — one lock hold per batch, not
  per tick (this is what the Redis flusher does in task 3).
- Remove round trips: `pool_pre_ping` (1 RTT per call), psycopg pipeline
  mode, or a single `INSERT ... SELECT ... WHERE status = 'open'` that folds
  validation into the insert.

## Task 3 — Redis boost

**First attempt:** validation `SELECT` stays in SQL; Redis only queues the
write + counts it accepted, background flush every 0.5s. Prediction:
~1.5-2x (the validation round trip stays in both paths). Measured (15s):
postgres 29.1 RPS, redis 47.8 RPS, **1.64x** (local: 1.60x) — capped by the
full Postgres transaction still on every request. Identity held: `accepted
(724) = landed + conflicts + still_buffered + lost`.

**Now:** the validation read moved too — a read-through Redis cache of
`Outcome.status` (`outcome_status_key`, 5s TTL; a miss reads Postgres once
and caches the result, including "missing" so bogus ids can't stampede
Postgres). Request path = 2 Redis round trips, **zero Postgres**. The
flusher also no longer sleeps after a full batch — sleeping unconditionally
capped ingest at 2 000 rows / 0.5s = 4 000/s, so at >10k RPS the queue
would have grown without bound.

Local (15s, 8 workers): postgres 1 124 RPS, redis **5 364 RPS, 4.77x**,
0 lost, flusher backlog 1 161 at the deadline (it keeps up). The Redis path
is now capped by the one Python process driving it (GIL), not by Redis
(task 1: 150k/s). Prediction for Redis Cloud/RDS: 8 / (2 × 34ms) ≈ 118 RPS
vs the ~29 RPS hot-row ceiling ≈ **4x**. _RDS/Redis Cloud rerun: TODO._

## Task 4 — crash demo

`oddsnapshot_flush_crash_demo.py` accepts `WRITES=50` writes, then SIGKILLs
a real subprocess mid-pop. It kills the **newest** queued writes, not the
oldest — losing an early tick is invisible (a later write already
supersedes it in `Outcome.latest_price`); losing the latest tick is what
actually leaves `latest_price` stale. Kill size is randomized per run.

Measured: 5 of 50 writes lost. `Outcome.latest_price` read **2.044**
instead of the true latest, **2.049** (never landed) — a concretely wrong
value, not just a missing history row.

A second thing given up, from the task 3 status cache: after an outcome is
settled, the Redis path keeps accepting ticks for up to the 5s TTL while
the SQL path rejects at once (`tests/test_odds_feed.py`, "stale-read
window").

## Task 5 — why acceptable here, not for Bet/Payment

A lost odds tick is superseded by the next one within moments — cosmetic
staleness, not owed money. A lost `Bet`/`Payment` is a wager or charge that
silently vanished after the UI confirmed it — needs `place_bet()`'s durable
transaction, not a buffer. Mirrors ADR 0001's reasoning, at two orders of
magnitude more writes/day.

Same for the status cache: a few ticks recorded against a just-settled
market are harmless history rows. It would *not* be acceptable as the
check for `place_bet()` — a bet accepted on a settled market is money owed
on a known result. That's why `place_bet()` never reads the cache; it
re-checks `Outcome.status` in Postgres under `FOR UPDATE`.

**"Which entities could you NOT move here?"** `Bet`, `Payment`,
`Subscription`: each write is money or entitlement, must survive a crash
once confirmed, and is guarded by invariants (stream limit, no double
payout) that need row locks in one transaction.

**"It's 3 a.m. and the cache is empty — what happens to Postgres?"** Each
open outcome's first tick misses and costs one `SELECT` by primary key; the
next 5s of ticks for that outcome hit Redis. Postgres load after a cold
start is bounded by *open outcomes per TTL*, not by request rate — hundreds
of indexed reads, not 10k/s. (If it had to be tighter: a lock on the miss
so only one request per outcome refills it.) The write queue is different:
if *Redis* is lost, everything accepted but not yet flushed — up to one
flush interval's worth — is gone, which is exactly task 4.

## What changed

- `models.py`: `Outcome.latest_price`/`latest_captured_at`, guarded against
  out-of-order writes.
- `operations/odds_feed.py`: `record_odds_update`, `record_odds_update_redis`
  (validates against a Redis status cache), `flush_odds_queue`.
- `benchmarks/`: four scripts for hw-3 tasks 1-4 + `_crash_worker_odds.py`.
- `tests/test_odds_feed.py`: guard/conflict/accounting logic and the
  status-cache stale window, run against the real RDS/Redis.

## Driver quirk

A multi-row `INSERT ... ON CONFLICT DO NOTHING`'s `Result.rowcount` returned
`-1` against real Postgres. Fixed via `RETURNING OddsSnapshot.snapshot_id`
and counting the returned rows instead.
