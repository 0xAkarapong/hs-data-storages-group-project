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

## Task 2 — OLTP baseline (`record_odds_update`)

Three ops, one transaction, one hot outcome (row lock, like `record_ping`).
Anchor: 20 `SELECT 1` round trips, p50=164ms. Prediction: 15-40 req/s
(dominated by the RDS round trip, not lock cost). Measured (15s): **28.1
RPS**, 428 accepted = 428 landed, 0 conflicts. Inside range; one-request-
one-row holds.

## Task 3 — Redis boost

Validation `SELECT` stays in SQL on both paths; Redis only queues the write
+ counts it accepted, background flush every 0.5s. Prediction: ~1.5-2x (the
validation round trip stays in both paths). Measured (15s): postgres 29.1
RPS, redis 47.8 RPS, **1.64x** — same shape as ADR 302's ping boost (1.53x),
capped by the SQL work left in the hot path. Identity held: `accepted (724)
= landed + conflicts + still_buffered + lost`.

## Task 4 — crash demo

`oddsnapshot_flush_crash_demo.py` accepts `WRITES=50` writes, then SIGKILLs
a real subprocess mid-pop. It kills the **newest** queued writes, not the
oldest — losing an early tick is invisible (a later write already
supersedes it in `Outcome.latest_price`); losing the latest tick is what
actually leaves `latest_price` stale. Kill size is randomized per run.

Measured: 5 of 50 writes lost. `Outcome.latest_price` read **2.044**
instead of the true latest, **2.049** (never landed) — a concretely wrong
value, not just a missing history row.

## Task 5 — why acceptable here, not for Bet/Payment

A lost odds tick is superseded by the next one within moments — cosmetic
staleness, not owed money. A lost `Bet`/`Payment` is a wager or charge that
silently vanished after the UI confirmed it — needs `place_bet()`'s durable
transaction, not a buffer. Mirrors ADR 0001's reasoning, at two orders of
magnitude more writes/day.

## What changed

- `models.py`: `Outcome.latest_price`/`latest_captured_at`, guarded against
  out-of-order writes.
- `operations/odds_feed.py`: `record_odds_update`, `record_odds_update_redis`,
  `flush_odds_queue`.
- `benchmarks/`: four scripts for hw-3 tasks 1-4 + `_crash_worker_odds.py`.
- `tests/test_odds_feed.py`: guard/conflict/accounting logic, run against the
  real RDS/Redis.

## Driver quirk

A multi-row `INSERT ... ON CONFLICT DO NOTHING`'s `Result.rowcount` returned
`-1` against real Postgres. Fixed via `RETURNING OddsSnapshot.snapshot_id`
and counting the returned rows instead.
