# hs-data-storages-group-project

A coursework project modeling a live-sports-streaming + real-time-betting platform, used to design and benchmark database write paths (PostgreSQL and Redis).

## Language

**Heartbeat** (superseded, task3 approach):
The `record_ping`/`record_ping_redis` load test — 300 threads each calling the ping operation exactly once, RPS reported as `total_calls / wall_clock_seconds` for that one fixed-count burst to drain. Retired for *how* it measured (a closed-loop burst timed at the client, sized to whatever the caller hardcodes), not for testing hot-row contention itself — the Oddsnapshot redesign deliberately keeps a hot contended row (see Q4/Q8 in the design discussion) and still wants that contention visible. Superseded by a duration-based run (fixed wall-clock T, not a fixed call count) with throughput read from a database-side row count instead of client-side timing.
_Avoid_: implying hot-row contention itself was the problem — it wasn't; the fixed-count client-timed burst was.

**RPS, in the Oddsnapshot benchmark**:
Client-side end-user request throughput, same quantity the heartbeat benchmark tried to measure — just observed at the database instead of timed at the client. Because each accepted request inserts exactly one `odds_snapshots` row, `Δrows / Δt` (row-count snapshot before and after a run) **is** requests/second, not a proxy for it. This only holds while the mapping stays one-request-one-row; if a path buffers/batches (see Redis write-behind), the row count at the SQL store measures flush throughput, not accepted-request throughput, and needs a separate count at the buffer itself.
_Avoid_: calling this "feed-ingestion throughput" as something different from end-user RPS — for the direct-insert path they are the same number.

**OddsSnapshot**:
The price for one outcome at one exact moment (`docs/team-buisness.md`); table `odds_snapshots`, unique per `(outcome_id, captured_at)`. Business-projected as the platform's highest-volume write (10M–50M rows/day). Previously only read (by `record_ping`) and statically seeded — never written by a live operation until this redesign.

### Accounting terms (Redis write-behind path, Oddsnapshot benchmark)

**Accepted**: A request whose write was durably queued (Redis `INCR`'d counter), whether or not it has reached Postgres yet.
**Landed**: A queued write that the flush job successfully committed into Postgres.
**Conflicts**: A landed-attempt write rejected by the `(outcome_id, captured_at)` unique constraint (`ON CONFLICT DO NOTHING`), counted separately rather than silently dropped or retried.
**Still-buffered**: Accepted writes sitting in the Redis queue (`LLEN`) that the flusher hasn't processed yet — a live, legitimate reading, not an error.
**Lost**: Accepted writes that will never land — only nonzero when a crash is deliberately injected between the queue pop and the Postgres commit (item 4's demo).
_Invariant_: `accepted = landed + conflicts + still_buffered + lost`. On a non-crash run, `lost == 0` is the correctness assertion (plays the role `ping_count == TOTAL_CALLS` played for the old ping benchmark).
