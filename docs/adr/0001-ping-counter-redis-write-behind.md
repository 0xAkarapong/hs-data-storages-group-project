# ADR 0001: Redis write-behind for ping counts

## Decision

Record streaming-session pings in Redis and periodically add those counters to
`sports_events.ping_count` in PostgreSQL. `flush_counters()` removes each Redis
counter with `GETDEL` before applying its value to PostgreSQL. This reduces the
database writes on the frequent ping path, but a crash between those two steps
can permanently lose pings.

We accept that risk for `ping_count`: it is an engagement metric, and an
undercount does not change a user's access or money. The same design is not
appropriate for `Bet` or `Payment`, where a lost or duplicated write could
change a balance, settlement, or obligation. Those operations need a durable,
transactional record.

## Observed loss

On 2026-09-24, I ran `benchmarks/flush_counters_crash_demo.py` against local
PostgreSQL and Redis. The demo used a temporary PostgreSQL schema, created
eight events, and recorded three pings for each event (24 pings total). It
then killed a worker process for four randomly selected events immediately
after each worker removed its Redis counter with `GETDEL`, before any
PostgreSQL update. The other four events flushed normally.

| Result | Observed count |
| --- | ---: |
| Events with an undercount | 4 of 8 |
| Pings recorded before reconciliation | 24 |
| Pings persisted in PostgreSQL | 12 |
| Pings permanently lost | 12 |

Each affected event had three pings in Redis before the crash and zero in
PostgreSQL afterward. The four unaffected events each persisted all three
pings. This is a deliberately forced crash of half the events, not an estimate
of the failure rate in normal operation or a run with `p=0.1`. It demonstrates
the loss window that the write-behind design accepts.
