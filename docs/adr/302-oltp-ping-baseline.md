# OLTP baseline for `record_ping`: predict, then measure, on local Postgres only

## Prediction (written before the benchmark runs)

`record_ping()`, inside one `tx()`:
1. `SELECT` streaming session (no lock).
2. `UPDATE sports_events SET ping_count = ping_count + 1 ... RETURNING ping_count` — takes the row lock.
3. `SELECT` latest odds snapshot (no lock, unrelated table).
4. `COMMIT` — releases the row lock.

Key points:
- Lock from step 2 is held through step 3 and the commit round-trip, not just the `UPDATE`.
- All workers hit the *same* `sports_event_id`, so every writer serializes on that one row: first getter runs 2-4, everyone else queues.
- READ COMMITTED, so a queued writer blocks and re-reads once the lock frees — no `40001` serialization error.
- Bottleneck = row lock, not connection/thread count. More workers ≈ longer queue, not more parallel throughput. Expect a flat curve across 100 → 500 threads.
- Single-writer latency guess (loopback, `synchronous_commit=on`): round trips sub-ms, commit fsync ~1-2ms → **≈1.5-3ms/transaction**.

**Prediction: ~300-800 req/s aggregate, flat across 100/300/500 workers.**

Also predicted:
- `retry_on_conflict` stays inert — READ COMMITTED blocks/re-reads instead of raising `40001`; that retry path targets `SERIALIZABLE`/deadlock cases this workload won't hit.
- Connection pool (sized under `max_connections=100`) adds a second queue at 300-500 workers, but shouldn't change the aggregate number since the row lock is already the tighter constraint.

## Measured results

`make benchmark-ping` — local Postgres, one hot `sports_event_id`, 300 worker threads, 300 total calls, pool `pool_size=20, max_overflow=70` (90 ≤ 100 `max_connections`):

```
oltp record_ping, 300 workers, 1 hot sports_event_id: 3.580s, 84 req/s
ping_count after run: 300 (== successful calls: 300)
```

Two control runs, to explain the number rather than move it:
- **Sequential, uncontended** (1 caller, 50 calls): mean 11.1ms/call → implied ceiling ≈ **91 req/s even at zero contention**.
- **Per-call latency at 300 workers**: min 40ms, p50 `1640ms`, p95 `2774ms`, max `3587ms` — later callers wait roughly proportional to queue position (single-server queue behavior).

## Post-mortem

**Mechanism — correct:**
- 300-way concurrent throughput (84 req/s) lands within ~10% of the zero-contention sequential ceiling (~91 req/s) → the row lock really does serialize this workload; extra threads buy almost nothing.
- `retry_on_conflict` fired 0 times, as predicted — inert under READ COMMITTED for single-row contention.
- No lost updates: `ping_count` = 300 = successful calls.

**Magnitude — wrong, by ~4-10x:**
- Predicted **300-800 req/s**, measured **84**.
- The 1.5-3ms figure was a round-trip-count guess made without measuring anything first, as the "predict before running" rule required — it missed psycopg/SQLAlchemy overhead and the docker-forwarded TCP loopback actually in use. Guessing round-trip *count* without a measured *per-trip cost* was never going to land on the right order of magnitude.

**Couldn't fully isolate:**
- The predicted "pool queueing" secondary factor (300 threads vs. 90-connection pool) can't be separated from row-lock wait using wall-clock time alone — both slow a request the same way.
- Since the sequential baseline (zero pool contention) already lands within 10% of the concurrent number, the row lock plausibly explains nearly all of the latency — but that's inference from a proxy, not a direct measurement. Isolating it would need SQLAlchemy pool-checkout timing events; skipped since the ADR's job was identifying the *dominant* bottleneck, not decomposing the full latency budget.