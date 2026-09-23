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

### Cross-machine comparison

Same command (`make benchmark-ping`), same 300 workers / 300 calls / one hot `sports_event_id`, different hardware:

| Machine | Wall time | Throughput | `ping_count` |
|---|---|---|---|
| Windows | 3.580s | 84 req/s | 300 |
| Mac (Apple Silicon) | 0.951s | 316 req/s | 300 |

~3.8x gap:
- Both correct — no lost updates, `ping_count` matches successful calls on both runs.
- Same mechanism on both — one row lock still serializes every worker — so the gap is hardware, not logic.
- Windows runs Postgres-in-Docker over WSL2/Hyper-V's virtualized disk path, adding fsync/commit latency per lock hand-off; Apple Silicon's Docker Desktop Linux VM does the same round trips faster.
- Confirms the original 1.5-3ms/transaction guess undershot because per-trip cost is host-dependent, not just query-plan-dependent.

### Additional MacBook Air run (2026-09-23)

Hardware: MacBook Air (Mac15,13), Apple M3 with 8 CPU cores (4 performance, 4 efficiency), 16 GB memory. Host OS: macOS 27.0. Database: local PostgreSQL 18 in Docker Compose.

Ran `make benchmark-ping` three times with `DATABASE_URL` set to the local PostgreSQL instance. Each run used 300 workers, 300 total calls, and one hot `sports_event_id`:

| Run | Wall time | Throughput | `ping_count` |
|---|---:|---:|---:|
| 1 | 0.475s | 631 req/s | 300 |
| 2 | 0.438s | 684 req/s | 300 |
| 3 | 0.439s | 683 req/s | 300 |
| **Median** | **0.439s** | **683 req/s** | **300** |

The median throughput is about 2.2x the earlier Mac result (316 req/s) and 8.1x the Windows result (84 req/s). All 300 calls succeeded with no lost updates. This run used the current working-tree benchmark script, which creates and removes a temporary schema; the earlier measurements used a different script revision. The workload is comparable, but the difference cannot be attributed to hardware alone.

## Post-mortem

**Mechanism — correct, on both machines:**
- Windows: 300-way concurrent throughput (84 req/s) lands within ~10% of the zero-contention sequential ceiling (~91 req/s) → the row lock really does serialize this workload; extra threads buy almost nothing.
- Mac reproduces the same shape (see [Cross-machine comparison](#cross-machine-comparison)) at ~3.8x the throughput — hardware changes the constant, not the mechanism.
- `retry_on_conflict` fired 0 times, as predicted — inert under READ COMMITTED for single-row contention.
- No lost updates: `ping_count` = 300 = successful calls, on both machines.

**Magnitude — wrong on Windows (~4-10x), roughly right on Mac:**
- Predicted **300-800 req/s**. Windows measured **84** (miss). Mac measured **316** (inside the predicted range).
- So the prediction's mechanism and order of magnitude were sound; it was the per-trip cost assumption (loopback + `synchronous_commit=on` fsync) that was Windows-pessimistic, not the model itself.
- The 1.5-3ms figure was a round-trip-count guess made without measuring anything first, as the "predict before running" rule required — it missed psycopg/SQLAlchemy overhead and the docker-forwarded TCP loopback actually in use, and turned out to describe the Mac host, not the Windows one. Guessing round-trip *count* without a measured *per-trip cost* was never going to land reliably across hardware.

**Couldn't fully isolate:**
- The predicted "pool queueing" secondary factor (300 threads vs. 90-connection pool) can't be separated from row-lock wait using wall-clock time alone — both slow a request the same way.
- Since the sequential baseline (zero pool contention) already lands within 10% of the concurrent number, the row lock plausibly explains nearly all of the latency — but that's inference from a proxy, not a direct measurement. Isolating it would need SQLAlchemy pool-checkout timing events; skipped since the ADR's job was identifying the *dominant* bottleneck, not decomposing the full latency budget.
