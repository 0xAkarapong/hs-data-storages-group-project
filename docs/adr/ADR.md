# ADR: Performance test for closing streaming sessions

## Before the test

I tested the `end_streaming_session()` operation.  The test closes **10,000**
open sessions.

**Prediction:** I expected about **1,000 requests per second** when closing one
session at a time.  I expected the slow part to be opening and committing 10,000
small database transactions.  Each call also does a `SELECT` before its
`UPDATE`.

## What I changed

I added `end_streaming_sessions_bulk()`.  It closes many session ids with one
`UPDATE` inside one transaction.  This is still safe for this maintenance job:
if the transaction fails, none of the sessions are closed.

## Result

I ran:

```bash
python benchmarks/streaming_session_close.py
```

| version | time for 10,000 sessions | RPS |
| --- | ---: | ---: |
| one session per transaction | 11.321 seconds | 883 |
| one batch transaction | 0.049 seconds | 202,034 |

The batch version was about **228.7× faster**, so it is more than the required
10× improvement.

## Post-mortem

My prediction about the bottleneck was correct: the original version was slower
than expected because it paid the transaction and ORM cost 10,000 times.  The
batch version sends one update instead of 10,000 select/update/commit cycles.
This result is from local SQLite, so the exact RPS will be different on
PostgreSQL, but the reason for the improvement is the same: fewer database
round-trips and commits.
