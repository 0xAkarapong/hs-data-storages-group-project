# ADR: Performance test for closing streaming sessions

## Before the test

I tested the `end_streaming_session()` operation.  The test closes **10,000**
open sessions.

**Prediction:**
From `operations.streaming`

```python
def end_streaming_session(session_id: int) -> bool:
    with tx() as s:
        sess = s.scalar(lock(select(StreamingSession)
                              .where(StreamingSession.session_id == session_id)))
        if not sess or sess.ended_at is not None:
            return False
        sess.ended_at = dt.datetime.now(dt.timezone.utc)
        return True
```

we can see this function does 3 round-trip: `SELECT`, `UPDATE` and `COMMIT`
Assume 5 ms per round-trip: 
    1000ms / 5ms = 200 round-trip per second
    one request use 3 round trip:
        1 second = 200 / 3 = 67 requests per second


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
