import random

from sqlalchemy import select, update

from db import BusinessError, retry_on_conflict, tx
from models import SCHEMA_NAME, OddsSnapshot, Outcome, SportsEvent, StreamingSession
from redis_client import get_redis


def ping_key(sports_event_id: int) -> str:
    """The Redis key `record_ping_redis` increments and `flush_counters` drains."""
    return f"event:{SCHEMA_NAME or 'public'}:{sports_event_id}:pings"


@retry_on_conflict()
def record_ping(streaming_session_id: int, *, fail_after_increment: bool = False) -> dict:
    """
    Update the ping count using OLTP database
    """
    with tx() as s:
        sess = s.scalar(select(StreamingSession)
                         .where(StreamingSession.session_id == streaming_session_id))
        if not sess or sess.ended_at is not None:
            raise BusinessError("invalid or closed streaming session")

        # ping_count + 1
        new_count = s.scalar(
            update(SportsEvent)
            .where(SportsEvent.sports_event_id == sess.sports_event_id)
            .values(ping_count=SportsEvent.ping_count + 1)
            .returning(SportsEvent.ping_count)
        )
        if fail_after_increment:
            raise RuntimeError("simulated failure after SQL increment")

        snap = s.execute(
            select(OddsSnapshot.outcome_id, OddsSnapshot.price)
            .join(Outcome, OddsSnapshot.outcome_id == Outcome.outcome_id)
            .where(Outcome.sports_event_id == sess.sports_event_id)
            .order_by(OddsSnapshot.captured_at.desc())
            .limit(1)
        ).first()

        return {
            "sports_event_id": sess.sports_event_id,
            "ping_count": new_count,
            "outcome_id": snap.outcome_id if snap else None,
            "latest_price": snap.price if snap else None,
        }


def flush_counters(p: float = 0.0) -> list[dict]:
    """Reconcile Redis's write-behind ping counters into `sports_events.ping_count`.

    For each event with a nonzero counter, atomically pops it out of Redis
    (GETDEL) and adds it to Postgres — except with probability `p`, where the
    Postgres write is skipped right after the pop, simulating a crash
    mid-reconciliation. The popped value is gone from Redis either way, so a
    skip is a genuine lost write, not a delayed one.
    """
    r = get_redis()
    results = []
    for key in r.scan_iter(match=f"event:{SCHEMA_NAME or 'public'}:*:pings"):

        # Redis only this part
        delta = int(r.getdel(key) or 0)
        if delta == 0:
            continue

        sports_event_id = int(key.split(":")[-2])
        if random.random() < p:
            results.append({"sports_event_id": sports_event_id, "delta": delta, "persisted": False})
            continue

        with tx() as s:
            s.execute(
                update(SportsEvent)
                .where(SportsEvent.sports_event_id == sports_event_id)
                .values(ping_count=SportsEvent.ping_count + delta)
            )

        results.append({"sports_event_id": sports_event_id, "delta": delta, "persisted": True})
    return results


def record_ping_redis(streaming_session_id: int, *, fail_after_increment: bool = False) -> dict:
    """Read session and odds in SQL; increment the event counter in Redis."""
    with tx() as s:
        sess = s.scalar(select(StreamingSession)
                        .where(StreamingSession.session_id == streaming_session_id))
        if not sess or sess.ended_at is not None:
            raise BusinessError("invalid or closed streaming session")

        new_count = get_redis().incr(ping_key(sess.sports_event_id))
        if fail_after_increment:
            raise RuntimeError("simulated failure after Redis increment")

        snap = s.execute(
            select(OddsSnapshot.outcome_id, OddsSnapshot.price)
            .join(Outcome, OddsSnapshot.outcome_id == Outcome.outcome_id)
            .where(Outcome.sports_event_id == sess.sports_event_id)
            .order_by(OddsSnapshot.captured_at.desc())
            .limit(1)
        ).first()

        return {
            "sports_event_id": sess.sports_event_id,
            "ping_count": new_count,
            "outcome_id": snap.outcome_id if snap else None,
            "latest_price": snap.price if snap else None,
        }
