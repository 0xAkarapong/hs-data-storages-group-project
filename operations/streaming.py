import datetime as dt

from sqlalchemy import func, select, update

from db import BusinessError, lock, retry_on_conflict, tx
from models import (
    EventStatus,
    Plan,
    SportsEvent,
    StreamingSession,
    Subscription,
    SubStatus,
)


@retry_on_conflict()
def start_streaming_session(user_id: int, sports_event_id: int) -> int:
    """"Book a seat": open a stream only if the plan's concurrent-stream cap allows it.
    Concurrency: the subscription row is locked FOR UPDATE *before* counting open
    sessions, so the count-then-insert sequence is serialized per subscription.
    Without that lock, N threads all read "1 of 2 used" and all insert."""
    with tx() as s:
        sub = s.scalar(lock(
            select(Subscription).where(Subscription.user_id == user_id,
                                       Subscription.status == SubStatus.active)))
        if not sub:
            raise BusinessError("no active subscription")
        if sub.end_date and sub.end_date < dt.datetime.now(dt.UTC).date():
            raise BusinessError("subscription expired")

        plan = s.scalar(select(Plan).where(Plan.plan_id == sub.plan_id))
        event = s.scalar(select(SportsEvent).where(SportsEvent.sports_event_id == sports_event_id))
        if not event or event.status not in (EventStatus.scheduled, EventStatus.live):
            raise BusinessError("event not streamable")

        open_streams = s.scalar(select(func.count()).select_from(StreamingSession).where(
            StreamingSession.subscription_id == sub.subscription_id,
            StreamingSession.ended_at.is_(None)))
        if open_streams >= plan.max_concurrent_streams:
            raise BusinessError(f"concurrent stream limit reached ({plan.max_concurrent_streams})")

        sess = StreamingSession(user_id=user_id, sports_event_id=sports_event_id,
                                subscription_id=sub.subscription_id, started_at=dt.datetime.now(dt.UTC))
        s.add(sess)
        s.flush()
        return sess.session_id


# ── releases a "seat" ───────────────────────────────────────────────────────
@retry_on_conflict()
def end_streaming_session(session_id: int) -> bool:
    with tx() as s:
        sess = s.scalar(lock(select(StreamingSession)
                              .where(StreamingSession.session_id == session_id)))
        if not sess or sess.ended_at is not None:
            return False
        sess.ended_at = dt.datetime.now(dt.UTC)
        return True


# ── faster batch version of the close operation ─────────────────────────────
def end_streaming_sessions_bulk(session_ids: list[int]) -> int:
    """Close all open sessions in one transaction and return how many were closed."""
    if not session_ids:
        return 0

    with tx() as s:
        result = s.execute(
            update(StreamingSession)
            .where(StreamingSession.session_id.in_(session_ids),
                   StreamingSession.ended_at.is_(None))
            .values(ended_at=dt.datetime.now(dt.UTC))
        )
        return result.rowcount
