from sqlalchemy import select, update

from db import BusinessError, retry_on_conflict, tx
from models import OddsSnapshot, Outcome, SportsEvent, StreamingSession


@retry_on_conflict()
def record_ping(streaming_session_id: int) -> dict:
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
