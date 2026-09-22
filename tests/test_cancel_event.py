import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select

from db import create_tables, init_engine, tx
from models import Bet, BetStatus, EventStatus, Outcome, Payment, SportsEvent
from operations.betting import cancel_event, settle_outcome
from seed import seed_demo_flow

if __name__ == "__main__":
    init_engine()
    create_tables(drop_first=True)
    ctx = seed_demo_flow()  # already places one $2,500 pending bet on outcome_id

    with tx() as s:
        bet_id = s.scalar(select(Bet.bet_id).where(Bet.user_id == ctx["user_id"]))

    # cancelling the event voids the pending bet and refunds the stake
    r1 = cancel_event(ctx["event_id"])
    assert r1 == {"cancelled_now": True, "already_cancelled": False, "voided": 1, "refunded": Decimal("2500.00")}, r1

    with tx() as s:
        assert s.get(Bet, bet_id).status == BetStatus.voided
        assert s.get(SportsEvent, ctx["event_id"]).status == EventStatus.cancelled
        refund = s.scalar(select(Payment).where(Payment.idempotency_key == f"void:{bet_id}"))
        assert refund is not None and refund.amount == Decimal("2500.00")
        assert s.get(Outcome, ctx["outcome_id"]).status.value == "open"  # Outcome has no cancelled state

    # cancelling twice is a no-op, not a double refund
    r2 = cancel_event(ctx["event_id"])
    assert r2 == {"cancelled_now": False, "already_cancelled": True, "voided": 0, "refunded": Decimal("0.00")}, r2

    # a cancelled event's outcome can no longer be settled
    r3 = settle_outcome(ctx["outcome_id"], won=True)
    assert r3 == {"settled": 0, "already_settled": True, "event_cancelled": True}, r3
    with tx() as s:
        assert s.get(Outcome, ctx["outcome_id"]).status.value == "open"

    print("test_cancel_event: all asserts passed")
