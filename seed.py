import datetime as dt
from decimal import Decimal

from db import create_tables, init_engine, tx
from models import EventStatus, OddsSnapshot, Outcome, Plan, SportsEvent
from operations import place_bet, purchase_subscription, register_user, start_streaming_session

import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def seed_reference_data():
    now = dt.datetime.now(dt.timezone.utc)
    with tx() as s:
        s.add_all([
            Plan(plan_name="Basic",   monthly_price=Decimal("4.99"),  max_concurrent_streams=1),
            Plan(plan_name="Standard",monthly_price=Decimal("9.99"),  max_concurrent_streams=2),
            Plan(plan_name="Premium", monthly_price=Decimal("14.99"), max_concurrent_streams=4),
        ])
        ev = SportsEvent(title="Lions vs Tigers", sport_type="football",
                         start_time=now + dt.timedelta(hours=2), status=EventStatus.live)
        s.add(ev)
        s.flush()
        o1 = Outcome(sports_event_id=ev.sports_event_id, description="Lions win")
        o2 = Outcome(sports_event_id=ev.sports_event_id, description="Tigers win")
        s.add_all([o1, o2])
        s.flush()
        s.add_all([
            OddsSnapshot(outcome_id=o1.outcome_id, captured_at=now - dt.timedelta(seconds=40), price=Decimal("2.10")),
            OddsSnapshot(outcome_id=o1.outcome_id, captured_at=now, price=Decimal("1.95")),
            OddsSnapshot(outcome_id=o2.outcome_id, captured_at=now, price=Decimal("3.40")),
        ])
        return ev.sports_event_id, o1.outcome_id


def seed_demo_flow():
    event_id, outcome_id = seed_reference_data()
    uid = register_user("ana@example.com", "s3cret!", "Ana")
    purchase_subscription(uid, plan_id=2, payment_method="card", idempotency_key=f"sub:{uid}:2026-09")
    sid = start_streaming_session(uid, event_id)
    place_bet(uid, outcome_id, Decimal("25.00"), sid, idempotency_key=f"bet:{uid}:1")
    return {"user_id": uid, "event_id": event_id, "outcome_id": outcome_id, "session_id": sid}


if __name__ == "__main__":
    init_engine(DATABASE_URL)
    create_tables(drop_first=True)
    print(seed_demo_flow())
