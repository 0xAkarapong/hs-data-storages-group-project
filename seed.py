import datetime as dt
from decimal import Decimal
import random

from db import create_tables, init_engine, tx
from models import (EventStatus, OddsSnapshot, Outcome, Plan, SportsEvent, User)
from operations import place_bet, purchase_subscription, register_user, start_streaming_session


import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def seed_plans(session) -> dict[str, int]:
    """
    Seed the plans table

    :param session: SQLAlchemy session
    :return: A dictionary mapping plan names to their IDs
    :rtype: dict[str, int]
    """

    plans = [
        Plan(plan_name="Basic",   monthly_price=Decimal("499"),  max_concurrent_streams=1),
        Plan(plan_name="Standard",monthly_price=Decimal("799"),  max_concurrent_streams=2),
        Plan(plan_name="Premium", monthly_price=Decimal("899"), max_concurrent_streams=4),
    ]
    session.add_all(plans)
    session.flush()

    return {
        p.plan_name: p.plan_id for p in plans
    }

def seed_sports_event(session) -> int:
    """
    Seed a sports event with outcomes and odds snapshots

    :param session: SQLAlchemy session
    :return: The ID of the created sports event
    :rtype: int
    """
    now = dt.datetime.now(dt.timezone.utc)
    ev = SportsEvent(
        title="Lions vs Tigers",
        sport_type="football",
        start_time=now + dt.timedelta(hours=2),
        status=EventStatus.live
    )
    session.add(ev)
    session.flush()

    return ev.sports_event_id

def seed_outcomes_and_odds(session, ev_id: int) -> list[int]:
    """
    Seed outcomes for a sports event

    :param session: SQLAlchemy session
    :param int ev_id: The ID of the sports event
    :return: List of outcome IDs for the created outcomes
    :rtype: list[int]
    """

    now = dt.datetime.now(dt.timezone.utc)

    outcomes = [
        Outcome(sports_event_id=ev_id, description="Lions win"),
        Outcome(sports_event_id=ev_id, description="Tigers win"),
    ]
    session.add_all(outcomes)
    session.flush()

    o1, o2 = outcomes
    odds_snapshots = [
        OddsSnapshot(outcome_id=o1.outcome_id, captured_at=now - dt.timedelta(seconds=40), price=Decimal("210")),
        OddsSnapshot(outcome_id=o1.outcome_id, captured_at=now, price=Decimal("195")),
        OddsSnapshot(outcome_id=o2.outcome_id, captured_at=now, price=Decimal("340")),
    ]
    session.add_all(odds_snapshots)
    session.flush()

    return [o1.outcome_id, o2.outcome_id]

def seed_reference_data():
    with tx() as s:
        plans_ids = seed_plans(s)
        event_id = seed_sports_event(s)
        outcome_ids = seed_outcomes_and_odds(s, event_id)

    return event_id, random.choice(outcome_ids), plans_ids

def seed_demo_flow() -> dict[str, int]:
    event_id, outcome_id, plan_ids = seed_reference_data()

    uid = register_user("ana@example.com", "s3cret!", "Ana")

    purchase_subscription(uid, plan_id=plan_ids["Standard"], payment_method="card", idempotency_key=f"sub:{uid}:2026-09")

    sid = start_streaming_session(uid, event_id)

    place_bet(uid, outcome_id, Decimal("2500.00"), sid, idempotency_key=f"bet:{uid}:1")

    return {"user_id": uid, "event_id": event_id, "outcome_id": outcome_id, "session_id": sid}


if __name__ == "__main__":
    init_engine(DATABASE_URL)
    create_tables(drop_first=True)
    print(seed_demo_flow())
