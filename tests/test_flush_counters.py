import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select

from db import create_tables, init_engine, tx
from models import SportsEvent
from operations.engagement import flush_counters, ping_key, record_ping_redis
from redis_client import get_redis, init_redis
from seed import seed_event_with_session, seed_plans

if __name__ == "__main__":
    init_engine()
    init_redis()
    create_tables(drop_first=True)
    get_redis().flushdb()

    with tx() as s:
        plan_ids = seed_plans(s)

    tag = uuid.uuid4().hex[:8]
    event_a, session_a = seed_event_with_session(plan_ids["Standard"], f"flushA{tag}")
    event_b, session_b = seed_event_with_session(plan_ids["Standard"], f"flushB{tag}")

    for _ in range(3):
        record_ping_redis(session_a)
    for _ in range(5):
        record_ping_redis(session_b)

    # p=0.0: every nonzero counter reconciles.
    results = flush_counters(p=0.0)
    by_event = {r["sports_event_id"]: r for r in results}
    assert by_event[event_a] == {"sports_event_id": event_a, "delta": 3, "persisted": True}
    assert by_event[event_b] == {"sports_event_id": event_b, "delta": 5, "persisted": True}

    with tx() as s:
        counts = {row.sports_event_id: row.ping_count for row in s.scalars(select(SportsEvent))}
    assert counts[event_a] == 3, counts
    assert counts[event_b] == 5, counts

    # Redis keys are popped on reconcile — nothing left to double count.
    r = get_redis()
    assert r.get(ping_key(event_a)) is None
    assert flush_counters(p=0.0) == []  # nothing nonzero left

    # A quiet event (never pinged) never shows up in flush_counters()'s output.
    event_c, _ = seed_event_with_session(plan_ids["Standard"], f"flushC{tag}")
    assert all(r["sports_event_id"] != event_c for r in flush_counters(p=0.0))

    # p=1.0: forced crash. The pop already happened, so the ping is truly lost —
    # not merely delayed — and a later flush can't recover it.
    for _ in range(4):
        record_ping_redis(session_a)
    crashed = flush_counters(p=1.0)
    assert crashed == [{"sports_event_id": event_a, "delta": 4, "persisted": False}]

    with tx() as s:
        assert s.get(SportsEvent, event_a).ping_count == 3  # unchanged, not 7

    assert flush_counters(p=0.0) == []  # the 4 lost pings are gone, not queued

    print("test_flush_counters: all asserts passed")
