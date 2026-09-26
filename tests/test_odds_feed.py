import datetime as dt
import sys
import uuid
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db import BusinessError, create_tables, init_engine, tx
from models import Outcome, OutcomeStatus
from operations.odds_feed import (
    ensure_latest_price_columns,
    flush_odds_queue,
    odds_accepted_key,
    odds_queue_key,
    record_odds_update,
    record_odds_update_redis,
)
from redis_client import get_redis, init_redis
from seed import seed_outcomes_and_odds, seed_sports_event

T0 = dt.datetime(2026, 9, 26, 12, 0, 0, tzinfo=dt.UTC)


def cleanup_redis(run_id: str) -> None:
    """Never flushdb() — this Redis is shared with the rest of the class.
    Delete only this run's own keys, by exact name."""
    get_redis().delete(odds_queue_key(run_id), odds_accepted_key(run_id))


if __name__ == "__main__":
    # This role has no CREATE SCHEMA privilege on the real RDS (checked —
    # permission denied) — the only schema it can use is the one the teacher
    # already created for it (SCHEMA_NAME from .env). create_tables() is
    # additive (creates missing tables, never drops/alters existing ones);
    # ensure_latest_price_columns() is an idempotent, additive column add —
    # safe to run against a schema that may already hold real data.
    engine = init_engine()
    init_redis()
    create_tables()
    ensure_latest_price_columns(engine)

    # Seed only what this test needs (event + outcomes) — never seed_plans()/
    # seed_reference_data(), which assume an empty schema and would collide
    # with the real Plan rows teammates have already seeded into drake888.
    with tx() as s:
        event_id = seed_sports_event(s)
        outcome_id, closed_outcome_id = seed_outcomes_and_odds(s, event_id)
        s.get(Outcome, closed_outcome_id).status = OutcomeStatus.won
    run_id = uuid.uuid4().hex[:12]

    try:
        # --- Path A: OLTP insert + guarded latest-price update ---
        r1 = record_odds_update(outcome_id, Decimal("2.10"), T0)
        assert r1["outcome_id"] == outcome_id, r1
        with tx() as s:
            o = s.get(Outcome, outcome_id)
            assert o.latest_price == Decimal("2.10"), o.latest_price
            assert o.latest_captured_at == T0, o.latest_captured_at

        # A newer write moves the cache forward.
        t1 = T0 + dt.timedelta(seconds=5)
        record_odds_update(outcome_id, Decimal("2.20"), t1)
        with tx() as s:
            assert s.get(Outcome, outcome_id).latest_price == Decimal("2.20")

        # An out-of-order write (older captured_at) must NOT roll the cache back.
        t_old = T0 - dt.timedelta(seconds=1)
        record_odds_update(outcome_id, Decimal("9.99"), t_old)
        with tx() as s:
            o = s.get(Outcome, outcome_id)
            assert o.latest_price == Decimal("2.20"), "out-of-order write rolled back latest_price"
            assert o.latest_captured_at == t1

        try:
            record_odds_update(999_999_999, Decimal("2.00"), T0)
            assert False, "expected BusinessError for a nonexistent outcome"
        except BusinessError:
            pass

        try:
            record_odds_update(closed_outcome_id, Decimal("2.00"), T0)
            assert False, "expected BusinessError for a non-open outcome"
        except BusinessError:
            pass

        # --- Path B: Redis-accepted writes, then flush ---
        for i in range(5):
            price = Decimal("3.00") + Decimal("0.01") * i
            record_odds_update_redis(outcome_id, price, T0 + dt.timedelta(seconds=10 + i), run_id)
        accepted = int(get_redis().get(odds_accepted_key(run_id)) or 0)
        assert accepted == 5, accepted

        result = flush_odds_queue(run_id)
        assert result == {"popped": 5, "landed": 5, "conflicts": 0, "lost": 0}, result
        with tx() as s:
            o = s.get(Outcome, outcome_id)
            assert o.latest_price == Decimal("3.04"), o.latest_price  # T0+14s, the newest of the batch

        # Draining an empty queue is a no-op, not an error.
        assert flush_odds_queue(run_id) == {"popped": 0, "landed": 0, "conflicts": 0, "lost": 0}

        # --- Conflicts: a duplicate (outcome_id, captured_at) in one batch ---
        # doesn't abort the whole batch — it's counted, the rest still lands.
        dup_time = T0 + dt.timedelta(seconds=100)
        record_odds_update_redis(outcome_id, Decimal("4.00"), dup_time, run_id)
        record_odds_update_redis(outcome_id, Decimal("4.50"), dup_time, run_id)  # same key -> conflict
        record_odds_update_redis(outcome_id, Decimal("4.10"), dup_time + dt.timedelta(seconds=1), run_id)
        result = flush_odds_queue(run_id)
        assert result == {"popped": 3, "landed": 2, "conflicts": 1, "lost": 0}, result

        # --- Crash: pop already happened, Postgres write is skipped -> genuinely lost ---
        record_odds_update_redis(outcome_id, Decimal("5.00"), T0 + dt.timedelta(seconds=200), run_id)
        record_odds_update_redis(outcome_id, Decimal("5.10"), T0 + dt.timedelta(seconds=201), run_id)
        result = flush_odds_queue(run_id, p=1.0)
        assert result == {"popped": 2, "landed": 0, "conflicts": 0, "lost": 2}, result
        assert get_redis().lpop(odds_queue_key(run_id), 10) is None  # gone, not queued for retry

        # --- Accounting identity across the whole run ---
        # accepted (Redis counter) == landed + conflicts + lost, with nothing
        # left still_buffered (we drained every flush above to empty).
        total_accepted = int(get_redis().get(odds_accepted_key(run_id)) or 0)
        assert total_accepted == 5 + 3 + 2, total_accepted
        still_buffered = get_redis().llen(odds_queue_key(run_id))
        landed_total = 5 + 2
        conflicts_total = 1
        lost_total = 2
        assert total_accepted == landed_total + conflicts_total + still_buffered + lost_total, (
            total_accepted, landed_total, conflicts_total, still_buffered, lost_total)

        # A non-open outcome is rejected on the Redis path too (not just path A above).
        try:
            record_odds_update_redis(closed_outcome_id, Decimal("2.00"), T0, run_id)
            assert False, "expected BusinessError for a non-open outcome"
        except BusinessError:
            pass

        print("test_odds_feed: all asserts passed")
    finally:
        cleanup_redis(run_id)
