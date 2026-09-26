"""Show a lost OddsSnapshot write after a real crash mid-flush — HW3-alternative task 4.

Accepts several OddsSnapshot writes through the Redis-buffered
record_odds_update_redis(), then reconciles them. `_crash_worker_odds.py`
runs as a real subprocess, pops one batch from the Redis queue — the point
of no return — announces it, and blocks; this script SIGKILLs it right
there, before it can ever reach the Postgres write. That's a genuine
OS-level process kill, not a `random.random() < p` roll (flush_odds_queue's
own `p` parameter does that; this script demonstrates the same failure mode
for real, mirroring flush_counters_crash_demo.py — see
docs/adr/0001-ping-counter-redis-write-behind.md).

The accepted-vs-landed mismatch after the kill IS the deliverable.

Run from the project root: python benchmarks/oddsnapshot_flush_crash_demo.py
"""
import datetime as dt
import random
import signal
import subprocess
import sys
import uuid
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select

from db import create_tables, init_engine, tx
from models import OddsSnapshot, Outcome
from operations.odds_feed import (
    count_snapshots_for,
    ensure_latest_price_columns,
    flush_odds_queue,
    odds_accepted_key,
    odds_queue_key,
    record_odds_update_redis,
)
from redis_client import init_redis
from seed import seed_outcomes_and_odds, seed_sports_event

WRITES = 50
CRASH_WORKER = Path(__file__).with_name("_crash_worker_odds.py")


def kill_mid_flush(run_id: str, batch_size: int) -> int:
    """Really crash a process right after it pops the Redis queue, before it can write Postgres.
    Returns how many payloads were popped (and are now permanently gone)."""
    print(f"  launching _crash_worker_odds.py to pop {batch_size} payloads off the queue...")
    proc = subprocess.Popen(
        [sys.executable, str(CRASH_WORKER), run_id, str(batch_size)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    )
    assert proc.stdout is not None
    line = proc.stdout.readline()  # "popped <n>" — the pop already happened, irreversibly
    print(f"  worker reports: {line.strip()} (irreversible — gone from Redis now)")
    print("  SIGKILLing the worker before it can reach the Postgres write...")
    proc.kill()                    # SIGKILL: no chance left to reach the Postgres write
    proc.wait()
    assert proc.returncode == -signal.SIGKILL, f"expected a real SIGKILL, got {proc.returncode}"
    print(f"  confirmed: worker died with signal {-proc.returncode} (SIGKILL), never reached Postgres")
    return int(line.split()[1])


def main():
    print("--- setup ---")
    engine = init_engine()
    redis = init_redis()
    redis.ping()
    create_tables()  # additive only
    ensure_latest_price_columns(engine)

    with tx() as s:
        event_id = seed_sports_event(s)
        outcome_id = seed_outcomes_and_odds(s, event_id)[0]
    run_id = uuid.uuid4().hex[:12]
    print(f"  seeded sports_event_id={event_id}, outcome_id={outcome_id}, run_id={run_id}")

    try:
        print(f"\n--- accepting {WRITES} OddsSnapshot writes through the Redis-buffered path ---")
        prices, captured_ats = [], []
        for i in range(WRITES):
            price = Decimal("2.000") + Decimal(i) / 1000
            captured_at = dt.datetime.now(dt.UTC) + dt.timedelta(microseconds=i)
            record_odds_update_redis(outcome_id, price, captured_at, run_id)
            prices.append(price)
            captured_ats.append(captured_at)
        accepted = int(redis.get(odds_accepted_key(run_id)) or 0)
        print(f"  {accepted} writes accepted (queued in Redis, none landed in Postgres yet)")
        assert accepted == WRITES, accepted

        before = count_snapshots_for(outcome_id)

        # Kill the most RECENT writes, not the oldest. Losing an early tick has
        # no visible effect — a later successful write already supersedes it in
        # Outcome.latest_price. Losing the latest tick is what actually leaves
        # latest_price stale, which is the real "what you gave up" consequence.
        killed_batch = random.randint(1, WRITES)  # random size each run
        survivors = WRITES - killed_batch

        conflicts = 0
        if survivors:
            print(f"\n--- reconciling the {survivors} oldest writes normally, before the crash ---")
            result = flush_odds_queue(run_id, batch_size=survivors)
            assert result["popped"] == survivors, result
            conflicts = result["conflicts"]
            assert result["lost"] == 0
            print(f"  flush_odds_queue: popped={result['popped']}, landed={result['landed']}, "
                  f"conflicts={result['conflicts']}")

        print(f"\n--- crashing a real subprocess mid-flush, on the {killed_batch} newest queued writes ---")
        lost = kill_mid_flush(run_id, killed_batch)

        landed = count_snapshots_for(outcome_id) - before
        still_buffered = redis.llen(odds_queue_key(run_id))

        print("\n--- per-write reconciliation ---")
        with tx() as s:
            persisted_times = {row.captured_at for row in s.scalars(
                select(OddsSnapshot).where(OddsSnapshot.outcome_id == outcome_id))}
            latest_price = s.get(Outcome, outcome_id).latest_price
        print(f"{'#':>3}  {'price':>7}  {'captured_at':>26}  outcome")
        for i, (price, captured_at) in enumerate(zip(prices, captured_ats)):
            status = "landed" if captured_at in persisted_times else "LOST (killed mid-flush)"
            print(f"{i:>3}  {price!s:>7}  {captured_at.isoformat():>26}  {status}")

        print("\n--- visible consequence ---")
        print(f"  true latest price (write #{WRITES - 1}, never landed): {prices[-1]}")
        if latest_price is None:
            print("  Outcome.latest_price: never set — every write was lost")
        else:
            print(f"  Outcome.latest_price actually reads: {latest_price} (stale by {killed_batch} ticks)")

        print("\n--- summary ---")
        print(f"  accepted: {accepted}")
        print(f"  landed in Postgres: {landed}")
        print(f"  conflicts: {conflicts}")
        print(f"  still buffered: {still_buffered}")
        print(f"  lost to the real crash (popped from Redis, never reached Postgres): {lost}")
        print(f"\n{lost} of {accepted} OddsSnapshot writes were permanently lost to a real crash mid-flush.")
        assert accepted == landed + conflicts + still_buffered + lost, (
            accepted, landed, conflicts, still_buffered, lost)
        assert lost == killed_batch
        assert latest_price != prices[-1]  # the stale-read consequence, made concrete
    finally:
        redis.delete(odds_queue_key(run_id), odds_accepted_key(run_id))


if __name__ == "__main__":
    main()
