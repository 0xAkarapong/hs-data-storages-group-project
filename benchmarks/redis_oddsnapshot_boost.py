"""Compare direct-SQL vs Redis-buffered OddsSnapshot writes — HW3-alternative task 3.

Postgres path: record_odds_update does the validation SELECT, INSERT and
hot-outcome latest_price UPDATE directly in Postgres. Redis path:
record_odds_update_redis validates against a Redis status cache and only
queues the write in Redis — no Postgres on the request path; a background
flusher drains the queue into Postgres in batches while the run is still going. Both report accepted requests/second:
Postgres from a row-count snapshot (accepted == landed, synchronous), Redis
from the accepted counter (never LLEN, which drops as the flusher drains
it) — cross-checked against an independent Postgres row-count delta.

Run from the project root: python benchmarks/redis_oddsnapshot_boost.py
"""
import datetime as dt
import random
import sys
import threading
import time
import uuid
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy.exc import IntegrityError

from db import create_tables, init_engine, tx
from operations.odds_feed import (
    count_snapshots_for,
    ensure_latest_price_columns,
    flush_odds_queue,
    odds_accepted_key,
    odds_queue_key,
    record_odds_update,
    record_odds_update_redis,
)
from redis_client import get_redis, init_redis
from seed import seed_outcomes_and_odds, seed_sports_event

WORKERS = 8
DURATION_SECONDS = 15
FLUSH_INTERVAL_SECONDS = 0.5
FLUSH_BATCH_SIZE = 2000


def _next_price(rng: random.Random, price: Decimal) -> Decimal:
    """Small random walk, like a live odds feed ticking up and down. Floored above 1 (CHECK constraint)."""
    delta = Decimal(str(round(rng.uniform(-0.05, 0.05), 3)))
    return max(Decimal("1.010"), price + delta)


def run_path_postgres(outcome_id: int) -> float:
    def worker(deadline: float, counters: dict, lock: threading.Lock) -> None:
        rng = random.Random()
        price = Decimal("2.000")
        calls = conflicts = 0
        while time.perf_counter() < deadline:
            price = _next_price(rng, price)
            try:
                record_odds_update(outcome_id, price, dt.datetime.now(dt.UTC))
                calls += 1
            except IntegrityError:
                conflicts += 1
        with lock:
            counters["calls"] += calls
            counters["conflicts"] += conflicts

    before = count_snapshots_for(outcome_id)

    counters = {"calls": 0, "conflicts": 0}
    lock = threading.Lock()
    deadline = time.perf_counter() + DURATION_SECONDS
    threads = [threading.Thread(target=worker, args=(deadline, counters, lock)) for _ in range(WORKERS)]
    started = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    seconds = time.perf_counter() - started

    landed = count_snapshots_for(outcome_id) - before
    print(f"  postgres: {counters['calls']} accepted, {counters['conflicts']} conflicts, "
          f"{landed} landed, {seconds:.1f}s -> {landed / seconds:,.1f} RPS")
    # `calls` counts successful calls only — conflicts are already excluded.
    assert landed == counters["calls"], (landed, counters)
    return landed / seconds


def run_path_redis(outcome_id: int, run_id: str) -> float:
    def worker(deadline: float) -> None:
        rng = random.Random()
        price = Decimal("2.000")
        while time.perf_counter() < deadline:
            price = _next_price(rng, price)
            record_odds_update_redis(outcome_id, price, dt.datetime.now(dt.UTC), run_id)

    landed = conflicts = lost = 0
    stop_flushing = threading.Event()

    def flusher() -> None:
        nonlocal landed, conflicts, lost
        while not stop_flushing.is_set():
            result = flush_odds_queue(run_id, batch_size=FLUSH_BATCH_SIZE)
            landed += result["landed"]
            conflicts += result["conflicts"]
            lost += result["lost"]
            # Only idle when the queue is drained — a full batch means we're behind, so go again
            # immediately. Sleeping unconditionally caps ingest at FLUSH_BATCH_SIZE / interval.
            if result["popped"] < FLUSH_BATCH_SIZE:
                stop_flushing.wait(FLUSH_INTERVAL_SECONDS)

    before = count_snapshots_for(outcome_id)
    deadline = time.perf_counter() + DURATION_SECONDS
    flush_thread = threading.Thread(target=flusher)
    flush_thread.start()
    threads = [threading.Thread(target=worker, args=(deadline,)) for _ in range(WORKERS)]

    started = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    seconds = time.perf_counter() - started
    backlog_at_deadline = get_redis().llen(odds_queue_key(run_id))

    stop_flushing.set()
    flush_thread.join()
    # Drain whatever the last interval didn't catch, so the final numbers reconcile.
    while True:
        result = flush_odds_queue(run_id, batch_size=FLUSH_BATCH_SIZE)
        landed += result["landed"]
        conflicts += result["conflicts"]
        lost += result["lost"]
        if result["popped"] == 0:
            break

    accepted = int(get_redis().get(odds_accepted_key(run_id)) or 0)
    still_buffered = get_redis().llen(odds_queue_key(run_id))
    rps = accepted / seconds

    print(f"  redis: {accepted} accepted, {landed} landed, {conflicts} conflicts, "
          f"{lost} lost, {still_buffered} still buffered, {seconds:.1f}s -> {rps:,.1f} RPS")
    print(f"         flusher backlog when workers stopped: {backlog_at_deadline} queued "
          f"(small = Postgres batch ingest keeps up with accepted RPS)")
    assert accepted == landed + conflicts + still_buffered + lost, (accepted, landed, conflicts, still_buffered, lost)
    # Cross-check flush_odds_queue's self-reported `landed` against an
    # independently observed Postgres row-count delta — not just numbers
    # that are internally consistent by construction.
    actually_landed = count_snapshots_for(outcome_id) - before
    assert actually_landed == landed, (actually_landed, landed)
    return rps


def main():
    engine = init_engine(pool_size=WORKERS + 2, max_overflow=2, pool_timeout=60)
    init_redis()
    create_tables()  # additive only
    ensure_latest_price_columns(engine)

    with tx() as s:
        event_id = seed_sports_event(s)
        outcome_id = seed_outcomes_and_odds(s, event_id)[0]
    run_id = uuid.uuid4().hex[:12]

    print(f"redis_oddsnapshot_boost, {WORKERS} workers, 1 hot outcome, {DURATION_SECONDS}s per path:")
    try:
        rps_postgres = run_path_postgres(outcome_id)
        rps_redis = run_path_redis(outcome_id, run_id)
    finally:
        get_redis().delete(odds_queue_key(run_id), odds_accepted_key(run_id))

    print(f"  boost: {rps_redis / rps_postgres:.2f}x")


if __name__ == "__main__":
    main()
