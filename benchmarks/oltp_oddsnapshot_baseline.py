"""OLTP baseline for record_odds_update on one hot outcome — HW3-alternative task 2.

Duration-based, not fixed-count: WORKERS threads all call record_odds_update
against the SAME outcome_id for a fixed wall-clock window (DURATION_SECONDS),
as fast as they can. RPS comes from a row-count snapshot at the database
(COUNT(*) on odds_snapshots for this one outcome, before vs after) instead of
client-side timing — see docs/adr/302-alternative.md for why.

Reads the whole connection (including POSTGRES_HOST) from .env, unlike
oltp_ping_baseline.py, so it runs unchanged against local Postgres or the
real RDS. It also never creates/drops its own schema: this role has no
CREATE SCHEMA privilege on the real RDS (checked — permission denied), so it
seeds a fresh SportsEvent/Outcome inside whatever SCHEMA_NAME already exists
and scopes every count to that one new outcome_id, never the whole table —
that table already holds other runs' and teammates' rows.

Run from the project root: python benchmarks/oltp_oddsnapshot_baseline.py
"""
import datetime as dt
import sys
import threading
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy.exc import IntegrityError

from db import create_tables, init_engine, tx
from operations.odds_feed import count_snapshots_for, ensure_latest_price_columns, record_odds_update
from seed import seed_outcomes_and_odds, seed_sports_event

WORKERS = 8
DURATION_SECONDS = 15


def worker(outcome_id: int, deadline: float, counters: dict, lock: threading.Lock) -> None:
    calls = conflicts = 0
    while time.perf_counter() < deadline:
        price = Decimal("2.000") + Decimal(calls % 500) / 1000
        try:
            record_odds_update(outcome_id, price, dt.datetime.now(dt.UTC))
            calls += 1
        except IntegrityError:
            # (outcome_id, captured_at) collision under concurrency on the
            # hot outcome — a real, expected rejection (see Q9), not a bug.
            conflicts += 1
    with lock:
        counters["calls"] += calls
        counters["conflicts"] += conflicts


def main():
    engine = init_engine(pool_size=WORKERS, max_overflow=2, pool_timeout=60)
    create_tables()  # additive only — creates missing tables, never drops/alters existing ones
    ensure_latest_price_columns(engine)

    with tx() as s:
        event_id = seed_sports_event(s)
        outcome_id = seed_outcomes_and_odds(s, event_id)[0]

    before = count_snapshots_for(outcome_id)

    counters = {"calls": 0, "conflicts": 0}
    lock = threading.Lock()
    deadline = time.perf_counter() + DURATION_SECONDS
    threads = [threading.Thread(target=worker, args=(outcome_id, deadline, counters, lock))
               for _ in range(WORKERS)]

    started = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    seconds = time.perf_counter() - started

    landed = count_snapshots_for(outcome_id) - before
    rps = landed / seconds

    print(f"oltp record_odds_update, {WORKERS} workers, 1 hot outcome, {seconds:.1f}s window:")
    print(f"  accepted calls: {counters['calls']}, conflicts: {counters['conflicts']}")
    print(f"  rows landed (DB row-count snapshot): {landed}, RPS: {rps:,.1f}")

    assert landed == counters["calls"] - counters["conflicts"], (landed, counters)


if __name__ == "__main__":
    main()
