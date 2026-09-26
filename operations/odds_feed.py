import datetime as dt
import json
import random
from decimal import Decimal

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from db import BusinessError, retry_on_conflict, tx
from models import SCHEMA_NAME, OddsSnapshot, Outcome, OutcomeStatus
from redis_client import get_redis


def odds_queue_key(run_id: str) -> str:
    """Redis List Path-B writers RPUSH to and the flusher LPOPs from, namespaced by SCHEMA_NAME."""
    return f"{SCHEMA_NAME or 'public'}:oddsfeed:{run_id}:queue"


def odds_accepted_key(run_id: str) -> str:
    """Redis counter, INCR'd once per accepted Path-B write — Path B's accepted-request count, never LLEN."""
    return f"{SCHEMA_NAME or 'public'}:oddsfeed:{run_id}:accepted"


def ensure_latest_price_columns(engine: Engine) -> None:
    """Idempotent additive migration for the two latest-price cache columns — no CREATE SCHEMA privilege
    on the real RDS, so `outcomes` may already exist without them; call once after create_tables()."""
    schema = SCHEMA_NAME or "public"
    with engine.begin() as conn:
        conn.exec_driver_sql(f'ALTER TABLE "{schema}".outcomes ADD COLUMN IF NOT EXISTS latest_price NUMERIC(8,3)')
        conn.exec_driver_sql(
            f'ALTER TABLE "{schema}".outcomes ADD COLUMN IF NOT EXISTS latest_captured_at TIMESTAMPTZ')


def count_snapshots_for(outcome_id: int) -> int:
    """Row-count snapshot for one outcome — the RPS measurement primitive, scoped to avoid other runs' rows."""
    with tx() as s:
        return s.scalar(select(func.count()).select_from(OddsSnapshot)
                         .where(OddsSnapshot.outcome_id == outcome_id))


def _validate_open_outcome(s: Session, outcome_id: int) -> Outcome:
    outcome = s.scalar(select(Outcome).where(Outcome.outcome_id == outcome_id))
    if not outcome or outcome.status != OutcomeStatus.open:
        raise BusinessError("outcome is not open")
    return outcome


def _apply_latest_price(s: Session, outcome_id: int, price: Decimal, captured_at: dt.datetime) -> None:
    """Guarded by latest_captured_at so an out-of-order write can never roll the cached price backwards."""
    s.execute(
        update(Outcome)
        .where(Outcome.outcome_id == outcome_id)
        .where((Outcome.latest_captured_at.is_(None)) | (Outcome.latest_captured_at < captured_at))
        .values(latest_price=price, latest_captured_at=captured_at)
    )


@retry_on_conflict()
def record_odds_update(outcome_id: int, price: Decimal, captured_at: dt.datetime) -> dict:
    """OLTP path (A): validate, insert, update cached latest price — one transaction, on the outcome's row lock."""
    with tx() as s:
        _validate_open_outcome(s, outcome_id)
        s.add(OddsSnapshot(outcome_id=outcome_id, captured_at=captured_at, price=price))
        _apply_latest_price(s, outcome_id, price, captured_at)
        return {"outcome_id": outcome_id, "price": price, "captured_at": captured_at}


def record_odds_update_redis(outcome_id: int, price: Decimal, captured_at: dt.datetime, run_id: str) -> dict:
    """NoSQL-boosted path (B): validate in SQL, then queue the write in Redis — INSERT/UPDATE deferred
    to flush_odds_queue(). One pipelined round trip so the queue and the accepted counter can't drift."""
    with tx() as s:
        _validate_open_outcome(s, outcome_id)

    payload = json.dumps({
        "outcome_id": outcome_id,
        "captured_at": captured_at.isoformat(),
        "price": str(price),
    })

    pipe = get_redis().pipeline(transaction=True)
    pipe.rpush(odds_queue_key(run_id), payload)
    pipe.incr(odds_accepted_key(run_id))
    pipe.execute()

    return {"outcome_id": outcome_id, "price": price, "captured_at": captured_at}


def flush_odds_queue(run_id: str, batch_size: int = 500, p: float = 0.0) -> dict:
    """Pop up to batch_size payloads, bulk INSERT ... ON CONFLICT DO NOTHING, roll each outcome's
    latest price forward from the batch's own max-captured_at entry. `p` simulates a crash: the
    pop already happened (irreversible) but the Postgres write is skipped — a genuine lost write,
    mirroring flush_counters(p) (docs/adr/0001-ping-counter-redis-write-behind.md). Returns
    {popped, landed, conflicts, lost}; accepted = landed + conflicts + still_buffered (LLEN) + lost."""
    raw = get_redis().lpop(odds_queue_key(run_id), batch_size)
    if not raw:
        return {"popped": 0, "landed": 0, "conflicts": 0, "lost": 0}

    if random.random() < p:
        return {"popped": len(raw), "landed": 0, "conflicts": 0, "lost": len(raw)}

    rows = [json.loads(item) for item in raw]

    latest_per_outcome: dict[int, tuple[dt.datetime, Decimal]] = {}
    for row in rows:
        captured_at = dt.datetime.fromisoformat(row["captured_at"])
        prior = latest_per_outcome.get(row["outcome_id"])
        if prior is None or captured_at > prior[0]:
            latest_per_outcome[row["outcome_id"]] = (captured_at, Decimal(row["price"]))

    with tx() as s:
        # rowcount is unreliable here (observed -1 against real PostgreSQL) — RETURNING is exact.
        result = s.execute(
            pg_insert(OddsSnapshot)
            .values([
                {"outcome_id": row["outcome_id"],
                 "captured_at": dt.datetime.fromisoformat(row["captured_at"]),
                 "price": Decimal(row["price"])}
                for row in rows
            ])
            .on_conflict_do_nothing(index_elements=["outcome_id", "captured_at"])
            .returning(OddsSnapshot.snapshot_id)
        )
        landed = len(result.fetchall())
        conflicts = len(rows) - landed

        for outcome_id, (captured_at, price) in latest_per_outcome.items():
            _apply_latest_price(s, outcome_id, price, captured_at)

    return {"popped": len(rows), "landed": landed, "conflicts": conflicts, "lost": 0}
