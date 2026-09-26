"""Real crash victim for oddsnapshot_flush_crash_demo.py.

Pops one batch of queued OddsSnapshot payloads from Redis (the point of no
return: once popped, that batch exists nowhere else), announces how many it
got on stdout, then blocks waiting for a go-ahead on stdin that never comes —
the parent SIGKILLs this process the instant it sees the announcement. Not a
`random.random() < p` roll: this process actually dies before it can reach
the Postgres write below.

Internal to oddsnapshot_flush_crash_demo.py — not meant to be run by hand.
"""
import datetime as dt
import json
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy.dialects.postgresql import insert as pg_insert

from db import init_engine, tx
from models import OddsSnapshot
from operations.odds_feed import odds_queue_key
from redis_client import init_redis

if __name__ == "__main__":
    run_id = sys.argv[1]
    batch_size = int(sys.argv[2])

    init_engine()
    r = init_redis()

    raw = r.lpop(odds_queue_key(run_id), batch_size) or []
    print(f"popped {len(raw)}", flush=True)

    sys.stdin.readline()  # the parent's go-ahead — it never sends one

    if raw:
        rows = [json.loads(item) for item in raw]
        with tx() as s:
            s.execute(
                pg_insert(OddsSnapshot).values([
                    {"outcome_id": row["outcome_id"],
                     "captured_at": dt.datetime.fromisoformat(row["captured_at"]),
                     "price": Decimal(row["price"])}
                    for row in rows
                ]).on_conflict_do_nothing(index_elements=["outcome_id", "captured_at"])
            )
    print("committed", flush=True)
