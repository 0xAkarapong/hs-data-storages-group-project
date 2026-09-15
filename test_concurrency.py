from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from sqlalchemy import func, select

from db import create_tables, init_engine, tx
from models import Payment
from operations import (
    BusinessError, place_bet, register_user, settle_outcome, start_streaming_session
)
from seed import seed_demo_flow


def run(fn, n):
    with ThreadPoolExecutor(max_workers=n) as ex:
        futs = [ex.submit(fn, i) for i in range(n)]
    ok, err = [], []
    for f in futs:
        try:
            ok.append(f.result())
        except BusinessError as e:
            err.append(str(e))
    return ok, err


if __name__ == "__main__":
    init_engine()
    create_tables(drop_first=True)
    ctx = seed_demo_flow()

    # A) 20 threads, same email → exactly 1 succeeds
    ok, err = run(lambda _: register_user("dup@example.com", "pw", "Dup"), 20)
    print("register:", len(ok), "created /", len(err), "rejected")     # 1 / 19

    # B) Standard plan caps at 2 streams; 1 is open → 10 threads, 1 more allowed
    ok, err = run(lambda _: start_streaming_session(ctx["user_id"], ctx["event_id"]), 10)
    print("streams:", len(ok), "opened /", len(err), "rejected")        # 1 / 9

    # C) Same idempotency key 8x → one bet, one payment
    ok, _ = run(lambda _: place_bet(ctx["user_id"], ctx["outcome_id"],
                                    Decimal("10.00"), None, "bet:dup:99"), 8)
    print("distinct bets:", len({r["bet_id"] for r in ok}))             # 1

    # D) 5 concurrent settlements → paid exactly once
    ok, _ = run(lambda _: settle_outcome(ctx["outcome_id"], won=True), 5)
    print("effective settlements:", sum(0 if r["already_settled"] else 1 for r in ok))  # 1

    with tx() as s:
        print("payout rows:", s.scalar(select(func.count()).select_from(Payment)
                                       .where(Payment.idempotency_key.like("payout:%"))))
