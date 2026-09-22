import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db import BusinessError, create_tables, init_engine, tx
from models import SportsEvent
from operations.engagement import record_ping
from operations.streaming import end_streaming_session
from seed import seed_demo_flow

if __name__ == "__main__":
    init_engine()
    create_tables(drop_first=True)
    ctx = seed_demo_flow()  # open streaming session on an event with two priced outcomes

    r1 = record_ping(ctx["session_id"])
    assert r1["sports_event_id"] == ctx["event_id"], r1
    assert r1["ping_count"] == 1, r1
    assert r1["latest_price"] is not None, r1

    r2 = record_ping(ctx["session_id"])
    assert r2["ping_count"] == 2, r2

    with tx() as s:
        assert s.get(SportsEvent, ctx["event_id"]).ping_count == 2

    end_streaming_session(ctx["session_id"])
    try:
        record_ping(ctx["session_id"])
        assert False, "expected BusinessError for a closed session"
    except BusinessError:
        pass

    print("test_record_ping: all asserts passed")
