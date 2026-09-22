import tempfile
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select

from benchmarks.streaming_session_close import seed_sessions
from db import create_tables, init_engine, tx
from models import StreamingSession
from operations.streaming import end_streaming_sessions_bulk


def test_bulk_close_transitions_only_open_unique_sessions():
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as database:
        database_path = database.name
    try:
        init_engine(f"sqlite+pysqlite:///{database_path}")
        create_tables(drop_first=True)
        session_ids = seed_sessions(3)

        assert end_streaming_sessions_bulk([session_ids[0], session_ids[0], session_ids[1]]) == 2
        assert end_streaming_sessions_bulk(session_ids) == 1

        with tx() as session:
            open_sessions = session.scalars(
                select(StreamingSession).where(StreamingSession.ended_at.is_(None))
            ).all()
        assert open_sessions == []
    finally:
        Path(database_path).unlink(missing_ok=True)
