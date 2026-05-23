"""End-to-end integration tests for :class:`PostgresWriter`.

Each test connects to the live PostgreSQL container, scopes its row writes to
a unique ``event_id`` namespace so concurrent runs don't collide, and cleans
up on teardown.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID, uuid4

import psycopg
import pytest

from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
)
from streaming_feature_store.sink.postgres_writer import PostgresWriter

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)


@pytest.fixture
def writer(docker_services_up, postgres_dsn):
    """Yield a :class:`PostgresWriter` and clean inserted rows on teardown."""
    inserted_ids: list[UUID] = []
    pw = PostgresWriter(postgres_dsn)

    yield pw, inserted_ids

    pw.close()
    if inserted_ids:
        with psycopg.connect(postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM raw_events WHERE event_id = ANY(%s)",
                    [inserted_ids],
                )
            conn.commit()


def _build_events(n: int, user_id: str = "u-pw-test") -> list[EcommerceEvent]:
    """Build *n* unique events for the per-test namespace."""
    now = datetime.now(tz=timezone.utc)
    return [
        EcommerceEvent(
            event_id=uuid4(),
            event_type=EventType.CLICK,
            user_id=user_id,
            session_id=f"s-{i}",
            event_timestamp=now,
            payload=ClickPayload(element_id="btn", page_url="/home"),
        )
        for i in range(n)
    ]


def _count_rows(dsn: str, ids: list[UUID]) -> int:
    """Return the number of ``raw_events`` rows whose event_id is in *ids*."""
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM raw_events WHERE event_id = ANY(%s)",
                [ids],
            )
            row = cur.fetchone()
    return row[0] if row else 0


def test_postgres_writer_end_to_end_inserts_rows(writer, postgres_dsn) -> None:
    """Real PG → flush 10 events → SELECT count(*) == 10 for those ids."""
    pw, inserted_ids = writer
    events = _build_events(10)
    inserted_ids.extend(e.event_id for e in events)
    result = pw.flush(events)
    assert result.inserted == 10
    assert result.skipped == 0
    assert _count_rows(postgres_dsn, inserted_ids) == 10


def test_postgres_writer_end_to_end_on_conflict_skips(
    writer, postgres_dsn
) -> None:
    """Second insert of the same batch returns skipped=N, table unchanged."""
    pw, inserted_ids = writer
    events = _build_events(5)
    inserted_ids.extend(e.event_id for e in events)
    first = pw.flush(events)
    second = pw.flush(events)
    assert first.inserted == 5
    assert first.skipped == 0
    assert second.inserted == 0
    assert second.skipped == 5
    assert _count_rows(postgres_dsn, inserted_ids) == 5


def test_postgres_writer_end_to_end_jsonb_payload_round_trip(
    writer, postgres_dsn
) -> None:
    """Payload survives the JSONB serialization round-trip."""
    pw, inserted_ids = writer
    events = _build_events(1)
    inserted_ids.extend(e.event_id for e in events)
    pw.flush(events)
    with psycopg.connect(postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT properties FROM raw_events WHERE event_id = %s",
                [events[0].event_id],
            )
            row = cur.fetchone()
    assert row is not None
    properties = row[0]
    assert properties == events[0].payload.model_dump()
