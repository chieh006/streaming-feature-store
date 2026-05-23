"""Unit tests for :class:`PostgresWriter` (psycopg connection mocked)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
)
from streaming_feature_store.sink.postgres_writer import (
    BatchInsertResult,
    PostgresWriter,
    _build_insert_sql,
    _event_to_row,
)


def _sample_event() -> EcommerceEvent:
    """Return one canned :class:`EcommerceEvent`."""
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.CLICK,
        user_id="u-1",
        session_id="s-1",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=ClickPayload(element_id="btn", page_url="/home"),
    )


@pytest.fixture
def mock_connect(monkeypatch):
    """Patch :func:`psycopg.connect` to return a configurable mock connection.

    Returns
    -------
    tuple
        ``(connect_mock, conn_mock, cursor_mock)``.
    """
    conn = MagicMock(name="psycopg.Connection")
    cursor = MagicMock(name="psycopg.Cursor")
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = False
    cursor.rowcount = 0
    connect = MagicMock(return_value=conn)
    monkeypatch.setattr(
        "streaming_feature_store.sink.postgres_writer.psycopg.connect",
        connect,
    )
    return connect, conn, cursor


# --- helpers -----------------------------------------------------------------


def test_build_insert_sql_emits_on_conflict_clause() -> None:
    sql = _build_insert_sql(3)
    assert "INSERT INTO raw_events" in sql
    assert "ON CONFLICT (event_id) DO NOTHING" in sql
    # Three rows × six columns = 18 placeholders.
    assert sql.count("%s") == 18


def test_build_insert_sql_rejects_zero_rows() -> None:
    with pytest.raises(ValueError):
        _build_insert_sql(0)


def test_event_to_row_includes_six_fields() -> None:
    row = _event_to_row(_sample_event())
    assert len(row) == 6


# --- writer behaviour --------------------------------------------------------


def test_postgres_writer_builds_correct_sql(mock_connect) -> None:
    _, _, cursor = mock_connect
    writer = PostgresWriter("postgresql://x")
    cursor.rowcount = 2
    writer.flush([_sample_event(), _sample_event()])
    insert_call = [
        c for c in cursor.execute.call_args_list
        if "INSERT INTO raw_events" in (c.args[0] if c.args else "")
    ]
    assert insert_call, "expected an INSERT execute call"
    assert "ON CONFLICT (event_id) DO NOTHING" in insert_call[0].args[0]


def test_postgres_writer_flush_returns_inserted_and_skipped_counts(
    mock_connect,
) -> None:
    _, _, cursor = mock_connect
    writer = PostgresWriter("postgresql://x")
    events = [_sample_event() for _ in range(10)]
    cursor.rowcount = 7
    result = writer.flush(events)
    assert result == BatchInsertResult(inserted=7, skipped=3)
    assert result.total == 10


def test_postgres_writer_flush_empty_batch_is_noop(mock_connect) -> None:
    connect, _, cursor = mock_connect
    writer = PostgresWriter("postgresql://x")
    result = writer.flush([])
    assert result == BatchInsertResult(inserted=0, skipped=0)
    connect.assert_not_called()
    cursor.execute.assert_not_called()


def test_postgres_writer_close_is_idempotent(mock_connect) -> None:
    _, conn, _ = mock_connect
    writer = PostgresWriter("postgresql://x")
    writer.flush([_sample_event()])
    writer.close()
    writer.close()
    # First close issues conn.close; second is a no-op.
    assert conn.close.call_count == 1


def test_postgres_writer_close_without_flush_is_safe(mock_connect) -> None:
    connect, _, _ = mock_connect
    writer = PostgresWriter("postgresql://x")
    writer.close()
    connect.assert_not_called()


def test_postgres_writer_flush_rolls_back_on_failure(mock_connect) -> None:
    _, conn, cursor = mock_connect
    writer = PostgresWriter("postgresql://x")
    cursor.execute.side_effect = [None, RuntimeError("boom")]
    with pytest.raises(RuntimeError, match="boom"):
        writer.flush([_sample_event()])
    conn.rollback.assert_called_once()


def test_postgres_writer_flush_after_close_raises(mock_connect) -> None:
    writer = PostgresWriter("postgresql://x")
    writer.close()
    with pytest.raises(RuntimeError, match="closed"):
        writer.flush([_sample_event()])


def test_postgres_writer_rejects_invalid_timeout() -> None:
    with pytest.raises(ValueError):
        PostgresWriter("postgresql://x", statement_timeout_ms=0)


def test_postgres_writer_context_manager_closes(mock_connect) -> None:
    _, conn, _ = mock_connect
    with PostgresWriter("postgresql://x") as writer:
        writer.flush([_sample_event()])
    conn.close.assert_called_once()


def test_postgres_writer_sets_statement_timeout(mock_connect) -> None:
    _, _, cursor = mock_connect
    writer = PostgresWriter("postgresql://x", statement_timeout_ms=12_345)
    writer.flush([_sample_event()])
    timeout_calls = [
        c for c in cursor.execute.call_args_list
        if c.args and c.args[0].startswith("SET statement_timeout")
    ]
    assert timeout_calls, "expected SET statement_timeout to be issued"
    assert "12345" in timeout_calls[0].args[0]


def test_postgres_writer_uses_dsn_property(mock_connect) -> None:
    writer = PostgresWriter("postgresql://abc")
    assert writer.dsn == "postgresql://abc"
    assert writer.statement_timeout_ms == 30_000


def test_postgres_writer_rowcount_none_treated_as_zero(mock_connect) -> None:
    _, _, cursor = mock_connect
    writer = PostgresWriter("postgresql://x")
    cursor.rowcount = None
    result = writer.flush([_sample_event(), _sample_event()])
    assert result.inserted == 0
    assert result.skipped == 2


def test_postgres_writer_does_not_reconnect(mock_connect) -> None:
    connect, _, cursor = mock_connect
    writer = PostgresWriter("postgresql://x")
    cursor.rowcount = 1
    writer.flush([_sample_event()])
    writer.flush([_sample_event()])
    # The connection is opened lazily once and reused.
    assert connect.call_count == 1


@patch("streaming_feature_store.sink.postgres_writer.psycopg.connect")
def test_postgres_writer_propagates_connect_failure(connect_mock) -> None:
    connect_mock.side_effect = RuntimeError("cannot connect")
    writer = PostgresWriter("postgresql://x")
    with pytest.raises(RuntimeError, match="cannot connect"):
        writer.flush([_sample_event()])
