"""Unit tests for :class:`SinkRunner` and :class:`Batch` (no real Kafka / PG)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
)
from streaming_feature_store.sink.accountant import SinkAccountant
from streaming_feature_store.sink.postgres_writer import BatchInsertResult
from streaming_feature_store.sink.sink_runner import (
    Batch,
    SinkRunConfig,
    SinkRunner,
)


def _sample_event(user_id: str = "u-1") -> EcommerceEvent:
    """Return a canned :class:`EcommerceEvent`."""
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.CLICK,
        user_id=user_id,
        session_id="s-1",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=ClickPayload(element_id="btn", page_url="/p"),
    )


def _avro_dict(event: EcommerceEvent) -> dict:
    """Return the Avro dict an :class:`AvroEventConsumer` would yield."""
    return {
        "event_id": str(event.event_id),
        "event_type": event.event_type.value,
        "user_id": event.user_id,
        "session_id": event.session_id,
        "event_timestamp": int(event.event_timestamp.timestamp() * 1_000_000),
        "payload": (
            "com.featurestore.ecommerce.v1.ClickPayload",
            event.payload.model_dump(),
        ),
    }


class FakeMsg:
    """Minimal stand-in for a confluent_kafka ``Message``."""

    def __init__(self, event: EcommerceEvent | None, partition: int = 0) -> None:
        self._event = event
        self._partition = partition

    def value(self):
        if self._event is None:
            return {"corrupt": True}  # missing keys → triggers KeyError
        return _avro_dict(self._event)

    def partition(self) -> int:
        return self._partition


class StepClock:
    """Monotonic stub: returns ``start, start+step, start+2*step, ...``."""

    def __init__(self, step: float = 0.5, start: float = 0.0) -> None:
        self.t = start
        self.step = step

    def __call__(self) -> float:
        v = self.t
        self.t += self.step
        return v


class FixedClock:
    """Monotonic stub: returns the value set by callers."""

    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def _cfg(**over) -> SinkRunConfig:
    base = dict(
        topic="e-commerce-events-feed",
        consumer_group_id="g-test",
        batch_max_rows=5,
        batch_max_age_s=10.0,
        poll_timeout_s=0.01,
        poll_max_records=100,
        flush_retry_attempts=1,
        flush_retry_backoff_s=0.0,
    )
    base.update(over)
    return SinkRunConfig(**base)


# --- config ----------------------------------------------------------------


def test_sink_run_config_refuses_benchmark_topic() -> None:
    with pytest.raises(ValueError, match="benchmark topic"):
        SinkRunConfig(topic="e-commerce-events", consumer_group_id="g")


def test_sink_run_config_accepts_feed_topic() -> None:
    cfg = SinkRunConfig(topic="e-commerce-events-feed", consumer_group_id="g")
    assert cfg.topic == "e-commerce-events-feed"


# --- Batch -----------------------------------------------------------------


def test_batch_should_flush_when_size_cap_reached() -> None:
    batch = Batch(max_rows=3, max_age_s=10.0, clock=FixedClock(0.0))
    for _ in range(3):
        batch.append(_sample_event())
    assert batch.should_flush() is True


def test_batch_should_flush_when_age_cap_exceeded() -> None:
    clock = FixedClock(0.0)
    batch = Batch(max_rows=10, max_age_s=5.0, clock=clock)
    batch.append(_sample_event())
    clock.t = 5.5
    assert batch.should_flush() is True


def test_batch_should_not_flush_when_empty() -> None:
    batch = Batch(max_rows=3, max_age_s=10.0, clock=FixedClock(0.0))
    assert batch.should_flush() is False
    assert bool(batch) is False
    assert len(batch) == 0


def test_batch_clear_resets_age_clock() -> None:
    clock = FixedClock(0.0)
    batch = Batch(max_rows=10, max_age_s=5.0, clock=clock)
    batch.append(_sample_event())
    batch.clear()
    clock.t = 100.0
    assert batch.should_flush() is False


def test_batch_invalid_max_rows() -> None:
    with pytest.raises(ValueError):
        Batch(max_rows=0, max_age_s=1.0)


def test_batch_exposes_caps_via_properties() -> None:
    batch = Batch(max_rows=42, max_age_s=7.5, clock=FixedClock(0.0))
    assert batch.max_rows == 42
    assert batch.max_age_s == 7.5


def test_batch_invalid_max_age() -> None:
    with pytest.raises(ValueError):
        Batch(max_rows=1, max_age_s=0.0)


def test_batch_events_returns_defensive_copy() -> None:
    batch = Batch(max_rows=3, max_age_s=10.0, clock=FixedClock(0.0))
    batch.append(_sample_event())
    copy = batch.events()
    copy.clear()
    assert len(batch) == 1


# --- helpers for runner tests ----------------------------------------------


def _runner(consumer, writer, accountant, *, cfg=None, clock=None, sleep=None) -> SinkRunner:
    return SinkRunner(
        consumer=consumer,
        writer=writer,
        accountant=accountant,
        config=cfg or _cfg(),
        clock=clock or FixedClock(0.0),
        sleep=sleep or (lambda _s: None),
    )


def _consumer_with(msgs_seq, poll_calls_until_shutdown=1):
    """Return a mock consumer.poll_batch that yields *msgs_seq* once then []."""
    c = MagicMock(name="AvroEventConsumer")
    c.poll_batch.side_effect = msgs_seq + [[]] * 20
    return c


def _writer_with(inserted: int, skipped: int = 0) -> MagicMock:
    w = MagicMock(name="PostgresWriter")
    w.flush.return_value = BatchInsertResult(inserted=inserted, skipped=skipped)
    return w


# --- runner --------------------------------------------------------------


def test_sink_runner_flushes_on_batch_full() -> None:
    events = [_sample_event() for _ in range(5)]
    msgs = [FakeMsg(e, partition=0) for e in events]
    consumer = _consumer_with([msgs])
    writer = _writer_with(inserted=5)
    accountant = SinkAccountant()
    cfg = _cfg(batch_max_rows=5)
    runner = _runner(consumer, writer, accountant, cfg=cfg)

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return msgs
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()

    writer.flush.assert_called()
    consumer.commit.assert_called()
    snap = accountant.snapshot()
    assert snap.consumed == 5
    assert snap.inserted == 5
    assert snap.batches_flushed == 1


def test_sink_runner_flushes_on_age_timeout() -> None:
    events = [_sample_event() for _ in range(2)]
    msgs = [FakeMsg(e, partition=1) for e in events]
    consumer = _consumer_with([msgs])
    writer = _writer_with(inserted=2)
    accountant = SinkAccountant()
    cfg = _cfg(batch_max_rows=100, batch_max_age_s=5.0)
    clock = FixedClock(0.0)
    runner = _runner(consumer, writer, accountant, cfg=cfg, clock=clock)

    call_counter = {"n": 0}

    def poll_side_effect(*_a, **_kw):
        call_counter["n"] += 1
        if call_counter["n"] == 1:
            return msgs
        if call_counter["n"] == 2:
            # Advance the clock past the age cap, no new messages.
            clock.t = 100.0
            return []
        # After flush, shutdown.
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()
    writer.flush.assert_called()
    # Inspect args: the call was made with the 2-event batch.
    flushed_events = writer.flush.call_args_list[0].args[0]
    assert len(flushed_events) == 2


def test_sink_runner_commits_after_flush_not_before() -> None:
    events = [_sample_event() for _ in range(3)]
    msgs = [FakeMsg(e) for e in events]
    consumer = _consumer_with([msgs])
    writer = _writer_with(inserted=3)
    accountant = SinkAccountant()
    cfg = _cfg(batch_max_rows=3)
    runner = _runner(consumer, writer, accountant, cfg=cfg)

    # Track ordering via a shared list.
    order: list[str] = []
    writer.flush.side_effect = lambda _e: (order.append("flush"),
                                           BatchInsertResult(inserted=3, skipped=0))[1]
    consumer.commit.side_effect = lambda: order.append("commit")

    def poll_side_effect(*_a, **_kw):
        if not order:
            return msgs
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()
    assert order.index("flush") < order.index("commit")


def test_sink_runner_deserialize_failure_does_not_break_batch() -> None:
    good = [_sample_event() for _ in range(4)]
    bad = FakeMsg(None, partition=0)
    msgs = [FakeMsg(good[0]), bad, *[FakeMsg(e) for e in good[1:]]]
    consumer = _consumer_with([msgs])
    writer = _writer_with(inserted=4)
    accountant = SinkAccountant()
    cfg = _cfg(batch_max_rows=4)
    runner = _runner(consumer, writer, accountant, cfg=cfg)

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return msgs
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()
    snap = accountant.snapshot()
    assert snap.consumed == 5
    assert snap.deserialize_failed == 1
    assert snap.inserted == 4


def test_sink_runner_shutdown_flushes_in_flight_batch() -> None:
    events = [_sample_event() for _ in range(2)]
    msgs = [FakeMsg(e) for e in events]
    consumer = _consumer_with([msgs])
    writer = _writer_with(inserted=2)
    accountant = SinkAccountant()
    cfg = _cfg(batch_max_rows=10)  # high cap so no auto-flush
    runner = _runner(consumer, writer, accountant, cfg=cfg)

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return msgs
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()
    writer.flush.assert_called_once()
    consumer.close.assert_called_once()
    writer.close.assert_called_once()


def test_sink_runner_retries_on_flush_failure() -> None:
    events = [_sample_event() for _ in range(2)]
    msgs = [FakeMsg(e) for e in events]
    consumer = _consumer_with([msgs])
    writer = MagicMock(name="PostgresWriter")
    writer.flush.side_effect = [
        RuntimeError("transient"),
        BatchInsertResult(inserted=2, skipped=0),
    ]
    accountant = SinkAccountant()
    cfg = _cfg(batch_max_rows=2, flush_retry_attempts=2)
    runner = _runner(consumer, writer, accountant, cfg=cfg)

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return msgs
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()
    assert writer.flush.call_count == 2


def test_sink_runner_raises_after_exhausting_retries() -> None:
    events = [_sample_event() for _ in range(2)]
    msgs = [FakeMsg(e) for e in events]
    consumer = _consumer_with([msgs])
    writer = MagicMock(name="PostgresWriter")
    writer.flush.side_effect = RuntimeError("permanent")
    accountant = SinkAccountant()
    cfg = _cfg(batch_max_rows=2, flush_retry_attempts=2)
    runner = _runner(consumer, writer, accountant, cfg=cfg)

    consumer.poll_batch.side_effect = [msgs, []]
    with pytest.raises(RuntimeError, match="permanent"):
        runner.run()
    assert writer.flush.call_count == 2


def test_sink_runner_skip_partition_when_none() -> None:
    event = _sample_event()
    msg = FakeMsg(event)
    msg._partition = None  # exercise the None-guard
    consumer = _consumer_with([[msg]])
    writer = _writer_with(inserted=1)
    accountant = SinkAccountant()
    cfg = _cfg(batch_max_rows=1)
    runner = _runner(consumer, writer, accountant, cfg=cfg)

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return [msg]
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()
    snap = accountant.snapshot()
    assert snap.consumed == 1
    assert snap.partition_counts == {}


def test_sink_runner_config_property() -> None:
    consumer = MagicMock()
    writer = MagicMock()
    accountant = SinkAccountant()
    cfg = _cfg()
    runner = SinkRunner(
        consumer=consumer,
        writer=writer,
        accountant=accountant,
        config=cfg,
    )
    assert runner.config is cfg


def test_sink_runner_logs_conflicts(caplog) -> None:
    events = [_sample_event() for _ in range(2)]
    msgs = [FakeMsg(e) for e in events]
    consumer = _consumer_with([msgs])
    writer = _writer_with(inserted=0, skipped=2)
    accountant = SinkAccountant()
    cfg = _cfg(batch_max_rows=2)
    runner = _runner(consumer, writer, accountant, cfg=cfg)

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return msgs
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    import logging

    with caplog.at_level(logging.INFO):
        runner.run()
    assert any("duplicate" in r.getMessage() for r in caplog.records)
