"""Unit tests for the sliding consumer's transactional EOS path (week2_03 §2.4).

Exercises the batched transactional loop with injected fakes (no broker):
begin → produce(late + feature) → send_offsets → commit → Redis-after-commit,
plus the empty-batch skip, the offsets-only commit, and the abort/fatal
branches.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException

from streaming_feature_store.eos import TransactionalConfig
from streaming_feature_store.sliding import consumer as consumer_module
from streaming_feature_store.sliding.consumer import SlidingFeaturesConsumer
from streaming_feature_store.sliding.models import (
    SlidingConsumerConfig,
    WindowResolution,
)


class _Err:
    """Fake KafkaError with a configurable ``txn_requires_abort``."""

    def __init__(self, abort: bool) -> None:
        self._abort = abort

    def txn_requires_abort(self) -> bool:
        return self._abort


class _RecProducer:
    """Transactional producer stand-in recording calls into a shared list."""

    def __init__(self, events: list, commit_exc: Exception | None = None) -> None:
        self.events = events
        self._commit_exc = commit_exc

    def init_transactions(self, *_a) -> None:
        self.events.append("init")

    def begin_transaction(self) -> None:
        self.events.append("begin")

    def produce(self, topic, key, value) -> None:  # noqa: ARG002
        self.events.append(("produce", topic, key))

    def send_offsets_to_transaction(self, _pos, _meta) -> None:
        self.events.append("send_offsets")

    def commit_transaction(self, *_a) -> None:
        self.events.append("commit")
        if self._commit_exc is not None:
            raise self._commit_exc

    def abort_transaction(self, *_a) -> None:
        self.events.append("abort")

    def flush(self, *_a) -> int:
        self.events.append("flush")
        return 0

    def close(self) -> None:
        self.events.append("close")


class _RecRedis:
    """Redis sink stand-in that records writes into the shared order list."""

    def __init__(self, events: list) -> None:
        self.events = events
        self.writes: list = []

    def write(self, record) -> None:
        self.events.append("redis")
        self.writes.append(record)

    def close(self) -> None:
        self.events.append("redis_close")


def _record(
    resolution: WindowResolution = WindowResolution.W_5M_SLIDE_1M,
    key: str = "u1:5m",
):
    record = MagicMock()
    record.window_resolution = resolution
    record.kafka_key.return_value = key
    return record


def _good_msg() -> MagicMock:
    msg = MagicMock()
    msg.error.return_value = None
    return msg


def _eos_consumer(
    events: list,
    *,
    commit_exc: Exception | None = None,
    consumer: MagicMock | None = None,
    manager: MagicMock | None = None,
    watermark: MagicMock | None = None,
):
    producer = _RecProducer(events, commit_exc=commit_exc)
    cons = SlidingFeaturesConsumer(
        SlidingConsumerConfig(),
        consumer=consumer or MagicMock(),
        manager=manager or MagicMock(),
        watermark=watermark or MagicMock(),
        redis_sink=_RecRedis(events),
        txn_producer=producer,
        txn_config=TransactionalConfig(
            enabled=True, transactional_id="sf-0", commit_timeout_s=5.0
        ),
    )
    return cons, producer


# --- constructor ------------------------------------------------------------


def test_eos_constructor_skips_default_kafka_sinks() -> None:
    cons, _ = _eos_consumer([])
    assert cons._eos is True
    assert cons._kafka_sink is None
    assert cons._late_sink is None
    assert cons._commit_timeout_s == 5.0


def test_eos_build_consumer_applies_static_membership() -> None:
    # Building the real consumer (consumer=None) under EOS must set
    # group.instance.id + read_committed (week2_03 §2.3 / §2.5).
    with (
        patch.object(consumer_module, "Consumer") as consumer_cls,
        patch.object(consumer_module, "SchemaRegistry"),
        patch.object(consumer_module, "AvroDeserializer"),
    ):
        SlidingFeaturesConsumer(
            SlidingConsumerConfig(),
            redis_sink=MagicMock(),
            txn_producer=_RecProducer([]),
            txn_config=TransactionalConfig(
                enabled=True, transactional_id="sf-0", group_instance_id="sf-0"
            ),
        )
    conf = consumer_cls.call_args.args[0]
    assert conf["group.instance.id"] == "sf-0"
    assert conf["isolation.level"] == "read_committed"


# --- _collect_batch ---------------------------------------------------------


def test_collect_batch_drains_and_skips_errored() -> None:
    consumer = MagicMock()
    good1, good2 = _good_msg(), _good_msg()
    bad = MagicMock()
    bad.error.return_value = "boom"
    consumer.poll.side_effect = [good1, bad, good2, None]
    cons, _ = _eos_consumer([], consumer=consumer)
    assert cons._collect_batch() == [good1, good2]


def test_collect_batch_caps_at_max_records(monkeypatch) -> None:
    monkeypatch.setattr(consumer_module, "_EOS_MAX_RECORDS", 2)
    consumer = MagicMock()
    consumer.poll.side_effect = [_good_msg(), _good_msg(), _good_msg(), None]
    cons, _ = _eos_consumer([], consumer=consumer)
    assert len(cons._collect_batch()) == 2


# --- _poll_batch_eos --------------------------------------------------------


def test_empty_batch_opens_no_transaction() -> None:
    events: list = []
    consumer = MagicMock()
    consumer.poll.side_effect = [None]
    watermark = MagicMock()
    watermark.watermark_ms.return_value = None
    manager = MagicMock()
    cons, _ = _eos_consumer(
        events, consumer=consumer, manager=manager, watermark=watermark
    )
    cons._poll_batch_eos()
    assert events == []
    manager.emit_due_windows.assert_not_called()


def test_batch_produces_offsets_commit_then_redis_in_order() -> None:
    events: list = []
    consumer = MagicMock()
    consumer.poll.side_effect = [_good_msg(), None]
    consumer.assignment.return_value = ["tp"]
    consumer.position.return_value = ["pos"]
    consumer.consumer_group_metadata.return_value = "meta"
    manager = MagicMock()
    record = _record()
    manager.emit_due_windows.return_value = [record]
    watermark = MagicMock()
    watermark.watermark_ms.return_value = 1000
    cons, _ = _eos_consumer(
        events, consumer=consumer, manager=manager, watermark=watermark
    )
    cons._fold_message = MagicMock(return_value=None)  # no late event

    cons._poll_batch_eos()

    assert events == [
        "begin",
        ("produce", "sliding-features", "u1:5m"),
        "send_offsets",
        "commit",
        "redis",
    ]
    assert cons._emitted[WindowResolution.W_5M_SLIDE_1M] == 1
    assert cons._consumed == 1


def test_late_event_routed_to_late_topic_inside_txn() -> None:
    events: list = []
    consumer = MagicMock()
    consumer.poll.side_effect = [_good_msg(), None]
    consumer.assignment.return_value = []
    consumer.position.return_value = []
    consumer.consumer_group_metadata.return_value = "meta"
    manager = MagicMock()
    manager.emit_due_windows.return_value = []
    watermark = MagicMock()
    watermark.watermark_ms.return_value = 1000
    cons, _ = _eos_consumer(
        events, consumer=consumer, manager=manager, watermark=watermark
    )
    late = MagicMock()
    late.user_id = "u9"
    cons._fold_message = MagicMock(return_value=late)

    cons._poll_batch_eos()

    assert ("produce", "sliding-features-late", "u9") in events
    assert cons._late == 1


def test_nonempty_batch_with_no_output_commits_offsets_only() -> None:
    events: list = []
    consumer = MagicMock()
    consumer.poll.side_effect = [_good_msg(), None]
    consumer.assignment.return_value = ["tp"]
    consumer.position.return_value = ["pos"]
    consumer.consumer_group_metadata.return_value = "meta"
    manager = MagicMock()
    manager.emit_due_windows.return_value = []
    watermark = MagicMock()
    watermark.watermark_ms.return_value = 1000
    cons, _ = _eos_consumer(
        events, consumer=consumer, manager=manager, watermark=watermark
    )
    cons._fold_message = MagicMock(return_value=None)

    cons._poll_batch_eos()

    assert events == ["begin", "send_offsets", "commit"]
    assert cons._consumed == 1


# --- _commit_batch_txn error handling --------------------------------------


def test_abortable_error_aborts_then_reraises() -> None:
    events: list = []
    consumer = MagicMock()
    consumer.assignment.return_value = []
    consumer.position.return_value = []
    consumer.consumer_group_metadata.return_value = "meta"
    cons, _ = _eos_consumer(
        events, commit_exc=KafkaException(_Err(abort=True)), consumer=consumer
    )
    with pytest.raises(KafkaException):
        cons._commit_batch_txn([], [_record()])
    assert events[-1] == "abort"


def test_fatal_error_reraises_without_abort() -> None:
    events: list = []
    consumer = MagicMock()
    consumer.assignment.return_value = []
    consumer.position.return_value = []
    consumer.consumer_group_metadata.return_value = "meta"
    cons, _ = _eos_consumer(
        events, commit_exc=KafkaException(_Err(abort=False)), consumer=consumer
    )
    with pytest.raises(KafkaException):
        cons._commit_batch_txn([], [])
    assert "abort" not in events


# --- run() / shutdown EOS branches -----------------------------------------


def test_run_eos_inits_transactions_before_subscribe() -> None:
    events: list = []
    consumer = MagicMock()
    consumer.subscribe.side_effect = lambda *a, **k: events.append("subscribe")
    consumer.poll.side_effect = [None]
    watermark = MagicMock()
    watermark.watermark_ms.return_value = None
    cons, _ = _eos_consumer(
        events, consumer=consumer, manager=MagicMock(), watermark=watermark
    )
    original = cons._poll_batch_eos

    def _once() -> None:
        original()
        cons.request_shutdown()

    cons._poll_batch_eos = _once
    cons.run()

    assert events[0] == "init"
    assert "subscribe" in events
    assert events.index("init") < events.index("subscribe")


def test_shutdown_sinks_eos_flushes_producer_and_skips_plain_commit() -> None:
    events: list = []
    consumer = MagicMock()
    cons, _ = _eos_consumer(events, consumer=consumer)
    cons._shutdown_sinks()
    assert events == ["flush", "close", "redis_close"]
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once_with()
