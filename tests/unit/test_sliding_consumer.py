"""Unit tests for :mod:`streaming_feature_store.sliding.consumer`."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from confluent_kafka import TopicPartition
from confluent_kafka.serialization import MessageField, SerializationContext

from streaming_feature_store.sliding import consumer as consumer_mod
from streaming_feature_store.sliding.consumer import (
    SlidingFeaturesConsumer,
    SlidingRunSnapshot,
)
from streaming_feature_store.sliding.models import (
    SlidingConsumerConfig,
    SlidingFeatureRecord,
    WindowResolution,
)

TOPIC = "validated-events"


def _make_consumer(config: SlidingConsumerConfig | None = None, **inject):
    """Build a consumer with mock collaborators injected (no real I/O)."""
    config = config or SlidingConsumerConfig()
    collaborators = {
        "consumer": MagicMock(),
        "manager": MagicMock(),
        "watermark": MagicMock(),
        "redis_sink": MagicMock(),
        "kafka_sink": MagicMock(),
        "late_sink": MagicMock(),
        "now_ms": lambda: 1_000_000,
    }
    collaborators.update(inject)
    return SlidingFeaturesConsumer(config, **collaborators)


def _msg(value=None, error=None, partition=0, topic=TOPIC):
    msg = MagicMock()
    msg.value.return_value = {} if value is None else value
    msg.error.return_value = error
    msg.partition.return_value = partition
    msg.topic.return_value = topic
    return msg


# ---------------------------------------------------------------------------
# _handle_message
# ---------------------------------------------------------------------------


def test_handle_message_folds_event_and_counts(sliding_events) -> None:
    event = sliding_events.click(ts_ms=1_700_000_000_000)
    cons = _make_consumer()
    cons._watermark.watermark_ms.return_value = 5_000
    cons._manager.add.return_value = None
    with patch.object(consumer_mod, "avro_dict_to_event", return_value=event):
        cons._handle_message(_msg(partition=3))
    cons._watermark.observe.assert_called_once_with(1_700_000_000_000)
    cons._manager.add.assert_called_once()
    assert cons._manager.add.call_args.args[3] == 3  # partition forwarded
    cons._late_sink.write_raw.assert_not_called()
    assert cons.snapshot().consumed == 1


def test_handle_message_routes_very_late_event(sliding_events) -> None:
    event = sliding_events.click(ts_ms=1_700_000_000_000)
    cons = _make_consumer()
    cons._watermark.watermark_ms.return_value = 9_999_999_999_999
    cons._manager.add.return_value = event  # signalled very-late
    with patch.object(consumer_mod, "avro_dict_to_event", return_value=event):
        cons._handle_message(_msg())
    cons._late_sink.write_raw.assert_called_once_with(event)
    assert cons.snapshot().late == 1


def test_decode_handles_raw_bytes_value(sliding_events) -> None:
    event = sliding_events.click()
    cons = _make_consumer()
    cons._deserializer = MagicMock(return_value={"decoded": True})
    msg = _msg(value=b"\x00\x01")
    with patch.object(consumer_mod, "avro_dict_to_event", return_value=event) as decode:
        assert cons._decode(msg) is event
    cons._deserializer.assert_called_once()
    payload, ctx = cons._deserializer.call_args.args
    assert payload == b"\x00\x01"
    assert isinstance(ctx, SerializationContext)
    assert ctx.topic == TOPIC
    assert ctx.field == MessageField.VALUE
    decode.assert_called_once_with({"decoded": True})


def test_decode_passes_dict_value_through_without_deserializer(sliding_events) -> None:
    event = sliding_events.click()
    cons = _make_consumer()
    cons._deserializer = MagicMock()
    msg = _msg(value={"already": "decoded"})
    with patch.object(consumer_mod, "avro_dict_to_event", return_value=event) as decode:
        assert cons._decode(msg) is event
    cons._deserializer.assert_not_called()
    decode.assert_called_once_with({"already": "decoded"})


# ---------------------------------------------------------------------------
# _emit_and_sink
# ---------------------------------------------------------------------------


def test_emit_and_sink_noop_when_watermark_none() -> None:
    cons = _make_consumer()
    cons._watermark.watermark_ms.return_value = None
    cons._emit_and_sink()
    cons._manager.emit_due_windows.assert_not_called()
    cons._redis_sink.write.assert_not_called()


def test_emit_and_sink_writes_records_to_both_sinks() -> None:
    cons = _make_consumer()
    cons._watermark.watermark_ms.return_value = 5_000
    record = SlidingFeatureRecord(
        user_id="u1", window_resolution=WindowResolution.W_5M_SLIDE_1M
    )
    cons._manager.emit_due_windows.return_value = [record]
    cons._emit_and_sink()
    cons._redis_sink.write.assert_called_once_with(record)
    cons._kafka_sink.write.assert_called_once_with(record)
    assert cons.snapshot().emitted_by_resolution["5m"] == 1


# ---------------------------------------------------------------------------
# _poll_once
# ---------------------------------------------------------------------------


def test_poll_once_handles_good_message(sliding_events) -> None:
    event = sliding_events.click()
    cons = _make_consumer()
    cons._consumer.poll.return_value = _msg()
    cons._watermark.watermark_ms.return_value = None
    cons._manager.add.return_value = None
    with patch.object(consumer_mod, "avro_dict_to_event", return_value=event):
        cons._poll_once()
    assert cons.snapshot().consumed == 1
    cons._consumer.commit.assert_called_once_with(asynchronous=True)


def test_poll_once_skips_none_message() -> None:
    cons = _make_consumer()
    cons._consumer.poll.return_value = None
    cons._watermark.watermark_ms.return_value = None
    cons._poll_once()
    assert cons.snapshot().consumed == 0
    cons._consumer.commit.assert_called_once()


def test_poll_once_skips_errored_message() -> None:
    cons = _make_consumer()
    cons._consumer.poll.return_value = _msg(error=MagicMock())
    cons._watermark.watermark_ms.return_value = None
    cons._poll_once()
    assert cons.snapshot().consumed == 0


# ---------------------------------------------------------------------------
# run / shutdown
# ---------------------------------------------------------------------------


def test_run_subscribes_loops_once_and_shuts_down() -> None:
    cons = _make_consumer()
    cons._watermark.watermark_ms.return_value = None

    def _poll(_timeout):
        cons.request_shutdown()
        return None

    cons._consumer.poll.side_effect = _poll
    snapshot = cons.run()
    subscribe_kwargs = cons._consumer.subscribe.call_args
    assert subscribe_kwargs.args[0] == [TOPIC]
    assert "on_assign" in subscribe_kwargs.kwargs
    assert "on_revoke" in subscribe_kwargs.kwargs
    cons._kafka_sink.flush.assert_called()
    cons._consumer.close.assert_called_once()
    assert isinstance(snapshot, SlidingRunSnapshot)


def test_shutdown_sinks_tolerates_commit_failure(caplog) -> None:
    import logging

    cons = _make_consumer()
    cons._consumer.commit.side_effect = RuntimeError("broker gone")
    with caplog.at_level(logging.WARNING):
        cons._shutdown_sinks()
    assert any("final commit failed" in r.getMessage() for r in caplog.records)
    cons._consumer.close.assert_called_once()
    cons._redis_sink.close.assert_called_once()


# ---------------------------------------------------------------------------
# rebalance callbacks
# ---------------------------------------------------------------------------


def test_on_assign_applies_warmup_seek_back() -> None:
    cons = _make_consumer()
    fake_consumer = MagicMock()
    fake_consumer.offsets_for_times.return_value = [
        TopicPartition(TOPIC, 0, 2_000),
        TopicPartition(TOPIC, 1, -1),  # no offset for the timestamp
    ]
    parts = [TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)]
    cons._on_assign(fake_consumer, parts)
    fake_consumer.offsets_for_times.assert_called_once()
    fake_consumer.assign.assert_called_once_with(parts)
    assert parts[0].offset == 2_000  # rewound
    assert parts[1].offset == -1001  # OFFSET_INVALID, left untouched


def test_on_assign_without_warmup_just_assigns() -> None:
    cons = _make_consumer(SlidingConsumerConfig(warmup_seek_back=False))
    fake_consumer = MagicMock()
    parts = [TopicPartition(TOPIC, 0)]
    cons._on_assign(fake_consumer, parts)
    fake_consumer.offsets_for_times.assert_not_called()
    fake_consumer.assign.assert_called_once_with(parts)


def test_warmup_seek_back_swallows_lookup_failure(caplog) -> None:
    import logging

    cons = _make_consumer()
    fake_consumer = MagicMock()
    fake_consumer.offsets_for_times.side_effect = RuntimeError("timeout")
    parts = [TopicPartition(TOPIC, 0)]
    with caplog.at_level(logging.WARNING):
        cons._apply_warmup_seek_back(fake_consumer, parts)
    assert any("warm-up seek-back skipped" in r.getMessage() for r in caplog.records)


def test_on_revoke_drops_partition_state() -> None:
    cons = _make_consumer()
    cons._manager.drop_partitions.return_value = 4
    cons._on_revoke(MagicMock(), [TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 2)])
    cons._manager.drop_partitions.assert_called_once_with({0, 2})


# ---------------------------------------------------------------------------
# snapshot
# ---------------------------------------------------------------------------


def test_snapshot_reports_counters() -> None:
    cons = _make_consumer()
    cons._manager.active_user_count = 7
    snap = cons.snapshot()
    assert snap.active_users == 7
    assert set(snap.emitted_by_resolution) == {"5m", "1h", "24h"}


# ---------------------------------------------------------------------------
# default collaborator construction
# ---------------------------------------------------------------------------


def test_constructor_builds_all_collaborators_by_default() -> None:
    config = SlidingConsumerConfig(warmup_seek_back=True)
    with (
        patch.object(consumer_mod, "SchemaRegistry"),
        patch.object(consumer_mod, "AvroDeserializer"),
        patch.object(consumer_mod, "Consumer") as consumer_cls,
        patch.object(consumer_mod, "SlidingWindowManager") as manager_cls,
        patch.object(consumer_mod, "WatermarkTracker") as wm_cls,
        patch.object(consumer_mod, "RedisHashSink") as redis_cls,
        patch.object(consumer_mod, "KafkaSlidingFeaturesSink") as kafka_cls,
        patch.object(consumer_mod, "KafkaLateEventsSink") as late_cls,
    ):
        SlidingFeaturesConsumer(config)
    consumer_cls.assert_called_once()
    manager_cls.assert_called_once()
    wm_cls.assert_called_once()
    redis_cls.assert_called_once()
    kafka_cls.assert_called_once()
    late_cls.assert_called_once()


def test_request_shutdown_sets_flag() -> None:
    cons = _make_consumer()
    assert not cons._shutdown.is_set()
    cons.request_shutdown()
    assert cons._shutdown.is_set()


def test_default_now_ms_returns_epoch_millis() -> None:
    before = consumer_mod._now_ms()
    assert isinstance(before, int)
    assert before > 1_700_000_000_000  # well past 2023 in ms
