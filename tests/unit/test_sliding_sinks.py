"""Unit tests for :mod:`streaming_feature_store.sliding.sinks`."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.sliding.models import (
    SlidingConsumerConfig,
    SlidingFeatureRecord,
    WindowResolution,
)
from streaming_feature_store.sliding.sinks import (
    KafkaLateEventsSink,
    KafkaSlidingFeaturesSink,
    RedisHashSink,
    _record_to_dict,
    load_sliding_schema_str,
)


def _record_5m(click_count: int | None = 7) -> SlidingFeatureRecord:
    return SlidingFeatureRecord(
        user_id="u1",
        window_resolution=WindowResolution.W_5M_SLIDE_1M,
        window_end_ms=300_000,
        click_count=click_count,
        page_view_count=2,
        purchase_count=0,
        revenue=0.0,
    )


# ---------------------------------------------------------------------------
# schema loader
# ---------------------------------------------------------------------------


def test_load_sliding_schema_str_contains_record_name() -> None:
    assert "SlidingFeatureRecord" in load_sliding_schema_str()


# ---------------------------------------------------------------------------
# RedisHashSink
# ---------------------------------------------------------------------------


@pytest.fixture
def sliding_config() -> SlidingConsumerConfig:
    return SlidingConsumerConfig(redis_host="localhost", redis_port=6379)


def test_redis_sink_pipelines_hset_and_expire(sliding_config) -> None:
    client = MagicMock()
    pipe = client.pipeline.return_value
    sink = RedisHashSink(sliding_config, client=client)
    sink.write(_record_5m())
    client.pipeline.assert_called_once()
    pipe.hset.assert_called_once()
    key_arg = pipe.hset.call_args.args[0]
    mapping = pipe.hset.call_args.kwargs["mapping"]
    assert key_arg == "feat:user:u1"
    assert mapping["clicks_5m"] == "7"
    pipe.expire.assert_called_once_with("feat:user:u1", 450)
    pipe.execute.assert_called_once()


def test_redis_sink_skips_null_fields(sliding_config) -> None:
    client = MagicMock()
    pipe = client.pipeline.return_value
    sink = RedisHashSink(sliding_config, client=client)
    sink.write(_record_5m(click_count=None))
    mapping = pipe.hset.call_args.kwargs["mapping"]
    assert "clicks_5m" not in mapping
    assert "page_views_5m" in mapping


def test_redis_sink_writes_nothing_when_all_fields_none(sliding_config) -> None:
    client = MagicMock()
    sink = RedisHashSink(sliding_config, client=client)
    bare = SlidingFeatureRecord(
        user_id="u1", window_resolution=WindowResolution.W_5M_SLIDE_1M
    )
    sink.write(bare)
    client.pipeline.assert_not_called()


def test_redis_sink_close_is_idempotent(sliding_config) -> None:
    client = MagicMock()
    sink = RedisHashSink(sliding_config, client=client)
    sink.close()
    sink.close()
    client.close.assert_called_once()


def test_redis_sink_context_manager(sliding_config) -> None:
    client = MagicMock()
    with RedisHashSink(sliding_config, client=client) as sink:
        assert sink is not None
    client.close.assert_called_once()


def test_redis_sink_builds_default_client_from_config(sliding_config) -> None:
    with patch("streaming_feature_store.sliding.sinks.redis.Redis") as redis_cls:
        RedisHashSink(sliding_config)
    redis_cls.assert_called_once_with(host="localhost", port=6379)


# ---------------------------------------------------------------------------
# KafkaSlidingFeaturesSink
# ---------------------------------------------------------------------------


def _kafka_patches():
    return [
        patch("streaming_feature_store.sliding.sinks.SchemaRegistry"),
        patch("streaming_feature_store.sliding.sinks.AvroSerializer"),
        patch("streaming_feature_store.sliding.sinks.SerializingProducer"),
    ]


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig()


@pytest.fixture
def registry_config() -> SchemaRegistryConfig:
    return SchemaRegistryConfig()


def test_kafka_sink_registers_schema_on_construction(kafka_config, registry_config) -> None:
    sr, ser, prod = _kafka_patches()
    with sr as sr_cls, ser, prod:
        sr_cls.return_value.register.return_value = 11
        KafkaSlidingFeaturesSink(
            kafka_config, registry_config, topic="sliding-features"
        )
        sr_cls.return_value.register.assert_called_once()
        assert sr_cls.return_value.register.call_args.args[0] == "sliding-features-value"


def test_kafka_sink_skips_registration_when_disabled(kafka_config, registry_config) -> None:
    sr, ser, prod = _kafka_patches()
    with sr as sr_cls, ser, prod:
        KafkaSlidingFeaturesSink(kafka_config, registry_config, register_schema=False)
        sr_cls.return_value.register.assert_not_called()


def test_kafka_sink_write_keys_on_resolution(kafka_config, registry_config) -> None:
    sr, ser, prod = _kafka_patches()
    with sr, ser, prod as prod_cls:
        sink = KafkaSlidingFeaturesSink(
            kafka_config, registry_config, register_schema=False
        )
        record = _record_5m()
        sink.write(record)
        kwargs = prod_cls.return_value.produce.call_args.kwargs
        assert kwargs["key"] == "u1:5m"
        assert kwargs["value"] is record


def test_kafka_sink_write_after_close_raises(kafka_config, registry_config) -> None:
    sr, ser, prod = _kafka_patches()
    with sr, ser, prod:
        sink = KafkaSlidingFeaturesSink(
            kafka_config, registry_config, register_schema=False
        )
        sink.close()
        with pytest.raises(RuntimeError, match="closed"):
            sink.write(_record_5m())


def test_kafka_sink_flush_and_properties(kafka_config, registry_config) -> None:
    sr, ser, prod = _kafka_patches()
    with sr, ser, prod as prod_cls:
        prod_cls.return_value.flush.return_value = 0
        sink = KafkaSlidingFeaturesSink(
            kafka_config, registry_config, register_schema=False
        )
        assert sink.topic == "sliding-features"
        assert "SlidingFeatureRecord" in sink.schema_str
        assert sink.flush(1.0) == 0


def test_kafka_sink_close_warns_on_remaining(kafka_config, registry_config, caplog) -> None:
    import logging

    sr, ser, prod = _kafka_patches()
    with sr, ser, prod as prod_cls:
        prod_cls.return_value.flush.return_value = 3
        sink = KafkaSlidingFeaturesSink(
            kafka_config, registry_config, register_schema=False
        )
        with caplog.at_level(logging.WARNING):
            sink.close()
        sink.close()  # idempotent second close
        assert any("unflushed" in r.getMessage() for r in caplog.records)


def test_kafka_sink_delivery_report_paths(kafka_config, registry_config, caplog) -> None:
    import logging

    sr, ser, prod = _kafka_patches()
    with sr, ser, prod:
        sink = KafkaSlidingFeaturesSink(
            kafka_config, registry_config, register_schema=False
        )
        with caplog.at_level(logging.WARNING):
            sink._default_delivery_report(MagicMock(), None)  # error branch
        sink._default_delivery_report(None, MagicMock())  # success branch (no log)
        assert any("delivery failed" in r.getMessage() for r in caplog.records)


def test_kafka_sink_context_manager(kafka_config, registry_config) -> None:
    sr, ser, prod = _kafka_patches()
    with sr, ser, prod as prod_cls:
        with KafkaSlidingFeaturesSink(
            kafka_config, registry_config, register_schema=False
        ):
            pass
        prod_cls.return_value.flush.assert_called()


def test_kafka_sink_close_clean_when_fully_flushed(kafka_config, registry_config) -> None:
    sr, ser, prod = _kafka_patches()
    with sr, ser, prod as prod_cls:
        prod_cls.return_value.flush.return_value = 0  # nothing left unflushed
        sink = KafkaSlidingFeaturesSink(
            kafka_config, registry_config, register_schema=False
        )
        sink.close()
        assert sink._closed is True


def test_record_to_dict_adapter() -> None:
    record = _record_5m()
    assert _record_to_dict(record, ctx=None) == record.to_avro_dict()


# ---------------------------------------------------------------------------
# KafkaLateEventsSink
# ---------------------------------------------------------------------------


def test_late_sink_wraps_avro_event_producer(kafka_config, registry_config, sliding_events) -> None:
    with patch(
        "streaming_feature_store.sliding.sinks.AvroEventProducer"
    ) as producer_cls:
        producer_cls.return_value.topic = "sliding-features-late"
        producer_cls.return_value.flush.return_value = 0
        sink = KafkaLateEventsSink(
            kafka_config, registry_config, topic="sliding-features-late"
        )
        assert sink.topic == "sliding-features-late"
        event = sliding_events.click()
        sink.write_raw(event)
        producer_cls.return_value.produce.assert_called_once_with(event)
        assert sink.flush(1.0) == 0
        sink.close()
        sink.close()  # idempotent
        producer_cls.return_value.close.assert_called_once()


def test_late_sink_context_manager(kafka_config, registry_config) -> None:
    with patch(
        "streaming_feature_store.sliding.sinks.AvroEventProducer"
    ) as producer_cls:
        with KafkaLateEventsSink(kafka_config, registry_config):
            pass
        producer_cls.return_value.close.assert_called_once()
