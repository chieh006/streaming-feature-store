"""Unit tests for ``AvroEventProducer`` with the underlying Kafka and registry
clients mocked out."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from streaming_feature_store.config import (
    KafkaConfig,
    ProducerTuning,
    SchemaRegistryConfig,
)
from streaming_feature_store.producer.avro_producer import (
    AvroEventProducer,
    _build_sample_event,
    _event_to_dict,
)
from streaming_feature_store.schemas import (
    ClickPayload,
    EcommerceEvent,
    EventType,
    PurchasePayload,
)


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers="broker-1:9092,broker-2:9092",
        security_protocol="PLAINTEXT",
        topic="e-commerce-events",
    )


@pytest.fixture
def registry_config() -> SchemaRegistryConfig:
    return SchemaRegistryConfig(url="http://registry:8081")


@pytest.fixture
def patched_clients(
    kafka_config: KafkaConfig, registry_config: SchemaRegistryConfig
):
    """Patch ``SchemaRegistryClient`` and ``SerializingProducer`` with mocks."""
    with patch(
        "streaming_feature_store.schemas.registry.SchemaRegistryClient"
    ) as registry_cls, patch(
        "streaming_feature_store.producer.avro_producer.SerializingProducer"
    ) as producer_cls, patch(
        "streaming_feature_store.producer.avro_producer.AvroSerializer"
    ) as serializer_cls:
        registry_instance = MagicMock(name="SchemaRegistryClient")
        registry_cls.return_value = registry_instance
        producer_instance = MagicMock(name="SerializingProducer")
        producer_instance.flush.return_value = 0
        producer_cls.return_value = producer_instance
        serializer_instance = MagicMock(name="AvroSerializer")
        serializer_cls.return_value = serializer_instance
        yield {
            "registry_cls": registry_cls,
            "registry_instance": registry_instance,
            "producer_cls": producer_cls,
            "producer_instance": producer_instance,
            "serializer_cls": serializer_cls,
        }


@pytest.fixture
def producer(
    patched_clients: dict,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> AvroEventProducer:
    return AvroEventProducer(kafka_config, registry_config)


@pytest.fixture
def click_event() -> EcommerceEvent:
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.CLICK,
        user_id="u-42",
        session_id="s-1",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=ClickPayload(element_id="btn", page_url="/home"),
    )


def test_producer_builds_with_expected_serializer_config(
    patched_clients: dict, producer: AvroEventProducer
) -> None:
    serializer_cls = patched_clients["serializer_cls"]
    serializer_cls.assert_called_once()
    kwargs = serializer_cls.call_args.kwargs
    assert kwargs["conf"]["auto.register.schemas"] is False
    assert kwargs["conf"]["use.latest.version"] is True
    assert kwargs["to_dict"] is _event_to_dict


def test_producer_uses_topic_from_kafka_config(
    producer: AvroEventProducer, kafka_config: KafkaConfig
) -> None:
    assert producer.topic == kafka_config.topic


def test_producer_topic_override(
    patched_clients: dict,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> None:
    p = AvroEventProducer(kafka_config, registry_config, topic="other")
    assert p.topic == "other"


def test_producer_propagates_bootstrap_servers(
    patched_clients: dict, producer: AvroEventProducer, kafka_config: KafkaConfig
) -> None:
    producer_kwargs = patched_clients["producer_cls"].call_args.args[0]
    assert producer_kwargs["bootstrap.servers"] == kafka_config.bootstrap_servers
    assert producer_kwargs["security.protocol"] == kafka_config.security_protocol


def test_produce_serializes_and_sends(
    patched_clients: dict,
    producer: AvroEventProducer,
    click_event: EcommerceEvent,
) -> None:
    producer.produce(click_event)
    underlying = patched_clients["producer_instance"]
    underlying.produce.assert_called_once()
    call = underlying.produce.call_args
    assert call.kwargs["topic"] == producer.topic
    assert call.kwargs["key"] == click_event.user_id
    assert call.kwargs["value"] is click_event


def test_produce_uses_default_delivery_callback(
    patched_clients: dict,
    producer: AvroEventProducer,
    click_event: EcommerceEvent,
) -> None:
    producer.produce(click_event)
    callback = patched_clients["producer_instance"].produce.call_args.kwargs["on_delivery"]
    assert callback is producer._delivery_report  # noqa: SLF001


def test_produce_accepts_custom_delivery_callback(
    patched_clients: dict,
    producer: AvroEventProducer,
    click_event: EcommerceEvent,
) -> None:
    custom = MagicMock()
    producer.produce(click_event, on_delivery=custom)
    callback = patched_clients["producer_instance"].produce.call_args.kwargs["on_delivery"]
    assert callback is custom


def test_produce_rejects_non_pydantic_input(producer: AvroEventProducer) -> None:
    with pytest.raises(TypeError):
        producer.produce({"event_id": "foo"})  # type: ignore[arg-type]


def test_produce_rejects_after_close(
    producer: AvroEventProducer, click_event: EcommerceEvent
) -> None:
    producer.close()
    with pytest.raises(RuntimeError):
        producer.produce(click_event)


def test_context_manager_flushes_on_exit(
    patched_clients: dict,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> None:
    with AvroEventProducer(kafka_config, registry_config):
        pass
    patched_clients["producer_instance"].flush.assert_called()


def test_close_is_idempotent(
    patched_clients: dict, producer: AvroEventProducer
) -> None:
    producer.close()
    producer.close()
    assert patched_clients["producer_instance"].flush.call_count == 1


def test_close_logs_warning_on_unflushed_messages(
    patched_clients: dict,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    caplog: pytest.LogCaptureFixture,
) -> None:
    patched_clients["producer_instance"].flush.return_value = 3
    p = AvroEventProducer(kafka_config, registry_config)
    with caplog.at_level(logging.WARNING):
        p.close()
    assert any("3 message" in rec.message for rec in caplog.records)


def test_default_delivery_report_logs_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    err = MagicMock()
    err.__str__ = lambda self: "boom"  # type: ignore[assignment]
    with caplog.at_level(logging.WARNING):
        AvroEventProducer._delivery_report(err, None)
    assert any("Delivery failed" in rec.message for rec in caplog.records)


def test_default_delivery_report_logs_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    msg = MagicMock()
    msg.topic.return_value = "t"
    msg.partition.return_value = 0
    msg.offset.return_value = 42
    with caplog.at_level(logging.DEBUG):
        AvroEventProducer._delivery_report(None, msg)
    assert any("Delivered" in rec.message for rec in caplog.records)


def test_default_delivery_report_handles_missing_msg() -> None:
    """No exception when both err and msg are None (defensive)."""
    AvroEventProducer._delivery_report(None, None)


def test_event_to_dict_adapter(click_event: EcommerceEvent) -> None:
    out = _event_to_dict(click_event, ctx=None)
    assert out["user_id"] == click_event.user_id


def test_schema_str_property_is_canonical_json(producer: AvroEventProducer) -> None:
    assert producer.schema_str.startswith("{")
    assert "EcommerceEvent" in producer.schema_str


@pytest.mark.parametrize(
    ("index", "expected_type"),
    [(0, EventType.CLICK), (1, EventType.PURCHASE), (2, EventType.PAGE_VIEW)],
)
def test_build_sample_event_cycles_event_types(
    index: int, expected_type: EventType
) -> None:
    event = _build_sample_event(index)
    assert event.event_type is expected_type


def test_produce_polls_underlying_producer(
    patched_clients: dict,
    producer: AvroEventProducer,
    click_event: EcommerceEvent,
) -> None:
    producer.produce(click_event)
    patched_clients["producer_instance"].poll.assert_called_with(0)


def test_flush_delegates_to_underlying(
    patched_clients: dict, producer: AvroEventProducer
) -> None:
    patched_clients["producer_instance"].flush.return_value = 0
    assert producer.flush(timeout_s=2.5) == 0
    patched_clients["producer_instance"].flush.assert_called_with(2.5)


def test_producer_applies_default_tuning(
    patched_clients: dict, producer: AvroEventProducer
) -> None:
    """Default ``ProducerTuning`` knobs must be merged into producer conf."""
    conf = patched_clients["producer_cls"].call_args.args[0]
    assert conf["linger.ms"] == 20
    assert conf["compression.type"] == "lz4"
    assert conf["queue.buffering.max.messages"] == 1_000_000
    assert conf["queue.buffering.max.kbytes"] == 1_048_576
    assert conf["acks"] == "1"
    assert conf["batch.size"] == 2_000_000


def test_producer_accepts_custom_tuning(
    patched_clients: dict,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> None:
    """A caller-supplied ``ProducerTuning`` overrides the defaults."""
    tuning = ProducerTuning(
        linger_ms=5,
        compression_type="zstd",
        queue_buffering_max_messages=200_000,
        queue_buffering_max_kbytes=524_288,
        acks="all",
        batch_size=131_072,
    )
    AvroEventProducer(kafka_config, registry_config, tuning=tuning)
    conf = patched_clients["producer_cls"].call_args.args[0]
    assert conf["linger.ms"] == 5
    assert conf["compression.type"] == "zstd"
    assert conf["queue.buffering.max.messages"] == 200_000
    assert conf["queue.buffering.max.kbytes"] == 524_288
    assert conf["acks"] == "all"
    assert conf["batch.size"] == 131_072


def test_producer_tuning_rejects_invalid_compression() -> None:
    """Pydantic validation must reject unknown compression codecs."""
    with pytest.raises(ValueError):
        ProducerTuning(compression_type="brotli")


def test_producer_tuning_rejects_invalid_acks() -> None:
    """Pydantic validation must reject unknown acks values."""
    with pytest.raises(ValueError):
        ProducerTuning(acks="2")


def test_producer_tuning_as_librdkafka_conf_keys() -> None:
    """``as_librdkafka_conf`` must emit librdkafka-style dotted keys."""
    keys = ProducerTuning().as_librdkafka_conf().keys()
    assert {
        "linger.ms",
        "compression.type",
        "queue.buffering.max.messages",
        "queue.buffering.max.kbytes",
        "acks",
        "batch.size",
    } == set(keys)


def test_purchase_event_routes_through_to_dict_adapter() -> None:
    """The to_dict adapter must produce serializer-compatible shape."""
    event = EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.PURCHASE,
        user_id="u-1",
        session_id="s-1",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=PurchasePayload(product_id="p-1", quantity=1, price_cents=10),
    )
    d = _event_to_dict(event, ctx=None)
    assert isinstance(d["payload"], tuple)
    fqn, body = d["payload"]
    assert "PurchasePayload" in fqn
    assert body["product_id"] == "p-1"
