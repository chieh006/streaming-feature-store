"""Unit tests for ``AvroEventConsumer`` with Kafka and registry mocked."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consumer.avro_consumer import (
    AvroEventConsumer,
    _coerce_event_id,
    _coerce_timestamp,
    _passthrough_from_dict,
    avro_dict_to_event,
)
from streaming_feature_store.schemas import EventType


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers="broker-1:9092",
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
    with patch(
        "streaming_feature_store.schemas.registry.SchemaRegistryClient"
    ) as registry_cls, patch(
        "streaming_feature_store.consumer.avro_consumer.DeserializingConsumer"
    ) as consumer_cls, patch(
        "streaming_feature_store.consumer.avro_consumer.AvroDeserializer"
    ) as deserializer_cls:
        registry_cls.return_value = MagicMock(name="SchemaRegistryClient")
        consumer_instance = MagicMock(name="DeserializingConsumer")
        consumer_cls.return_value = consumer_instance
        deserializer_cls.return_value = MagicMock(name="AvroDeserializer")
        yield {
            "registry_cls": registry_cls,
            "consumer_cls": consumer_cls,
            "consumer_instance": consumer_instance,
            "deserializer_cls": deserializer_cls,
        }


@pytest.fixture
def consumer(
    patched_clients: dict,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> AvroEventConsumer:
    return AvroEventConsumer(
        kafka_config, registry_config, group_id="test-group"
    )


# ---------------------------------------------------------------------------
# Construction wiring
# ---------------------------------------------------------------------------


def test_consumer_builds_with_expected_config(
    patched_clients: dict, consumer: AvroEventConsumer
) -> None:
    conf = patched_clients["consumer_cls"].call_args.args[0]
    assert conf["auto.offset.reset"] == "earliest"
    assert conf["enable.auto.commit"] is False
    assert conf["group.id"] == "test-group"
    assert conf["bootstrap.servers"].startswith("broker-1")


def test_consumer_passes_reader_schema_when_provided(
    patched_clients: dict,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> None:
    AvroEventConsumer(
        kafka_config,
        registry_config,
        group_id="g",
        reader_schema_str="{\"type\":\"record\"}",
    )
    kwargs = patched_clients["deserializer_cls"].call_args.kwargs
    assert kwargs["schema_str"] == "{\"type\":\"record\"}"


def test_consumer_falls_back_to_writer_schema_when_no_reader(
    patched_clients: dict, consumer: AvroEventConsumer
) -> None:
    kwargs = patched_clients["deserializer_cls"].call_args.kwargs
    assert kwargs["schema_str"] is None


def test_consumer_topic_override(
    patched_clients: dict,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> None:
    c = AvroEventConsumer(
        kafka_config, registry_config, group_id="g", topic="other"
    )
    assert c.topic == "other"


def test_consumer_uses_default_topic(
    consumer: AvroEventConsumer, kafka_config: KafkaConfig
) -> None:
    assert consumer.topic == kafka_config.topic


def test_reader_schema_str_property(
    patched_clients: dict,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> None:
    c = AvroEventConsumer(
        kafka_config, registry_config, group_id="g", reader_schema_str="X"
    )
    assert c.reader_schema_str == "X"


# ---------------------------------------------------------------------------
# Polling and conversion
# ---------------------------------------------------------------------------


def _make_event_dict(event_type: EventType = EventType.CLICK) -> dict:
    """Build a minimal Avro-deserialized event dict."""
    if event_type is EventType.CLICK:
        payload = (
            "com.featurestore.ecommerce.v1.ClickPayload",
            {"element_id": "btn-1", "page_url": "/home"},
        )
    elif event_type is EventType.PURCHASE:
        payload = (
            "com.featurestore.ecommerce.v1.PurchasePayload",
            {
                "product_id": "p",
                "quantity": 1,
                "price_cents": 100,
                "currency": "USD",
            },
        )
    else:
        payload = (
            "com.featurestore.ecommerce.v1.PageViewPayload",
            {"page_url": "/x", "referrer": None},
        )
    return {
        "event_id": str(uuid4()),
        "event_type": event_type.value,
        "user_id": "u-1",
        "session_id": "s-1",
        "event_timestamp": datetime.now(tz=timezone.utc),
        "payload": payload,
    }


def test_consume_returns_list_of_pydantic_events(
    patched_clients: dict, consumer: AvroEventConsumer
) -> None:
    events = [_make_event_dict() for _ in range(3)]
    messages = []
    for d in events:
        m = MagicMock()
        m.error.return_value = None
        m.value.return_value = d
        messages.append(m)
    patched_clients["consumer_instance"].poll.side_effect = messages + [None] * 10

    out = consumer.consume(timeout_s=1.0, max_messages=3)
    assert len(out) == 3
    patched_clients["consumer_instance"].subscribe.assert_called_once_with(
        [consumer.topic]
    )


def test_consume_raw_skips_pydantic_validation(
    patched_clients: dict, consumer: AvroEventConsumer
) -> None:
    weird = {"not_a_real_event": True}
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = weird
    patched_clients["consumer_instance"].poll.side_effect = [msg, None]

    out = consumer.consume_raw(timeout_s=0.5, max_messages=1)
    assert out == [weird]


def test_consume_respects_max_messages(
    patched_clients: dict, consumer: AvroEventConsumer
) -> None:
    events = [_make_event_dict() for _ in range(5)]
    messages = []
    for d in events:
        m = MagicMock()
        m.error.return_value = None
        m.value.return_value = d
        messages.append(m)
    patched_clients["consumer_instance"].poll.side_effect = messages

    out = consumer.consume(timeout_s=10.0, max_messages=2)
    assert len(out) == 2


def test_consume_returns_empty_list_when_no_messages(
    patched_clients: dict, consumer: AvroEventConsumer
) -> None:
    patched_clients["consumer_instance"].poll.return_value = None
    out = consumer.consume(timeout_s=0.05, max_messages=5)
    assert out == []


def test_consume_raises_on_kafka_error(
    patched_clients: dict, consumer: AvroEventConsumer
) -> None:
    err = MagicMock()
    err.__str__ = lambda self: "boom"  # type: ignore[assignment]
    msg = MagicMock()
    msg.error.return_value = err
    patched_clients["consumer_instance"].poll.return_value = msg

    with pytest.raises(RuntimeError):
        consumer.consume(timeout_s=0.5, max_messages=1)


def test_consume_subscribes_only_once(
    patched_clients: dict, consumer: AvroEventConsumer
) -> None:
    patched_clients["consumer_instance"].poll.return_value = None
    consumer.consume(timeout_s=0.05, max_messages=1)
    consumer.consume(timeout_s=0.05, max_messages=1)
    assert patched_clients["consumer_instance"].subscribe.call_count == 1


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_context_manager_closes_consumer(
    patched_clients: dict,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> None:
    with AvroEventConsumer(kafka_config, registry_config, group_id="g"):
        pass
    patched_clients["consumer_instance"].close.assert_called_once()


def test_close_is_idempotent(
    patched_clients: dict, consumer: AvroEventConsumer
) -> None:
    consumer.close()
    consumer.close()
    assert patched_clients["consumer_instance"].close.call_count == 1


# ---------------------------------------------------------------------------
# Adapters and conversion helpers
# ---------------------------------------------------------------------------


def test_passthrough_from_dict_returns_input() -> None:
    d = {"a": 1}
    assert _passthrough_from_dict(d, ctx=None) is d


def test_coerce_event_id_accepts_uuid_instance() -> None:
    u = uuid4()
    assert _coerce_event_id(u) is u


def test_coerce_event_id_parses_string() -> None:
    u = uuid4()
    assert _coerce_event_id(str(u)) == u


def test_coerce_timestamp_accepts_datetime() -> None:
    now = datetime.now(tz=timezone.utc)
    assert _coerce_timestamp(now) == now


def test_coerce_timestamp_assumes_utc_for_naive_datetime() -> None:
    naive = datetime(2026, 5, 4, 12, 0, 0)
    out = _coerce_timestamp(naive)
    assert out.tzinfo is not None


def test_coerce_timestamp_parses_micros_int() -> None:
    micros = 1_700_000_000_000_000
    out = _coerce_timestamp(micros)
    assert out.tzinfo is not None
    assert out.year >= 2023


def test_avro_dict_to_event_round_trip_click() -> None:
    d = _make_event_dict(EventType.CLICK)
    event = avro_dict_to_event(d)
    assert event.event_type is EventType.CLICK
    assert event.payload.element_id == "btn-1"  # type: ignore[union-attr]


def test_avro_dict_to_event_round_trip_purchase() -> None:
    d = _make_event_dict(EventType.PURCHASE)
    event = avro_dict_to_event(d)
    assert event.event_type is EventType.PURCHASE
    assert event.payload.product_id == "p"  # type: ignore[union-attr]


def test_avro_dict_to_event_round_trip_page_view() -> None:
    d = _make_event_dict(EventType.PAGE_VIEW)
    event = avro_dict_to_event(d)
    assert event.event_type is EventType.PAGE_VIEW
    assert event.payload.page_url == "/x"  # type: ignore[union-attr]


def test_avro_dict_to_event_handles_payload_without_record_name() -> None:
    """Fallback path: payload is plain dict; discriminator comes from event_type."""
    d = _make_event_dict(EventType.CLICK)
    fqn, body = d["payload"]
    d["payload"] = body
    event = avro_dict_to_event(d)
    assert event.payload.element_id == "btn-1"  # type: ignore[union-attr]
