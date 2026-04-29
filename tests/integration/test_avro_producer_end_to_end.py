"""End-to-end integration tests for ``AvroEventProducer``.

Each test produces a small batch of events to the live Kafka cluster and
consumes them back via a plain ``confluent_kafka.Consumer`` (or a
deserializing consumer where roundtrip semantics matter).
"""

from __future__ import annotations

import importlib.util
import json
import logging
import struct
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
)

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.schemas import (
    ClickPayload,
    EcommerceEvent,
    EventType,
    PageViewPayload,
    PurchasePayload,
    SchemaRegistry,
)
from streaming_feature_store.schemas.loader import SCHEMAS_ROOT

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)

V1_DIR: Path = SCHEMAS_ROOT / "ecommerce" / "v1"
SCRIPT_PATH: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "register_schemas.py"
)


def _load_register_module():
    spec = importlib.util.spec_from_file_location("register_schemas_e2e", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["register_schemas_e2e"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def kafka_config() -> KafkaConfig:
    return KafkaConfig(topic=f"e2e-events-{uuid.uuid4().hex[:8]}")


@pytest.fixture(scope="module")
def registry_config() -> SchemaRegistryConfig:
    return SchemaRegistryConfig()


@pytest.fixture(scope="module")
def admin(kafka_config: KafkaConfig) -> AdminClient:
    return AdminClient({"bootstrap.servers": kafka_config.bootstrap_servers})


@pytest.fixture(scope="module")
def kafka_topic(
    docker_services_up: None,
    admin: AdminClient,
    kafka_config: KafkaConfig,
) -> str:
    futures = admin.create_topics(
        [NewTopic(kafka_config.topic, num_partitions=3, replication_factor=1)]
    )
    for topic, fut in futures.items():
        try:
            fut.result(timeout=10)
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise
    yield kafka_config.topic
    admin.delete_topics([kafka_config.topic])


@pytest.fixture(scope="module")
def registered_subject(
    docker_services_up: None,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    kafka_topic: str,
) -> str:
    subject = f"{kafka_config.topic}-value"
    registry = SchemaRegistry(registry_config)
    cli = _load_register_module()
    rc = cli.main(["--subject", subject])
    assert rc == 0
    yield subject
    try:
        registry.delete_subject(subject, permanent=False)
    except Exception:  # pragma: no cover - best-effort teardown
        pass
    try:
        registry.delete_subject(subject, permanent=True)
    except Exception:  # pragma: no cover - best-effort teardown
        pass


@pytest.fixture
def producer(
    registered_subject: str,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> AvroEventProducer:
    p = AvroEventProducer(kafka_config, registry_config)
    yield p
    p.close()


@pytest.fixture
def consumer(kafka_config: KafkaConfig) -> Consumer:
    c = Consumer(
        {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "group.id": f"test-{uuid.uuid4().hex}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    c.subscribe([kafka_config.topic])
    # Warm-up poll to trigger group assignment so subsequent ``produce`` calls
    # are observed by this consumer.
    deadline = 5.0
    while deadline > 0 and not c.assignment():
        c.poll(0.5)
        deadline -= 0.5
    yield c
    c.close()


def _click_event(user_id: str = "u-1") -> EcommerceEvent:
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.CLICK,
        user_id=user_id,
        session_id="s-1",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=ClickPayload(element_id="btn-cta", page_url="/home"),
    )


def _purchase_event(user_id: str = "u-2") -> EcommerceEvent:
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.PURCHASE,
        user_id=user_id,
        session_id="s-2",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=PurchasePayload(product_id="sku-9", quantity=2, price_cents=2599),
    )


def _page_view_event(user_id: str = "u-3") -> EcommerceEvent:
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.PAGE_VIEW,
        user_id=user_id,
        session_id="s-3",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=PageViewPayload(page_url="/products", referrer=None),
    )


def _consume_one(consumer: Consumer, timeout_s: float = 10.0):
    msg = consumer.poll(timeout_s)
    assert msg is not None, "no message received within timeout"
    if msg.error() is not None and msg.error().code() != KafkaError._PARTITION_EOF:
        raise AssertionError(f"consume error: {msg.error()}")
    return msg


def test_producer_connects_and_sends_single_event(
    producer: AvroEventProducer, consumer: Consumer
) -> None:
    producer.produce(_click_event())
    remaining = producer.flush()
    assert remaining == 0
    msg = _consume_one(consumer)
    assert msg.value() is not None


def test_produced_bytes_have_confluent_wire_format(
    producer: AvroEventProducer, consumer: Consumer
) -> None:
    producer.produce(_click_event())
    producer.flush()
    msg = _consume_one(consumer)
    raw = msg.value()
    assert raw[0] == 0x00
    schema_id = struct.unpack(">I", raw[1:5])[0]
    assert schema_id > 0


def _roundtrip(
    producer: AvroEventProducer,
    consumer: Consumer,
    registry_config: SchemaRegistryConfig,
    event: EcommerceEvent,
) -> dict:
    producer.produce(event)
    producer.flush()
    msg = _consume_one(consumer)
    deserializer = AvroDeserializer(
        schema_registry_client=SchemaRegistry(registry_config).client,
    )
    ctx = SerializationContext(msg.topic(), MessageField.VALUE)
    return deserializer(msg.value(), ctx)


def test_roundtrip_click_event(
    producer: AvroEventProducer,
    consumer: Consumer,
    registry_config: SchemaRegistryConfig,
) -> None:
    event = _click_event()
    out = _roundtrip(producer, consumer, registry_config, event)
    assert out["event_type"] == "CLICK"
    assert out["user_id"] == event.user_id
    assert out["payload"]["element_id"] == "btn-cta"


def test_roundtrip_purchase_event(
    producer: AvroEventProducer,
    consumer: Consumer,
    registry_config: SchemaRegistryConfig,
) -> None:
    event = _purchase_event()
    out = _roundtrip(producer, consumer, registry_config, event)
    assert out["event_type"] == "PURCHASE"
    assert out["payload"]["product_id"] == "sku-9"
    assert out["payload"]["quantity"] == 2
    assert out["payload"]["currency"] == "USD"


def test_roundtrip_page_view_event_with_null_referrer(
    producer: AvroEventProducer,
    consumer: Consumer,
    registry_config: SchemaRegistryConfig,
) -> None:
    event = _page_view_event()
    out = _roundtrip(producer, consumer, registry_config, event)
    assert out["event_type"] == "PAGE_VIEW"
    assert out["payload"]["referrer"] is None


def test_message_key_is_user_id(
    producer: AvroEventProducer, consumer: Consumer
) -> None:
    event = _click_event(user_id="u-key-test")
    producer.produce(event)
    producer.flush()
    msg = _consume_one(consumer)
    assert StringDeserializer("utf_8")(msg.key(), None) == "u-key-test"


def test_same_user_id_lands_in_same_partition(
    producer: AvroEventProducer, consumer: Consumer
) -> None:
    user_id = "u-stick-42"
    n = 10
    for _ in range(n):
        producer.produce(_click_event(user_id=user_id))
    producer.flush()
    partitions = set()
    for _ in range(n):
        msg = _consume_one(consumer)
        partitions.add(msg.partition())
    assert len(partitions) == 1, f"per-user partitioning broken: saw {partitions}"


def test_small_batch_produce_flush_count(
    producer: AvroEventProducer, consumer: Consumer
) -> None:
    factories = [_click_event, _purchase_event, _page_view_event]
    n = 30
    for i in range(n):
        producer.produce(factories[i % 3](user_id=f"u-{i}"))
    remaining = producer.flush()
    assert remaining == 0


def test_produce_fails_when_schema_not_registered(
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    registered_subject: str,
) -> None:
    """Deleting the subject and producing must fail because
    ``auto.register.schemas=False``."""
    registry = SchemaRegistry(registry_config)
    registry.delete_subject(registered_subject, permanent=False)
    registry.delete_subject(registered_subject, permanent=True)
    try:
        with AvroEventProducer(kafka_config, registry_config) as p:
            with pytest.raises(Exception):
                p.produce(_click_event())
                p.flush()
    finally:
        cli = _load_register_module()
        cli.main(["--subject", registered_subject])
