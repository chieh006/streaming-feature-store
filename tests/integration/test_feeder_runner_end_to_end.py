"""End-to-end integration tests for :class:`FeederRunner`.

The feeder is a producer-only daemon; the assertions here cover (a) rate
pacing, (b) graceful shutdown, and (c) that the messages land on the
expected feed topic (read back via a fresh :class:`AvroEventConsumer`).
"""

from __future__ import annotations

import importlib.util
import logging
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consumer.avro_consumer import AvroEventConsumer
from streaming_feature_store.feeder.feeder_runner import (
    FeederRunConfig,
    FeederRunner,
)
from streaming_feature_store.load.accountant import DeliveryAccountant
from streaming_feature_store.load.pacer import TokenBucketPacer
from streaming_feature_store.load.synthetic import SyntheticEventGenerator
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.schemas.registry import SchemaRegistry

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)

_REGISTER_SCRIPT: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "register_schemas.py"
)


def _load_register_module():
    """Import the register_schemas script as a module."""
    spec = importlib.util.spec_from_file_location(
        "register_schemas_feederrunner", _REGISTER_SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def kafka_config() -> KafkaConfig:
    """Per-test Kafka config bound to a unique feed-topic name."""
    return KafkaConfig(
        topic=f"e-commerce-events-feed-feedertest-{uuid.uuid4().hex[:8]}"
    )


@pytest.fixture
def registry_config() -> SchemaRegistryConfig:
    """Default Schema Registry config."""
    return SchemaRegistryConfig()


@pytest.fixture
def topic_admin(docker_services_up, kafka_config) -> TopicAdmin:
    """Live :class:`TopicAdmin`."""
    return TopicAdmin(kafka_config)


@pytest.fixture
def feedertest_topic(topic_admin, kafka_config):
    """Create a per-test topic; delete on teardown."""
    topic_admin.ensure_topic(
        kafka_config.topic, num_partitions=12, replication_factor=3
    )
    yield kafka_config.topic
    try:
        topic_admin.delete_topic(kafka_config.topic)
    except Exception:  # pragma: no cover - best-effort
        pass


@pytest.fixture
def registered_subject(feedertest_topic, kafka_config, registry_config):
    """Register the per-topic value subject; clean up on teardown."""
    subject = f"{kafka_config.topic}-value"
    cli = _load_register_module()
    rc = cli.main(["--subject", subject])
    assert rc == 0
    yield subject
    registry = SchemaRegistry(registry_config)
    for permanent in (False, True):
        try:
            registry.delete_subject(subject, permanent=permanent)
        except Exception:  # pragma: no cover
            pass


def _build_runner(
    kafka_config, registry_config, topic: str, rate: float
) -> FeederRunner:
    config = FeederRunConfig(
        topic=topic,
        rate_evt_per_sec=rate,
        batch_size=50,
        seed=7,
        snapshot_interval_s=30.0,
    )
    producer = AvroEventProducer(
        kafka_config, registry_config, topic=topic
    )
    generator = SyntheticEventGenerator(seed=config.seed)
    pacer = TokenBucketPacer(rate, burst=config.batch_size)
    accountant = DeliveryAccountant()
    return FeederRunner(
        config=config,
        producer=producer,
        generator=generator,
        pacer=pacer,
        accountant=accountant,
    )


def test_feeder_runner_end_to_end_produces_at_target_rate(
    registered_subject, kafka_config, registry_config
) -> None:
    """Feeder at 200 evt/s for ~5 s → roughly 1000 messages produced."""
    rate = 200.0
    runner = _build_runner(
        kafka_config, registry_config, kafka_config.topic, rate
    )
    result_box: list = []

    def target():
        try:
            result_box.append(runner.run())
        except Exception as exc:  # pragma: no cover
            result_box.append(exc)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    time.sleep(5.0)
    runner.request_shutdown()
    thread.join(timeout=15.0)
    assert not thread.is_alive()
    snap = result_box[0]
    expected = rate * 5.0
    # 50% tolerance covers token-bucket jitter and the producer's startup cost.
    assert 0.5 * expected <= snap.delivery.acked <= 1.5 * expected


def test_feeder_runner_end_to_end_messages_arrive_on_feed_topic(
    registered_subject, kafka_config, registry_config
) -> None:
    """Produce for ~3 s then drain → consumer reads ≥1 message."""
    runner = _build_runner(
        kafka_config, registry_config, kafka_config.topic, rate=200.0
    )
    result_box: list = []

    def target():
        try:
            result_box.append(runner.run())
        except Exception as exc:  # pragma: no cover
            result_box.append(exc)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    time.sleep(3.0)
    runner.request_shutdown()
    thread.join(timeout=15.0)
    assert not thread.is_alive()

    consumer = AvroEventConsumer(
        kafka_config,
        registry_config,
        group_id=f"feedertest-drain-{uuid.uuid4().hex[:6]}",
        topic=kafka_config.topic,
    )
    try:
        events = consumer.consume(timeout_s=10.0, max_messages=2000)
    finally:
        consumer.close()
    assert len(events) >= 1
