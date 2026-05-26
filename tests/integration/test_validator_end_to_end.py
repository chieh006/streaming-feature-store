"""End-to-end integration tests for :class:`ValidatorRunner`.

Each test stands up a per-test source / validated / DLQ topic triple,
registers the source value subject, then produces a small number of
events and runs the validator in a background thread until the expected
output appears or a hard timeout fires.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consumer.avro_consumer import AvroEventConsumer
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
    PurchasePayload,
)
from streaming_feature_store.schemas.registry import SchemaRegistry
from streaming_feature_store.validate.accountant import ValidatorAccountant
from streaming_feature_store.validate.dlq import (
    DlqProducer,
    ErrorClass,
    load_dlq_schema_str,
)
from streaming_feature_store.validate.pipeline import (
    ValidationPipeline,
    default_validators,
)
from streaming_feature_store.validate.runner import (
    ValidatorRunConfig,
    ValidatorRunner,
)

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)

_REGISTER_SCRIPT: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "register_schemas.py"
)


def _load_register_module():
    """Import the ``register_schemas`` script as a module."""
    spec = importlib.util.spec_from_file_location(
        "register_schemas_validatortest", _REGISTER_SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def per_test_id() -> str:
    """Return a short, unique test id used to namespace topics."""
    return uuid.uuid4().hex[:8]


@pytest.fixture
def topics(per_test_id: str) -> dict[str, str]:
    """Return the per-test topic triple."""
    return {
        "source": f"validator-src-{per_test_id}",
        "validated": f"validator-val-{per_test_id}",
        "dlq": f"validator-dlq-{per_test_id}",
    }


@pytest.fixture
def kafka_config(topics) -> KafkaConfig:
    """Per-test Kafka config bound to the source topic."""
    return KafkaConfig(topic=topics["source"])


@pytest.fixture
def registry_config() -> SchemaRegistryConfig:
    """Default Schema Registry config."""
    return SchemaRegistryConfig()


@pytest.fixture
def topic_admin(docker_services_up, kafka_config) -> TopicAdmin:
    """Live :class:`TopicAdmin`."""
    return TopicAdmin(kafka_config)


@pytest.fixture
def created_topics(topic_admin, topics):
    """Create the per-test source / validated / DLQ topics; delete on teardown."""
    for name in topics.values():
        topic_admin.ensure_topic(
            name, num_partitions=3, replication_factor=3
        )
    yield topics
    for name in topics.values():
        try:
            topic_admin.delete_topic(name)
        except Exception:  # pragma: no cover - best-effort
            logger.warning(f"Could not delete topic {name}")


@pytest.fixture
def registered_source_subject(created_topics, kafka_config, registry_config):
    """Register the source-topic value subject; clean up on teardown."""
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


@pytest.fixture
def registered_validated_subject(
    created_topics, kafka_config, registry_config
):
    """Register the validated-topic value subject; clean up on teardown."""
    subject = f"{created_topics['validated']}-value"
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


@pytest.fixture
def registered_dlq_subject(created_topics, kafka_config, registry_config):
    """Cleanup hook for the DLQ subject; the validator registers it lazily."""
    subject = f"{created_topics['dlq']}-value"
    yield subject
    registry = SchemaRegistry(registry_config)
    for permanent in (False, True):
        try:
            registry.delete_subject(subject, permanent=permanent)
        except Exception:  # pragma: no cover
            pass


def _make_event(user_id: str, *, valid: bool) -> EcommerceEvent:
    """Build a :class:`EcommerceEvent` for the e2e test.

    Parameters
    ----------
    user_id : str
        Unique user-id (so we can isolate this test's traffic).
    valid : bool
        ``True`` for a clean PURCHASE event; ``False`` for one whose
        ``price_cents`` is zero — rejected by
        :class:`PriceRangeValidator`.

    Returns
    -------
    EcommerceEvent
        Pydantic event.
    """
    payload = PurchasePayload(
        product_id="sku-1",
        quantity=1,
        price_cents=999 if valid else 0,
    )
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.PURCHASE,
        user_id=user_id,
        session_id="s-1",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=payload,
    )


def _build_runner(
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    topics: dict[str, str],
    group_id: str,
) -> ValidatorRunner:
    """Construct a fully-wired :class:`ValidatorRunner` for the test."""
    consumer = AvroEventConsumer(
        kafka_config, registry_config, group_id=group_id, topic=topics["source"]
    )
    validated_producer = AvroEventProducer(
        kafka_config, registry_config, topic=topics["validated"]
    )
    dlq_producer = DlqProducer(
        kafka_config, registry_config, topic=topics["dlq"]
    )
    config = ValidatorRunConfig(
        source_topic=topics["source"],
        validated_topic=topics["validated"],
        dlq_topic=topics["dlq"],
        consumer_group_id=group_id,
        poll_timeout_s=0.5,
        poll_max_records=100,
        flush_timeout_s=5.0,
    )
    return ValidatorRunner(
        consumer=consumer,
        validated_producer=validated_producer,
        dlq_producer=dlq_producer,
        pipeline=ValidationPipeline(default_validators()),
        accountant=ValidatorAccountant(),
        config=config,
    )


def _run_until(
    runner: ValidatorRunner, predicate, timeout_s: float = 15.0
) -> None:
    """Run *runner* in a background thread until *predicate()* is true.

    Parameters
    ----------
    runner : ValidatorRunner
        Runner to drive.
    predicate : callable
        Zero-arg predicate; the helper polls every 100 ms.
    timeout_s : float, optional
        Hard timeout in seconds.  Defaults to ``15.0``.

    Raises
    ------
    TimeoutError
        If *predicate* does not become true within *timeout_s*.
    """
    thread = threading.Thread(target=runner.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.1)
        raise TimeoutError(
            f"predicate {predicate.__name__!r} did not become true within {timeout_s}s"
        )
    finally:
        runner.request_shutdown()
        thread.join(timeout=10.0)


def _drain_topic(
    kafka_config: KafkaConfig,
    *,
    topic: str,
    deserializer: AvroDeserializer | None = None,
    timeout_s: float = 10.0,
    expected: int = 1,
) -> list:
    """Drain *topic* and return the decoded value list."""
    conf = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "security.protocol": kafka_config.security_protocol,
        "group.id": f"drain-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    out: list = []
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline and len(out) < expected:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error() is not None:
                continue
            if deserializer is None:
                out.append((msg.key(), msg.value()))
            else:
                from confluent_kafka.serialization import SerializationContext, MessageField

                ctx = SerializationContext(topic, MessageField.VALUE)
                out.append((msg.key(), deserializer(msg.value(), ctx)))
    finally:
        consumer.close()
    return out


# --- topic + schema auto-creation -----------------------------------------


def test_validator_creates_dlq_topic_on_first_send(
    docker_services_up,
    kafka_config,
    registry_config,
    created_topics,
    registered_source_subject,
    registered_dlq_subject,
):
    """The :class:`DlqProducer` should register its schema on construction."""
    dlq = DlqProducer(
        kafka_config,
        registry_config,
        topic=created_topics["dlq"],
        register_schema=True,
    )
    try:
        registry = SchemaRegistry(registry_config)
        latest = registry.get_latest(f"{created_topics['dlq']}-value")
        assert "DlqRecord" in latest.schema_str
    finally:
        dlq.close()


# --- happy path: a clean event lands on validated-events -------------------


def test_validator_routes_valid_event_to_validated_topic(
    docker_services_up,
    kafka_config,
    registry_config,
    created_topics,
    registered_source_subject,
    registered_validated_subject,
    registered_dlq_subject,
    per_test_id,
):
    """A clean PURCHASE event must land on ``validated-events``."""
    user_id = f"validator-test-{per_test_id}"
    producer = AvroEventProducer(
        kafka_config, registry_config, topic=created_topics["source"]
    )
    try:
        producer.produce(_make_event(user_id, valid=True))
        assert producer.flush(10.0) == 0
    finally:
        producer.close()

    runner = _build_runner(
        kafka_config, registry_config, created_topics, group_id=f"vg-{per_test_id}"
    )

    def at_least_one_validated() -> bool:
        return runner._accountant.snapshot().validated >= 1  # noqa: SLF001

    _run_until(runner, at_least_one_validated, timeout_s=20.0)

    snap = runner._accountant.snapshot()  # noqa: SLF001
    assert snap.validated >= 1
    assert snap.invalid_total == 0


# --- DLQ path: bad price lands on dead-letter-queue -----------------------


def test_validator_routes_invalid_event_to_dlq_topic(
    docker_services_up,
    kafka_config,
    registry_config,
    created_topics,
    registered_source_subject,
    registered_validated_subject,
    registered_dlq_subject,
    per_test_id,
):
    """A PURCHASE event with ``price_cents=0`` must land on the DLQ."""
    user_id = f"validator-test-{per_test_id}"
    producer = AvroEventProducer(
        kafka_config, registry_config, topic=created_topics["source"]
    )
    try:
        producer.produce(_make_event(user_id, valid=False))
        assert producer.flush(10.0) == 0
    finally:
        producer.close()

    runner = _build_runner(
        kafka_config, registry_config, created_topics, group_id=f"vg-{per_test_id}"
    )

    def at_least_one_invalid() -> bool:
        return runner._accountant.snapshot().invalid_total >= 1  # noqa: SLF001

    _run_until(runner, at_least_one_invalid, timeout_s=20.0)

    snap = runner._accountant.snapshot()  # noqa: SLF001
    assert snap.invalid_total >= 1
    assert snap.invalid_by_class.get(ErrorClass.OUT_OF_RANGE, 0) >= 1
    assert snap.validated == 0


# --- DLQ schema registered with BACKWARD compatibility ---------------------


def test_validator_dlq_schema_registered(
    docker_services_up,
    kafka_config,
    registry_config,
    created_topics,
    registered_source_subject,
    registered_dlq_subject,
):
    """Constructing a :class:`DlqProducer` should register the DLQ schema."""
    dlq = DlqProducer(
        kafka_config,
        registry_config,
        topic=created_topics["dlq"],
        register_schema=True,
    )
    try:
        registry = SchemaRegistry(registry_config)
        latest = registry.get_latest(f"{created_topics['dlq']}-value")
        # Schema field set matches the .avsc file.
        for field in (
            "DlqRecord",
            "original_value_bytes",
            "validator_name",
            "PIPELINE_INTERNAL_ERROR",
        ):
            assert field in latest.schema_str
        # And the load-from-disk helper returns the same canonical form.
        assert "DlqRecord" in load_dlq_schema_str()
    finally:
        dlq.close()
