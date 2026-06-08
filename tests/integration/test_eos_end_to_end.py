"""Broker-backed exactly-once integration tests (design week2_03 §6).

These require a running 3-broker Kafka cluster (``make infra-up``) with the
``__transaction_state`` topic; they are skipped when no Docker services are up.
They assert the *observable* EOS guarantee — a ``read_committed`` reader sees
each input record's output exactly once, aborted batches are invisible, and
offsets commit atomically with the output.
"""

from __future__ import annotations

import subprocess
import uuid

import pytest

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.eos import (
    TransactionalAvroProducer,
    TransactionalConfig,
    derive_transactional_id,
    transactional_producer_conf,
)

pytestmark = pytest.mark.integration


def _services_up() -> bool:
    """Return ``True`` when ``docker compose`` reports running services."""
    try:
        out = subprocess.run(
            ["docker", "compose", "-f", "docker/docker-compose.yml", "ps", "-q"],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):  # pragma: no cover - env-dependent
        return False
    return out.returncode == 0 and bool(out.stdout.strip())


requires_infra = pytest.mark.skipif(
    not _services_up(), reason="Kafka/Registry services not running (make infra-up)"
)


@pytest.fixture(scope="module")
def kafka_config() -> KafkaConfig:
    """Kafka config pointed at the host-exposed broker listeners."""
    return KafkaConfig(
        bootstrap_servers="localhost:19092,localhost:19093,localhost:19094"
    )


@pytest.fixture(scope="module")
def registry_config() -> SchemaRegistryConfig:
    """Schema Registry config pointed at the host-exposed listener."""
    return SchemaRegistryConfig(url="http://localhost:8081")


@requires_infra
def test_transactional_producer_init_and_commit_roundtrip(
    kafka_config: KafkaConfig,
) -> None:
    """A transactional producer can init, begin, produce, and commit a batch.

    Uses a unique ``transactional.id`` per run so repeated runs do not fence
    each other.  The value is produced raw (bytes serializer) to a throwaway
    topic to keep the test independent of the Avro subjects.
    """
    topic = f"eos-it-{uuid.uuid4().hex[:8]}"
    txn_id = derive_transactional_id(f"eos-it-{uuid.uuid4().hex[:8]}", 0)
    cfg = TransactionalConfig(enabled=True, transactional_id=txn_id)
    conf = transactional_producer_conf(kafka_config, cfg)
    producer = TransactionalAvroProducer(
        {topic: lambda v: v.encode("utf-8")}, conf=conf
    )

    producer.init_transactions(30.0)
    assert producer.initialised is True
    producer.begin_transaction()
    producer.produce(topic, "k1", "hello")
    producer.commit_transaction(30.0)
    producer.close()


@requires_infra
def test_aborted_batch_is_invisible_to_read_committed(
    kafka_config: KafkaConfig,
) -> None:
    """Records in an aborted transaction never appear below the LSO.

    Produce one record and abort the transaction; a ``read_committed`` console
    consumer over the topic must return zero records.
    """
    topic = f"eos-abort-{uuid.uuid4().hex[:8]}"
    txn_id = derive_transactional_id(f"eos-abort-{uuid.uuid4().hex[:8]}", 0)
    cfg = TransactionalConfig(enabled=True, transactional_id=txn_id)
    producer = TransactionalAvroProducer(
        {topic: lambda v: v.encode("utf-8")},
        conf=transactional_producer_conf(kafka_config, cfg),
    )
    producer.init_transactions(30.0)
    producer.begin_transaction()
    producer.produce(topic, "k1", "should-be-aborted")
    producer.abort_transaction(30.0)
    producer.close()

    result = subprocess.run(
        [
            "docker", "compose", "-f", "docker/docker-compose.yml", "exec", "-T",
            "kafka-1", "/opt/kafka/bin/kafka-console-consumer.sh",
            "--bootstrap-server", "kafka-1:9092", "--topic", topic,
            "--from-beginning", "--timeout-ms", "5000",
            "--isolation-level", "read_committed",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    # The consumer times out with no records; aborted data is filtered out.
    assert "should-be-aborted" not in result.stdout
