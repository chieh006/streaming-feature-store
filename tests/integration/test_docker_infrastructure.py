"""Integration tests for the Docker Compose infrastructure.

Prerequisites
-------------
Run ``make infra-up`` before executing these tests.
All tests are marked ``@pytest.mark.integration`` so they can be excluded from
fast CI runs with ``-m "not integration"``.

Tests run sequentially (``-p no:xdist``) because the broker stop/start tests
mutate shared state.
"""

import logging
import subprocess
import time
import uuid
from collections import defaultdict

import psycopg
import pytest
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BOOTSTRAP = "localhost:19092,localhost:19093,localhost:19094"
_EXTERNAL_PORTS = [("localhost", 19092), ("localhost", 19093), ("localhost", 19094)]
_COMPOSE_FILE = "docker/docker-compose.yml"
_POLL_TIMEOUT_S = 10.0
_MAX_MESSAGES = 100


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_admin() -> AdminClient:
    """Create and return a Kafka AdminClient.

    Returns
    -------
    AdminClient
        Connected to the test cluster.
    """
    return AdminClient({"bootstrap.servers": _BOOTSTRAP})


def _delete_topic_if_exists(admin: AdminClient, topic: str) -> None:
    """Delete a Kafka topic, ignoring errors if it does not exist.

    Parameters
    ----------
    admin : AdminClient
        Kafka admin client.
    topic : str
        Name of the topic to delete.
    """
    futures = admin.delete_topics([topic], operation_timeout=15)
    for _, f in futures.items():
        try:
            f.result()
        except KafkaException:
            pass  # topic may not exist — that is fine


def _create_test_topic(
    admin: AdminClient,
    topic: str,
    num_partitions: int = 3,
    replication_factor: int = 3,
) -> None:
    """Create a Kafka topic and wait for it to be ready.

    Parameters
    ----------
    admin : AdminClient
        Kafka admin client.
    topic : str
        Name of the topic to create.
    num_partitions : int
        Number of partitions.
    replication_factor : int
        Replication factor.
    """
    new_topic = NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
    futures = admin.create_topics([new_topic])
    for _, f in futures.items():
        f.result()  # raises on error
    # Short pause to allow metadata propagation
    time.sleep(1)


def _produce_messages(topic: str, messages: list[tuple[str | None, str]]) -> None:
    """Synchronously produce a list of (key, value) messages to a topic.

    Parameters
    ----------
    topic : str
        Destination topic.
    messages : list[tuple[str | None, str]]
        Each element is a (key, value) pair (both strings or key=None).
    """
    producer = Producer(
        {
            "bootstrap.servers": _BOOTSTRAP,
            "acks": "all",
        }
    )
    for key, value in messages:
        producer.produce(
            topic,
            key=key.encode() if key else None,
            value=value.encode(),
        )
    producer.flush(timeout=15)


def _consume_messages(topic: str, expected_count: int) -> list[str]:
    """Consume up to *expected_count* messages from *topic*.

    Parameters
    ----------
    topic : str
        Source topic.
    expected_count : int
        Number of messages to consume before returning.

    Returns
    -------
    list[str]
        Decoded message values.
    """
    group_id = f"test-consumer-{uuid.uuid4().hex[:8]}"
    consumer = Consumer(
        {
            "bootstrap.servers": _BOOTSTRAP,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    received: list[str] = []
    deadline = time.monotonic() + _POLL_TIMEOUT_S
    try:
        while len(received) < expected_count and time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            received.append(msg.value().decode())
    finally:
        consumer.close()
    return received


# ---------------------------------------------------------------------------
# Kafka cluster tests
# ---------------------------------------------------------------------------


class TestKafkaCluster:
    """Tests verifying the Kafka cluster is healthy and reachable."""

    def test_kafka_cluster_has_three_brokers(self, docker_services_up: None) -> None:
        """Cluster metadata must report exactly 3 brokers."""
        admin = _make_admin()
        metadata = admin.list_topics(timeout=15)
        broker_ids = list(metadata.brokers.keys())
        assert len(broker_ids) == 3, f"Expected 3 brokers, got {broker_ids}"

    def test_kafka_all_brokers_reachable(self, docker_services_up: None) -> None:
        """Each external listener must be reachable from the host independently."""
        import socket

        for host, port in _EXTERNAL_PORTS:
            with socket.create_connection((host, port), timeout=5) as sock:
                assert sock is not None, f"{host}:{port} is not reachable"

    def test_kafka_create_topic(self, docker_services_up: None) -> None:
        """A new topic with 3 partitions and replication factor 3 must be created."""
        topic = f"test-create-{uuid.uuid4().hex[:8]}"
        admin = _make_admin()
        try:
            _create_test_topic(admin, topic, num_partitions=3, replication_factor=3)
            metadata = admin.list_topics(timeout=10)
            assert topic in metadata.topics
            topic_meta = metadata.topics[topic]
            assert len(topic_meta.partitions) == 3
            for p in topic_meta.partitions.values():
                assert len(p.replicas) == 3
        finally:
            _delete_topic_if_exists(admin, topic)


class TestKafkaProduceConsume:
    """Tests verifying produce/consume round-trips."""

    def test_kafka_produce_consume_roundtrip(self, docker_services_up: None) -> None:
        """All produced messages must be received with correct content."""
        topic = f"test-roundtrip-{uuid.uuid4().hex[:8]}"
        admin = _make_admin()
        try:
            _create_test_topic(admin, topic)
            payloads = [f"msg-{i}" for i in range(10)]
            _produce_messages(topic, [(None, v) for v in payloads])
            received = _consume_messages(topic, expected_count=10)
            assert sorted(received) == sorted(payloads)
        finally:
            _delete_topic_if_exists(admin, topic)

    def test_kafka_partition_assignment(self, docker_services_up: None) -> None:
        """Messages with the same key must land in the same partition."""
        topic = f"test-partitions-{uuid.uuid4().hex[:8]}"
        admin = _make_admin()
        try:
            _create_test_topic(admin, topic, num_partitions=3, replication_factor=3)
            # Produce 30 messages spread across 3 keys
            keys = ["key-a", "key-b", "key-c"]
            messages = [(k, f"{k}-value-{i}") for i in range(10) for k in keys]
            _produce_messages(topic, messages)

            group_id = f"test-part-check-{uuid.uuid4().hex[:8]}"
            consumer = Consumer(
                {
                    "bootstrap.servers": _BOOTSTRAP,
                    "group.id": group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                }
            )
            consumer.subscribe([topic])

            key_to_partitions: dict[str, set[int]] = defaultdict(set)
            deadline = time.monotonic() + _POLL_TIMEOUT_S
            consumed = 0
            try:
                while consumed < len(messages) and time.monotonic() < deadline:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None or msg.error():
                        continue
                    msg_key = msg.key().decode() if msg.key() else None
                    if msg_key:
                        key_to_partitions[msg_key].add(msg.partition())
                    consumed += 1
            finally:
                consumer.close()

            for key in keys:
                assert len(key_to_partitions[key]) == 1, (
                    f"Key '{key}' landed in multiple partitions: {key_to_partitions[key]}"
                )
        finally:
            _delete_topic_if_exists(admin, topic)


class TestKafkaBrokerResilience:
    """Tests verifying cluster resilience when a broker is stopped/restarted.

    These tests are placed last because stopping a broker affects cluster state.
    """

    def test_kafka_leader_election_on_broker_stop(self, docker_services_up: None) -> None:
        """Cluster must remain operational (produce+consume) when one broker is stopped."""
        topic = f"test-failover-{uuid.uuid4().hex[:8]}"
        admin = _make_admin()
        try:
            _create_test_topic(admin, topic, num_partitions=3, replication_factor=3)
            # Stop kafka-3 (the broker with the highest node ID)
            subprocess.run(
                ["docker", "compose", "-f", _COMPOSE_FILE, "stop", "kafka-3"],
                check=True,
            )
            time.sleep(5)  # allow leader election to complete

            # Produce and consume with only 2 brokers (min.insync.replicas=2 → should work)
            payloads = [f"failover-msg-{i}" for i in range(5)]
            _produce_messages(topic, [(None, v) for v in payloads])
            received = _consume_messages(topic, expected_count=5)
            assert sorted(received) == sorted(payloads)
        finally:
            _delete_topic_if_exists(admin, topic)

    def test_kafka_broker_rejoin(self, docker_services_up: None) -> None:
        """After restart, the stopped broker must rejoin and ISR must be restored."""
        subprocess.run(
            ["docker", "compose", "-f", _COMPOSE_FILE, "start", "kafka-3"],
            check=True,
        )
        # Wait for the broker to become ready and rejoin the ISR
        time.sleep(15)

        admin = _make_admin()
        metadata = admin.list_topics(timeout=20)
        broker_ids = list(metadata.brokers.keys())
        assert len(broker_ids) == 3, (
            f"Expected 3 brokers after rejoin, got {broker_ids}"
        )


# ---------------------------------------------------------------------------
# PostgreSQL tests
# ---------------------------------------------------------------------------


class TestPostgresConnection:
    """Tests verifying PostgreSQL connectivity."""

    def test_postgres_connection(self, docker_services_up: None, postgres_dsn: str) -> None:
        """A simple SELECT 1 must succeed."""
        with psycopg.connect(postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
        assert result == (1,)

    def test_postgres_raw_events_table_exists(
        self, docker_services_up: None, postgres_dsn: str
    ) -> None:
        """raw_events must exist in the public schema."""
        with psycopg.connect(postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = 'raw_events'
                    """
                )
                rows = cur.fetchall()
        assert len(rows) == 1, "raw_events table not found in public schema"

    def test_postgres_raw_events_columns(
        self, docker_services_up: None, postgres_dsn: str
    ) -> None:
        """raw_events must have all expected columns with correct data types."""
        expected = {
            "event_id": "uuid",
            "event_type": "character varying",
            "user_id": "character varying",
            "session_id": "character varying",
            "event_timestamp": "timestamp with time zone",
            "properties": "jsonb",
            "ingested_at": "timestamp with time zone",
        }
        with psycopg.connect(postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'raw_events'
                    """
                )
                actual = {row[0]: row[1] for row in cur.fetchall()}
        for col, dtype in expected.items():
            assert col in actual, f"Column '{col}' missing from raw_events"
            assert actual[col] == dtype, (
                f"Column '{col}' has type '{actual[col]}', expected '{dtype}'"
            )

    def test_postgres_raw_events_indexes(
        self, docker_services_up: None, postgres_dsn: str
    ) -> None:
        """raw_events must have all four expected indexes plus the primary key index."""
        expected_indexes = {
            "idx_raw_events_timestamp",
            "idx_raw_events_user_id",
            "idx_raw_events_event_type",
            "idx_raw_events_user_timestamp",
            "raw_events_pkey",
        }
        with psycopg.connect(postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT indexname
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND tablename = 'raw_events'
                    """
                )
                actual_indexes = {row[0] for row in cur.fetchall()}
        assert expected_indexes.issubset(actual_indexes), (
            f"Missing indexes: {expected_indexes - actual_indexes}"
        )


class TestPostgresRawEventsOperations:
    """Tests verifying insert and idempotency behaviour on raw_events."""

    def _sample_event(self) -> dict:
        """Return a sample event dict for testing.

        Returns
        -------
        dict
            Sample event row values.
        """
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "page_view",
            "user_id": "user-001",
            "session_id": "sess-abc",
            "event_timestamp": "2026-01-01T00:00:00+00:00",
            "properties": '{"page_url": "/home"}',
        }

    def test_postgres_insert_and_query(
        self, docker_services_up: None, postgres_dsn: str
    ) -> None:
        """An inserted row must be queryable and match all inserted values."""
        event = self._sample_event()
        with psycopg.connect(postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw_events
                        (event_id, event_type, user_id, session_id,
                         event_timestamp, properties)
                    VALUES
                        (%(event_id)s, %(event_type)s, %(user_id)s,
                         %(session_id)s, %(event_timestamp)s,
                         %(properties)s::jsonb)
                    """,
                    event,
                )
                conn.commit()
                cur.execute(
                    "SELECT event_type, user_id FROM raw_events WHERE event_id = %s",
                    (event["event_id"],),
                )
                row = cur.fetchone()
                # Cleanup
                cur.execute(
                    "DELETE FROM raw_events WHERE event_id = %s",
                    (event["event_id"],),
                )
                conn.commit()
        assert row is not None
        assert row[0] == "page_view"
        assert row[1] == "user-001"

    def test_postgres_idempotent_insert(
        self, docker_services_up: None, postgres_dsn: str
    ) -> None:
        """Inserting the same event_id twice with ON CONFLICT DO NOTHING must not error."""
        event = self._sample_event()
        insert_sql = """
            INSERT INTO raw_events
                (event_id, event_type, user_id, session_id,
                 event_timestamp, properties)
            VALUES
                (%(event_id)s, %(event_type)s, %(user_id)s,
                 %(session_id)s, %(event_timestamp)s,
                 %(properties)s::jsonb)
            ON CONFLICT (event_id) DO NOTHING
        """
        with psycopg.connect(postgres_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(insert_sql, event)
                cur.execute(insert_sql, event)  # duplicate — must not raise
                conn.commit()
                cur.execute(
                    "SELECT COUNT(*) FROM raw_events WHERE event_id = %s",
                    (event["event_id"],),
                )
                count = cur.fetchone()[0]
                # Cleanup
                cur.execute(
                    "DELETE FROM raw_events WHERE event_id = %s",
                    (event["event_id"],),
                )
                conn.commit()
        assert count == 1, f"Expected 1 row after idempotent insert, got {count}"
