"""End-to-end integration tests for :class:`SinkRunner`.

These tests stand up a per-test feed topic, register the value subject, seed
the topic with a known number of events, then run :class:`SinkRunner` in a
background thread until ``shutdown`` is requested.  Each test scopes its row
deletions to the per-test ``user_id`` namespace so concurrent runs don't
collide.
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

import psycopg
import pytest

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consumer.avro_consumer import AvroEventConsumer
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
)
from streaming_feature_store.schemas.registry import SchemaRegistry
from streaming_feature_store.sink.accountant import SinkAccountant
from streaming_feature_store.sink.postgres_writer import PostgresWriter
from streaming_feature_store.sink.sink_runner import SinkRunConfig, SinkRunner

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)

_REGISTER_SCRIPT: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "register_schemas.py"
)


def _load_register_module():
    """Import the register_schemas script as a module."""
    spec = importlib.util.spec_from_file_location(
        "register_schemas_sinkrunner", _REGISTER_SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def per_test_namespace() -> str:
    """Return a unique user-id-prefix to scope row deletions."""
    return f"sinktest-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def kafka_config() -> KafkaConfig:
    """Per-test Kafka config bound to a unique feed-topic name."""
    return KafkaConfig(
        topic=f"e-commerce-events-feed-sinktest-{uuid.uuid4().hex[:8]}"
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
def sinktest_topic(topic_admin, kafka_config):
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
def registered_subject(sinktest_topic, kafka_config, registry_config):
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


@pytest.fixture
def cleanup_rows(postgres_dsn, per_test_namespace):
    """Delete every ``raw_events`` row produced by this test on teardown."""
    yield
    with psycopg.connect(postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM raw_events WHERE user_id LIKE %s",
                [f"{per_test_namespace}-%"],
            )
        conn.commit()


def _seed_topic(
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    topic: str,
    n: int,
    namespace: str,
) -> list[EcommerceEvent]:
    """Produce *n* events scoped to *namespace* and return them."""
    now = datetime.now(tz=timezone.utc)
    events = [
        EcommerceEvent(
            event_id=uuid4(),
            event_type=EventType.CLICK,
            user_id=f"{namespace}-u{i % 12}",
            session_id=f"s-{i}",
            event_timestamp=now,
            payload=ClickPayload(element_id="btn", page_url="/home"),
        )
        for i in range(n)
    ]
    with AvroEventProducer(kafka_config, registry_config, topic=topic) as prod:
        for event in events:
            prod.produce(event)
        remaining = prod.flush(30.0)
    assert remaining == 0
    return events


def _count_rows_by_namespace(dsn: str, namespace: str) -> int:
    """Return the number of ``raw_events`` rows whose user_id matches *namespace*."""
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM raw_events WHERE user_id LIKE %s",
                [f"{namespace}-%"],
            )
            row = cur.fetchone()
    return row[0] if row else 0


def _run_sink_in_thread(runner: SinkRunner) -> tuple[threading.Thread, list]:
    """Run *runner* in a daemon thread; return ``(thread, result_box)``."""
    result_box: list = []

    def target():
        try:
            result_box.append(runner.run())
        except Exception as exc:  # pragma: no cover
            result_box.append(exc)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread, result_box


def test_sink_runner_end_to_end_consumes_and_inserts(
    registered_subject,
    kafka_config,
    registry_config,
    postgres_dsn,
    per_test_namespace,
    cleanup_rows,
) -> None:
    """Produce 500 events → run sink → expect ≥500 rows in raw_events."""
    n = 500
    _seed_topic(
        kafka_config, registry_config, kafka_config.topic, n, per_test_namespace
    )

    config = SinkRunConfig(
        topic=kafka_config.topic,
        consumer_group_id=f"sinktest-{uuid.uuid4().hex[:6]}",
        batch_max_rows=100,
        batch_max_age_s=2.0,
        poll_timeout_s=0.5,
        poll_max_records=200,
    )
    consumer = AvroEventConsumer(
        kafka_config,
        registry_config,
        group_id=config.consumer_group_id,
        topic=config.topic,
    )
    writer = PostgresWriter(postgres_dsn)
    accountant = SinkAccountant()
    runner = SinkRunner(
        consumer=consumer, writer=writer, accountant=accountant, config=config
    )

    thread, result_box = _run_sink_in_thread(runner)
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        if accountant.snapshot().inserted >= n:
            break
        time.sleep(0.5)
    runner.request_shutdown()
    thread.join(timeout=30.0)
    assert not thread.is_alive(), "SinkRunner did not stop gracefully"
    assert _count_rows_by_namespace(postgres_dsn, per_test_namespace) >= n


def test_sink_runner_end_to_end_idempotent_replay_no_duplicates(
    registered_subject,
    kafka_config,
    registry_config,
    postgres_dsn,
    per_test_namespace,
    cleanup_rows,
) -> None:
    """Run sink twice on the same topic; row count unchanged on replay."""
    n = 200
    _seed_topic(
        kafka_config, registry_config, kafka_config.topic, n, per_test_namespace
    )

    def _run_once(group_suffix: str) -> SinkAccountant:
        config = SinkRunConfig(
            topic=kafka_config.topic,
            consumer_group_id=f"sinktest-replay-{group_suffix}",
            batch_max_rows=50,
            batch_max_age_s=1.0,
            poll_timeout_s=0.5,
            poll_max_records=200,
        )
        consumer = AvroEventConsumer(
            kafka_config,
            registry_config,
            group_id=config.consumer_group_id,
            topic=config.topic,
        )
        writer = PostgresWriter(postgres_dsn)
        accountant = SinkAccountant()
        runner = SinkRunner(
            consumer=consumer,
            writer=writer,
            accountant=accountant,
            config=config,
        )
        thread, _ = _run_sink_in_thread(runner)
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            if accountant.snapshot().consumed >= n:
                break
            time.sleep(0.3)
        runner.request_shutdown()
        thread.join(timeout=30.0)
        return accountant

    first = _run_once(uuid.uuid4().hex[:6])
    rows_after_first = _count_rows_by_namespace(postgres_dsn, per_test_namespace)
    assert rows_after_first == n

    # Replay with a NEW group id → same messages re-read; sink writes find
    # them all already in PG and increments conflict_skipped.
    second = _run_once(uuid.uuid4().hex[:6])
    rows_after_second = _count_rows_by_namespace(
        postgres_dsn, per_test_namespace
    )
    assert rows_after_second == n  # unchanged
    assert second.snapshot().conflict_skipped >= n
    assert first.snapshot().inserted == n
