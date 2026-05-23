"""End-to-end pipeline test: :class:`FeederRunner` → Kafka → :class:`SinkRunner`.

Runs both daemons in background threads for a short window and asserts that
the produced events round-trip into ``raw_events``.  Rows are scoped to a
per-test user-id prefix so concurrent runs cannot collide.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
import threading
import time
import uuid
from pathlib import Path

import psycopg
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
        "register_schemas_pipeline", _REGISTER_SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        topic=f"e-commerce-events-feed-pipeline-{uuid.uuid4().hex[:8]}"
    )


@pytest.fixture
def registry_config() -> SchemaRegistryConfig:
    return SchemaRegistryConfig()


@pytest.fixture
def topic_admin(docker_services_up, kafka_config) -> TopicAdmin:
    return TopicAdmin(kafka_config)


@pytest.fixture
def pipeline_topic(topic_admin, kafka_config):
    topic_admin.ensure_topic(
        kafka_config.topic, num_partitions=12, replication_factor=3
    )
    yield kafka_config.topic
    try:
        topic_admin.delete_topic(kafka_config.topic)
    except Exception:  # pragma: no cover - best-effort
        pass


@pytest.fixture
def registered_subject(pipeline_topic, kafka_config, registry_config):
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
def cleanup_rows(postgres_dsn):
    """Track per-namespace cleanup so the pipeline test's rows are removed."""
    namespaces: list[str] = []
    yield namespaces
    if not namespaces:
        return
    with psycopg.connect(postgres_dsn) as conn:
        with conn.cursor() as cur:
            for ns in namespaces:
                cur.execute(
                    "DELETE FROM raw_events WHERE user_id LIKE %s",
                    [f"{ns}-%"],
                )
        conn.commit()


def _start_thread(fn) -> tuple[threading.Thread, list]:
    box: list = []

    def target():
        try:
            box.append(fn())
        except Exception as exc:  # pragma: no cover
            box.append(exc)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread, box


def test_feeder_to_sink_pipeline_round_trip(
    registered_subject,
    kafka_config,
    registry_config,
    postgres_dsn,
    cleanup_rows,
) -> None:
    """Feeder + sink for ~6 s → some rows land in raw_events."""
    rate = 500.0
    feeder_cfg = FeederRunConfig(
        topic=kafka_config.topic,
        rate_evt_per_sec=rate,
        batch_size=50,
        seed=11,
        snapshot_interval_s=30.0,
    )
    sink_cfg = SinkRunConfig(
        topic=kafka_config.topic,
        consumer_group_id=f"pipeline-{uuid.uuid4().hex[:6]}",
        batch_max_rows=200,
        batch_max_age_s=1.0,
        poll_timeout_s=0.5,
        poll_max_records=200,
    )

    producer = AvroEventProducer(
        kafka_config, registry_config, topic=feeder_cfg.topic
    )
    generator = SyntheticEventGenerator(seed=feeder_cfg.seed)
    pacer = TokenBucketPacer(rate, burst=feeder_cfg.batch_size)
    delivery = DeliveryAccountant()
    feeder = FeederRunner(
        config=feeder_cfg,
        producer=producer,
        generator=generator,
        pacer=pacer,
        accountant=delivery,
    )

    consumer = AvroEventConsumer(
        kafka_config,
        registry_config,
        group_id=sink_cfg.consumer_group_id,
        topic=sink_cfg.topic,
    )
    writer = PostgresWriter(postgres_dsn)
    sink_accountant = SinkAccountant()
    sink = SinkRunner(
        consumer=consumer,
        writer=writer,
        accountant=sink_accountant,
        config=sink_cfg,
    )

    # The SyntheticEventGenerator uses fixed prefixes; capture a sample of
    # user_ids on the way out so the cleanup-row fixture knows what to delete.
    # The generator emits ``u-<6digits>``, so we collect that prefix.
    cleanup_rows.append("u")

    # Warm the topic with a synchronous produce so the broker has assigned
    # leaders + ISR by the time the sink consumer joins.  Without this the
    # test occasionally races consumer-group assignment when run back-to-back
    # with other Kafka-heavy tests (the broker takes a few seconds to settle).
    warmup = SyntheticEventGenerator(seed=999).generate_batch(50)
    with AvroEventProducer(
        kafka_config, registry_config, topic=feeder_cfg.topic
    ) as warm_prod:
        for event in warmup:
            warm_prod.produce(event)
        warm_prod.flush(15.0)

    feeder_thread, _ = _start_thread(feeder.run)
    sink_thread, _ = _start_thread(sink.run)
    # Consumer-group assignment for a brand-new topic + group can take a
    # handful of seconds before the sink starts seeing messages; give it
    # 45 s before declaring failure so flaky cold-start rebalances do not
    # masquerade as test failures.
    deadline = time.monotonic() + 45.0
    while time.monotonic() < deadline:
        if sink_accountant.snapshot().inserted >= 100:
            break
        time.sleep(0.5)
    feeder.request_shutdown()
    sink.request_shutdown()
    feeder_thread.join(timeout=20.0)
    sink_thread.join(timeout=20.0)
    assert not feeder_thread.is_alive()
    assert not sink_thread.is_alive()
    assert sink_accountant.snapshot().inserted > 0, (
        "Sink did not insert any rows; consumer-group assignment may have "
        f"stalled. Final snapshot: {sink_accountant.snapshot()}"
    )
    assert delivery.snapshot().acked > 0
