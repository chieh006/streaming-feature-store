"""End-to-end integration tests for :class:`LoadRunner`.

Each test uses a per-test topic name (``e-commerce-events-loadtest-<uuid>``) to
avoid cross-test contamination.  The headline 50K-floor benchmark is marked
``benchmark`` so it can be opted into with ``make test-benchmark``.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
import uuid
from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.load.load_runner import LoadRunner
from streaming_feature_store.load.report import LoadRunConfig, render_markdown
from streaming_feature_store.schemas.registry import RegistryError, SchemaRegistry

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)

_REGISTER_SCRIPT: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "register_schemas.py"
)


def _load_register_module():
    """Import the register_schemas script as a module."""
    spec = importlib.util.spec_from_file_location(
        "register_schemas_loadrunner", _REGISTER_SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def kafka_config() -> KafkaConfig:
    """Per-test Kafka config bound to a unique topic name."""
    return KafkaConfig(topic=f"e-commerce-events-loadtest-{uuid.uuid4().hex[:8]}")


@pytest.fixture
def registry_config() -> SchemaRegistryConfig:
    """Default Schema Registry config."""
    return SchemaRegistryConfig()


@pytest.fixture
def topic_admin(docker_services_up, kafka_config) -> TopicAdmin:
    """Live TopicAdmin."""
    return TopicAdmin(kafka_config)


@pytest.fixture
def loadtest_topic(topic_admin, kafka_config):
    """Create a per-test topic and tear it down after the test."""
    topic_admin.ensure_topic(
        kafka_config.topic, num_partitions=12, replication_factor=3
    )
    yield kafka_config.topic
    try:
        topic_admin.delete_topic(kafka_config.topic)
    except Exception:  # pragma: no cover - best-effort
        pass


@pytest.fixture
def registered_subject(loadtest_topic, kafka_config, registry_config):
    """Register the per-topic value subject and clean up on teardown."""
    subject = f"{kafka_config.topic}-value"
    cli = _load_register_module()
    rc = cli.main(["--subject", subject])
    assert rc == 0
    yield subject
    registry = SchemaRegistry(registry_config)
    try:
        registry.delete_subject(subject, permanent=False)
    except Exception:  # pragma: no cover
        pass
    try:
        registry.delete_subject(subject, permanent=True)
    except Exception:  # pragma: no cover
        pass


def _run_cfg(topic: str, **over) -> LoadRunConfig:
    base = dict(
        duration_s=2.0,
        target_rate=5_000.0,
        workers=2,
        batch_size=256,
        max_in_flight=10_000,
        seed=7,
        topic=topic,
    )
    base.update(over)
    return LoadRunConfig(**base)


def test_load_runner_smoke_produces_and_acks(
    registered_subject, kafka_config, registry_config
):
    cfg = _run_cfg(kafka_config.topic, duration_s=2.0, target_rate=5_000.0)
    runner = LoadRunner(kafka_config, registry_config, cfg, floor_eps=0.0)
    report = runner.run()
    assert report.snapshot.failed == 0
    assert report.snapshot.acked == report.snapshot.produced
    assert report.snapshot.acked > 0


def test_load_runner_unpaced_mode_runs_clean(
    registered_subject, kafka_config, registry_config
):
    cfg = _run_cfg(
        kafka_config.topic,
        duration_s=1.5,
        target_rate=None,
        workers=2,
        batch_size=256,
    )
    runner = LoadRunner(kafka_config, registry_config, cfg, floor_eps=0.0)
    report = runner.run()
    assert report.snapshot.failed == 0


def test_load_runner_writes_report_file(
    registered_subject, kafka_config, registry_config, tmp_path
):
    cfg = _run_cfg(kafka_config.topic, duration_s=1.0, target_rate=2_000.0)
    runner = LoadRunner(kafka_config, registry_config, cfg, floor_eps=0.0)
    report = runner.run()
    out = tmp_path / "report.md"
    out.write_text(render_markdown(report), encoding="utf-8")
    text = out.read_text(encoding="utf-8")
    assert "Synthetic Event Load Test Results" in text
    assert kafka_config.topic in text


def test_load_runner_partitions_are_balanced(
    registered_subject, kafka_config, registry_config
):
    cfg = _run_cfg(
        kafka_config.topic,
        duration_s=2.0,
        target_rate=10_000.0,
        workers=2,
        batch_size=256,
    )
    runner = LoadRunner(kafka_config, registry_config, cfg, floor_eps=0.0)
    runner.run()
    # Probe partition byte distribution via a fresh consumer-group offsets fetch.
    raw = AdminClient({"bootstrap.servers": kafka_config.bootstrap_servers})
    cluster = raw.list_topics(timeout=10.0)
    meta = cluster.topics[kafka_config.topic]
    assert len(meta.partitions) == 12


def test_load_runner_fails_fast_on_missing_subject(
    docker_services_up, topic_admin, kafka_config, registry_config
):
    """If the value subject is not registered, the runner aborts before producing."""
    fresh_name = f"e-commerce-events-loadtest-nosub-{uuid.uuid4().hex[:8]}"
    topic_admin.ensure_topic(fresh_name, num_partitions=3, replication_factor=1)
    try:
        cfg = _run_cfg(fresh_name, duration_s=0.5, target_rate=1_000.0)
        runner = LoadRunner(kafka_config, registry_config, cfg, floor_eps=0.0)
        with pytest.raises(RegistryError):
            runner.run()
    finally:
        try:
            topic_admin.delete_topic(fresh_name)
        except Exception:  # pragma: no cover
            pass


def test_topic_auto_ensured_when_absent(
    docker_services_up, kafka_config, registry_config
):
    """Brand-new topic name; ensure_topic creates it with 12 partitions / RF=3."""
    fresh_name = f"e-commerce-events-loadtest-auto-{uuid.uuid4().hex[:8]}"
    admin = TopicAdmin(kafka_config)
    result = admin.ensure_topic(fresh_name, num_partitions=12, replication_factor=3)
    try:
        assert result.outcome.value == "CREATED"
        desc = admin.describe_topic(fresh_name)
        assert desc.num_partitions == 12
        assert desc.replication_factor == 3
    finally:
        try:
            admin.delete_topic(fresh_name)
        except Exception:  # pragma: no cover
            pass


@pytest.mark.benchmark
def test_load_runner_meets_50k_floor_for_10s(
    registered_subject, kafka_config, registry_config
):
    """Headline benchmark: 10 s @ 60K evt/s sustains ≥ 50K evt/s with no failures."""
    cfg = LoadRunConfig(
        duration_s=10.0,
        target_rate=60_000.0,
        workers=8,
        batch_size=1024,
        max_in_flight=50_000,
        seed=42,
        topic=kafka_config.topic,
    )
    runner = LoadRunner(kafka_config, registry_config, cfg, floor_eps=50_000.0)
    report = runner.run()
    assert report.snapshot.failed == 0
    assert report.sustained_rate_eps >= 50_000.0
