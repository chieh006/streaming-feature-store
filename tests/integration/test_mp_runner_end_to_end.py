"""End-to-end integration tests for :class:`MultiprocessLoadRunner`.

Each test uses a per-test topic name to avoid cross-test contamination.
Tests are skipped on platforms that cannot use the ``spawn`` start method
(``multiprocessing.get_context("spawn")`` must succeed).
"""

from __future__ import annotations

import importlib.util
import logging
import multiprocessing as mp
import sys
import uuid
from pathlib import Path

import pytest

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.load_mp.mp_runner import MultiprocessLoadRunner
from streaming_feature_store.load_mp.report import (
    MultiprocessLoadConfig,
    render_markdown,
)
from streaming_feature_store.schemas.registry import SchemaRegistry

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)

_REGISTER_SCRIPT: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "register_schemas.py"
)


def _spawn_available() -> bool:
    """Return ``True`` iff the ``spawn`` start method is usable."""
    try:
        mp.get_context("spawn")
        return True
    except ValueError:  # pragma: no cover - exotic platforms
        return False


pytestmark = [pytest.mark.integration, pytest.mark.skipif(
    not _spawn_available(), reason="spawn start method unavailable"
)]


def _load_register_module():
    """Import the register_schemas script as a module."""
    spec = importlib.util.spec_from_file_location(
        "register_schemas_mp_runner", _REGISTER_SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def kafka_config() -> KafkaConfig:
    """Per-test Kafka config bound to a unique topic name."""
    return KafkaConfig(topic=f"e-commerce-events-mp-{uuid.uuid4().hex[:8]}")


@pytest.fixture
def registry_config() -> SchemaRegistryConfig:
    """Default Schema Registry config."""
    return SchemaRegistryConfig()


@pytest.fixture
def topic_admin(docker_services_up, kafka_config) -> TopicAdmin:
    """Live :class:`TopicAdmin`."""
    return TopicAdmin(kafka_config)


@pytest.fixture
def loadtest_topic(topic_admin, kafka_config):
    """Create a per-test topic with 12 partitions; delete on teardown."""
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


def test_mp_runner_smoke_two_processes_produce_and_ack(
    registered_subject, kafka_config, registry_config
):
    """Two processes, low rate, no errors, aggregate report makes sense."""
    cfg = MultiprocessLoadConfig(
        duration_s=2.0,
        target_rate=5_000.0,
        processes=2,
        workers_per_process=2,
        batch_size=256,
        max_in_flight=10_000,
        seed=11,
        topic=kafka_config.topic,
    )
    runner = MultiprocessLoadRunner(
        kafka_config, registry_config, cfg, floor_eps=0.0
    )
    report = runner.run()
    assert report.aggregate_snapshot.failed == 0
    assert report.aggregate_snapshot.acked == report.aggregate_snapshot.produced
    assert report.aggregate_snapshot.acked > 0
    assert len(report.process_outcomes) == 2
    # Each child must produce at least one event.
    assert all(o.report.snapshot.produced > 0 for o in report.process_outcomes)


def test_mp_runner_writes_report_file(
    registered_subject, kafka_config, registry_config, tmp_path
):
    """Markdown rendering includes the per-process breakdown."""
    cfg = MultiprocessLoadConfig(
        duration_s=1.0,
        target_rate=2_000.0,
        processes=2,
        workers_per_process=1,
        batch_size=128,
        seed=7,
        topic=kafka_config.topic,
    )
    runner = MultiprocessLoadRunner(
        kafka_config, registry_config, cfg, floor_eps=0.0
    )
    report = runner.run()
    out = tmp_path / "report.md"
    out.write_text(render_markdown(report), encoding="utf-8")
    text = out.read_text(encoding="utf-8")
    assert "Multi-Process Synthetic Event Load Test Results" in text
    assert kafka_config.topic in text
    assert "Per-process breakdown" in text


def test_mp_runner_fails_fast_on_missing_subject(
    docker_services_up, topic_admin, kafka_config, registry_config
):
    """If the value subject is not registered, the runner aborts before spawning."""
    fresh_name = f"e-commerce-events-mp-nosub-{uuid.uuid4().hex[:8]}"
    topic_admin.ensure_topic(fresh_name, num_partitions=3, replication_factor=1)
    try:
        from streaming_feature_store.schemas.registry import RegistryError

        cfg = MultiprocessLoadConfig(
            duration_s=0.5,
            target_rate=1_000.0,
            processes=2,
            workers_per_process=1,
            topic=fresh_name,
        )
        runner = MultiprocessLoadRunner(
            kafka_config, registry_config, cfg, floor_eps=0.0
        )
        with pytest.raises(RegistryError):
            runner.run()
    finally:
        try:
            topic_admin.delete_topic(fresh_name)
        except Exception:  # pragma: no cover
            pass
