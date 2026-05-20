"""End-to-end integration tests for :class:`MultiprocessConsumeRunner`.

The headline is the symmetric-GIL contrast (design doc §2.1 / §6.3): a
1-member group cannot drain a producer running at ~50K evt/s (lag and
end-to-end latency ramp) while a planned *N*-member group can.  Those two
are tagged ``benchmark`` and excluded from the default integration run
(use ``make test-benchmark``); the rest are lightweight.

Skipped where the ``spawn`` start method is unavailable.
"""

from __future__ import annotations

import importlib.util
import logging
import multiprocessing as mp
import sys
import threading
import uuid
from pathlib import Path

import numpy as np
import pytest

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consume_mp.mp_runner import MultiprocessConsumeRunner
from streaming_feature_store.consume_mp.process_planner import (
    plan_consume_processes,
    resolve_cpu_budget,
)
from streaming_feature_store.consume_mp.report import (
    MultiprocessConsumeConfig,
    render_markdown,
)
from streaming_feature_store.load.synthetic import SyntheticEventGenerator
from streaming_feature_store.load_mp.mp_runner import MultiprocessLoadRunner
from streaming_feature_store.load_mp.report import MultiprocessLoadConfig
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.schemas.registry import SchemaRegistry

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


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _spawn_available(), reason="spawn start method unavailable"
    ),
]


def _load_register_module():
    """Import the register_schemas script as a module."""
    spec = importlib.util.spec_from_file_location(
        "register_schemas_consumemp", _REGISTER_SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def kafka_config() -> KafkaConfig:
    """Per-test Kafka config bound to a unique topic name."""
    return KafkaConfig(
        topic=f"e-commerce-events-consumemp-{uuid.uuid4().hex[:8]}"
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
def consumemp_topic(topic_admin, kafka_config):
    """Create a per-test 12-partition topic; delete on teardown."""
    topic_admin.ensure_topic(
        kafka_config.topic, num_partitions=12, replication_factor=3
    )
    yield kafka_config.topic
    try:
        topic_admin.delete_topic(kafka_config.topic)
    except Exception:  # pragma: no cover - best-effort
        pass


@pytest.fixture
def registered_subject(consumemp_topic, kafka_config, registry_config):
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


def _seed_topic(
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    topic: str,
    n: int,
    *,
    seed: int = 7,
) -> None:
    """Produce exactly *n* events into *topic* and flush."""
    gen = SyntheticEventGenerator(seed=seed)
    produced = 0
    with AvroEventProducer(kafka_config, registry_config, topic=topic) as prod:
        while produced < n:
            for event in gen.generate_batch(min(1000, n - produced)):
                prod.produce(event)
                produced += 1
            prod.poll(0)
        assert prod.flush(30.0) == 0


def _mp_cfg(topic: str, members: int, **over) -> MultiprocessConsumeConfig:
    base = dict(
        duration_s=20.0,
        group_id=f"wk1-consume-mp-{uuid.uuid4().hex[:6]}",
        members=members,
        topic=topic,
        until_caught_up=True,
    )
    base.update(over)
    return MultiprocessConsumeConfig(**base)


def _producer_thread(
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    topic: str,
    *,
    duration_s: float,
    rate: float,
) -> threading.Thread:
    """Return a started daemon thread running a multi-process producer."""

    def _run() -> None:
        cfg = MultiprocessLoadConfig(
            duration_s=duration_s,
            target_rate=rate,
            processes=4,
            workers_per_process=2,
            topic=topic,
        )
        MultiprocessLoadRunner(
            kafka_config, registry_config, cfg, floor_eps=0.0
        ).run()

    t = threading.Thread(target=_run, name="bg-producer", daemon=True)
    t.start()
    return t


# --- lightweight (default integration run) --------------------------------


def test_group_splits_partitions_across_members(
    registered_subject, kafka_config, registry_config
):
    """6 members on 12 partitions → disjoint assignments whose union is all 12."""
    _seed_topic(kafka_config, registry_config, kafka_config.topic, 24_000)
    cfg = _mp_cfg(
        kafka_config.topic, members=6, duration_s=12.0, until_caught_up=False
    )
    report = MultiprocessConsumeRunner(
        kafka_config, registry_config, cfg
    ).run()
    assert len(report.process_outcomes) == 6
    seen: set[int] = set()
    for o in report.process_outcomes:
        parts = set(o.report.assigned_partitions)
        assert seen.isdisjoint(parts), f"overlap: {seen & parts}"
        seen |= parts
    assert seen == set(range(12))


def test_aggregate_latency_is_union_percentile(
    registered_subject, kafka_config, registry_config
):
    """Aggregate p99 equals the percentile over the merged reservoir (±ε)."""
    _seed_topic(kafka_config, registry_config, kafka_config.topic, 10_000)
    cfg = _mp_cfg(kafka_config.topic, members=3)
    report = MultiprocessConsumeRunner(
        kafka_config, registry_config, cfg
    ).run()
    merged: list[float] = []
    for o in report.process_outcomes:
        merged.extend(o.e2e_samples_s)
    if merged:
        expected = float(np.percentile(np.asarray(merged), 99.0) * 1000.0)
        assert report.aggregate_snapshot.e2e_p99_ms == pytest.approx(
            expected, rel=1e-6
        )


def test_rebalance_on_member_change_no_loss(
    registered_subject, kafka_config, registry_config
):
    """A membership change (rebalance) loses no records (at-least-once)."""
    n = 6_000
    _seed_topic(kafka_config, registry_config, kafka_config.topic, n)
    group = f"wk1-rebalance-{uuid.uuid4().hex[:6]}"
    # First a 1-member group consumes part of the backlog and commits.
    r1 = MultiprocessConsumeRunner(
        kafka_config,
        registry_config,
        _mp_cfg(
            kafka_config.topic,
            members=1,
            group_id=group,
            duration_s=1.5,
            until_caught_up=False,
        ),
    ).run()
    # Then a 2-member group joins the SAME group → rebalance; drains the rest.
    r2 = MultiprocessConsumeRunner(
        kafka_config,
        registry_config,
        _mp_cfg(
            kafka_config.topic, members=2, group_id=group, duration_s=30.0
        ),
    ).run()
    consumed = r1.aggregate_snapshot.consumed + r2.aggregate_snapshot.consumed
    assert consumed >= n  # no record permanently lost across the rebalance


def test_report_file_written_and_self_documents_profile(
    registered_subject, kafka_config, registry_config, tmp_path
):
    """The rendered artifact carries the Members / Isolation / Deserialize rows."""
    _seed_topic(kafka_config, registry_config, kafka_config.topic, 2_000)
    cfg = _mp_cfg(
        kafka_config.topic,
        members=2,
        isolation_level="read_committed",
        deserialize_mode="raw",
    )
    report = MultiprocessConsumeRunner(
        kafka_config, registry_config, cfg
    ).run()
    out = tmp_path / "consume_mp.md"
    out.write_text(render_markdown(report), encoding="utf-8")
    text = out.read_text(encoding="utf-8")
    assert "Multi-Process Consumer Group" in text
    assert "Members (processes) | 2" in text
    assert "read_committed" in text
    assert "Deserialize mode | raw" in text
    assert "Per-process breakdown" in text


# --- benchmark (the symmetric-GIL headline) -------------------------------


@pytest.mark.benchmark
def test_single_member_cannot_drain_50k_backlog(
    registered_subject, kafka_config, registry_config
):
    """1 member vs a ~50K evt/s producer → lag ramps (the GIL ceiling proof)."""
    duration = 12.0
    prod = _producer_thread(
        kafka_config,
        registry_config,
        kafka_config.topic,
        duration_s=duration,
        rate=50_000.0,
    )
    cfg = _mp_cfg(
        kafka_config.topic,
        members=1,
        duration_s=duration,
        until_caught_up=False,
    )
    report = MultiprocessConsumeRunner(
        kafka_config, registry_config, cfg
    ).run()
    prod.join(timeout=30.0)
    assert report.aggregate_snapshot.lag_ramped is True
    assert report.passed is False


@pytest.mark.benchmark
def test_member_group_drains_50k_backlog(
    registered_subject, kafka_config, registry_config
):
    """A planned N-member group keeps up with the same producer (flat lag)."""
    duration = 12.0
    cpu_budget = resolve_cpu_budget(on_host_brokers=True)
    plan = plan_consume_processes(partitions=12, cpu_budget=cpu_budget)
    prod = _producer_thread(
        kafka_config,
        registry_config,
        kafka_config.topic,
        duration_s=duration,
        rate=50_000.0,
    )
    cfg = _mp_cfg(
        kafka_config.topic,
        members=plan.members,
        duration_s=duration,
        until_caught_up=False,
    )
    report = MultiprocessConsumeRunner(
        kafka_config, registry_config, cfg
    ).run()
    prod.join(timeout=30.0)
    assert report.aggregate_snapshot.lag_ramped is False
    assert report.aggregate_snapshot.consumed > 0
