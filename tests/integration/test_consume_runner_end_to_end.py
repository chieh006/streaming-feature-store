"""End-to-end integration tests for :class:`ConsumeRunner`.

Each test uses a per-test topic (``e-commerce-events-consumetest-<uuid>``)
and seeds it with an *exact* number of events via a direct
:class:`AvroEventProducer` so ``consumed == N`` assertions are precise.
Requires PR #1 infra and PR #2 schemas.
"""

from __future__ import annotations

import importlib.util
import logging
import sys
import uuid
from pathlib import Path

import pytest

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consume.accountant import ConsumeAccountant
from streaming_feature_store.consume.consume_runner import ConsumeRunner
from streaming_feature_store.consume.report import ConsumeRunConfig
from streaming_feature_store.load.synthetic import SyntheticEventGenerator
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.schemas.registry import RegistryError, SchemaRegistry

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)

_REGISTER_SCRIPT: Path = (
    Path(__file__).resolve().parents[2] / "scripts" / "register_schemas.py"
)


def _load_register_module():
    """Import the register_schemas script as a module."""
    spec = importlib.util.spec_from_file_location(
        "register_schemas_consumerunner", _REGISTER_SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def kafka_config() -> KafkaConfig:
    """Per-test Kafka config bound to a unique topic name."""
    return KafkaConfig(
        topic=f"e-commerce-events-consumetest-{uuid.uuid4().hex[:8]}"
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
def consumetest_topic(topic_admin, kafka_config):
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
def registered_subject(consumetest_topic, kafka_config, registry_config):
    """Register the per-topic value subject and clean up on teardown."""
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
    """Produce exactly *n* events into *topic* and flush.

    Parameters
    ----------
    kafka_config, registry_config : config
        Connection settings.
    topic : str
        Target per-test topic.
    n : int
        Exact number of events to produce.
    seed : int, optional
        Synthetic-generator seed.
    """
    gen = SyntheticEventGenerator(seed=seed)
    produced = 0
    with AvroEventProducer(kafka_config, registry_config, topic=topic) as prod:
        while produced < n:
            batch = gen.generate_batch(min(1000, n - produced))
            for event in batch:
                prod.produce(event)
                produced += 1
            prod.poll(0)
        remaining = prod.flush(30.0)
    assert remaining == 0, f"{remaining} message(s) unflushed"


def _cfg(topic: str, **over) -> ConsumeRunConfig:
    base = dict(
        duration_s=20.0,
        group_id=f"wk1-consume-{uuid.uuid4().hex[:6]}",
        topic=topic,
        poll_timeout_s=1.0,
        max_batch=1024,
        until_caught_up=True,
    )
    base.update(over)
    return ConsumeRunConfig(**base)


def test_single_member_smoke_consumes_and_commits(
    registered_subject, kafka_config, registry_config
):
    """Seed 5K, 1 member → consumed == 5000, no deserialize failures."""
    _seed_topic(kafka_config, registry_config, kafka_config.topic, 5_000)
    runner = ConsumeRunner(
        kafka_config, registry_config, _cfg(kafka_config.topic)
    )
    report = runner.run()
    assert report.snapshot.consumed == 5_000
    assert report.snapshot.deserialize_failed == 0
    assert report.snapshot.end_lag == 0


def test_e2e_latency_is_sane(
    registered_subject, kafka_config, registry_config
):
    """Freshly produced backlog → e2e p99 under a generous laptop ceiling."""
    _seed_topic(kafka_config, registry_config, kafka_config.topic, 5_000)
    report = ConsumeRunner(
        kafka_config, registry_config, _cfg(kafka_config.topic)
    ).run()
    assert report.snapshot.consumed == 5_000
    assert report.snapshot.e2e_p99_ms < 250.0


def test_until_caught_up_terminates(
    registered_subject, kafka_config, registry_config
):
    """``until_caught_up`` ends with end_lag == 0 before ``duration_s``."""
    _seed_topic(kafka_config, registry_config, kafka_config.topic, 3_000)
    cfg = _cfg(kafka_config.topic, duration_s=60.0, until_caught_up=True)
    report = ConsumeRunner(kafka_config, registry_config, cfg).run()
    assert report.snapshot.end_lag == 0
    assert report.snapshot.wallclock_s < 60.0


def test_offsets_resume_after_restart(
    registered_subject, kafka_config, registry_config
):
    """Restart with the same group.id resumes from the committed offset."""
    n = 4_000
    _seed_topic(kafka_config, registry_config, kafka_config.topic, n)
    group = f"wk1-resume-{uuid.uuid4().hex[:6]}"
    # First member: short, bounded — consumes only part of the backlog.
    cfg1 = _cfg(
        kafka_config.topic,
        group_id=group,
        duration_s=1.0,
        until_caught_up=False,
        max_batch=256,
    )
    r1 = ConsumeRunner(kafka_config, registry_config, cfg1).run()
    assert 0 < r1.snapshot.consumed < n
    # Second member: same group → resumes from the committed offset.
    cfg2 = _cfg(
        kafka_config.topic, group_id=group, duration_s=30.0, until_caught_up=True
    )
    r2 = ConsumeRunner(kafka_config, registry_config, cfg2).run()
    assert r2.snapshot.consumed > 0
    # At-least-once: union covers every produced record (no gap).
    assert r1.snapshot.consumed + r2.snapshot.consumed >= n


def test_raw_mode_decode_only(
    registered_subject, kafka_config, registry_config, monkeypatch
):
    """``deserialize_mode='raw'`` → consumed == N, avro_dict_to_event unused."""
    import streaming_feature_store.consume.consume_runner as cr

    spy = []
    real = cr.avro_dict_to_event
    monkeypatch.setattr(
        cr,
        "avro_dict_to_event",
        lambda d: (spy.append(1), real(d))[1],
    )
    _seed_topic(kafka_config, registry_config, kafka_config.topic, 2_000)
    cfg = _cfg(kafka_config.topic, deserialize_mode="raw")
    report = ConsumeRunner(kafka_config, registry_config, cfg).run()
    assert report.snapshot.consumed == 2_000
    assert spy == []  # avro_dict_to_event never invoked in raw mode


def test_fails_fast_on_missing_subject(
    docker_services_up, topic_admin, kafka_config, registry_config
):
    """An unregistered ``<topic>-value`` aborts before the first poll."""
    fresh = f"e-commerce-events-consumetest-nosub-{uuid.uuid4().hex[:8]}"
    topic_admin.ensure_topic(fresh, num_partitions=3, replication_factor=1)
    try:
        cfg = _cfg(fresh, duration_s=2.0)
        runner = ConsumeRunner(kafka_config, registry_config, cfg)
        with pytest.raises(RegistryError):
            runner.run()
    finally:
        try:
            topic_admin.delete_topic(fresh)
        except Exception:  # pragma: no cover
            pass


def test_injected_accountant_is_probeable(
    registered_subject, kafka_config, registry_config
):
    """An injected accountant exposes the raw reservoir post-run (MP seam)."""
    _seed_topic(kafka_config, registry_config, kafka_config.topic, 1_000)
    acct = ConsumeAccountant()
    ConsumeRunner(
        kafka_config,
        registry_config,
        _cfg(kafka_config.topic),
        accountant=acct,
    ).run()
    assert acct.consumed == 1_000
    assert len(acct.e2e_samples_s()) > 0
