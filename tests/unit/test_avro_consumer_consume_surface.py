"""Unit tests for the consume-runner surface added to ``AvroEventConsumer``.

Covers the methods :class:`ConsumeRunner` depends on: ``isolation_level``
wiring, ``subscribe``, ``poll_batch``, ``commit``, ``assigned_partitions``,
and the ``consumer_lag`` watermark/position math.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consumer.avro_consumer import AvroEventConsumer


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig(bootstrap_servers="b:9092", topic="e-commerce-events")


@pytest.fixture
def registry_config() -> SchemaRegistryConfig:
    return SchemaRegistryConfig(url="http://r:8081")


@pytest.fixture
def patched(kafka_config, registry_config):
    with patch(
        "streaming_feature_store.schemas.registry.SchemaRegistryClient"
    ), patch(
        "streaming_feature_store.consumer.avro_consumer.DeserializingConsumer"
    ) as consumer_cls, patch(
        "streaming_feature_store.consumer.avro_consumer.AvroDeserializer"
    ):
        instance = MagicMock(name="DeserializingConsumer")
        consumer_cls.return_value = instance
        yield {"consumer_cls": consumer_cls, "instance": instance}


def _make(kafka_config, registry_config, **kw) -> AvroEventConsumer:
    return AvroEventConsumer(
        kafka_config, registry_config, group_id="g", **kw
    )


def test_default_isolation_level_is_read_uncommitted(
    patched, kafka_config, registry_config
) -> None:
    c = _make(kafka_config, registry_config)
    conf = patched["consumer_cls"].call_args.args[0]
    assert conf["isolation.level"] == "read_uncommitted"
    assert c.isolation_level == "read_uncommitted"


def test_isolation_level_read_committed_is_wired(
    patched, kafka_config, registry_config
) -> None:
    c = _make(kafka_config, registry_config, isolation_level="read_committed")
    conf = patched["consumer_cls"].call_args.args[0]
    assert conf["isolation.level"] == "read_committed"
    assert c.isolation_level == "read_committed"


def test_subscribe_is_issued_once(
    patched, kafka_config, registry_config
) -> None:
    c = _make(kafka_config, registry_config)
    c.subscribe()
    c.subscribe()
    patched["instance"].subscribe.assert_called_once_with(["e-commerce-events"])


def test_poll_batch_subscribes_then_returns_messages(
    patched, kafka_config, registry_config
) -> None:
    msg = MagicMock()
    msg.error.return_value = None
    patched["instance"].poll.side_effect = [msg, None]
    c = _make(kafka_config, registry_config)
    out = c.poll_batch(timeout_s=0.5, max_messages=1)
    assert out == [msg]
    patched["instance"].subscribe.assert_called_once()


def test_poll_batch_raises_on_kafka_error(
    patched, kafka_config, registry_config
) -> None:
    bad = MagicMock()
    bad.error.return_value = MagicMock()
    patched["instance"].poll.return_value = bad
    c = _make(kafka_config, registry_config)
    with pytest.raises(RuntimeError):
        c.poll_batch(timeout_s=0.2, max_messages=1)


def test_commit_is_synchronous(
    patched, kafka_config, registry_config
) -> None:
    c = _make(kafka_config, registry_config)
    c.commit()
    patched["instance"].commit.assert_called_once_with(asynchronous=False)


def test_assigned_partitions_sorted(
    patched, kafka_config, registry_config
) -> None:
    patched["instance"].assignment.return_value = [
        SimpleNamespace(topic="t", partition=5),
        SimpleNamespace(topic="t", partition=1),
        SimpleNamespace(topic="t", partition=3),
    ]
    c = _make(kafka_config, registry_config)
    assert c.assigned_partitions() == [1, 3, 5]


def test_consumer_lag_zero_when_no_assignment(
    patched, kafka_config, registry_config
) -> None:
    patched["instance"].assignment.return_value = []
    c = _make(kafka_config, registry_config)
    assert c.consumer_lag() == 0


def test_consumer_lag_sums_and_skips_invalid_positions(
    patched, kafka_config, registry_config
) -> None:
    tp0 = SimpleNamespace(topic="t", partition=0)
    tp1 = SimpleNamespace(topic="t", partition=1)
    inst = patched["instance"]
    inst.assignment.return_value = [tp0, tp1]
    inst.position.return_value = [
        SimpleNamespace(topic="t", partition=0, offset=10),
        SimpleNamespace(topic="t", partition=1, offset=-1001),  # invalid
    ]
    inst.get_watermark_offsets.side_effect = lambda tp, **kw: (0, 100)
    c = _make(kafka_config, registry_config)
    # Partition 0: 100 - 10 = 90; partition 1 skipped (invalid position).
    assert c.consumer_lag() == 90


def test_consumer_lag_never_negative(
    patched, kafka_config, registry_config
) -> None:
    tp0 = SimpleNamespace(topic="t", partition=0)
    inst = patched["instance"]
    inst.assignment.return_value = [tp0]
    inst.position.return_value = [
        SimpleNamespace(topic="t", partition=0, offset=120)
    ]
    inst.get_watermark_offsets.side_effect = lambda tp, **kw: (0, 100)
    c = _make(kafka_config, registry_config)
    assert c.consumer_lag() == 0
