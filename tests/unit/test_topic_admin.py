"""Unit tests for ``TopicAdmin`` (mocked ``AdminClient``)."""

from __future__ import annotations

from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaError, KafkaException

from streaming_feature_store.admin.topic_admin import (
    EnsureTopicOutcome,
    TopicAdmin,
)
from streaming_feature_store.config import KafkaConfig


def _make_partition(pid: int, leader: int, replicas: list[int]):
    """Return a fake ``PartitionMetadata`` object."""
    p = MagicMock()
    p.id = pid
    p.leader = leader
    p.replicas = replicas
    return p


def _make_topic_metadata(name: str, partitions: int, rf: int, error=None):
    """Return a fake ``TopicMetadata`` with the requested layout."""
    meta = MagicMock()
    meta.error = error
    meta.partitions = {
        i: _make_partition(i, leader=1, replicas=list(range(rf)))
        for i in range(partitions)
    }
    return meta


def _make_cluster(topics: dict):
    """Return a fake ``ClusterMetadata`` containing *topics*."""
    cluster = MagicMock()
    cluster.topics = topics
    return cluster


@pytest.fixture
def kafka_config() -> KafkaConfig:
    """Return a default :class:`KafkaConfig`."""
    return KafkaConfig()


@pytest.fixture
def patched_admin(monkeypatch, kafka_config):
    """Patch the AdminClient ctor so :class:`TopicAdmin` uses a mock."""
    mock_client = MagicMock()
    monkeypatch.setattr(
        "streaming_feature_store.admin.topic_admin.AdminClient",
        lambda conf: mock_client,
    )
    admin = TopicAdmin(kafka_config)
    return admin, mock_client


def _completed_future(result_value=None, exc: BaseException | None = None) -> Future:
    """Return a finished future."""
    fut: Future = Future()
    if exc is not None:
        fut.set_exception(exc)
    else:
        fut.set_result(result_value)
    return fut


def test_ensure_creates_when_absent(patched_admin):
    admin, client = patched_admin
    client.list_topics.return_value = _make_cluster({})
    client.create_topics.return_value = {"e-commerce-events": _completed_future()}
    result = admin.ensure_topic(
        "e-commerce-events", num_partitions=12, replication_factor=3
    )
    assert result.outcome is EnsureTopicOutcome.CREATED
    assert client.create_topics.call_count == 1
    new_topics = client.create_topics.call_args[0][0]
    assert new_topics[0].topic == "e-commerce-events"
    assert new_topics[0].num_partitions == 12
    assert new_topics[0].replication_factor == 3


def test_ensure_returns_already_exists_matching(patched_admin):
    admin, client = patched_admin
    client.list_topics.return_value = _make_cluster(
        {"t": _make_topic_metadata("t", partitions=12, rf=3)}
    )
    result = admin.ensure_topic("t", num_partitions=12, replication_factor=3)
    assert result.outcome is EnsureTopicOutcome.ALREADY_EXISTS_MATCHING
    client.create_topics.assert_not_called()


def test_ensure_returns_mismatch_with_diff(patched_admin):
    admin, client = patched_admin
    client.list_topics.return_value = _make_cluster(
        {"t": _make_topic_metadata("t", partitions=6, rf=3)}
    )
    result = admin.ensure_topic("t", num_partitions=12, replication_factor=3)
    assert result.outcome is EnsureTopicOutcome.ALREADY_EXISTS_MISMATCH
    fields = {d.field for d in result.diff}
    assert "num_partitions" in fields
    client.create_topics.assert_not_called()


def test_ensure_swallows_topic_already_exists_race(patched_admin):
    admin, client = patched_admin
    client.list_topics.return_value = _make_cluster({})
    err = KafkaError(KafkaError.TOPIC_ALREADY_EXISTS)
    client.create_topics.return_value = {
        "t": _completed_future(exc=KafkaException(err))
    }
    result = admin.ensure_topic("t", num_partitions=12, replication_factor=3)
    assert result.outcome is EnsureTopicOutcome.CREATED


def test_ensure_propagates_unexpected_kafka_error(patched_admin):
    admin, client = patched_admin
    client.list_topics.return_value = _make_cluster({})
    err = KafkaError(KafkaError.INVALID_CONFIG)
    client.create_topics.return_value = {
        "t": _completed_future(exc=KafkaException(err))
    }
    with pytest.raises(KafkaException):
        admin.ensure_topic("t", num_partitions=12, replication_factor=3)


def test_describe_topic_returns_partition_count(patched_admin):
    admin, client = patched_admin
    client.list_topics.return_value = _make_cluster(
        {"t": _make_topic_metadata("t", partitions=12, rf=3)}
    )
    desc = admin.describe_topic("t")
    assert desc.num_partitions == 12
    assert desc.replication_factor == 3
    assert len(desc.partitions) == 12


def test_describe_topic_missing_raises(patched_admin):
    admin, client = patched_admin
    client.list_topics.return_value = _make_cluster({})
    with pytest.raises(KeyError):
        admin.describe_topic("nope")


def test_delete_topic_invokes_delete_topics(patched_admin):
    admin, client = patched_admin
    client.delete_topics.return_value = {"t": _completed_future()}
    admin.delete_topic("t")
    client.delete_topics.assert_called_once()
    assert client.delete_topics.call_args[0][0] == ["t"]


def test_context_manager_close_is_noop_idempotent(patched_admin):
    admin, _ = patched_admin
    with admin as a:
        a.close()
    admin.close()  # second close also fine


def test_admin_client_built_with_security_protocol(monkeypatch):
    received: dict = {}

    def factory(conf):
        received.update(conf)
        return MagicMock()

    monkeypatch.setattr(
        "streaming_feature_store.admin.topic_admin.AdminClient",
        factory,
    )
    cfg = KafkaConfig(security_protocol="PLAINTEXT")
    TopicAdmin(cfg)
    assert received["security.protocol"] == "PLAINTEXT"
    assert "bootstrap.servers" in received


def test_topic_with_error_treated_as_absent(patched_admin):
    """A topic entry with a non-None ``error`` attribute is treated as missing."""
    admin, client = patched_admin
    error_meta = _make_topic_metadata("t", partitions=12, rf=3, error="UNKNOWN_TOPIC")
    client.list_topics.return_value = _make_cluster({"t": error_meta})
    client.create_topics.return_value = {"t": _completed_future()}
    result = admin.ensure_topic("t", num_partitions=12, replication_factor=3)
    assert result.outcome is EnsureTopicOutcome.CREATED


def test_ensure_replication_factor_mismatch(patched_admin):
    admin, client = patched_admin
    client.list_topics.return_value = _make_cluster(
        {"t": _make_topic_metadata("t", partitions=12, rf=1)}
    )
    result = admin.ensure_topic("t", num_partitions=12, replication_factor=3)
    assert result.outcome is EnsureTopicOutcome.ALREADY_EXISTS_MISMATCH
    fields = {d.field for d in result.diff}
    assert "replication_factor" in fields
