"""End-to-end integration tests for :class:`TopicAdmin`."""

from __future__ import annotations

import logging
import uuid

import pytest
from confluent_kafka.admin import AdminClient, NewTopic

from streaming_feature_store.admin.topic_admin import (
    EnsureTopicOutcome,
    TopicAdmin,
)
from streaming_feature_store.config import KafkaConfig

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)


@pytest.fixture
def kafka_config() -> KafkaConfig:
    """Per-test :class:`KafkaConfig`."""
    return KafkaConfig()


@pytest.fixture
def topic_name() -> str:
    """Random per-test topic name."""
    return f"e-commerce-events-loadtest-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def topic_admin(docker_services_up, kafka_config) -> TopicAdmin:
    """Live :class:`TopicAdmin`."""
    return TopicAdmin(kafka_config)


@pytest.fixture
def cleanup_topic(topic_admin, topic_name):
    """Delete *topic_name* (best-effort) on teardown."""
    yield topic_name
    try:
        topic_admin.delete_topic(topic_name)
    except Exception as exc:  # pragma: no cover - best-effort
        logger.debug(f"teardown delete failed for {topic_name}: {exc}")


def test_ensure_creates_topic_with_expected_partitions(topic_admin, cleanup_topic):
    name = cleanup_topic
    result = topic_admin.ensure_topic(name, num_partitions=12, replication_factor=3)
    assert result.outcome is EnsureTopicOutcome.CREATED
    desc = topic_admin.describe_topic(name)
    assert desc.num_partitions == 12
    assert desc.replication_factor == 3


def test_ensure_idempotent_second_call(topic_admin, cleanup_topic):
    name = cleanup_topic
    first = topic_admin.ensure_topic(name, num_partitions=12, replication_factor=3)
    second = topic_admin.ensure_topic(name, num_partitions=12, replication_factor=3)
    assert first.outcome is EnsureTopicOutcome.CREATED
    assert second.outcome is EnsureTopicOutcome.ALREADY_EXISTS_MATCHING


def test_ensure_detects_partition_mismatch(kafka_config, topic_admin, cleanup_topic):
    name = cleanup_topic
    raw = AdminClient({"bootstrap.servers": kafka_config.bootstrap_servers})
    raw.create_topics([NewTopic(name, num_partitions=6, replication_factor=3)])[
        name
    ].result(timeout=10)
    result = topic_admin.ensure_topic(name, num_partitions=12, replication_factor=3)
    assert result.outcome is EnsureTopicOutcome.ALREADY_EXISTS_MISMATCH
    fields = {d.field for d in result.diff}
    assert "num_partitions" in fields
    desc = topic_admin.describe_topic(name)
    assert desc.num_partitions == 6  # not auto-altered


def test_describe_topic_returns_real_leader_assignment(topic_admin, cleanup_topic):
    name = cleanup_topic
    topic_admin.ensure_topic(name, num_partitions=12, replication_factor=3)
    desc = topic_admin.describe_topic(name)
    assert len(desc.partitions) == 12
    assert all(p.leader is not None for p in desc.partitions)


def test_delete_topic_removes_from_metadata(kafka_config, topic_admin, topic_name):
    topic_admin.ensure_topic(topic_name, num_partitions=3, replication_factor=1)
    topic_admin.delete_topic(topic_name)
    raw = AdminClient({"bootstrap.servers": kafka_config.bootstrap_servers})
    cluster = raw.list_topics(timeout=5.0)
    if topic_name in cluster.topics:
        # Stale metadata is possible right after delete; check error code marks it absent.
        assert cluster.topics[topic_name].error is not None
