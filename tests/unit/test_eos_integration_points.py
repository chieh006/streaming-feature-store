"""Tests for the EOS edits to existing modules (design week2_03 §2.3/§2.4/§2.5).

Covers the ``ProducerTuning.transactional_id`` emission, the new
:class:`AvroEventConsumer` transactional accessors, and the
``SlidingConsumerConfig.isolation_level`` default flip.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from streaming_feature_store.config import (
    KafkaConfig,
    ProducerTuning,
    SchemaRegistryConfig,
)
from streaming_feature_store.consumer.avro_consumer import AvroEventConsumer
from streaming_feature_store.sliding.models import SlidingConsumerConfig

# --- ProducerTuning.transactional_id ---------------------------------------


def test_no_transactional_id_by_default() -> None:
    conf = ProducerTuning().as_librdkafka_conf()
    assert "transactional.id" not in conf


def test_transactional_id_forces_idempotent_foundation() -> None:
    conf = ProducerTuning(transactional_id="val-0").as_librdkafka_conf()
    assert conf["transactional.id"] == "val-0"
    assert conf["enable.idempotence"] is True
    assert conf["acks"] == "all"
    assert conf["max.in.flight.requests.per.connection"] == 5


def test_transactional_id_overrides_acks_one() -> None:
    # Even with the throughput acks=1 default, a txn id forces acks=all.
    conf = ProducerTuning(acks="1", transactional_id="val-0").as_librdkafka_conf()
    assert conf["acks"] == "all"


# --- AvroEventConsumer transactional accessors -----------------------------


def _consumer() -> AvroEventConsumer:
    consumer = AvroEventConsumer(
        KafkaConfig(), SchemaRegistryConfig(), group_id="g", topic="t"
    )
    consumer._consumer = MagicMock()
    return consumer


def test_assignment_delegates() -> None:
    c = _consumer()
    c._consumer.assignment.return_value = ["tp0", "tp1"]
    assert c.assignment() == ["tp0", "tp1"]


def test_position_delegates() -> None:
    c = _consumer()
    c._consumer.position.return_value = ["pos"]
    assert c.position(["tp0"]) == ["pos"]
    c._consumer.position.assert_called_once_with(["tp0"])


def test_consumer_group_metadata_delegates() -> None:
    c = _consumer()
    c._consumer.consumer_group_metadata.return_value = "meta"
    assert c.consumer_group_metadata() == "meta"


def test_default_isolation_unchanged_read_uncommitted() -> None:
    # The validator's own source is the non-transactional feeder, so the class
    # default stays read_uncommitted (design week2_03 §2.5).
    assert _consumer().isolation_level == "read_uncommitted"


# --- SlidingConsumerConfig.isolation_level ---------------------------------


def test_sliding_default_isolation_is_read_committed() -> None:
    assert SlidingConsumerConfig().isolation_level == "read_committed"


def test_sliding_accepts_read_uncommitted_override() -> None:
    cfg = SlidingConsumerConfig(isolation_level="read_uncommitted")
    assert cfg.isolation_level == "read_uncommitted"


def test_sliding_rejects_unknown_isolation() -> None:
    with pytest.raises(ValidationError):
        SlidingConsumerConfig(isolation_level="weird")
