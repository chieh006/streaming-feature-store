"""Unit tests for :class:`DlqProducer` using mocked SR + librdkafka."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from streaming_feature_store.config import (
    KafkaConfig,
    ProducerTuning,
    SchemaRegistryConfig,
)
from streaming_feature_store.validate.dlq import (
    DEFAULT_DLQ_TOPIC,
    DlqProducer,
    DlqRecord,
    ErrorClass,
    _dlq_to_dict,
    serialize_dlq_record,
)


@pytest.fixture
def dlq_record() -> DlqRecord:
    return DlqRecord(
        original_topic="t",
        original_partition=1,
        original_offset=2,
        original_timestamp_ms=3,
        original_key_bytes=b"k",
        original_value_bytes=b"v",
        rejected_at_ms=4,
        error_class=ErrorClass.OUT_OF_RANGE,
        validator_name="V",
        error_field_path="payload.f",
        error_message="msg",
    )


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig()


@pytest.fixture
def registry_config() -> SchemaRegistryConfig:
    return SchemaRegistryConfig()


def _patch_dependencies():
    """Patch out :class:`SchemaRegistry`, the Avro serializer, and the producer."""
    return [
        patch("streaming_feature_store.validate.dlq.SchemaRegistry"),
        patch("streaming_feature_store.validate.dlq.AvroSerializer"),
        patch("streaming_feature_store.validate.dlq.SerializingProducer"),
    ]


def test_dlq_producer_registers_schema_on_construction(
    kafka_config, registry_config
):
    patches = _patch_dependencies()
    with patches[0] as sr, patches[1], patches[2]:
        sr.return_value.register.return_value = 17
        DlqProducer(
            kafka_config,
            registry_config,
            topic="my-dlq",
            register_schema=True,
        )
        sr.return_value.register.assert_called_once()
        args = sr.return_value.register.call_args.args
        assert args[0] == "my-dlq-value"


def test_dlq_producer_skips_registration_when_disabled(
    kafka_config, registry_config
):
    patches = _patch_dependencies()
    with patches[0] as sr, patches[1], patches[2]:
        DlqProducer(
            kafka_config,
            registry_config,
            register_schema=False,
        )
        sr.return_value.register.assert_not_called()


def test_dlq_producer_send_produces_with_idempotency_key(
    kafka_config, registry_config, dlq_record
):
    patches = _patch_dependencies()
    with patches[0], patches[1], patches[2] as producer_cls:
        producer = DlqProducer(
            kafka_config, registry_config, register_schema=False
        )
        producer.send(dlq_record)
        inst = producer_cls.return_value
        inst.produce.assert_called_once()
        call_kwargs = inst.produce.call_args.kwargs
        assert call_kwargs["key"] == dlq_record.idempotency_key()
        assert call_kwargs["value"] is dlq_record


def test_dlq_producer_send_uses_default_callback(
    kafka_config, registry_config, dlq_record
):
    patches = _patch_dependencies()
    with patches[0], patches[1], patches[2] as producer_cls:
        producer = DlqProducer(
            kafka_config, registry_config, register_schema=False
        )
        producer.send(dlq_record)
        cb = producer_cls.return_value.produce.call_args.kwargs["on_delivery"]
        # Exercise the default callback (no-op happy path).
        cb(None, MagicMock(topic=lambda: "t", partition=lambda: 0, offset=lambda: 1))
        # And the error path.
        err = MagicMock()
        err.__str__ = lambda self: "boom"
        cb(err, None)


def test_dlq_producer_send_accepts_custom_callback(
    kafka_config, registry_config, dlq_record
):
    patches = _patch_dependencies()
    with patches[0], patches[1], patches[2]:
        producer = DlqProducer(
            kafka_config, registry_config, register_schema=False
        )
        custom = MagicMock()
        producer.send(dlq_record, on_delivery=custom)


def test_dlq_producer_send_after_close_raises(
    kafka_config, registry_config, dlq_record
):
    patches = _patch_dependencies()
    with patches[0], patches[1], patches[2]:
        producer = DlqProducer(
            kafka_config, registry_config, register_schema=False
        )
        producer.close()
        with pytest.raises(RuntimeError, match="closed"):
            producer.send(dlq_record)


def test_dlq_producer_close_is_idempotent(kafka_config, registry_config):
    patches = _patch_dependencies()
    with patches[0], patches[1], patches[2] as producer_cls:
        producer = DlqProducer(
            kafka_config, registry_config, register_schema=False
        )
        producer.close()
        producer.close()  # second close is a no-op
        # close() called flush at least once on the underlying producer
        assert producer_cls.return_value.flush.called


def test_dlq_producer_close_warns_on_remaining_messages(
    kafka_config, registry_config, caplog
):
    import logging

    patches = _patch_dependencies()
    with patches[0], patches[1], patches[2] as producer_cls:
        producer_cls.return_value.flush.return_value = 5
        with caplog.at_level(logging.WARNING):
            producer = DlqProducer(
                kafka_config, registry_config, register_schema=False
            )
            producer.close()
        assert any("remain unflushed" in r.getMessage() for r in caplog.records)


def test_dlq_producer_flush_and_poll(kafka_config, registry_config):
    patches = _patch_dependencies()
    with patches[0], patches[1], patches[2] as producer_cls:
        producer_cls.return_value.flush.return_value = 0
        producer_cls.return_value.poll.return_value = 3
        producer = DlqProducer(
            kafka_config, registry_config, register_schema=False
        )
        assert producer.flush(2.0) == 0
        assert producer.poll(0.5) == 3


def test_dlq_producer_context_manager(kafka_config, registry_config):
    patches = _patch_dependencies()
    with patches[0], patches[1], patches[2]:
        with DlqProducer(
            kafka_config, registry_config, register_schema=False
        ) as p:
            assert p.topic == DEFAULT_DLQ_TOPIC
            assert isinstance(p.schema_str, str)


def test_dlq_producer_topic_and_schema_properties(
    kafka_config, registry_config
):
    patches = _patch_dependencies()
    with patches[0], patches[1], patches[2]:
        producer = DlqProducer(
            kafka_config, registry_config, topic="custom-dlq", register_schema=False
        )
        assert producer.topic == "custom-dlq"
        assert "DlqRecord" in producer.schema_str


def test_dlq_producer_honors_custom_tuning(kafka_config, registry_config):
    patches = _patch_dependencies()
    tuning = ProducerTuning(linger_ms=42, acks="all")
    with patches[0], patches[1], patches[2] as producer_cls:
        DlqProducer(
            kafka_config,
            registry_config,
            tuning=tuning,
            register_schema=False,
        )
        conf = producer_cls.call_args.args[0]
        assert conf["linger.ms"] == 42


def test_dlq_to_dict_calls_serialize(dlq_record):
    out = _dlq_to_dict(dlq_record, ctx=None)
    assert out == serialize_dlq_record(dlq_record)
