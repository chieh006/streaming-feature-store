"""Unit tests for :class:`DlqRecord` and :func:`serialize_dlq_record`."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from streaming_feature_store.validate.dlq import (
    DlqRecord,
    ErrorClass,
    _kafka_key_bytes,
    _kafka_timestamp_ms,
    load_dlq_schema_str,
    serialize_dlq_record,
)
from streaming_feature_store.validate.pipeline import Invalid


def _fake_msg(
    *,
    topic: str = "e-commerce-events",
    partition: int = 3,
    offset: int = 42,
    timestamp_type: int = 1,
    timestamp_ms: int = 1_700_000_000_000,
    key=b"u-1",
    value: bytes = b"\x00\x00\x00\x00\x01payload",
) -> MagicMock:
    """Return a mock that mimics ``confluent_kafka.Message``."""
    msg = MagicMock()
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.timestamp.return_value = (timestamp_type, timestamp_ms)
    msg.key.return_value = key
    msg.value.return_value = value
    return msg


def test_dlq_record_from_raw_populates_kafka_coordinates() -> None:
    msg = _fake_msg()
    record = DlqRecord.from_raw(
        msg,
        error_class=ErrorClass.OUT_OF_RANGE,
        validator_name="PriceRangeValidator",
        error_field_path="payload.price_cents",
        error_message="price_cents=-1",
    )
    assert record.original_topic == "e-commerce-events"
    assert record.original_partition == 3
    assert record.original_offset == 42
    assert record.original_timestamp_ms == 1_700_000_000_000
    assert record.original_value_bytes == b"\x00\x00\x00\x00\x01payload"


def test_dlq_record_from_raw_preserves_value_bytes() -> None:
    raw_bytes = bytes(range(0, 64))
    msg = _fake_msg(value=raw_bytes)
    record = DlqRecord.from_raw(
        msg,
        error_class=ErrorClass.DESERIALIZE_FAILURE,
        validator_name="AvroEventConsumer",
        error_field_path=None,
        error_message="bad bytes",
    )
    assert record.original_value_bytes == raw_bytes


def test_dlq_record_handles_null_key() -> None:
    msg = _fake_msg(key=None)
    record = DlqRecord.from_raw(
        msg,
        error_class=ErrorClass.OUT_OF_RANGE,
        validator_name="PriceRangeValidator",
        error_field_path=None,
        error_message="bad",
    )
    assert record.original_key_bytes is None


def test_dlq_record_idempotency_key_format() -> None:
    msg = _fake_msg(topic="t", partition=2, offset=99)
    record = DlqRecord.from_raw(
        msg,
        error_class=ErrorClass.MALFORMED_RECORD,
        validator_name="UserIdShapeValidator",
        error_field_path="user_id",
        error_message="too long",
    )
    assert record.idempotency_key() == "t:2:99"


def test_dlq_record_default_schema_version_and_validator_version() -> None:
    msg = _fake_msg()
    record = DlqRecord.from_raw(
        msg,
        error_class=ErrorClass.OUT_OF_RANGE,
        validator_name="V",
        error_field_path=None,
        error_message="x",
    )
    assert record.schema_version == 1
    assert record.validator_version == "1.0.0"


def test_dlq_record_custom_validator_version() -> None:
    msg = _fake_msg()
    record = DlqRecord.from_raw(
        msg,
        error_class=ErrorClass.OUT_OF_RANGE,
        validator_name="V",
        error_field_path=None,
        error_message="x",
        validator_version="1.2.3",
    )
    assert record.validator_version == "1.2.3"


def test_dlq_record_rejected_at_override_preserved() -> None:
    msg = _fake_msg()
    record = DlqRecord.from_raw(
        msg,
        error_class=ErrorClass.OUT_OF_RANGE,
        validator_name="V",
        error_field_path=None,
        error_message="x",
        rejected_at_ms=123456789,
    )
    assert record.rejected_at_ms == 123456789


def test_dlq_record_from_raw_serializes_dict_value() -> None:
    msg = _fake_msg(value={"event_id": "abc", "user_id": "u-1"})
    record = DlqRecord.from_raw(
        msg,
        error_class=ErrorClass.OUT_OF_RANGE,
        validator_name="V",
        error_field_path=None,
        error_message="x",
    )
    # Dict values are JSON-encoded so the DLQ retains the decoded shape
    # even when a DeserializingConsumer is used upstream.
    decoded = record.original_value_bytes.decode("utf-8")
    assert "user_id" in decoded and "u-1" in decoded


def test_dlq_record_from_raw_handles_none_value() -> None:
    msg = _fake_msg(value=None)
    record = DlqRecord.from_raw(
        msg,
        error_class=ErrorClass.OUT_OF_RANGE,
        validator_name="V",
        error_field_path=None,
        error_message="x",
    )
    assert record.original_value_bytes == b""


def test_dlq_record_from_raw_falls_back_to_repr() -> None:
    class _Weird:
        def __str__(self) -> str:
            return "weird-thing"

    msg = _fake_msg(value=_Weird())
    record = DlqRecord.from_raw(
        msg,
        error_class=ErrorClass.OUT_OF_RANGE,
        validator_name="V",
        error_field_path=None,
        error_message="x",
    )
    assert record.original_value_bytes == b"weird-thing"


def test_dlq_record_handles_avro_dict_with_tuple_payload() -> None:
    # The DeserializingConsumer returns the Avro `payload` union as a
    # ``(fqn, body)`` tuple; the JSON encoder handles tuples.
    value = {
        "event_id": "abc",
        "payload": (
            "com.featurestore.ecommerce.v1.ClickPayload",
            {"element_id": "b", "page_url": "/p"},
        ),
    }
    msg = _fake_msg(value=value)
    record = DlqRecord.from_raw(
        msg,
        error_class=ErrorClass.OUT_OF_RANGE,
        validator_name="V",
        error_field_path=None,
        error_message="x",
    )
    decoded = record.original_value_bytes.decode("utf-8")
    assert "ClickPayload" in decoded
    assert "element_id" in decoded


def test_dlq_record_handles_avro_dict_with_bytes_field() -> None:
    msg = _fake_msg(value={"k": b"\x00\x01\x02"})
    record = DlqRecord.from_raw(
        msg,
        error_class=ErrorClass.OUT_OF_RANGE,
        validator_name="V",
        error_field_path=None,
        error_message="x",
    )
    # bytes fields are surrogate-escaped to keep the JSON encoder happy.
    decoded = record.original_value_bytes.decode("utf-8")
    assert "k" in decoded


def test_serialize_dlq_record_round_trip_fields() -> None:
    record = DlqRecord(
        original_topic="t",
        original_partition=1,
        original_offset=2,
        original_timestamp_ms=3,
        original_key_bytes=b"k",
        original_value_bytes=b"v",
        rejected_at_ms=4,
        error_class=ErrorClass.SCHEMA_MISMATCH,
        validator_name="V",
        error_field_path="payload.f",
        error_message="msg",
        validator_version="2.0.0",
    )
    d = serialize_dlq_record(record)
    assert d["original_topic"] == "t"
    assert d["error_class"] == "SCHEMA_MISMATCH"
    assert d["validator_version"] == "2.0.0"
    assert d["error_field_path"] == "payload.f"
    assert d["original_value_bytes"] == b"v"


def test_kafka_timestamp_ms_returns_zero_for_no_timestamp() -> None:
    msg = MagicMock()
    msg.timestamp.return_value = None
    assert _kafka_timestamp_ms(msg) == 0


def test_kafka_timestamp_ms_returns_zero_for_type_zero() -> None:
    msg = MagicMock()
    msg.timestamp.return_value = (0, 999)
    assert _kafka_timestamp_ms(msg) == 0


def test_kafka_timestamp_ms_returns_value_for_valid_type() -> None:
    msg = MagicMock()
    msg.timestamp.return_value = (1, 1_700_000_000_000)
    assert _kafka_timestamp_ms(msg) == 1_700_000_000_000


def test_kafka_key_bytes_returns_none_for_missing() -> None:
    msg = MagicMock()
    msg.key.return_value = None
    assert _kafka_key_bytes(msg) is None


def test_kafka_key_bytes_passes_through_bytes() -> None:
    msg = MagicMock()
    msg.key.return_value = b"abc"
    assert _kafka_key_bytes(msg) == b"abc"


def test_kafka_key_bytes_encodes_str() -> None:
    msg = MagicMock()
    msg.key.return_value = "u-1"
    assert _kafka_key_bytes(msg) == b"u-1"


def test_kafka_key_bytes_coerces_other_types() -> None:
    msg = MagicMock()
    msg.key.return_value = bytearray(b"abc")
    assert _kafka_key_bytes(msg) == b"abc"


def test_load_dlq_schema_str_returns_canonical_json() -> None:
    schema_str = load_dlq_schema_str()
    assert '"DlqRecord"' in schema_str
    assert '"DESERIALIZE_FAILURE"' in schema_str
    assert '"PIPELINE_INTERNAL_ERROR"' in schema_str


def test_dlq_record_from_event_via_invalid_uses_event_metadata() -> None:
    msg = _fake_msg(topic="src", partition=4, offset=7)
    invalid = Invalid(
        error_class=ErrorClass.OUT_OF_RANGE,
        validator_name="PriceRangeValidator",
        error_field_path="payload.price_cents",
        error_message="negative",
    )
    record = DlqRecord.from_raw(
        msg,
        error_class=invalid.error_class,
        validator_name=invalid.validator_name,
        error_field_path=invalid.error_field_path,
        error_message=invalid.error_message,
    )
    assert record.validator_name == "PriceRangeValidator"
    assert record.error_class == ErrorClass.OUT_OF_RANGE
    assert record.error_field_path == "payload.price_cents"
