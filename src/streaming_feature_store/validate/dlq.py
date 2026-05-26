"""Dead-letter-queue envelope (:class:`DlqRecord`) and :class:`DlqProducer`.

Rejected messages are republished to the ``dead-letter-queue`` topic as
Avro-serialized :class:`DlqRecord` envelopes that preserve the original
raw bytes verbatim (design doc §2.3 / §2.7).  The Pydantic model is the
in-process representation; :func:`serialize_dlq_record` converts it to
the Avro dict shape expected by ``AvroSerializer``.

The DLQ topic uses a stable idempotency key
``f"{topic}:{partition}:{offset}"`` so forensic readers can deduplicate at
will and replays of the same source offset produce a record with the
identical key.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Any

from confluent_kafka import KafkaError, Message, SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from pydantic import BaseModel, ConfigDict, Field

from streaming_feature_store.config import (
    KafkaConfig,
    ProducerTuning,
    SchemaRegistryConfig,
)
from streaming_feature_store.schemas import SCHEMAS_ROOT, SchemaRegistry
from streaming_feature_store.schemas.loader import dump_schema, load_avro_file

logger = logging.getLogger(__name__)

DLQ_SCHEMA_VERSION_DIR: str = "dlq/v1"
DLQ_SCHEMA_FILENAME: str = "dead_letter_record.avsc"
DEFAULT_DLQ_TOPIC: str = "dead-letter-queue"
DEFAULT_VALIDATOR_VERSION: str = "1.0.0"

DlqDeliveryCallback = Callable[[KafkaError | None, Message | None], None]


class ErrorClass(str, Enum):
    """Coarse error bucket for rejected messages.

    Mirrors the ``ErrorClass`` enum inside the DLQ Avro schema.  The enum
    is **append-only** under ``BACKWARD`` compatibility — adding a new
    symbol is safe; removing or reordering symbols is not (design doc
    §2.3).
    """

    DESERIALIZE_FAILURE = "DESERIALIZE_FAILURE"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    NULL_REQUIRED_FIELD = "NULL_REQUIRED_FIELD"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    MALFORMED_RECORD = "MALFORMED_RECORD"
    UNKNOWN_EVENT_TYPE = "UNKNOWN_EVENT_TYPE"
    PIPELINE_INTERNAL_ERROR = "PIPELINE_INTERNAL_ERROR"


class DlqRecord(BaseModel):
    """In-process Pydantic mirror of the Avro :class:`DlqRecord` envelope.

    Parameters
    ----------
    original_topic : str
        Topic the rejected message was consumed from.
    original_partition : int
        Kafka partition id.
    original_offset : int
        Kafka offset.
    original_timestamp_ms : int
        Broker-side timestamp in milliseconds since the Unix epoch.
    original_key_bytes : bytes or None
        Original message key bytes (may be ``None`` for keyless producers).
    original_value_bytes : bytes
        Original message value bytes, verbatim.
    rejected_at_ms : int
        Wall-clock time of rejection in milliseconds since the Unix epoch.
    error_class : ErrorClass
        Coarse error bucket.
    validator_name : str
        Name of the validator (or upstream component) that produced the
        rejection.
    error_field_path : str or None
        Dotted path of the offending field; ``None`` when not field-scoped.
    error_message : str
        Human-readable explanation.
    schema_version : int, optional
        Literal version-int.  Defaults to ``1``.
    validator_version : str, optional
        Semver of the validator catalog that produced the rejection.
        Defaults to :data:`DEFAULT_VALIDATOR_VERSION`.
    """

    model_config = ConfigDict(frozen=True)

    original_topic: str = Field(..., min_length=1)
    original_partition: int = Field(..., ge=0)
    original_offset: int = Field(..., ge=0)
    original_timestamp_ms: int
    original_key_bytes: bytes | None = None
    original_value_bytes: bytes
    rejected_at_ms: int
    error_class: ErrorClass
    validator_name: str = Field(..., min_length=1)
    error_field_path: str | None = None
    error_message: str
    schema_version: int = Field(default=1, ge=1)
    validator_version: str = Field(default=DEFAULT_VALIDATOR_VERSION, min_length=1)

    def idempotency_key(self) -> str:
        """Stable Kafka message key for the DLQ produce.

        Returns
        -------
        str
            ``"{original_topic}:{original_partition}:{original_offset}"``.
            Replays of the same source offset produce a record with the
            identical key, so forensic readers can dedupe at-will (design
            doc §2.7).
        """
        return f"{self.original_topic}:{self.original_partition}:{self.original_offset}"

    @classmethod
    def from_raw(
        cls,
        msg: Message,
        error_class: ErrorClass,
        validator_name: str,
        error_field_path: str | None,
        error_message: str,
        *,
        validator_version: str = DEFAULT_VALIDATOR_VERSION,
        rejected_at_ms: int | None = None,
    ) -> "DlqRecord":
        """Build a :class:`DlqRecord` from a raw Kafka :class:`Message`.

        Parameters
        ----------
        msg : confluent_kafka.Message
            The source message (already failed deserialization / validation).
        error_class : ErrorClass
            Coarse error bucket.
        validator_name : str
            Producing validator / component.
        error_field_path : str or None
            Dotted path of the offending field; ``None`` when not
            field-scoped.
        error_message : str
            Human-readable explanation.
        validator_version : str, optional
            Semver of the validator catalog.  Defaults to
            :data:`DEFAULT_VALIDATOR_VERSION`.
        rejected_at_ms : int or None, optional
            Override for the rejection wall-clock timestamp; defaults to
            :func:`time.time` × 1000.

        Returns
        -------
        DlqRecord
            Pydantic envelope ready to serialize.
        """
        rejected = (
            rejected_at_ms
            if rejected_at_ms is not None
            else int(time.time() * 1000)
        )
        ts = _kafka_timestamp_ms(msg)
        key_bytes = _kafka_key_bytes(msg)
        value_bytes = _coerce_value_bytes(msg.value())
        return cls(
            original_topic=msg.topic() or "",
            original_partition=int(msg.partition() or 0),
            original_offset=int(msg.offset() or 0),
            original_timestamp_ms=ts,
            original_key_bytes=key_bytes,
            original_value_bytes=value_bytes,
            rejected_at_ms=rejected,
            error_class=error_class,
            validator_name=validator_name,
            error_field_path=error_field_path,
            error_message=error_message,
            validator_version=validator_version,
        )


def _coerce_value_bytes(value: Any) -> bytes:
    """Coerce a Kafka :meth:`Message.value` payload into raw bytes.

    Parameters
    ----------
    value : Any
        Message value as returned by ``msg.value()``.  May be:

        * ``bytes`` / ``bytearray`` — the raw on-wire payload (returned by
          a non-deserializing consumer; the ideal case for forensic
          replay).
        * ``dict`` — the post-decode Avro dict returned by
          ``DeserializingConsumer``; serialized to a UTF-8 JSON string and
          encoded.  Note: this loses fidelity relative to the original
          wire bytes (the Avro magic byte + schema id prefix is gone),
          but preserves enough of the message to drive forensic
          investigation.
        * ``None`` — empty payload; encoded as ``b""``.
        * Anything else — ``str(value).encode("utf-8")`` as a fallback.

    Returns
    -------
    bytes
        Bytes suitable for the DLQ envelope's ``original_value_bytes``
        field.
    """
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, dict):
        try:
            return json.dumps(value, default=_json_default, sort_keys=True).encode(
                "utf-8"
            )
        except (TypeError, ValueError):  # pragma: no cover - extremely rare
            return repr(value).encode("utf-8")
    return str(value).encode("utf-8")


def _json_default(obj: Any) -> Any:
    """JSON-encode helper for non-JSONable Avro-decoded values.

    Parameters
    ----------
    obj : Any
        Object encountered during ``json.dumps``.

    Returns
    -------
    Any
        A JSON-encodable representation: ``bytes`` rendered as a UTF-8
        string with surrogate escaping, tuples rendered as lists,
        everything else falls back to ``repr``.
    """
    if isinstance(obj, (bytes, bytearray)):
        return obj.decode("utf-8", errors="backslashreplace")
    if isinstance(obj, tuple):
        return list(obj)
    return repr(obj)


def _kafka_timestamp_ms(msg: Message) -> int:
    """Extract the broker-side timestamp from a Kafka :class:`Message`.

    Parameters
    ----------
    msg : confluent_kafka.Message
        Source message.

    Returns
    -------
    int
        Timestamp in milliseconds since the Unix epoch; ``0`` when the
        message has no timestamp.
    """
    ts_info = msg.timestamp()
    if not ts_info:
        return 0
    # confluent-kafka returns (type, ts_ms); type can be 0 (no-timestamp).
    ts_type, ts_ms = ts_info
    if ts_type == 0:
        return 0
    return int(ts_ms)


def _kafka_key_bytes(msg: Message) -> bytes | None:
    """Return the raw key bytes for a Kafka :class:`Message`.

    Parameters
    ----------
    msg : confluent_kafka.Message
        Source message.

    Returns
    -------
    bytes or None
        Raw key bytes; ``None`` when the message has no key.
    """
    key = msg.key()
    if key is None:
        return None
    if isinstance(key, (bytes, bytearray)):
        return bytes(key)
    if isinstance(key, str):
        return key.encode("utf-8")
    return bytes(key)


def serialize_dlq_record(record: DlqRecord) -> dict:
    """Convert a :class:`DlqRecord` to the Avro-shaped dict.

    Parameters
    ----------
    record : DlqRecord
        Pydantic envelope.

    Returns
    -------
    dict
        Dict shape accepted by ``AvroSerializer.to_dict``.
    """
    return {
        "schema_version": record.schema_version,
        "original_topic": record.original_topic,
        "original_partition": record.original_partition,
        "original_offset": record.original_offset,
        "original_timestamp_ms": record.original_timestamp_ms,
        "original_key_bytes": record.original_key_bytes,
        "original_value_bytes": record.original_value_bytes,
        "rejected_at_ms": record.rejected_at_ms,
        "error_class": record.error_class.value,
        "validator_name": record.validator_name,
        "error_field_path": record.error_field_path,
        "error_message": record.error_message,
        "validator_version": record.validator_version,
    }


def load_dlq_schema_str(
    schemas_root: Path | None = None,
    version_dir: str = DLQ_SCHEMA_VERSION_DIR,
    filename: str = DLQ_SCHEMA_FILENAME,
) -> str:
    """Read the DLQ Avro schema file and return its canonical JSON string.

    Parameters
    ----------
    schemas_root : Path or None, optional
        Override for :data:`SCHEMAS_ROOT`.  Defaults to ``None`` (use the
        project default).
    version_dir : str, optional
        Subpath under *schemas_root* containing the DLQ schema.  Defaults
        to ``"dlq/v1"``.
    filename : str, optional
        Avro schema filename.  Defaults to ``"dead_letter_record.avsc"``.

    Returns
    -------
    str
        Canonical (sort-keys, no-whitespace) JSON form of the schema,
        suitable for :class:`AvroSerializer` and
        :meth:`SchemaRegistry.register`.
    """
    base = schemas_root if schemas_root is not None else SCHEMAS_ROOT
    schema_path = base / version_dir / filename
    schema_dict = load_avro_file(schema_path)
    return dump_schema(schema_dict)


class DlqProducer:
    """Avro-serializing Kafka producer for :class:`DlqRecord` envelopes.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    topic : str, optional
        Destination DLQ topic.  Defaults to ``"dead-letter-queue"``.
    schema_version_dir : str, optional
        Subpath under ``schemas/`` containing the DLQ ``.avsc`` file.
        Defaults to :data:`DLQ_SCHEMA_VERSION_DIR` (``"dlq/v1"``).
    schema_filename : str, optional
        Avro schema filename.  Defaults to :data:`DLQ_SCHEMA_FILENAME`.
    tuning : ProducerTuning, optional
        Throughput-oriented librdkafka knobs.  Defaults to
        :class:`ProducerTuning` defaults.
    register_schema : bool, optional
        Idempotently register the DLQ schema under ``f"{topic}-value"`` on
        construction.  Defaults to ``True``.

    Notes
    -----
    The DLQ topic is *not* compaction-keyed; the idempotency key is used
    purely for partitioning and forensic dedup, not for log compaction
    (design doc §2.10).
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        registry_config: SchemaRegistryConfig,
        topic: str = DEFAULT_DLQ_TOPIC,
        schema_version_dir: str = DLQ_SCHEMA_VERSION_DIR,
        schema_filename: str = DLQ_SCHEMA_FILENAME,
        tuning: ProducerTuning | None = None,
        register_schema: bool = True,
    ) -> None:
        self._kafka_config = kafka_config
        self._registry_config = registry_config
        self._topic = topic
        self._tuning = tuning if tuning is not None else ProducerTuning()
        self._schema_str = load_dlq_schema_str(
            version_dir=schema_version_dir, filename=schema_filename
        )
        self._registry = SchemaRegistry(registry_config)
        if register_schema:
            self._ensure_schema_registered()
        self._serializer = self._build_serializer()
        self._producer = self._build_producer()
        self._closed = False

    @property
    def topic(self) -> str:
        """Destination DLQ topic.

        Returns
        -------
        str
            Topic name.
        """
        return self._topic

    @property
    def schema_str(self) -> str:
        """Avro schema JSON string bound to this producer.

        Returns
        -------
        str
            Canonical JSON of the DLQ schema.
        """
        return self._schema_str

    def _ensure_schema_registered(self) -> None:
        """Idempotently register the DLQ schema under ``f"{topic}-value"``."""
        subject = f"{self._topic}-value"
        schema_id = self._registry.register(subject, self._schema_str)
        logger.info(
            f"DlqProducer: ensured DLQ schema registered "
            f"subject={subject!r} schema_id={schema_id}"
        )

    def _build_serializer(self) -> AvroSerializer:
        """Construct the :class:`AvroSerializer` bound to the DLQ schema.

        Returns
        -------
        AvroSerializer
            Configured with ``auto.register.schemas=False`` and
            ``use.latest.version=True``.
        """
        return AvroSerializer(
            schema_registry_client=self._registry.client,
            schema_str=self._schema_str,
            to_dict=_dlq_to_dict,
            conf={
                "auto.register.schemas": False,
                "use.latest.version": True,
            },
        )

    def _build_producer(self) -> SerializingProducer:
        """Construct the underlying :class:`SerializingProducer`.

        Returns
        -------
        SerializingProducer
            Kafka producer with string-key + Avro-value serialization.
        """
        conf: dict[str, object] = {
            "bootstrap.servers": self._kafka_config.bootstrap_servers,
            "security.protocol": self._kafka_config.security_protocol,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": self._serializer,
        }
        conf.update(self._tuning.as_librdkafka_conf())
        return SerializingProducer(conf)

    @staticmethod
    def _default_delivery_report(
        err: KafkaError | None, msg: Message | None
    ) -> None:
        """Default per-message delivery callback.

        Parameters
        ----------
        err : KafkaError or None
            Delivery error, if any.
        msg : Message or None
            The produced message, if available.
        """
        if err is not None:
            logger.warning(f"DlqProducer delivery failed: {err}")
            return
        if msg is not None:
            logger.debug(
                f"DlqProducer delivered to {msg.topic()} "
                f"partition={msg.partition()} offset={msg.offset()}"
            )

    def send(
        self,
        record: DlqRecord,
        on_delivery: DlqDeliveryCallback | None = None,
    ) -> None:
        """Asynchronously produce *record* to the DLQ topic.

        Parameters
        ----------
        record : DlqRecord
            Envelope to publish.
        on_delivery : callable, optional
            Per-message delivery callback.  Defaults to a logger-based
            callback.

        Raises
        ------
        RuntimeError
            If the producer has been closed.
        """
        if self._closed:
            raise RuntimeError("DlqProducer is closed")
        callback = (
            on_delivery if on_delivery is not None else self._default_delivery_report
        )
        self._producer.produce(
            topic=self._topic,
            key=record.idempotency_key(),
            value=record,
            on_delivery=callback,
        )
        self._producer.poll(0)

    def poll(self, timeout_s: float = 0.0) -> int:
        """Drive the delivery-callback pump.

        Parameters
        ----------
        timeout_s : float, optional
            Maximum seconds to block.  Defaults to ``0.0`` (non-blocking).

        Returns
        -------
        int
            Number of events served.
        """
        return self._producer.poll(timeout_s)

    def flush(self, timeout_s: float = 10.0) -> int:
        """Block until all queued messages are delivered or timeout elapses.

        Parameters
        ----------
        timeout_s : float, optional
            Maximum seconds to wait.  Defaults to ``10.0``.

        Returns
        -------
        int
            Number of messages still in the queue after the call.
        """
        return self._producer.flush(timeout_s)

    def close(self) -> None:
        """Flush outstanding messages and mark the producer closed.

        Idempotent: subsequent calls are no-ops.
        """
        if self._closed:
            return
        remaining = self._producer.flush(10.0)
        if remaining:
            logger.warning(
                f"DlqProducer.close: {remaining} message(s) remain unflushed"
            )
        self._closed = True

    def __enter__(self) -> "DlqProducer":
        """Return ``self`` for use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Flush and close on context-manager exit."""
        self.close()


def _dlq_to_dict(record: DlqRecord, ctx: object) -> dict:  # noqa: ARG001
    """Adapter passed to :class:`AvroSerializer` as ``to_dict``.

    Parameters
    ----------
    record : DlqRecord
        Pydantic envelope.
    ctx : object
        Serialization context (unused).

    Returns
    -------
    dict
        Avro-shaped dict produced by :func:`serialize_dlq_record`.
    """
    return serialize_dlq_record(record)
