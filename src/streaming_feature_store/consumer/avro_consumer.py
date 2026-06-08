"""Avro-deserializing Kafka consumer for ``EcommerceEvent`` messages.

Mirrors :class:`streaming_feature_store.producer.avro_producer.AvroEventProducer`
in shape: each helper owns a single responsibility (build the deserializer,
build the consumer, subscribe and wait for assignment) so the wiring is easy
to test with mocks.

The consumer accepts an explicit ``reader_schema_str`` at construction time.
When provided, the underlying :class:`AvroDeserializer` uses it as the reader
schema, exercising Avro's schema-resolution rules (e.g. promoting an ``int``
written by an older producer to ``long``, dropping fields the new reader does
not know, filling defaults for fields the writer omitted). When ``None``, the
deserializer falls back to the writer schema referenced by the message's
wire-format schema ID.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from types import TracebackType
from typing import Any
from uuid import UUID

from confluent_kafka import OFFSET_INVALID, DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.schemas import (
    ClickPayload,
    EcommerceEvent,
    EventType,
    PageViewPayload,
    PurchasePayload,
    SchemaRegistry,
)

logger = logging.getLogger(__name__)

_PAYLOAD_BY_FQN: dict[str, type] = {
    "com.featurestore.ecommerce.v1.ClickPayload": ClickPayload,
    "com.featurestore.ecommerce.v1.PurchasePayload": PurchasePayload,
    "com.featurestore.ecommerce.v1.PageViewPayload": PageViewPayload,
}

_PAYLOAD_BY_EVENT_TYPE: dict[str, type] = {
    EventType.CLICK.value: ClickPayload,
    EventType.PURCHASE.value: PurchasePayload,
    EventType.PAGE_VIEW.value: PageViewPayload,
}


def _coerce_event_id(value: Any) -> UUID:
    """Coerce a deserialized ``event_id`` value to :class:`uuid.UUID`.

    Parameters
    ----------
    value : Any
        Either a UUID instance (``logicalType=uuid`` decoded by ``fastavro``)
        or a string (older decoders or `int → string` resolutions).

    Returns
    -------
    UUID
        Parsed UUID.
    """
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _coerce_timestamp(value: Any) -> datetime:
    """Coerce a deserialized ``event_timestamp`` value to :class:`datetime`.

    Parameters
    ----------
    value : Any
        Either a timezone-aware datetime (``logicalType=timestamp-micros``
        decoded by ``fastavro``) or an integer microseconds-since-epoch.

    Returns
    -------
    datetime
        UTC-aware datetime.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return datetime.fromtimestamp(int(value) / 1_000_000, tz=UTC)


def _build_payload(d: dict) -> ClickPayload | PurchasePayload | PageViewPayload:
    """Construct the typed payload model from a deserialized dict.

    Parameters
    ----------
    d : dict
        The full deserialized event dict.  ``d["payload"]`` may be a
        ``(fqn, body)`` tuple (when the deserializer is constructed with
        ``return_record_name=True``) or a plain dict (when the discriminator
        is implicit and the consumer falls back on ``event_type``).

    Returns
    -------
    ClickPayload or PurchasePayload or PageViewPayload
        Concrete payload instance.
    """
    raw = d["payload"]
    if isinstance(raw, tuple) and len(raw) == 2:
        fqn, body = raw
        model_cls = _PAYLOAD_BY_FQN[fqn]
        return model_cls(**body)
    model_cls = _PAYLOAD_BY_EVENT_TYPE[d["event_type"]]
    return model_cls(**raw)


def avro_dict_to_event(d: dict) -> EcommerceEvent:
    """Convert an Avro-deserialized dict to a validated :class:`EcommerceEvent`.

    Parameters
    ----------
    d : dict
        Deserialized payload returned by :class:`AvroDeserializer`.

    Returns
    -------
    EcommerceEvent
        Validated Pydantic event instance.
    """
    return EcommerceEvent(
        event_id=_coerce_event_id(d["event_id"]),
        event_type=EventType(d["event_type"]),
        user_id=d["user_id"],
        session_id=d["session_id"],
        event_timestamp=_coerce_timestamp(d["event_timestamp"]),
        payload=_build_payload(d),
    )


class AvroEventConsumer:
    """Avro-deserializing Kafka consumer for :class:`EcommerceEvent` messages.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    group_id : str
        Consumer group ID.  Drills generate per-test group IDs to avoid
        offset bleed-through across runs.
    topic : str, optional
        Target topic.  Defaults to ``kafka_config.topic``.
    reader_schema_str : str or None, optional
        Reader schema for Avro resolution.  When ``None`` the deserializer
        falls back to the writer schema referenced by each message's
        wire-format schema ID.
    auto_offset_reset : str, optional
        Offset reset policy for the consumer group.  Defaults to
        ``"earliest"`` (drills want to read every produced message).
    isolation_level : str, optional
        librdkafka ``isolation.level`` — ``"read_uncommitted"`` (default) or
        ``"read_committed"``.  ``read_committed`` is wired here as a config
        seam for the deferred read-side EOS PR; against the default
        non-transactional producer it only adds last-stable-offset wait and
        is otherwise inert (see design doc §2.7).
    group_instance_id : str or None, optional
        librdkafka ``group.instance.id``.  When set, the consumer joins as a
        **static member**: on restart within ``session.timeout.ms`` it reclaims
        its previous partition assignment without triggering a rebalance,
        keeping the EOS ``transactional.id`` ⇄ partition mapping stable across
        restarts (design week2_03 §2.3 / §10.1).  Must be unique per live
        member of the group.  ``None`` (default) leaves membership dynamic.
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        registry_config: SchemaRegistryConfig,
        group_id: str,
        topic: str | None = None,
        reader_schema_str: str | None = None,
        auto_offset_reset: str = "earliest",
        isolation_level: str = "read_uncommitted",
        group_instance_id: str | None = None,
    ) -> None:
        self._kafka_config = kafka_config
        self._registry_config = registry_config
        self._topic = topic or kafka_config.topic
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._isolation_level = isolation_level
        self._group_instance_id = group_instance_id
        self._reader_schema_str = reader_schema_str
        self._registry = SchemaRegistry(registry_config)
        self._deserializer = self._build_deserializer()
        self._consumer = self._build_consumer()
        self._subscribed = False
        self._closed = False

    @property
    def topic(self) -> str:
        """Target Kafka topic.

        Returns
        -------
        str
            Topic name.
        """
        return self._topic

    @property
    def reader_schema_str(self) -> str | None:
        """Reader schema string passed at construction.

        Returns
        -------
        str or None
            Reader schema; ``None`` if writer = reader.
        """
        return self._reader_schema_str

    @property
    def isolation_level(self) -> str:
        """Configured librdkafka ``isolation.level``.

        Returns
        -------
        str
            ``"read_uncommitted"`` or ``"read_committed"``.
        """
        return self._isolation_level

    def _build_deserializer(self) -> AvroDeserializer:
        """Construct the value :class:`AvroDeserializer`.

        Returns
        -------
        AvroDeserializer
            Configured with the optional reader schema and a ``from_dict``
            adapter that returns the dict unchanged (Pydantic validation
            happens in :meth:`consume`, not inside the deserializer).
        """
        return AvroDeserializer(
            schema_registry_client=self._registry.client,
            schema_str=self._reader_schema_str,
            from_dict=_passthrough_from_dict,
            return_record_name=True,
        )

    def _build_consumer(self) -> DeserializingConsumer:
        """Construct the underlying :class:`DeserializingConsumer`.

        Returns
        -------
        DeserializingConsumer
            Kafka consumer with string key deserialization and Avro value
            deserialization.
        """
        conf: dict[str, object] = {
            "bootstrap.servers": self._kafka_config.bootstrap_servers,
            "security.protocol": self._kafka_config.security_protocol,
            "group.id": self._group_id,
            "auto.offset.reset": self._auto_offset_reset,
            "enable.auto.commit": False,
            "isolation.level": self._isolation_level,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": self._deserializer,
        }
        if self._group_instance_id is not None:
            conf["group.instance.id"] = self._group_instance_id
        return DeserializingConsumer(conf)

    def _ensure_subscribed(self) -> None:
        """Subscribe to the configured topic exactly once.

        Notes
        -----
        Partition assignment happens lazily on the first poll; callers do not
        block here.
        """
        if not self._subscribed:
            self._consumer.subscribe([self._topic])
            self._subscribed = True

    def _poll_messages(
        self, timeout_s: float, max_messages: int
    ) -> list[Any]:
        """Poll the underlying consumer until the budget is exhausted.

        Parameters
        ----------
        timeout_s : float
            Wall-clock budget.
        max_messages : int
            Maximum number of successful messages to collect.

        Returns
        -------
        list of Message
            Messages in delivery order.

        Raises
        ------
        RuntimeError
            On any non-EOF Kafka error surfaced by ``msg.error()``.
        """
        deadline = time.monotonic() + timeout_s
        collected: list[Any] = []
        while len(collected) < max_messages and time.monotonic() < deadline:
            remaining = max(0.0, deadline - time.monotonic())
            msg = self._consumer.poll(timeout=min(1.0, remaining))
            if msg is None:
                continue
            err = msg.error()
            if err is not None:
                raise RuntimeError(f"Consumer error: {err}")
            collected.append(msg)
        return collected

    def consume(
        self, timeout_s: float = 10.0, max_messages: int = 100
    ) -> list[EcommerceEvent]:
        """Poll, deserialize, and Pydantic-validate up to *max_messages*.

        Parameters
        ----------
        timeout_s : float, optional
            Wall-clock budget for the poll loop.  Defaults to 10 seconds.
        max_messages : int, optional
            Maximum number of messages to return.  Defaults to 100.

        Returns
        -------
        list of EcommerceEvent
            Validated event instances in delivery order.
        """
        self._ensure_subscribed()
        messages = self._poll_messages(timeout_s, max_messages)
        return [avro_dict_to_event(msg.value()) for msg in messages]

    def consume_raw(
        self, timeout_s: float = 10.0, max_messages: int = 100
    ) -> list[dict]:
        """Poll and deserialize but skip Pydantic validation.

        Parameters
        ----------
        timeout_s : float, optional
            Wall-clock budget for the poll loop.
        max_messages : int, optional
            Maximum number of messages to return.

        Returns
        -------
        list of dict
            Raw deserialized dicts (with payload as ``(fqn, body)`` tuple).
        """
        self._ensure_subscribed()
        messages = self._poll_messages(timeout_s, max_messages)
        return [msg.value() for msg in messages]

    def subscribe(self) -> None:
        """Subscribe to the configured topic (group-managed assignment).

        Idempotent: the underlying ``subscribe`` call is issued exactly
        once.  Partition assignment is performed lazily by the broker on
        the first :meth:`poll_batch`.
        """
        self._ensure_subscribed()

    def poll_batch(
        self, timeout_s: float = 1.0, max_messages: int = 1024
    ) -> list[Any]:
        """Poll up to *max_messages* raw Kafka messages.

        Unlike :meth:`consume`, this returns the raw ``Message`` objects so
        the caller can read per-message metadata (``timestamp()``,
        ``partition()``) and decide how to deserialize.  Used by
        :class:`~streaming_feature_store.consume.consume_runner.ConsumeRunner`
        to measure end-to-end latency.

        Parameters
        ----------
        timeout_s : float, optional
            Wall-clock budget for the poll loop.  Defaults to ``1.0``.
        max_messages : int, optional
            Maximum number of messages to collect.  Defaults to ``1024``.

        Returns
        -------
        list of Message
            Messages in delivery order (possibly empty).
        """
        self._ensure_subscribed()
        return self._poll_messages(timeout_s, max_messages)

    def commit(self) -> None:
        """Synchronously commit the current consume position.

        Notes
        -----
        ``enable.auto.commit`` is ``False``; the runner calls this once per
        fully-processed batch so a crash / rebalance resumes from the last
        *processed* offset (at-least-once read — design doc §2.3).
        """
        self._consumer.commit(asynchronous=False)

    def assigned_partitions(self) -> list[int]:
        """Return the partition numbers currently assigned to this member.

        Returns
        -------
        list of int
            Sorted partition ids; empty before the first poll / assignment.
        """
        return sorted(tp.partition for tp in self._consumer.assignment())

    def assignment(self) -> list[Any]:
        """Return the raw ``TopicPartition`` list currently assigned.

        Returns
        -------
        list of TopicPartition
            The assignment as returned by librdkafka; empty before the first
            poll / assignment.  Needed to bind offsets into a transaction
            (design week2_03 §2.4 / §4.4).
        """
        return self._consumer.assignment()

    def position(self, partitions: list[Any]) -> list[Any]:
        """Return the current consume positions for *partitions*.

        Parameters
        ----------
        partitions : list of TopicPartition
            Partitions to query (typically :meth:`assignment`).

        Returns
        -------
        list of TopicPartition
            Each carries the next offset to be consumed — the offsets a
            transactional producer binds via
            ``send_offsets_to_transaction`` (design week2_03 §4.4).
        """
        return self._consumer.position(partitions)

    def consumer_group_metadata(self) -> Any:
        """Return the opaque consumer-group metadata for transactional commits.

        Returns
        -------
        object
            Group metadata required by
            ``producer.send_offsets_to_transaction`` so the offset commit
            joins the producer's transaction (design week2_03 §2.4 / §4.4).
        """
        return self._consumer.consumer_group_metadata()

    def consumer_lag(self) -> int:
        """Return total lag = Σ ``(high_watermark − position)`` over assignment.

        Returns
        -------
        int
            Sum of per-partition lag across all assigned partitions.  ``0``
            when no partitions are assigned yet or every position is still
            invalid (no fetch has happened).  Never negative.

        Notes
        -----
        Lag is the consumer's primary health metric (design doc §2.5): a
        flat series means the group keeps up; a monotonically rising series
        is the "consumer slower than producer" collapse.
        """
        assignment = self._consumer.assignment()
        if not assignment:
            return 0
        positions = self._consumer.position(assignment)
        pos_by_tp = {
            (tp.topic, tp.partition): tp.offset for tp in positions
        }
        total = 0
        for tp in assignment:
            _, high = self._consumer.get_watermark_offsets(
                tp, timeout=5.0, cached=False
            )
            pos = pos_by_tp.get((tp.topic, tp.partition), OFFSET_INVALID)
            if pos is None or pos < 0 or high is None or high < 0:
                continue
            total += max(0, high - pos)
        return total

    def close(self) -> None:
        """Close the underlying consumer.

        Idempotent: subsequent calls are no-ops.
        """
        if self._closed:
            return
        self._consumer.close()
        self._closed = True

    def __enter__(self) -> AvroEventConsumer:
        """Return ``self`` for use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close on context-manager exit."""
        self.close()


def _passthrough_from_dict(d: dict, ctx: object) -> dict:  # noqa: ARG001
    """``from_dict`` adapter that returns the deserialized dict unchanged.

    Parameters
    ----------
    d : dict
        Deserialized Avro record.
    ctx : object
        Serialization context (unused).

    Returns
    -------
    dict
        The same dict.
    """
    return d
