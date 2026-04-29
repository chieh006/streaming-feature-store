"""Avro-serializing Kafka producer for ``EcommerceEvent`` messages.

This wrapper combines:

* a :class:`confluent_kafka.SerializingProducer`,
* a :class:`confluent_kafka.schema_registry.avro.AvroSerializer` configured to
  fetch (not auto-register) the latest schema for the topic's value subject,
* the project's :class:`EcommerceEvent` Pydantic model for call-site
  validation.

It is intentionally thin: each helper owns a single responsibility (build the
serializer, build the producer, default delivery report) so the code is easy
to test in isolation.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from types import TracebackType
from typing import Optional

from confluent_kafka import KafkaError, Message, SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.schemas import (
    EcommerceEvent,
    SchemaRegistry,
    SCHEMAS_ROOT,
    dump_schema,
    load_schema_set,
)

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA_VERSION_DIR: str = "ecommerce/v1"
DeliveryCallback = Callable[[Optional[KafkaError], Optional[Message]], None]


class AvroEventProducer:
    """Avro-serializing Kafka producer for :class:`EcommerceEvent` messages.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap server configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    topic : str, optional
        Target topic. Defaults to ``kafka_config.topic``.
    schema_version_dir : str, optional
        Subpath under ``schemas/`` containing the ``.avsc`` files to bind to
        the serializer.  Defaults to ``ecommerce/v1``.

    Notes
    -----
    * Uses the default ``TopicNameStrategy``; the value subject is
      ``<topic>-value``.
    * ``auto.register.schemas=False`` — schemas must be pre-registered via
      ``scripts/register_schemas.py``.
    * ``use.latest.version=True`` — serializes against the latest registered
      schema for the subject.
    * Not thread-safe.  Construct one instance per producing thread.
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        registry_config: SchemaRegistryConfig,
        topic: str | None = None,
        schema_version_dir: str = DEFAULT_SCHEMA_VERSION_DIR,
    ) -> None:
        self._kafka_config = kafka_config
        self._registry_config = registry_config
        self._topic = topic or kafka_config.topic
        self._schema_dir = SCHEMAS_ROOT / schema_version_dir
        self._registry = SchemaRegistry(registry_config)
        self._schema_str = dump_schema(load_schema_set(self._schema_dir))
        self._serializer = self._build_serializer()
        self._producer = self._build_producer()
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
    def schema_str(self) -> str:
        """Avro schema JSON string bound to this producer.

        Returns
        -------
        str
            Canonical JSON of the composite schema.
        """
        return self._schema_str

    def _build_serializer(self) -> AvroSerializer:
        """Construct the value :class:`AvroSerializer`.

        Returns
        -------
        AvroSerializer
            Configured to fetch (never register) the latest registered schema
            for the topic's value subject.
        """
        return AvroSerializer(
            schema_registry_client=self._registry.client,
            schema_str=self._schema_str,
            to_dict=_event_to_dict,
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
            Kafka producer with string key serialization and Avro value
            serialization.
        """
        return SerializingProducer(
            {
                "bootstrap.servers": self._kafka_config.bootstrap_servers,
                "security.protocol": self._kafka_config.security_protocol,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": self._serializer,
            }
        )

    @staticmethod
    def _delivery_report(err: KafkaError | None, msg: Message | None) -> None:
        """Default per-message delivery callback.

        Parameters
        ----------
        err : KafkaError or None
            Delivery error, if any.
        msg : Message or None
            The produced message, if available.
        """
        if err is not None:
            logger.warning(f"Delivery failed: {err}")
            return
        if msg is not None:
            logger.debug(
                f"Delivered to {msg.topic()} "
                f"partition={msg.partition()} offset={msg.offset()}"
            )

    def produce(
        self,
        event: EcommerceEvent,
        on_delivery: DeliveryCallback | None = None,
    ) -> None:
        """Validate, serialize, and enqueue an event for delivery.

        Parameters
        ----------
        event : EcommerceEvent
            Pydantic event instance.
        on_delivery : callable, optional
            Per-message delivery callback.  Defaults to a logger-based
            callback.

        Raises
        ------
        TypeError
            If *event* is not an :class:`EcommerceEvent` instance.
        RuntimeError
            If the producer has been closed.
        """
        if self._closed:
            raise RuntimeError("AvroEventProducer is closed")
        if not isinstance(event, EcommerceEvent):
            raise TypeError(
                f"event must be EcommerceEvent, got {type(event).__name__}"
            )
        callback = on_delivery if on_delivery is not None else self._delivery_report
        self._producer.produce(
            topic=self._topic,
            key=event.user_id,
            value=event,
            on_delivery=callback,
        )
        self._producer.poll(0)

    def flush(self, timeout_s: float = 10.0) -> int:
        """Block until all queued messages are delivered or *timeout_s* elapses.

        Parameters
        ----------
        timeout_s : float, optional
            Maximum seconds to wait.  Defaults to 10.0.

        Returns
        -------
        int
            Number of messages still in the queue after the call.
        """
        return self._producer.flush(timeout_s)

    def close(self) -> None:
        """Flush outstanding messages and mark the producer closed.

        Calling :meth:`close` more than once is a no-op.
        """
        if self._closed:
            return
        remaining = self._producer.flush(10.0)
        if remaining:
            logger.warning(f"close(): {remaining} message(s) remain unflushed")
        self._closed = True

    def __enter__(self) -> "AvroEventProducer":
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


def _event_to_dict(event: EcommerceEvent, ctx: object) -> dict:  # noqa: ARG001
    """Adapter passed to :class:`AvroSerializer` as ``to_dict``.

    Parameters
    ----------
    event : EcommerceEvent
        The event handed to ``AvroSerializer.__call__``.
    ctx : object
        Serialization context (unused).

    Returns
    -------
    dict
        Avro-shaped dict produced by :meth:`EcommerceEvent.to_avro_dict`.
    """
    return event.to_avro_dict()


def _build_sample_event(index: int) -> EcommerceEvent:
    """Construct a deterministic sample :class:`EcommerceEvent`.

    Parameters
    ----------
    index : int
        Cycles through the three event types via ``index % 3``.

    Returns
    -------
    EcommerceEvent
        A fully-populated event suitable for ``produce-sample`` smoke runs.
    """
    from datetime import datetime, timezone
    from uuid import uuid4

    from streaming_feature_store.schemas import (
        ClickPayload,
        EventType,
        PageViewPayload,
        PurchasePayload,
    )

    user_id = f"u-{index:04d}"
    payload_choice = index % 3
    payload: ClickPayload | PurchasePayload | PageViewPayload
    if payload_choice == 0:
        event_type = EventType.CLICK
        payload = ClickPayload(element_id="btn-cta", page_url="/home")
    elif payload_choice == 1:
        event_type = EventType.PURCHASE
        payload = PurchasePayload(product_id="sku-123", quantity=1, price_cents=999)
    else:
        event_type = EventType.PAGE_VIEW
        payload = PageViewPayload(page_url="/products", referrer=None)
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=event_type,
        user_id=user_id,
        session_id=f"s-{index:04d}",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=payload,
    )


def _main(argv: list[str] | None = None) -> int:
    """CLI entry point for ``make produce-sample``.

    Parameters
    ----------
    argv : list of str, optional
        Argument vector.  Uses :data:`sys.argv` when ``None``.

    Returns
    -------
    int
        Process exit code.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Send sample EcommerceEvents.")
    parser.add_argument("--sample", type=int, default=5, help="Number of events to send.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    kafka_config = KafkaConfig()
    registry_config = SchemaRegistryConfig()
    with AvroEventProducer(kafka_config, registry_config) as producer:
        for i in range(args.sample):
            event = _build_sample_event(i)
            logger.info(
                f"Producing sample event {i + 1}/{args.sample}: "
                f"{event.event_type.value} user={event.user_id}"
            )
            producer.produce(event)
        remaining = producer.flush()
    logger.info(f"Flushed {args.sample - remaining} message(s) to {kafka_config.topic}")
    return 0 if remaining == 0 else 1


if __name__ == "__main__":  # pragma: no cover - manual smoke-run only
    import sys

    sys.exit(_main())
