"""Sinks for emitted feature records and very-late events (design §2.8 / §4.4).

Three sinks implement the engine-agnostic sink contract over plain
``redis-py`` + ``confluent-kafka`` (no Flink ``SinkFunction``):

* :class:`RedisHashSink` — one pipelined ``HSET`` + ``EXPIRE`` per emission
  into ``feat:user:{user_id}`` (the online store).
* :class:`KafkaSlidingFeaturesSink` — Avro-serialized produce to
  ``sliding-features`` keyed on ``{user_id}:{resolution}`` (the offline /
  history topic).
* :class:`KafkaLateEventsSink` — produces the *raw* late ``EcommerceEvent`` to
  ``sliding-features-late`` for the Week 4 consistency audit (design doc §2.6).
"""

from __future__ import annotations

import logging
from pathlib import Path
from types import TracebackType

import redis
from confluent_kafka import KafkaError, Message, SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from streaming_feature_store.config import (
    KafkaConfig,
    ProducerTuning,
    SchemaRegistryConfig,
)
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.schemas import (
    SCHEMAS_ROOT,
    EcommerceEvent,
    SchemaRegistry,
)
from streaming_feature_store.schemas.loader import dump_schema, load_avro_file
from streaming_feature_store.sliding.models import (
    SlidingConsumerConfig,
    SlidingFeatureRecord,
)

logger = logging.getLogger(__name__)

SLIDING_SCHEMA_VERSION_DIR: str = "sliding/v1"
SLIDING_SCHEMA_FILENAME: str = "sliding_feature_record.avsc"
DEFAULT_SLIDING_TOPIC: str = "sliding-features"
DEFAULT_LATE_TOPIC: str = "sliding-features-late"


def load_sliding_schema_str(
    schemas_root: Path | None = None,
    version_dir: str = SLIDING_SCHEMA_VERSION_DIR,
    filename: str = SLIDING_SCHEMA_FILENAME,
) -> str:
    """Read the sliding-feature Avro schema and return its canonical JSON.

    Parameters
    ----------
    schemas_root : Path or None, optional
        Override for :data:`SCHEMAS_ROOT`.  Defaults to the project default.
    version_dir : str, optional
        Subpath under *schemas_root* holding the schema.  Defaults to
        ``"sliding/v1"``.
    filename : str, optional
        Avro schema filename.  Defaults to ``"sliding_feature_record.avsc"``.

    Returns
    -------
    str
        Canonical (sort-keys, no-whitespace) JSON form of the schema.
    """
    base = schemas_root if schemas_root is not None else SCHEMAS_ROOT
    schema_dict = load_avro_file(base / version_dir / filename)
    return dump_schema(schema_dict)


class RedisHashSink:
    """Pipelined ``HSET`` + ``EXPIRE`` into ``feat:user:{user_id}`` (design §2.8).

    Parameters
    ----------
    config : SlidingConsumerConfig
        Supplies the Redis host / port and the per-resolution TTL factor.
    client : redis.Redis or None, optional
        Pre-built Redis client (injected in tests).  When ``None`` a client is
        constructed from *config*.

    Notes
    -----
    The TTL is set with a plain ``EXPIRE`` (latest-wins) per the design's §4.4
    sketch.  The first-write-vs-``XX`` nuance and the per-resolution TTL
    interaction are an acknowledged open question (design doc §10.2); a plain
    ``EXPIRE`` keeps the contract simple and is corrected at code-review time
    if the smoke run shows premature 24 h expiry.
    """

    def __init__(
        self, config: SlidingConsumerConfig, client: redis.Redis | None = None
    ) -> None:
        self._config = config
        self._redis = client if client is not None else redis.Redis(
            host=config.redis_host, port=config.redis_port
        )
        self._closed = False

    def write(self, record: SlidingFeatureRecord) -> None:
        """Sink one feature record to its per-user Redis hash.

        Parameters
        ----------
        record : SlidingFeatureRecord
            Record to write.  A record whose features are all ``None`` writes
            nothing (sparsity / null fields, design doc §2.7).
        """
        fields = record.redis_field_updates()
        if not fields:
            return
        key = f"feat:user:{record.user_id}"
        ttl = self._config.ttl_seconds_for(record.window_resolution)
        pipe = self._redis.pipeline()
        pipe.hset(key, mapping=fields)
        pipe.expire(key, ttl)
        pipe.execute()

    def close(self) -> None:
        """Close the underlying Redis client (idempotent)."""
        if self._closed:
            return
        self._redis.close()
        self._closed = True

    def __enter__(self) -> "RedisHashSink":
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


class KafkaSlidingFeaturesSink:
    """Avro-serializing producer for the ``sliding-features`` topic (design §2.8).

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    topic : str, optional
        Destination topic.  Defaults to ``"sliding-features"``.
    tuning : ProducerTuning, optional
        librdkafka knobs.  Defaults to :class:`ProducerTuning` defaults.
    register_schema : bool, optional
        Idempotently register the schema under ``f"{topic}-value"`` on
        construction.  Defaults to ``True``.
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        registry_config: SchemaRegistryConfig,
        topic: str = DEFAULT_SLIDING_TOPIC,
        tuning: ProducerTuning | None = None,
        register_schema: bool = True,
    ) -> None:
        self._kafka_config = kafka_config
        self._registry_config = registry_config
        self._topic = topic
        self._tuning = tuning if tuning is not None else ProducerTuning()
        self._schema_str = load_sliding_schema_str()
        self._registry = SchemaRegistry(registry_config)
        if register_schema:
            self._ensure_schema_registered()
        self._serializer = self._build_serializer()
        self._producer = self._build_producer()
        self._closed = False

    @property
    def topic(self) -> str:
        """Destination topic.

        Returns
        -------
        str
            Topic name.
        """
        return self._topic

    @property
    def schema_str(self) -> str:
        """Avro schema JSON string bound to this sink.

        Returns
        -------
        str
            Canonical JSON of the sliding-feature schema.
        """
        return self._schema_str

    def _ensure_schema_registered(self) -> None:
        """Idempotently register the schema under ``f"{topic}-value"``."""
        subject = f"{self._topic}-value"
        schema_id = self._registry.register(subject, self._schema_str)
        logger.info(
            f"KafkaSlidingFeaturesSink: ensured schema registered "
            f"subject={subject!r} schema_id={schema_id}"
        )

    def _build_serializer(self) -> AvroSerializer:
        """Construct the :class:`AvroSerializer` bound to the schema.

        Returns
        -------
        AvroSerializer
            Configured with ``auto.register.schemas=False`` and
            ``use.latest.version=True``.
        """
        return AvroSerializer(
            schema_registry_client=self._registry.client,
            schema_str=self._schema_str,
            to_dict=_record_to_dict,
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
    def _default_delivery_report(err: KafkaError | None, msg: Message | None) -> None:
        """Default per-message delivery callback.

        Parameters
        ----------
        err : KafkaError or None
            Delivery error, if any.
        msg : Message or None
            The produced message, if available.
        """
        if err is not None:
            logger.warning(f"KafkaSlidingFeaturesSink delivery failed: {err}")

    def write(self, record: SlidingFeatureRecord) -> None:
        """Asynchronously produce *record* to the sliding-features topic.

        Parameters
        ----------
        record : SlidingFeatureRecord
            Record to publish, keyed on ``record.kafka_key()``.

        Raises
        ------
        RuntimeError
            If the sink has been closed.
        """
        if self._closed:
            raise RuntimeError("KafkaSlidingFeaturesSink is closed")
        self._producer.produce(
            topic=self._topic,
            key=record.kafka_key(),
            value=record,
            on_delivery=self._default_delivery_report,
        )
        self._producer.poll(0)

    def flush(self, timeout_s: float = 10.0) -> int:
        """Block until queued messages are delivered or *timeout_s* elapses.

        Parameters
        ----------
        timeout_s : float, optional
            Maximum seconds to wait.  Defaults to ``10.0``.

        Returns
        -------
        int
            Messages still queued after the call.
        """
        return self._producer.flush(timeout_s)

    def close(self) -> None:
        """Flush outstanding messages and mark the sink closed (idempotent)."""
        if self._closed:
            return
        remaining = self._producer.flush(10.0)
        if remaining:
            logger.warning(
                f"KafkaSlidingFeaturesSink.close: {remaining} message(s) unflushed"
            )
        self._closed = True

    def __enter__(self) -> "KafkaSlidingFeaturesSink":
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


class KafkaLateEventsSink:
    """Produces the raw very-late ``EcommerceEvent`` to the late topic (design §2.6).

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    topic : str, optional
        Destination late topic.  Defaults to ``"sliding-features-late"``.

    Notes
    -----
    The late topic carries the *raw event* (not a reduced accumulator) so the
    Week 4 online/offline consistency report can audit late-event impact.  This
    is exactly the existing :class:`AvroEventProducer` contract (Avro
    ``EcommerceEvent`` keyed on ``user_id``), reused verbatim.
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        registry_config: SchemaRegistryConfig,
        topic: str = DEFAULT_LATE_TOPIC,
    ) -> None:
        self._producer = AvroEventProducer(kafka_config, registry_config, topic=topic)
        self._closed = False

    @property
    def topic(self) -> str:
        """Destination late topic.

        Returns
        -------
        str
            Topic name.
        """
        return self._producer.topic

    def write_raw(self, event: EcommerceEvent) -> None:
        """Produce a raw late event to the late topic.

        Parameters
        ----------
        event : EcommerceEvent
            The very-late event to preserve forensically.
        """
        self._producer.produce(event)

    def flush(self, timeout_s: float = 10.0) -> int:
        """Flush the underlying producer.

        Parameters
        ----------
        timeout_s : float, optional
            Maximum seconds to wait.  Defaults to ``10.0``.

        Returns
        -------
        int
            Messages still queued after the call.
        """
        return self._producer.flush(timeout_s)

    def close(self) -> None:
        """Flush and close the underlying producer (idempotent)."""
        if self._closed:
            return
        self._producer.close()
        self._closed = True

    def __enter__(self) -> "KafkaLateEventsSink":
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


def _record_to_dict(record: SlidingFeatureRecord, ctx: object) -> dict:  # noqa: ARG001
    """Adapter passed to :class:`AvroSerializer` as ``to_dict``.

    Parameters
    ----------
    record : SlidingFeatureRecord
        Record handed to ``AvroSerializer.__call__``.
    ctx : object
        Serialization context (unused).

    Returns
    -------
    dict
        Avro-shaped dict from :meth:`SlidingFeatureRecord.to_avro_dict`.
    """
    return record.to_avro_dict()
