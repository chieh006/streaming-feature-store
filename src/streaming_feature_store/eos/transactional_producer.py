"""Single multi-topic transactional producer (design §2.4 / §4.2).

A Kafka transaction is **producer-scoped**: ``begin_transaction`` /
``commit_transaction`` operate on one producer instance with one
``transactional.id``.  Atomicity across *several* output topics (the
validator's ``validated-events`` + ``dead-letter-queue``, or the sliding
consumer's ``sliding-features`` + ``sliding-features-late``) therefore requires
all of them to be produced by **one** producer.

:class:`TransactionalAvroProducer` wraps a single ``confluent_kafka.Producer``
plus a per-topic serializer registry, so heterogeneous Avro value schemas are
multiplexed through one transaction.  The underlying producer is injectable so
the transaction *protocol* is unit-testable without a live broker.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from types import TracebackType

from confluent_kafka import Producer
from pydantic import BaseModel, ConfigDict, Field, model_validator

from streaming_feature_store.config import KafkaConfig

logger = logging.getLogger(__name__)

# A per-topic value serializer: turns a value object into the on-wire bytes.
Serializer = Callable[[object], bytes | None]


class TransactionalConfig(BaseModel):
    """Per-process EOS knob bag (design §3.4).

    Parameters
    ----------
    enabled : bool
        The ``--eos`` master switch.  When ``False`` the loop keeps the
        at-least-once commit contract and this config is otherwise unused.
    transactional_id : str or None
        Stable, unique-per-process ``transactional.id`` (design §2.3).
        **Required** when *enabled* is ``True``.
    group_instance_id : str or None
        librdkafka ``group.instance.id`` for static consumer-group membership,
        which keeps the id ⇄ partition-subset mapping stable across restarts
        (design §2.3).  ``None`` leaves membership dynamic.
    transaction_timeout_ms : int
        librdkafka ``transaction.timeout.ms`` — must exceed the worst-case
        per-batch ``poll → process → produce`` span and stay below the broker's
        ``transaction.max.timeout.ms`` (design §2.8).
    commit_timeout_s : float
        Budget handed to ``commit_transaction`` / ``abort_transaction``.

    Notes
    -----
    ``extra="forbid"`` rejects unknown fields so a typo in a CLI-built config
    fails loudly rather than being silently ignored.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    transactional_id: str | None = None
    group_instance_id: str | None = None
    transaction_timeout_ms: int = Field(default=60_000, ge=1_000)
    commit_timeout_s: float = Field(default=30.0, gt=0.0)

    @model_validator(mode="after")
    def _id_required_when_enabled(self) -> TransactionalConfig:
        """Reject an enabled config that lacks a ``transactional.id``.

        Returns
        -------
        TransactionalConfig
            The validated model.

        Raises
        ------
        ValueError
            When *enabled* is ``True`` but *transactional_id* is unset/blank.
        """
        if self.enabled and not (self.transactional_id or "").strip():
            raise ValueError(
                "transactional_id is required when EOS is enabled"
            )
        return self


def transactional_producer_conf(
    kafka_config: KafkaConfig, txn_config: TransactionalConfig
) -> dict[str, object]:
    """Build the librdkafka conf for a transactional producer (design §4.2).

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap / security settings.
    txn_config : TransactionalConfig
        EOS knobs; ``transactional_id`` must be set.

    Returns
    -------
    dict
        librdkafka config dict.  ``enable.idempotence=true`` and ``acks=all``
        are forced because a transactional producer requires them.

    Raises
    ------
    ValueError
        If *txn_config* has no ``transactional_id``.

    Notes
    -----
    ``group_instance_id`` is **not** set here — it is a *consumer* property
    (static group membership) and belongs on the consumer, not the producer
    (design week2_03 §2.3 / §10.1).
    """
    if not (txn_config.transactional_id or "").strip():
        raise ValueError("transactional_id is required to build a producer conf")
    return {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "security.protocol": kafka_config.security_protocol,
        "enable.idempotence": True,
        "acks": "all",
        "transactional.id": txn_config.transactional_id,
        "transaction.timeout.ms": txn_config.transaction_timeout_ms,
    }


class TransactionalAvroProducer:
    """A single producer that multiplexes several topics in one transaction.

    Parameters
    ----------
    serializers : dict of str to callable
        Per-topic value serializers (``topic -> value_object -> bytes``).  The
        caller wires the registry-backed Avro serializers; the keys define the
        set of topics this producer may write.
    conf : dict, optional
        librdkafka config (see :func:`transactional_producer_conf`).  Used to
        build the underlying ``confluent_kafka.Producer`` when *producer* is
        ``None``.
    producer : object, optional
        Pre-built producer (injected in tests).  Must expose
        ``init_transactions`` / ``begin_transaction`` / ``produce`` /
        ``send_offsets_to_transaction`` / ``commit_transaction`` /
        ``abort_transaction`` / ``flush``.

    Raises
    ------
    ValueError
        If neither *conf* nor *producer* is supplied, or *serializers* is empty.
    """

    def __init__(
        self,
        serializers: dict[str, Serializer],
        *,
        conf: dict[str, object] | None = None,
        producer: object | None = None,
    ) -> None:
        if not serializers:
            raise ValueError("serializers must register at least one topic")
        if producer is None and conf is None:
            raise ValueError("either conf or producer must be supplied")
        self._serializers: dict[str, Serializer] = dict(serializers)
        self._producer = producer if producer is not None else Producer(conf)
        self._initialised = False
        self._closed = False

    @property
    def initialised(self) -> bool:
        """Whether :meth:`init_transactions` has completed.

        Returns
        -------
        bool
            ``True`` once the transactional id is registered with the
            coordinator.
        """
        return self._initialised

    @property
    def topics(self) -> tuple[str, ...]:
        """Topics this producer is allowed to write.

        Returns
        -------
        tuple of str
            The registered serializer keys, sorted.
        """
        return tuple(sorted(self._serializers))

    def init_transactions(self, timeout_s: float = 30.0) -> None:
        """Register the txn id, fence the prior epoch, recover dangling txns.

        Parameters
        ----------
        timeout_s : float, optional
            Budget for the coordinator round-trip.  Defaults to ``30.0``.

        Notes
        -----
        Must run once at startup, before the first :meth:`begin_transaction`
        and the first consumer poll (design §2.3).
        """
        self._producer.init_transactions(timeout_s)
        self._initialised = True

    def begin_transaction(self) -> None:
        """Open a transaction for the next poll-batch (design §2.7)."""
        self._producer.begin_transaction()

    def produce(self, topic: str, key: str, value: object) -> None:
        """Serialize *value* with the topic's serializer and enqueue it.

        Parameters
        ----------
        topic : str
            Destination topic; must be one of the registered serializer keys.
        key : str
            Message key (preserves the existing per-topic keying so
            partitioning is unchanged — design §2.4).
        value : object
            The value object handed to the topic's serializer.

        Raises
        ------
        KeyError
            If *topic* has no registered serializer.
        """
        try:
            serializer = self._serializers[topic]
        except KeyError:
            raise KeyError(
                f"no serializer registered for topic {topic!r}; "
                f"known topics: {self.topics}"
            ) from None
        self._producer.produce(topic=topic, key=key, value=serializer(value))

    def send_offsets_to_transaction(
        self, offsets: object, group_metadata: object
    ) -> None:
        """Bind the consumed input offsets into the open transaction.

        Parameters
        ----------
        offsets : object
            Consumer positions (list of ``TopicPartition``).
        group_metadata : object
            Opaque consumer-group metadata from
            ``consumer.consumer_group_metadata()``.
        """
        self._producer.send_offsets_to_transaction(offsets, group_metadata)

    def commit_transaction(self, timeout_s: float | None = None) -> None:
        """Atomically commit the produced records + the bound offsets.

        Parameters
        ----------
        timeout_s : float or None, optional
            Commit budget.  ``None`` lets librdkafka use its default.
        """
        if timeout_s is None:
            self._producer.commit_transaction()
        else:
            self._producer.commit_transaction(timeout_s)

    def abort_transaction(self, timeout_s: float | None = None) -> None:
        """Discard the open transaction's records and leave offsets unadvanced.

        Parameters
        ----------
        timeout_s : float or None, optional
            Abort budget.  ``None`` lets librdkafka use its default.
        """
        if timeout_s is None:
            self._producer.abort_transaction()
        else:
            self._producer.abort_transaction(timeout_s)

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
        """Flush outstanding messages and mark the producer closed.

        Idempotent: subsequent calls are no-ops.
        """
        if self._closed:
            return
        remaining = self._producer.flush(10.0)
        if remaining:
            logger.warning(
                f"TransactionalAvroProducer.close: {remaining} message(s) "
                f"remain unflushed"
            )
        self._closed = True

    def __enter__(self) -> TransactionalAvroProducer:
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
