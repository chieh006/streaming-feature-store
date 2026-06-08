"""The consume → window → emit main loop (design doc §3.1 / §4.5).

:class:`SlidingFeaturesConsumer` reads ``validated-events`` with a plain
``confluent_kafka`` consumer, folds each event into the in-memory
:class:`SlidingWindowManager`, advances the :class:`WatermarkTracker`, and on
every batch / idle tick emits the windows the watermark has crossed to the
Redis and Kafka sinks.  Fault tolerance is at-least-once consumption +
idempotent writes + a bounded cold-start warm-up (design doc §2.10); rebalances
drop and rebuild per-partition user state (design doc §2.12).

Collaborators are dependency-injected so the loop logic is unit-testable
without a live broker; the real ``DeserializingConsumer`` is built lazily from
config when none is supplied.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import BaseModel, ConfigDict

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consumer.avro_consumer import (
    _passthrough_from_dict,
    avro_dict_to_event,
)
from streaming_feature_store.eos import (
    TransactionalAvroProducer,
    TransactionalConfig,
    requires_abort,
)
from streaming_feature_store.schemas import SchemaRegistry
from streaming_feature_store.sliding.models import (
    SlidingConsumerConfig,
    WindowResolution,
    event_timestamp_ms,
)
from streaming_feature_store.sliding.panes import SlidingWindowManager
from streaming_feature_store.sliding.sinks import (
    KafkaLateEventsSink,
    KafkaSlidingFeaturesSink,
    RedisHashSink,
)
from streaming_feature_store.sliding.watermark import WatermarkTracker

logger = logging.getLogger(__name__)

# Event-time lookback used to rebuild 5 m / 1 h pane state on assignment; the
# 1 h window subsumes the 5 m one, and 24 h warms over wall-clock (design §2.10).
_WARMUP_LOOKBACK_MS: int = WindowResolution.W_1H_SLIDE_5M.window_size_ms

# Per-transaction poll-batch cap under EOS (design week2_03 §2.7 — one
# transaction per poll-batch, never per message).
_EOS_MAX_RECORDS: int = 500


def _now_ms() -> int:
    """Return the current wall-clock time in milliseconds since the epoch.

    Returns
    -------
    int
        ``time.time() × 1000`` truncated to an int.
    """
    return int(time.time() * 1000)


class SlidingRunSnapshot(BaseModel):
    """Lightweight end-of-run summary for the results report.

    Parameters
    ----------
    consumed : int
        Number of source events folded into pane state.
    late : int
        Number of very-late events routed to the late sink.
    emitted_by_resolution : dict of str to int
        Emission count keyed by resolution value (``"5m"`` / ``"1h"`` / ``"24h"``).
    active_users : int
        Distinct active users with live pane state at shutdown.
    """

    model_config = ConfigDict(frozen=True)

    consumed: int
    late: int
    emitted_by_resolution: dict[str, int]
    active_users: int


class SlidingFeaturesConsumer:
    """Consume → window → emit loop for sliding-window features.

    Parameters
    ----------
    config : SlidingConsumerConfig
        Runtime configuration.
    consumer : confluent_kafka.Consumer or None, optional
        Pre-built consumer (injected in tests).  Built from *config* when
        ``None``.
    manager : SlidingWindowManager or None, optional
        Window-state manager.  Built from *config* when ``None``.
    watermark : WatermarkTracker or None, optional
        Watermark tracker.  Built from *config* when ``None``.
    redis_sink, kafka_sink, late_sink : sink or None, optional
        Output sinks.  Built from *config* / *kafka_config* / *registry_config*
        when ``None``.
    kafka_config, registry_config : config or None, optional
        Connection settings used when building the real consumer / sinks.
    now_ms : callable, optional
        Wall-clock millisecond clock (injected for tests).  Defaults to
        :func:`_now_ms`.
    txn_producer : TransactionalAvroProducer or None, optional
        When supplied, switches the consumer to **transactional EOS** (design
        week2_03 §2.4): the ``sliding-features`` and ``sliding-features-late``
        writes plus the input-offset commit are produced through this single
        transactional producer per poll-batch, and Redis is written only after
        the transaction commits (§2.6).  ``None`` keeps the at-least-once path.
    txn_config : TransactionalConfig or None, optional
        EOS knobs (commit timeout) used when *txn_producer* is set.

    Notes
    -----
    Under EOS the standalone ``kafka_sink`` / ``late_sink`` are not built — both
    Kafka writes are multiplexed through *txn_producer* so they commit
    atomically with the offset advance.
    """

    def __init__(
        self,
        config: SlidingConsumerConfig,
        *,
        consumer: Consumer | None = None,
        manager: SlidingWindowManager | None = None,
        watermark: WatermarkTracker | None = None,
        redis_sink: RedisHashSink | None = None,
        kafka_sink: KafkaSlidingFeaturesSink | None = None,
        late_sink: KafkaLateEventsSink | None = None,
        kafka_config: KafkaConfig | None = None,
        registry_config: SchemaRegistryConfig | None = None,
        now_ms: Callable[[], int] = _now_ms,
        txn_producer: TransactionalAvroProducer | None = None,
        txn_config: TransactionalConfig | None = None,
    ) -> None:
        self._config = config
        self._kafka_config = kafka_config or KafkaConfig(
            bootstrap_servers=config.bootstrap
        )
        self._registry_config = registry_config or SchemaRegistryConfig(
            url=config.registry_url
        )
        self._now_ms = now_ms
        # EOS fields are set before building the consumer so _build_consumer can
        # apply static membership (group.instance.id) under EOS.
        self._txn_producer = txn_producer
        self._txn_config = txn_config
        self._eos = txn_producer is not None
        self._commit_timeout_s = (
            txn_config.commit_timeout_s if txn_config is not None else 30.0
        )
        self._consumer = consumer if consumer is not None else self._build_consumer()
        self._manager = manager if manager is not None else SlidingWindowManager(
            allowed_lateness_ms=config.allowed_lateness_seconds * 1000
        )
        self._watermark = watermark if watermark is not None else WatermarkTracker(
            out_of_orderness_ms=config.out_of_orderness_seconds * 1000,
            idleness_ms=config.idleness_seconds * 1000,
        )
        self._redis_sink = redis_sink if redis_sink is not None else RedisHashSink(
            config
        )
        # Under EOS the two Kafka writes go through the single transactional
        # producer (§2.4), so the standalone feature/late sinks are not built;
        # they remain the at-least-once path's producers otherwise.
        if self._eos:
            self._kafka_sink = kafka_sink
            self._late_sink = late_sink
        else:
            self._kafka_sink = kafka_sink if kafka_sink is not None else (
                KafkaSlidingFeaturesSink(
                    self._kafka_config, self._registry_config, topic=config.sink_topic
                )
            )
            self._late_sink = late_sink if late_sink is not None else (
                KafkaLateEventsSink(
                    self._kafka_config,
                    self._registry_config,
                    topic=config.late_sink_topic,
                )
            )
        self._shutdown = threading.Event()
        self._consumed = 0
        self._late = 0
        self._emitted: dict[WindowResolution, int] = dict.fromkeys(WindowResolution, 0)

    def _build_consumer(self) -> Consumer:
        """Construct the underlying Avro-deserializing :class:`Consumer`.

        Returns
        -------
        confluent_kafka.Consumer
            Consumer with manual offset commit and ``latest`` reset (warm-up
            seek-back overrides the start position on assignment, design §2.10).
        """
        registry = SchemaRegistry(self._registry_config)
        self._deserializer = AvroDeserializer(
            schema_registry_client=registry.client,
            from_dict=_passthrough_from_dict,
            return_record_name=True,
        )
        conf: dict[str, object] = {
            "bootstrap.servers": self._kafka_config.bootstrap_servers,
            "security.protocol": self._kafka_config.security_protocol,
            "group.id": self._config.consumer_group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            # validated-events is produced transactionally under EOS, so read
            # only past the LSO and drop aborted records (week2_03 §2.5).
            "isolation.level": self._config.isolation_level,
        }
        if self._eos and self._txn_config and self._txn_config.group_instance_id:
            # Static membership keeps transactional.id ⇄ partition stable across
            # restarts (week2_03 §2.3 / §10.1).
            conf["group.instance.id"] = self._txn_config.group_instance_id
        return Consumer(conf)

    def request_shutdown(self) -> None:
        """Signal-handler-safe shutdown request.

        Sets an internal flag polled at the top of each loop iteration; no
        Kafka calls happen here (``librdkafka`` is not signal-handler-safe).
        """
        self._shutdown.set()

    def _decode(self, msg: Message):
        """Decode a raw Kafka message into an :class:`EcommerceEvent`.

        Parameters
        ----------
        msg : confluent_kafka.Message
            Source message with an Avro-deserialized dict value.

        Returns
        -------
        EcommerceEvent
            Validated event instance.
        """
        value = msg.value()
        if isinstance(value, (bytes, bytearray)):
            ctx = SerializationContext(msg.topic(), MessageField.VALUE)
            value = self._deserializer(bytes(value), ctx)
        return avro_dict_to_event(value)

    def _fold_message(self, msg: Message):
        """Decode and fold one message into pane state; return any late event.

        Parameters
        ----------
        msg : confluent_kafka.Message
            Source message.

        Returns
        -------
        EcommerceEvent or None
            The very-late event the manager rejected from its panes (for the
            caller to route to the late sink), or ``None``.
        """
        event = self._decode(msg)
        ts_ms = event_timestamp_ms(event)
        self._watermark.observe(ts_ms)
        watermark = self._watermark.watermark_ms(self._now_ms())
        return self._manager.add(event, ts_ms, watermark, msg.partition())

    def _handle_message(self, msg: Message) -> None:
        """Fold one message into pane state, routing very-late events aside.

        Parameters
        ----------
        msg : confluent_kafka.Message
            Source message.
        """
        late = self._fold_message(msg)
        self._consumed += 1
        if late is not None:
            self._late_sink.write_raw(late)
            self._late += 1

    def _emit_and_sink(self) -> None:
        """Emit windows the watermark has crossed and write them to the sinks."""
        watermark = self._watermark.watermark_ms(self._now_ms())
        if watermark is None:
            return
        for record in self._manager.emit_due_windows(watermark):
            self._redis_sink.write(record)
            self._kafka_sink.write(record)
            self._emitted[record.window_resolution] += 1

    def _poll_once(self) -> None:
        """Run one poll → handle → emit → commit iteration (at-least-once)."""
        msg = self._consumer.poll(self._config.poll_timeout_seconds)
        if msg is not None and msg.error() is None:
            self._handle_message(msg)
        self._emit_and_sink()
        self._consumer.commit(asynchronous=True)

    def _collect_batch(self) -> list[Message]:
        """Poll up to ``_EOS_MAX_RECORDS`` error-free messages for one txn.

        Returns
        -------
        list of confluent_kafka.Message
            The per-transaction poll-batch (possibly empty); design
            week2_03 §2.7 — one transaction per batch, never per message.
        """
        batch: list[Message] = []
        msg = self._consumer.poll(self._config.poll_timeout_seconds)
        while msg is not None:
            if msg.error() is None:
                batch.append(msg)
            if len(batch) >= _EOS_MAX_RECORDS:
                break
            msg = self._consumer.poll(0)
        return batch

    def _poll_batch_eos(self) -> None:
        """Run one transactional consume → window → emit → commit batch.

        Folds the batch into pane state, materializes the windows the watermark
        has crossed, and — if there is anything to write or any offset to
        advance — produces the late events + feature records through the single
        transactional producer, binds the input offsets into the transaction,
        and commits.  Redis is written only **after** the commit (design
        week2_03 §2.6); counters advance post-commit so an abort leaves them
        untouched.
        """
        batch = self._collect_batch()
        lates = [
            late
            for late in (self._fold_message(msg) for msg in batch)
            if late is not None
        ]
        watermark = self._watermark.watermark_ms(self._now_ms())
        records = (
            list(self._manager.emit_due_windows(watermark))
            if watermark is not None
            else []
        )
        if not batch and not records:
            return  # nothing written and no offset advanced — open no txn
        self._commit_batch_txn(lates, records)
        for record in records:
            self._redis_sink.write(record)
            self._emitted[record.window_resolution] += 1
        self._consumed += len(batch)
        self._late += len(lates)

    def _commit_batch_txn(self, lates: list, records: list) -> None:
        """Produce the batch's Kafka writes + offsets atomically (design §2.4).

        Parameters
        ----------
        lates : list of EcommerceEvent
            Very-late raw events for ``sliding-features-late``.
        records : list of SlidingFeatureRecord
            Emitted feature records for ``sliding-features``.

        Raises
        ------
        confluent_kafka.KafkaException
            On any transactional error.  An *abortable* error aborts the
            transaction and re-raises so the process exits and cold-starts
            cleanly: re-folding the batch into the **live** in-memory panes
            would double-count, so this stateful consumer treats an abort as a
            mini-restart rather than an in-process retry (design week2_03
            §2.8 / §2.10).
        """
        self._txn_producer.begin_transaction()
        try:
            for late in lates:
                self._txn_producer.produce(
                    self._config.late_sink_topic, late.user_id, late
                )
            for record in records:
                self._txn_producer.produce(
                    self._config.sink_topic, record.kafka_key(), record
                )
            self._txn_producer.send_offsets_to_transaction(
                self._consumer.position(self._consumer.assignment()),
                self._consumer.consumer_group_metadata(),
            )
            self._txn_producer.commit_transaction(self._commit_timeout_s)
        except KafkaException as exc:
            if requires_abort(exc):
                logger.warning(
                    f"sliding EOS transaction aborted; exiting for a clean "
                    f"cold-start (in-process replay would double-count): {exc}"
                )
                self._txn_producer.abort_transaction(self._commit_timeout_s)
            raise

    def run(self) -> SlidingRunSnapshot:
        """Run the loop until :meth:`request_shutdown` is set.

        Returns
        -------
        SlidingRunSnapshot
            End-of-run counters for the results report.
        """
        if self._eos:
            self._txn_producer.init_transactions()
        self._consumer.subscribe(
            [self._config.source_topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )
        try:
            while not self._shutdown.is_set():
                if self._eos:
                    self._poll_batch_eos()
                else:
                    self._poll_once()
        finally:
            self._shutdown_sinks()
        return self.snapshot()

    def _shutdown_sinks(self) -> None:
        """Flush sinks/producer, commit final offsets, and close resources.

        Under EOS offsets are committed transactionally per batch, so the final
        plain ``consumer.commit()`` is skipped (a non-transactional commit would
        be wrong) and the single transactional producer is flushed/closed in
        place of the standalone sinks.
        """
        if self._eos:
            self._txn_producer.flush()
            self._consumer.close()
            self._txn_producer.close()
            self._redis_sink.close()
            return
        self._kafka_sink.flush()
        self._late_sink.flush()
        try:
            self._consumer.commit(asynchronous=False)
        except Exception as exc:  # noqa: BLE001 - best-effort final commit
            logger.warning(f"final commit failed: {exc}")
        self._consumer.close()
        self._kafka_sink.close()
        self._late_sink.close()
        self._redis_sink.close()

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Rebalance ``on_assign`` callback (design doc §2.10 / §2.12).

        Parameters
        ----------
        consumer : confluent_kafka.Consumer
            The consumer being assigned partitions.
        partitions : list of TopicPartition
            Newly assigned partitions (offsets default to the reset policy).
        """
        logger.info(
            f"assigned partitions: {sorted(tp.partition for tp in partitions)}"
        )
        if self._config.warmup_seek_back and partitions:
            self._apply_warmup_seek_back(consumer, partitions)
        consumer.assign(partitions)

    def _apply_warmup_seek_back(
        self, consumer: Consumer, partitions: list[TopicPartition]
    ) -> None:
        """Rewind assigned partitions one window of event-time (design §2.10).

        Parameters
        ----------
        consumer : confluent_kafka.Consumer
            The consumer whose offsets are being rewound.
        partitions : list of TopicPartition
            Partitions to rewind; their ``offset`` is mutated in place to the
            offset of ``now − 1 h``.
        """
        target_ms = self._now_ms() - _WARMUP_LOOKBACK_MS
        lookups = [TopicPartition(tp.topic, tp.partition, target_ms) for tp in partitions]
        try:
            resolved = consumer.offsets_for_times(lookups)
        except Exception as exc:  # noqa: BLE001 - warm-up is best-effort
            logger.warning(f"warm-up seek-back skipped (offsets_for_times): {exc}")
            return
        by_partition = {tp.partition: tp.offset for tp in resolved}
        for tp in partitions:
            offset = by_partition.get(tp.partition)
            if offset is not None and offset >= 0:
                tp.offset = offset

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Rebalance ``on_revoke`` callback: drop revoked partitions' state.

        Parameters
        ----------
        consumer : confluent_kafka.Consumer
            The consumer losing partitions.
        partitions : list of TopicPartition
            Partitions being revoked.
        """
        revoked = {tp.partition for tp in partitions}
        dropped = self._manager.drop_partitions(revoked)
        logger.info(f"revoked partitions {sorted(revoked)}; dropped {dropped} users")

    def snapshot(self) -> SlidingRunSnapshot:
        """Return a snapshot of the run counters.

        Returns
        -------
        SlidingRunSnapshot
            Current consumed / late / per-resolution-emitted / active-user
            counts.
        """
        return SlidingRunSnapshot(
            consumed=self._consumed,
            late=self._late,
            emitted_by_resolution={
                res.value: count for res, count in self._emitted.items()
            },
            active_users=self._manager.active_user_count,
        )
