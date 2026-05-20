"""Single consumer-group-member drain loop with end-to-end latency.

:class:`ConsumeRunner` is the consume-side counterpart of
:class:`streaming_feature_store.load.load_runner.LoadRunner`.  It wraps the
PR #2 :class:`AvroEventConsumer` with a subscribe → poll → deserialize →
account → manual-commit loop, an end-to-end-latency
:class:`ConsumeAccountant`, and a per-poll lag probe.  One instance is one
group member; horizontal scale comes from spawning *N* members that share a
``group.id`` (see :mod:`streaming_feature_store.consume_mp`).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from confluent_kafka import TIMESTAMP_NOT_AVAILABLE

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consume.accountant import ConsumeAccountant
from streaming_feature_store.consume.report import ConsumeRunConfig, ConsumeRunReport
from streaming_feature_store.consumer.avro_consumer import (
    AvroEventConsumer,
    avro_dict_to_event,
)
from streaming_feature_store.schemas.registry import RegistryError, SchemaRegistry

logger = logging.getLogger(__name__)


class ConsumeRunner:
    """Drive one :class:`AvroEventConsumer` as a single consumer-group member.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    run_config : ConsumeRunConfig
        Per-run knobs (duration, group id, isolation level, ...).
    consumer : AvroEventConsumer, optional
        Pre-constructed consumer; tests inject a mock.  When ``None`` one is
        built from ``run_config``.
    accountant : ConsumeAccountant, optional
        Pre-constructed accountant; tests / the MP worker inject one so they
        can probe the raw latency reservoir afterwards.
    floor_eps : float, optional
        Sustained-rate floor recorded in the report.  Defaults to ``0.0``
        (the verdict is driven by the lag signature, not an absolute rate).
    clock : callable, optional
        Wall-clock source (epoch seconds) for the end-to-end latency term.
        Defaults to :func:`time.time` (the producer's ``CreateTime`` is
        epoch milliseconds, so the difference must use wall clock).
    monotonic : callable, optional
        Monotonic source for the run deadline.  Defaults to
        :func:`time.monotonic`.
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        registry_config: SchemaRegistryConfig,
        run_config: ConsumeRunConfig,
        *,
        consumer: AvroEventConsumer | None = None,
        accountant: ConsumeAccountant | None = None,
        floor_eps: float = 0.0,
        clock=time.time,
        monotonic=time.monotonic,
    ) -> None:
        self._kafka_config = kafka_config
        self._registry_config = registry_config
        self._run_config = run_config
        self._consumer = consumer
        self._accountant = accountant
        self._floor_eps = floor_eps
        self._clock = clock
        self._monotonic = monotonic

    def _assert_subject_registered(self) -> None:
        """Fail-fast if the ``<topic>-value`` subject is not registered.

        Raises
        ------
        RegistryError
            If the registry call fails or the subject is missing.
        """
        subject = f"{self._run_config.topic}-value"
        registry = SchemaRegistry(self._registry_config)
        try:
            latest = registry.get_latest(subject)
        except RegistryError:
            raise
        logger.info(
            f"Subject {subject!r}: registered "
            f"(id={latest.schema_id}, version={latest.version})"
        )

    def _build_components(
        self,
    ) -> tuple[AvroEventConsumer, ConsumeAccountant]:
        """Construct or pass-through the consumer / accountant.

        Returns
        -------
        tuple
            ``(consumer, accountant)``.
        """
        cfg = self._run_config
        consumer = self._consumer or AvroEventConsumer(
            self._kafka_config,
            self._registry_config,
            group_id=cfg.group_id,
            topic=cfg.topic,
            isolation_level=cfg.isolation_level,
        )
        accountant = self._accountant or ConsumeAccountant()
        return consumer, accountant

    def _compute_e2e_s(self, msg) -> float:
        """Return end-to-end latency for *msg* in seconds.

        Parameters
        ----------
        msg : Message
            A polled Kafka message.

        Returns
        -------
        float
            ``consumer_receive_wallclock − msg.timestamp()`` in seconds, or
            ``-1.0`` when the message carries no usable timestamp (the
            accountant treats a negative value as "do not sample").
        """
        tstype, ts_ms = msg.timestamp()
        if tstype == TIMESTAMP_NOT_AVAILABLE or ts_ms is None or ts_ms < 0:
            return -1.0
        return self._clock() - (ts_ms / 1000.0)

    def _process_batch(
        self, messages: list, accountant: ConsumeAccountant
    ) -> None:
        """Account every message in *messages*; deserialize in pydantic mode.

        Parameters
        ----------
        messages : list of Message
            Polled messages.
        accountant : ConsumeAccountant
            Accountant to record into.

        Notes
        -----
        A deserialize / validation failure is *recorded* (at-least-once
        read tolerates it) and the loop continues.  Any other exception
        propagates so the batch is **not** committed (design doc §2.3).
        """
        pydantic_mode = self._run_config.deserialize_mode == "pydantic"
        for msg in messages:
            accountant.record(e2e_latency_s=self._compute_e2e_s(msg))
            if not pydantic_mode:
                continue
            try:
                avro_dict_to_event(msg.value())
            except Exception as exc:  # noqa: BLE001 - classified, not swallowed
                accountant.record_deserialize_error(type(exc).__name__)

    def run(self) -> ConsumeRunReport:
        """Execute the consume run end-to-end.

        Returns
        -------
        ConsumeRunReport
            Per-member result, including the lag verdict.
        """
        self._assert_subject_registered()
        consumer, accountant = self._build_components()
        cfg = self._run_config
        started_at = datetime.now(tz=timezone.utc)
        consumer.subscribe()
        deadline = self._monotonic() + cfg.duration_s
        assigned: list[int] = []
        try:
            polls = 0
            while self._monotonic() < deadline:
                messages = consumer.poll_batch(cfg.poll_timeout_s, cfg.max_batch)
                if messages:
                    self._process_batch(messages, accountant)
                    consumer.commit()
                lag = consumer.consumer_lag()
                accountant.sample_lag(lag)
                polls += 1
                if cfg.until_caught_up and polls >= 1 and lag <= 0:
                    logger.info(
                        f"until_caught_up: lag=0 after {polls} poll(s); "
                        f"ending early"
                    )
                    break
            assigned = consumer.assigned_partitions()
        finally:
            consumer.close()
        snap = accountant.snapshot()
        sustained = snap.consumed / max(snap.wallclock_s, 1e-9)
        report = ConsumeRunReport(
            config=cfg,
            started_at=started_at,
            snapshot=snap,
            sustained_consume_eps=sustained,
            assigned_partitions=assigned,
            floor_eps=self._floor_eps,
        )
        logger.info(
            f"ConsumeRunner done: consumed={snap.consumed} "
            f"deserialize_failed={snap.deserialize_failed} "
            f"sustained={sustained:.0f} evt/s "
            f"max_lag={snap.max_lag} end_lag={snap.end_lag} "
            f"lag_ramped={snap.lag_ramped}"
        )
        return report
