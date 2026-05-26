"""Single-process consume → validate → route → produce loop.

The :class:`ValidatorRunner` reads from a source Kafka topic, applies the
:class:`ValidationPipeline` to each decoded :class:`EcommerceEvent`, and
routes the message to either the ``validated-events`` topic (on
:class:`Valid`) or the ``dead-letter-queue`` topic (on :class:`Invalid`).

Loop ordering is strictly *consume → validate → produce → flush →
commit*; a crash between the flush and the commit leaves the message
twice on the destination topic, which downstream consumers absorb via
idempotency keys (``event_id`` UUID for ``validated-events`` and
``f"{topic}:{partition}:{offset}"`` for the DLQ).  Design doc §2.7.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Message
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from streaming_feature_store.consumer.avro_consumer import (
    AvroEventConsumer,
    avro_dict_to_event,
)
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.schemas import EcommerceEvent
from streaming_feature_store.validate.accountant import (
    ValidatorAccountant,
    ValidatorSnapshot,
)
from streaming_feature_store.validate.dlq import (
    DlqProducer,
    DlqRecord,
    ErrorClass,
)
from streaming_feature_store.validate.pipeline import (
    Invalid,
    Valid,
    ValidationPipeline,
)
from streaming_feature_store.validate.report import ValidatorRunReport

logger = logging.getLogger(__name__)

DEFAULT_SOURCE_TOPIC: str = "e-commerce-events-feed"
DEFAULT_VALIDATED_TOPIC: str = "validated-events"
DEFAULT_DLQ_TOPIC: str = "dead-letter-queue"
DEFAULT_GROUP_ID: str = "validator-feed"


class ValidatorRunConfig(BaseModel):
    """Per-run configuration for :class:`ValidatorRunner`.

    Parameters
    ----------
    source_topic : str
        Topic to subscribe to.  Defaults to ``"e-commerce-events-feed"``.
    validated_topic : str
        Output topic for ``Valid`` events.  Defaults to ``"validated-events"``.
    dlq_topic : str
        Output topic for ``Invalid`` events.  Defaults to
        ``"dead-letter-queue"``.
    consumer_group_id : str
        Kafka consumer group id.  Defaults to ``"validator-feed"``.
    poll_timeout_s : float, optional
        Per-poll budget.  Defaults to ``1.0``.
    poll_max_records : int, optional
        Per-poll batch cap.  Defaults to ``500``.
    flush_timeout_s : float, optional
        Maximum seconds to wait for in-flight produces before committing
        consumer offsets.  Defaults to ``5.0``.

    Notes
    -----
    Cross-field validation forbids overlap between the three topics, and
    forbids subscribing to ``validated-events`` as the source (self-loop
    detection).
    """

    model_config = ConfigDict(frozen=True)

    source_topic: str = Field(default=DEFAULT_SOURCE_TOPIC, min_length=1)
    validated_topic: str = Field(default=DEFAULT_VALIDATED_TOPIC, min_length=1)
    dlq_topic: str = Field(default=DEFAULT_DLQ_TOPIC, min_length=1)
    consumer_group_id: str = Field(default=DEFAULT_GROUP_ID, min_length=1)
    poll_timeout_s: float = Field(default=1.0, gt=0.0)
    poll_max_records: int = Field(default=500, ge=1)
    flush_timeout_s: float = Field(default=5.0, gt=0.0)

    @field_validator("validated_topic")
    @classmethod
    def _no_dlq_collision(cls, value: str) -> str:
        """Stub field-validator for early rejection of obvious clashes.

        Parameters
        ----------
        value : str
            Candidate validated-topic name.

        Returns
        -------
        str
            The same value unchanged; cross-field checks run later via
            :meth:`_assert_topic_disjoint`.
        """
        return value

    def _assert_topic_disjoint(self) -> None:
        """Assert that the three configured topics are pairwise distinct.

        Raises
        ------
        ValueError
            If any two of ``source_topic`` / ``validated_topic`` /
            ``dlq_topic`` collide, or if the validator would consume from
            its own output (``source_topic == validated_topic``).
        """
        topics = {
            "source_topic": self.source_topic,
            "validated_topic": self.validated_topic,
            "dlq_topic": self.dlq_topic,
        }
        seen: dict[str, str] = {}
        for label, name in topics.items():
            other = seen.get(name)
            if other is not None:
                raise ValueError(
                    f"{label} and {other} must be distinct topics; "
                    f"both are {name!r}"
                )
            seen[name] = label
        if self.source_topic == self.validated_topic:
            raise ValueError(
                f"source_topic and validated_topic must differ to avoid a "
                f"self-loop; both are {self.source_topic!r}"
            )


class ValidatorRunner:
    """Drive a single consume → validate → route → produce loop.

    Parameters
    ----------
    consumer : AvroEventConsumer
        Pre-constructed Avro consumer.  ``enable.auto.commit`` is enforced
        ``False`` (the consumer's constructor already does this — see
        :class:`AvroEventConsumer`); ``ValidatorRunConfig`` cross-checks
        the configured group id matches.
    validated_producer : AvroEventProducer
        Producer for ``validated-events``.  Re-keys on ``event.user_id``
        so downstream feature compute can do per-user keyed state without
        a repartition (design doc §2.10).
    dlq_producer : DlqProducer
        Producer for ``dead-letter-queue``.  Re-keys on the source
        ``(topic, partition, offset)`` idempotency key.
    pipeline : ValidationPipeline
        Pipeline to apply to each decoded :class:`EcommerceEvent`.
    accountant : ValidatorAccountant
        Counter / latency aggregator.
    config : ValidatorRunConfig
        Per-run knobs.
    validator_version : str, optional
        Semver of the validator catalog, surfaced in every DLQ record.
        Defaults to ``"1.0.0"``.
    clock : callable, optional
        Monotonic clock function.  Defaults to :func:`time.monotonic`.
    """

    def __init__(
        self,
        *,
        consumer: AvroEventConsumer,
        validated_producer: AvroEventProducer,
        dlq_producer: DlqProducer,
        pipeline: ValidationPipeline,
        accountant: ValidatorAccountant,
        config: ValidatorRunConfig,
        validator_version: str = "1.0.0",
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        config._assert_topic_disjoint()
        if validated_producer.topic != config.validated_topic:
            raise ValueError(
                f"validated_producer.topic={validated_producer.topic!r} does "
                f"not match config.validated_topic={config.validated_topic!r}"
            )
        if dlq_producer.topic != config.dlq_topic:
            raise ValueError(
                f"dlq_producer.topic={dlq_producer.topic!r} does not match "
                f"config.dlq_topic={config.dlq_topic!r}"
            )
        self._consumer = consumer
        self._validated_producer = validated_producer
        self._dlq_producer = dlq_producer
        self._pipeline = pipeline
        self._accountant = accountant
        self._config = config
        self._validator_version = validator_version
        self._clock = clock
        self._shutdown = threading.Event()

    @property
    def config(self) -> ValidatorRunConfig:
        """Read-only configuration accessor.

        Returns
        -------
        ValidatorRunConfig
            The configuration supplied at construction.
        """
        return self._config

    def request_shutdown(self) -> None:
        """Signal-handler-safe shutdown request.

        Sets an internal flag the main loop polls at the top of each
        iteration.  No Kafka calls are performed here — ``librdkafka`` is
        not signal-handler-safe (design doc §2.9).
        """
        self._shutdown.set()

    def _decode(self, msg: Message) -> EcommerceEvent | Invalid:
        """Decode a raw Kafka message into an :class:`EcommerceEvent`.

        Parameters
        ----------
        msg : confluent_kafka.Message
            Source message.

        Returns
        -------
        EcommerceEvent or Invalid
            The decoded event on success; an :class:`Invalid` describing
            the failure on schema/value error.
        """
        try:
            return avro_dict_to_event(msg.value())
        except ValidationError as exc:
            field_path = _field_path_from_pydantic(exc)
            return Invalid(
                error_class=ErrorClass.NULL_REQUIRED_FIELD,
                validator_name="PydanticAdapter",
                error_field_path=field_path,
                error_message=str(exc),
            )
        except KafkaException as exc:  # pragma: no cover - rare
            return Invalid(
                error_class=ErrorClass.SCHEMA_MISMATCH,
                validator_name="AvroEventConsumer",
                error_field_path=None,
                error_message=str(exc),
            )
        except Exception as exc:  # noqa: BLE001 - classified below
            return Invalid(
                error_class=ErrorClass.DESERIALIZE_FAILURE,
                validator_name="AvroEventConsumer",
                error_field_path=None,
                error_message=f"{type(exc).__name__}: {exc}",
            )

    def _route_valid(self, event: EcommerceEvent) -> None:
        """Produce *event* to the ``validated-events`` topic.

        Parameters
        ----------
        event : EcommerceEvent
            Event that passed every applicable validator.
        """
        self._validated_producer.produce(event)
        self._accountant.record_valid()

    def _route_invalid(self, msg: Message, reason: Invalid) -> None:
        """Produce a :class:`DlqRecord` for *msg* to the DLQ topic.

        Parameters
        ----------
        msg : confluent_kafka.Message
            The raw source message.
        reason : Invalid
            Pipeline decision describing the rejection.
        """
        record = DlqRecord.from_raw(
            msg,
            error_class=reason.error_class,
            validator_name=reason.validator_name,
            error_field_path=reason.error_field_path,
            error_message=reason.error_message,
            validator_version=self._validator_version,
        )
        self._dlq_producer.send(record)
        self._accountant.record_invalid(
            error_class=reason.error_class,
            validator_name=reason.validator_name,
            error_field_path=reason.error_field_path,
        )

    def _handle_msg(self, msg: Message) -> None:
        """Process one raw message: decode, validate, route, account.

        Parameters
        ----------
        msg : confluent_kafka.Message
            Source message.
        """
        self._accountant.record_consumed(1)
        partition = msg.partition()
        if partition is not None:
            self._accountant.record_partition(int(partition))

        started = self._clock()
        decoded = self._decode(msg)
        if isinstance(decoded, Invalid):
            self._route_invalid(msg, decoded)
        else:
            outcome = self._pipeline.validate(decoded)
            if isinstance(outcome, Valid):
                self._route_valid(outcome.event)
            else:
                self._route_invalid(msg, outcome)
        elapsed_us = (self._clock() - started) * 1_000_000.0
        self._accountant.record_validation_latency_us(elapsed_us)

    def _flush_and_commit(self) -> None:
        """Flush both producers, then commit consumer offsets.

        Notes
        -----
        Ordering is load-bearing: a commit issued before the broker
        acknowledges the produce would risk silent message loss after a
        crash (design doc §2.7).
        """
        cfg = self._config
        self._validated_producer.flush(cfg.flush_timeout_s)
        self._dlq_producer.flush(cfg.flush_timeout_s)
        self._consumer.commit()

    def run(self) -> ValidatorRunReport:
        """Run the loop until :meth:`request_shutdown` is set.

        Returns
        -------
        ValidatorRunReport
            Frozen Pydantic report including the final accountant snapshot.
        """
        cfg = self._config
        started_at = datetime.now(tz=timezone.utc)
        self._consumer.subscribe()
        try:
            while not self._shutdown.is_set():
                messages = self._consumer.poll_batch(
                    cfg.poll_timeout_s, cfg.poll_max_records
                )
                for msg in messages:
                    self._handle_msg(msg)
                if messages:
                    self._flush_and_commit()
            # graceful shutdown: drain producers + commit any pending offsets
            self._flush_and_commit()
        finally:
            self._consumer.close()
            self._validated_producer.close()
            self._dlq_producer.close()
        ended_at = datetime.now(tz=timezone.utc)
        snapshot: ValidatorSnapshot = self._accountant.snapshot()
        report = ValidatorRunReport(
            source_topic=cfg.source_topic,
            validated_topic=cfg.validated_topic,
            dlq_topic=cfg.dlq_topic,
            consumer_group=cfg.consumer_group_id,
            started_at=started_at,
            ended_at=ended_at,
            snapshot=snapshot,
        )
        logger.info(
            f"ValidatorRunner done: consumed={snapshot.consumed} "
            f"validated={snapshot.validated} "
            f"invalid={snapshot.invalid_total} "
            f"invalid_rate={snapshot.invalid_rate * 100.0:.2f}% "
            f"skew={snapshot.partition_skew_ratio:.2f}"
        )
        return report


def _field_path_from_pydantic(exc: ValidationError) -> str | None:
    """Extract a dotted field path from a Pydantic :class:`ValidationError`.

    Parameters
    ----------
    exc : pydantic.ValidationError
        Exception raised by the Pydantic adapter.

    Returns
    -------
    str or None
        Dotted path of the first errored field, or ``None`` when the
        error report contains no usable location.
    """
    errors: list[dict[str, Any]] = exc.errors()
    if not errors:
        return None
    loc = errors[0].get("loc")
    if not loc:
        return None
    return ".".join(str(part) for part in loc)


# Keep KafkaError import alive for downstream extension (typed callbacks).
_ = KafkaError
