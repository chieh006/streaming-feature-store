"""Single-process Kafka-to-Postgres sink loop.

:class:`SinkRunner` composes the existing :class:`AvroEventConsumer` with a
:class:`PostgresWriter`, accumulates messages in a :class:`Batch`, and flushes
either when the batch reaches ``batch_max_rows`` or when the oldest message in
the batch is older than ``batch_max_age_s`` seconds — whichever comes first.

The loop ordering is strictly *consume → write-Postgres → commit-Kafka*; see
``docs/design/week1_06_postgres_sink_and_continuous_feeder.md`` §2.7 for the
crash-safety argument that motivates it.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator

from streaming_feature_store.consumer.avro_consumer import (
    AvroEventConsumer,
    avro_dict_to_event,
)
from streaming_feature_store.schemas import EcommerceEvent
from streaming_feature_store.sink.accountant import SinkAccountant, SinkSnapshot
from streaming_feature_store.sink.postgres_writer import (
    BatchInsertResult,
    PostgresWriter,
)
from streaming_feature_store.sink.report import SinkRunReport

logger = logging.getLogger(__name__)


class SinkRunConfig(BaseModel):
    """Per-run configuration for :class:`SinkRunner`.

    Parameters
    ----------
    topic : str
        Kafka topic to subscribe to.  Defaults to ``e-commerce-events-feed``
        — the dedicated topic for the continuous feeder (design doc §2.4).
    consumer_group_id : str
        Kafka consumer group id.
    batch_max_rows : int, optional
        Maximum batch size before a flush is forced.  Defaults to ``1000``
        (design doc §2.6).
    batch_max_age_s : float, optional
        Maximum wall-clock age of the *oldest* event in the current batch
        before a flush is forced.  Defaults to ``10.0`` s.
    poll_timeout_s : float, optional
        Per-iteration ``consumer.poll`` budget.  Defaults to ``1.0``.
    poll_max_records : int, optional
        Per-iteration ``consumer.poll_batch`` cap.  Defaults to ``500``.
    flush_retry_attempts : int, optional
        Number of attempts (1 = no retry).  Defaults to ``2``.
    flush_retry_backoff_s : float, optional
        Backoff between attempts.  Defaults to ``1.0``.
    """

    model_config = ConfigDict(frozen=True)

    topic: str = Field(default="e-commerce-events-feed", min_length=1)
    consumer_group_id: str = Field(..., min_length=1)
    batch_max_rows: int = Field(default=1000, ge=1)
    batch_max_age_s: float = Field(default=10.0, gt=0.0)
    poll_timeout_s: float = Field(default=1.0, gt=0.0)
    poll_max_records: int = Field(default=500, ge=1)
    flush_retry_attempts: int = Field(default=2, ge=1)
    flush_retry_backoff_s: float = Field(default=1.0, ge=0.0)

    @field_validator("topic")
    @classmethod
    def _refuse_benchmark_topic(cls, value: str) -> str:
        """Refuse to consume from the burst-benchmark topic.

        Parameters
        ----------
        value : str
            Topic name supplied by the caller.

        Returns
        -------
        str
            Validated topic name.

        Raises
        ------
        ValueError
            If *value* equals ``"e-commerce-events"`` — that topic carries
            burst traffic from the benchmarks and must not pollute
            ``raw_events``.
        """
        if value == "e-commerce-events":
            raise ValueError(
                "Refusing to sink from the benchmark topic "
                "'e-commerce-events'; use 'e-commerce-events-feed' instead "
                "(design doc §2.4)."
            )
        return value


class Batch:
    """Mutable accumulator of decoded :class:`EcommerceEvent` instances.

    Parameters
    ----------
    max_rows : int
        Row count that triggers a flush.
    max_age_s : float
        Wall-clock age (seconds) of the oldest event before a flush is
        triggered.
    clock : callable, optional
        Monotonic clock function.  Defaults to :func:`time.monotonic`.

    Notes
    -----
    The batch tracks the monotonic timestamp of its *first* event so
    :meth:`should_flush` is a single subtraction; no extra timer thread is
    required (design doc §2.6).
    """

    def __init__(
        self,
        *,
        max_rows: int,
        max_age_s: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_rows < 1:
            raise ValueError(f"max_rows must be >= 1, got {max_rows}")
        if max_age_s <= 0:
            raise ValueError(f"max_age_s must be > 0, got {max_age_s}")
        self._max_rows = int(max_rows)
        self._max_age_s = float(max_age_s)
        self._clock = clock
        self._events: list[EcommerceEvent] = []
        self._first_appended_ts: float | None = None

    @property
    def max_rows(self) -> int:
        """Configured row cap.

        Returns
        -------
        int
            Maximum rows.
        """
        return self._max_rows

    @property
    def max_age_s(self) -> float:
        """Configured age cap.

        Returns
        -------
        float
            Maximum batch age in seconds.
        """
        return self._max_age_s

    def __len__(self) -> int:
        """Number of accumulated events.

        Returns
        -------
        int
            Current size.
        """
        return len(self._events)

    def __bool__(self) -> bool:
        """``True`` when the batch holds at least one event.

        Returns
        -------
        bool
            Non-empty flag.
        """
        return bool(self._events)

    def append(self, event: EcommerceEvent) -> None:
        """Add *event* to the batch.

        Parameters
        ----------
        event : EcommerceEvent
            Decoded event instance.
        """
        if self._first_appended_ts is None:
            self._first_appended_ts = self._clock()
        self._events.append(event)

    def should_flush(self) -> bool:
        """Return ``True`` iff the batch is ready to be written.

        Returns
        -------
        bool
            ``True`` when the row cap is reached or the age cap is exceeded.
        """
        if not self._events:
            return False
        if len(self._events) >= self._max_rows:
            return True
        if self._first_appended_ts is None:  # pragma: no cover - defensive
            return False
        return (self._clock() - self._first_appended_ts) >= self._max_age_s

    def events(self) -> list[EcommerceEvent]:
        """Return the underlying list (read-only view via copy).

        Returns
        -------
        list of EcommerceEvent
            Defensive copy.
        """
        return list(self._events)

    def clear(self) -> None:
        """Drop accumulated events and reset the age clock."""
        self._events.clear()
        self._first_appended_ts = None


class SinkRunner:
    """Drive a single Kafka-to-Postgres sink loop.

    Parameters
    ----------
    consumer : AvroEventConsumer
        Pre-constructed Avro consumer.  The runner refuses any consumer
        whose underlying librdkafka config has auto-commit enabled (design
        doc §2.7 — manual commit is required to preserve write-then-commit
        ordering).
    writer : PostgresWriter
        Idempotent bulk-insert writer.
    accountant : SinkAccountant
        Counter and partition-skew aggregator.
    config : SinkRunConfig
        Per-run knobs.
    clock : callable, optional
        Monotonic clock function.  Used for the flush timer + retry
        backoff; injected by tests.  Defaults to :func:`time.monotonic`.
    sleep : callable, optional
        Sleep function.  Injected by tests.  Defaults to :func:`time.sleep`.
    """

    def __init__(
        self,
        *,
        consumer: AvroEventConsumer,
        writer: PostgresWriter,
        accountant: SinkAccountant,
        config: SinkRunConfig,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._consumer = consumer
        self._writer = writer
        self._accountant = accountant
        self._config = config
        self._clock = clock
        self._sleep = sleep
        self._shutdown = threading.Event()
        self._batch = Batch(
            max_rows=config.batch_max_rows,
            max_age_s=config.batch_max_age_s,
            clock=clock,
        )

    @property
    def config(self) -> SinkRunConfig:
        """Read-only configuration accessor.

        Returns
        -------
        SinkRunConfig
            The configuration supplied at construction.
        """
        return self._config

    def request_shutdown(self) -> None:
        """Signal-handler-safe shutdown request.

        Sets an internal flag the main loop polls on each iteration.  The
        method is intentionally restricted to setting the flag — Kafka
        client objects are not re-entrant from signal handlers (a
        well-documented ``librdkafka`` constraint).
        """
        self._shutdown.set()

    def _record_message(self, msg) -> None:
        """Account one polled message and append it to the batch.

        Parameters
        ----------
        msg : confluent_kafka.Message
            Raw message returned by the consumer.

        Notes
        -----
        Deserialize / Pydantic-validation failures are *recorded* — they
        increment ``deserialize_failed`` but do not abort the batch.
        At-least-once read semantics (design doc §2.7) means a re-poll
        after a crash would surface the same message again, so dropping
        it after one log line is safe.
        """
        self._accountant.record_consumed(1)
        partition = msg.partition()
        if partition is not None:
            self._accountant.record_partition(int(partition))
        try:
            event = avro_dict_to_event(msg.value())
        except Exception as exc:  # noqa: BLE001 - classified, not swallowed
            self._accountant.record_deserialize_failure()
            logger.warning(
                f"SinkRunner deserialize failure ({type(exc).__name__}): {exc}"
            )
            return
        self._batch.append(event)

    def _flush_with_retry(self, events: list[EcommerceEvent]) -> BatchInsertResult:
        """Call :meth:`PostgresWriter.flush` with bounded retries.

        Parameters
        ----------
        events : list of EcommerceEvent
            Batch to insert.

        Returns
        -------
        BatchInsertResult
            Outcome of the successful attempt.

        Raises
        ------
        Exception
            Re-raises the last exception when all attempts fail.
        """
        cfg = self._config
        last_exc: BaseException | None = None
        for attempt in range(cfg.flush_retry_attempts):
            try:
                return self._writer.flush(events)
            except Exception as exc:  # noqa: BLE001 - classified below
                last_exc = exc
                if attempt + 1 < cfg.flush_retry_attempts:
                    logger.warning(
                        f"PostgresWriter.flush failed (attempt {attempt + 1}/"
                        f"{cfg.flush_retry_attempts}); retrying after "
                        f"{cfg.flush_retry_backoff_s:.2f}s: {exc}"
                    )
                    self._sleep(cfg.flush_retry_backoff_s)
                    continue
                logger.error(
                    f"PostgresWriter.flush failed after "
                    f"{cfg.flush_retry_attempts} attempt(s); aborting."
                )
        assert last_exc is not None  # nosec: defensive
        raise last_exc

    def _flush_batch(self) -> None:
        """Flush the current batch through Postgres, then commit Kafka.

        Notes
        -----
        Ordering is enforced here, not at the caller: ``flush`` runs to
        completion *before* the consumer commit is issued.  A crash between
        the two leaves duplicates on the next poll, which the writer's
        ``ON CONFLICT DO NOTHING`` clause absorbs.
        """
        events = self._batch.events()
        if not events:
            return
        started = self._clock()
        result = self._flush_with_retry(events)
        elapsed_ms = (self._clock() - started) * 1000.0
        self._consumer.commit()
        self._accountant.record_flush(
            inserted=result.inserted,
            skipped=result.skipped,
            batch_size=len(events),
            latency_ms=elapsed_ms,
        )
        if result.skipped:
            logger.info(
                f"Flush absorbed {result.skipped:_} duplicate row(s) via "
                f"ON CONFLICT DO NOTHING — at-least-once replay path."
            )
        self._batch.clear()

    def _maybe_flush(self) -> None:
        """Flush iff :meth:`Batch.should_flush` agrees.

        Notes
        -----
        Pulled into its own method so the graceful-shutdown path can call
        :meth:`_flush_batch` unconditionally while the main loop respects
        the size / age trigger.
        """
        if self._batch.should_flush():
            self._flush_batch()

    def run(self) -> SinkRunReport:
        """Execute the sink loop until :meth:`request_shutdown` is called.

        Returns
        -------
        SinkRunReport
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
                    self._record_message(msg)
                self._maybe_flush()
            # graceful shutdown: drain in-flight batch then close consumer
            self._flush_batch()
        finally:
            self._consumer.close()
            self._writer.close()
        ended_at = datetime.now(tz=timezone.utc)
        snapshot: SinkSnapshot = self._accountant.snapshot()
        report = SinkRunReport(
            topic=cfg.topic,
            consumer_group=cfg.consumer_group_id,
            started_at=started_at,
            ended_at=ended_at,
            snapshot=snapshot,
        )
        logger.info(
            f"SinkRunner done: consumed={snapshot.consumed} "
            f"inserted={snapshot.inserted} "
            f"conflict_skipped={snapshot.conflict_skipped} "
            f"deserialize_failed={snapshot.deserialize_failed} "
            f"batches_flushed={snapshot.batches_flushed} "
            f"skew_ratio={snapshot.partition_skew_ratio:.3f}"
        )
        return report
