"""Single-process, rate-paced, long-running event feeder daemon.

The feeder is a thin wrapper around :class:`AvroEventProducer`,
:class:`SyntheticEventGenerator`, and :class:`TokenBucketPacer`.  It runs
until :meth:`FeederRunner.request_shutdown` is called by a signal handler
(``SIGTERM`` / ``SIGINT``), at which point the producer is flushed and a
final snapshot is returned.

Design doc: ``docs/design/week1_06_postgres_sink_and_continuous_feeder.md``
§§ 2.4 (separate topic), 2.5 (default rate), 2.9 (graceful shutdown).
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator

from streaming_feature_store.load.accountant import (
    AccountantSnapshot,
    DeliveryAccountant,
)
from streaming_feature_store.load.pacer import TokenBucketPacer
from streaming_feature_store.load.synthetic import SyntheticEventGenerator
from streaming_feature_store.producer.avro_producer import AvroEventProducer

logger = logging.getLogger(__name__)


class FeederRunConfig(BaseModel):
    """Per-run configuration for :class:`FeederRunner`.

    Parameters
    ----------
    topic : str
        Destination Kafka topic.  Defaults to ``e-commerce-events-feed``
        (design doc §2.4).
    rate_evt_per_sec : float
        Target produce rate; must be ``> 0``.  Defaults to ``200.0``
        (design doc §2.5).
    batch_size : int, optional
        Per-iteration generator batch size.  Defaults to ``200``.
    seed : int, optional
        Synthetic generator seed.  Defaults to ``42``.
    snapshot_interval_s : float, optional
        Cadence for the long-run heartbeat log line.  Defaults to ``60.0``
        seconds (design doc §4.4 — periodic flush of intermediate metrics).
    """

    model_config = ConfigDict(frozen=True)

    topic: str = Field(default="e-commerce-events-feed", min_length=1)
    rate_evt_per_sec: float = Field(default=200.0, gt=0.0)
    batch_size: int = Field(default=200, ge=1)
    seed: int = Field(default=42)
    snapshot_interval_s: float = Field(default=60.0, gt=0.0)

    @field_validator("topic")
    @classmethod
    def _refuse_benchmark_topic(cls, value: str) -> str:
        """Refuse to feed into the burst-benchmark topic.

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
            If *value* equals ``"e-commerce-events"`` — that topic is owned
            by the load-test benchmarks and must not receive the feeder's
            steady trickle (design doc §2.4 / §4.4).
        """
        if value == "e-commerce-events":
            raise ValueError(
                "Refusing to feed into the benchmark topic "
                "'e-commerce-events'; use 'e-commerce-events-feed' instead "
                "(design doc §2.4)."
            )
        return value


class FeederSnapshot(BaseModel):
    """Immutable end-of-run snapshot of the feeder daemon.

    Parameters
    ----------
    started_at : datetime
        Wall-clock start time (UTC).
    ended_at : datetime
        Wall-clock end time (UTC).
    topic : str
        Destination topic.
    rate_evt_per_sec : float
        Target rate the feeder was paced at.
    delivery : AccountantSnapshot
        Final :class:`DeliveryAccountant` snapshot (produced / acked /
        failed / in_flight / latency percentiles).
    """

    model_config = ConfigDict(frozen=True)

    started_at: datetime
    ended_at: datetime
    topic: str
    rate_evt_per_sec: float
    delivery: AccountantSnapshot

    @property
    def duration_s(self) -> float:
        """Wall-clock duration in seconds.

        Returns
        -------
        float
            ``(ended_at - started_at).total_seconds()``.
        """
        return (self.ended_at - self.started_at).total_seconds()


class FeederRunner:
    """Drive a continuous, rate-paced event feeder.

    Parameters
    ----------
    config : FeederRunConfig
        Per-run knobs.
    producer : AvroEventProducer
        Pre-constructed producer; the runner does not own the producer's
        topic, so the caller is free to configure tuning / EOS profile.
    generator : SyntheticEventGenerator
        Pre-constructed generator (so tests can inject a deterministic
        fake).
    pacer : TokenBucketPacer
        Rate-limiter shared across the main loop iterations.
    accountant : DeliveryAccountant
        Aggregator for the producer's on-delivery callback.
    clock : callable, optional
        Monotonic clock function.  Injected by tests.  Defaults to
        :func:`time.monotonic`.

    Notes
    -----
    The runner is single-process / single-thread by design: the
    :class:`TokenBucketPacer` keeps it idle ≈99% of the time at 200 evt/s,
    so there is no scaling motivation to introduce ``multiprocessing``
    here (design doc §2.3).
    """

    def __init__(
        self,
        *,
        config: FeederRunConfig,
        producer: AvroEventProducer,
        generator: SyntheticEventGenerator,
        pacer: TokenBucketPacer,
        accountant: DeliveryAccountant,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._producer = producer
        self._generator = generator
        self._pacer = pacer
        self._accountant = accountant
        self._clock = clock
        self._shutdown = threading.Event()

    @property
    def config(self) -> FeederRunConfig:
        """Read-only configuration accessor.

        Returns
        -------
        FeederRunConfig
            The configuration supplied at construction.
        """
        return self._config

    def request_shutdown(self) -> None:
        """Signal-handler-safe shutdown request.

        Sets an internal flag the main loop polls at the top of each
        iteration.  No producer / Kafka calls are made here — the same
        ``librdkafka`` re-entrancy constraint that applies to the sink
        applies here (design doc §2.9).
        """
        self._shutdown.set()

    def _produce_batch(self, n: int) -> None:
        """Generate *n* events and enqueue them on the producer.

        Parameters
        ----------
        n : int
            Number of events to produce.
        """
        events = self._generator.generate_batch(n)
        for event in events:
            self._producer.produce(event, on_delivery=self._accountant.record)
            self._accountant.record_produced()
        self._producer.poll(0)

    def _maybe_heartbeat(self, last_log_ts: float) -> float:
        """Emit an INFO-level heartbeat every ``snapshot_interval_s`` seconds.

        Parameters
        ----------
        last_log_ts : float
            Monotonic timestamp of the previous heartbeat.

        Returns
        -------
        float
            Updated heartbeat timestamp (unchanged if no heartbeat fired).
        """
        now = self._clock()
        if (now - last_log_ts) < self._config.snapshot_interval_s:
            return last_log_ts
        snap = self._accountant.snapshot()
        logger.info(
            f"FeederRunner heartbeat: produced={snap.produced} "
            f"acked={snap.acked} failed={snap.failed} "
            f"in_flight={snap.in_flight} "
            f"sustained_eps={snap.acked / max(snap.wallclock_s, 1e-9):.0f} "
            f"ack_latency_p95_ms={snap.ack_latency_p95_ms:.1f}"
        )
        return now

    def run(self) -> FeederSnapshot:
        """Execute the feeder loop until :meth:`request_shutdown` is set.

        Returns
        -------
        FeederSnapshot
            Frozen Pydantic snapshot of the run.
        """
        cfg = self._config
        started_at = datetime.now(tz=timezone.utc)
        last_log_ts = self._clock()
        try:
            while not self._shutdown.is_set():
                self._pacer.acquire(cfg.batch_size)
                self._produce_batch(cfg.batch_size)
                last_log_ts = self._maybe_heartbeat(last_log_ts)
        finally:
            remaining = self._producer.flush(10.0)
            if remaining:
                logger.warning(
                    f"FeederRunner shutdown: {remaining} message(s) "
                    f"remained unflushed after 10 s timeout."
                )
        ended_at = datetime.now(tz=timezone.utc)
        delivery = self._accountant.snapshot()
        snapshot = FeederSnapshot(
            started_at=started_at,
            ended_at=ended_at,
            topic=cfg.topic,
            rate_evt_per_sec=cfg.rate_evt_per_sec,
            delivery=delivery,
        )
        logger.info(
            f"FeederRunner done: produced={delivery.produced} "
            f"acked={delivery.acked} failed={delivery.failed} "
            f"sustained_eps={delivery.acked / max(delivery.wallclock_s, 1e-9):.0f}"
        )
        return snapshot
