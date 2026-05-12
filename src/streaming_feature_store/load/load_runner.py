"""Compose generator + producer + pacer + accountant into a load harness."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.load.accountant import DeliveryAccountant
from streaming_feature_store.load.pacer import TokenBucketPacer
from streaming_feature_store.load.report import LoadRunConfig, LoadRunReport
from streaming_feature_store.load.synthetic import SyntheticEventGenerator
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.schemas.registry import RegistryError, SchemaRegistry

logger = logging.getLogger(__name__)


class LoadRunner:
    """Multi-worker pump driving :class:`AvroEventProducer` at a target rate.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap server configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    run_config : LoadRunConfig
        Per-run knobs (duration, target rate, workers, ...).
    producer : AvroEventProducer, optional
        Pre-constructed producer; if ``None``, one is created internally.
        Tests inject a mock here.
    generator : SyntheticEventGenerator, optional
        Pre-constructed generator; tests inject a fake.
    accountant : DeliveryAccountant, optional
        Pre-constructed accountant; tests inject a fake.
    pacer : TokenBucketPacer, optional
        Pre-constructed pacer; tests inject a fake.
    floor_eps : float, optional
        Throughput floor for pass/fail in the report.  Defaults to ``50_000``.
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        registry_config: SchemaRegistryConfig,
        run_config: LoadRunConfig,
        *,
        producer: AvroEventProducer | None = None,
        generator: SyntheticEventGenerator | None = None,
        accountant: DeliveryAccountant | None = None,
        pacer: TokenBucketPacer | None = None,
        floor_eps: float = 50_000.0,
    ) -> None:
        self._kafka_config = kafka_config
        self._registry_config = registry_config
        self._run_config = run_config
        self._producer = producer
        self._generator = generator
        self._accountant = accountant
        self._pacer = pacer
        self._floor_eps = floor_eps

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
            f"Subject {subject!r}: registered (id={latest.schema_id}, version={latest.version})"
        )

    def _build_components(
        self,
    ) -> tuple[AvroEventProducer, SyntheticEventGenerator, DeliveryAccountant, TokenBucketPacer]:
        """Construct or pass-through producer/generator/accountant/pacer.

        Returns
        -------
        tuple
            ``(producer, generator, accountant, pacer)``.
        """
        producer = self._producer or AvroEventProducer(
            self._kafka_config, self._registry_config, topic=self._run_config.topic
        )
        generator = self._generator or SyntheticEventGenerator(
            seed=self._run_config.seed
        )
        accountant = self._accountant or DeliveryAccountant()
        pacer = self._pacer or TokenBucketPacer(self._run_config.target_rate)
        return producer, generator, accountant, pacer

    def _worker_loop(
        self,
        *,
        deadline: float,
        producer: AvroEventProducer,
        generator: SyntheticEventGenerator,
        accountant: DeliveryAccountant,
        pacer: TokenBucketPacer,
        stop_event: threading.Event,
        error_box: list,
    ) -> None:
        """Body of each worker thread.

        Parameters
        ----------
        deadline : float
            ``time.monotonic`` deadline.
        producer : AvroEventProducer
            Shared producer.
        generator : SyntheticEventGenerator
            Per-worker generator.
        accountant : DeliveryAccountant
            Shared accountant.
        pacer : TokenBucketPacer
            Shared pacer.
        stop_event : threading.Event
            Set by the orchestrator to abort early.
        """
        cfg = self._run_config
        try:
            while time.monotonic() < deadline and not stop_event.is_set():
                pacer.acquire(cfg.batch_size)
                accountant.wait_for_in_flight_below(cfg.max_in_flight)
                events = generator.generate_batch(cfg.batch_size)
                for event in events:
                    self._produce_with_retry(producer, event, accountant)
        except BaseException as exc:
            error_box.append(exc)
            stop_event.set()

    def _produce_with_retry(
        self,
        producer: AvroEventProducer,
        event,
        accountant: DeliveryAccountant,
        *,
        attempts: int = 3,
    ) -> None:
        """Call ``producer.produce`` with bounded retries on ``BufferError``.

        Parameters
        ----------
        producer : AvroEventProducer
            Shared producer.
        event : EcommerceEvent
            Event to send.
        accountant : DeliveryAccountant
            Shared accountant.
        attempts : int, optional
            Maximum attempts before re-raising.  Defaults to ``3``.
        """
        last_exc: Optional[BaseException] = None
        for attempt in range(attempts):
            try:
                producer.produce(event, on_delivery=accountant.record)
                accountant.record_produced()
                return
            except BufferError as exc:
                last_exc = exc
                # Backpressure: wait for in-flight to drop and retry.
                accountant.wait_for_in_flight_below(
                    self._run_config.max_in_flight // 2 or 1
                )
        if last_exc is not None:
            raise last_exc

    def run(self) -> LoadRunReport:
        """Execute the load run end-to-end.

        Returns
        -------
        LoadRunReport
            Aggregate result, including pass/fail verdict.
        """
        self._assert_subject_registered()
        producer, generator, accountant, pacer = self._build_components()
        cfg = self._run_config
        started_at = datetime.now(tz=timezone.utc)
        deadline = time.monotonic() + cfg.duration_s
        stop_event = threading.Event()
        threads: list[threading.Thread] = []
        error_box: list[BaseException] = []
        # Each worker gets its OWN generator (different stream) to avoid lock contention.
        for i in range(cfg.workers):
            worker_gen = (
                generator
                if self._generator is not None
                else SyntheticEventGenerator(seed=cfg.seed + i)
            )
            t = threading.Thread(
                target=self._worker_loop,
                kwargs={
                    "deadline": deadline,
                    "producer": producer,
                    "generator": worker_gen,
                    "accountant": accountant,
                    "pacer": pacer,
                    "stop_event": stop_event,
                    "error_box": error_box,
                },
                name=f"loadrunner-worker-{i}",
                daemon=True,
            )
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        producer.flush(30.0)
        if error_box:
            raise error_box[0]
        snap = accountant.snapshot()
        sustained = snap.acked / max(snap.wallclock_s, 1e-9)
        report = LoadRunReport(
            config=cfg,
            started_at=started_at,
            snapshot=snap,
            sustained_rate_eps=sustained,
            floor_eps=self._floor_eps,
        )
        logger.info(
            f"LoadRunner done: produced={snap.produced} acked={snap.acked} "
            f"failed={snap.failed} sustained={sustained:.0f} evt/s"
        )
        return report
