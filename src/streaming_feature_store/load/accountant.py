"""Aggregate delivery outcomes from the producer's ``on_delivery`` callback.

The :class:`DeliveryAccountant` is hooked into each
``producer.produce(..., on_delivery=accountant.record)`` call, runs from the
librdkafka poll thread, and exposes a thread-safe in-flight counter that
:class:`LoadRunner` workers use for backpressure.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

import numpy as np
from confluent_kafka import KafkaError, Message
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)

_RESERVOIR_SIZE: int = 4096


class AccountantSnapshot(BaseModel):
    """Immutable snapshot of accumulated delivery counters.

    Parameters
    ----------
    produced : int
        Number of synchronous ``produce()`` calls.
    acked : int
        Number of successful broker acknowledgements.
    failed : int
        Number of failed deliveries (any error).
    in_flight : int
        ``produced - (acked + failed)``.
    errors_by_class : dict of str to int
        Histogram keyed by ``KafkaError`` symbolic name.
    ack_latency_p50_ms : float
        50th-percentile ack latency in milliseconds.
    ack_latency_p95_ms : float
        95th-percentile ack latency in milliseconds.
    ack_latency_p99_ms : float
        99th-percentile ack latency in milliseconds.
    wallclock_s : float
        Seconds elapsed since the accountant started.
    """

    model_config = ConfigDict(frozen=True)

    produced: int
    acked: int
    failed: int
    in_flight: int
    errors_by_class: dict[str, int] = Field(default_factory=dict)
    ack_latency_p50_ms: float
    ack_latency_p95_ms: float
    ack_latency_p99_ms: float
    wallclock_s: float


class DeliveryAccountant:
    """Aggregate delivery outcomes from a Kafka producer.

    Parameters
    ----------
    reservoir_size : int, optional
        Capacity of the ack-latency reservoir sampler.  Defaults to ``4096``.
    seed : int, optional
        RNG seed for the reservoir sampler.  Defaults to ``0``.
    clock : callable, optional
        Monotonic clock function.  Defaults to :func:`time.monotonic`.

    Notes
    -----
    All mutating methods are protected by a single :class:`threading.Lock`;
    the section under the lock is tiny so contention is negligible up to
    the throughput floor measured in §8 of the design doc.
    """

    def __init__(
        self,
        *,
        reservoir_size: int = _RESERVOIR_SIZE,
        seed: int = 0,
        clock=time.monotonic,
    ) -> None:
        if reservoir_size < 1:
            raise ValueError(f"reservoir_size must be >= 1, got {reservoir_size}")
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._produced: int = 0
        self._acked: int = 0
        self._failed: int = 0
        self._errors_by_class: dict[str, int] = {}
        self._reservoir: list[float] = []
        self._reservoir_size = reservoir_size
        self._reservoir_seen: int = 0
        self._rng = np.random.default_rng(seed)
        self._clock = clock
        self._t0 = clock()

    @property
    def in_flight(self) -> int:
        """Synchronous in-flight count (``produced - acked - failed``).

        Returns
        -------
        int
            Current in-flight messages.
        """
        with self._lock:
            return self._produced - self._acked - self._failed

    def record_produced(self) -> None:
        """Increment the *produced* counter.

        Notes
        -----
        Call **synchronously** from the worker, immediately after a
        successful ``producer.produce()`` returns.
        """
        with self._lock:
            self._produced += 1

    def _sample_latency_locked(self, latency_s: float) -> None:
        """Insert *latency_s* into the reservoir (lock held).

        Parameters
        ----------
        latency_s : float
            Ack latency in seconds.
        """
        self._reservoir_seen += 1
        if len(self._reservoir) < self._reservoir_size:
            self._reservoir.append(latency_s)
            return
        idx = int(self._rng.integers(0, self._reservoir_seen))
        if idx < self._reservoir_size:
            self._reservoir[idx] = latency_s

    def record(self, err: KafkaError | None, msg: Message | None) -> None:
        """``on_delivery`` callback invoked from librdkafka's poll thread.

        Parameters
        ----------
        err : KafkaError or None
            Delivery error, if any.
        msg : Message or None
            The delivered message, if available.
        """
        with self._cond:
            if err is not None:
                self._failed += 1
                key = err.name() if hasattr(err, "name") else str(err)
                self._errors_by_class[key] = self._errors_by_class.get(key, 0) + 1
            else:
                self._acked += 1
                if msg is not None:
                    latency_s = self._safe_latency(msg)
                    if latency_s is not None and latency_s >= 0:
                        self._sample_latency_locked(latency_s)
            self._cond.notify_all()

    @staticmethod
    def _safe_latency(msg: Message) -> Optional[float]:
        """Return ``msg.latency()`` or ``None`` if unsupported.

        Parameters
        ----------
        msg : Message
            Delivered Kafka message.

        Returns
        -------
        float or None
            Latency in seconds; ``None`` if the message reports no latency.
        """
        try:
            value = msg.latency()
        except Exception:  # pragma: no cover - defensive
            return None
        return value

    def wait_for_in_flight_below(self, threshold: int, *, timeout_s: float = 30.0) -> None:
        """Block until ``in_flight`` drops strictly below *threshold*.

        Parameters
        ----------
        threshold : int
            Target ceiling.
        timeout_s : float, optional
            Maximum wait per ``Condition.wait`` cycle.  Defaults to ``30.0``.
        """
        with self._cond:
            while (self._produced - self._acked - self._failed) >= threshold:
                self._cond.notify_all  # noqa: B018 - intentional, see notes
                if not self._cond.wait(timeout=timeout_s):
                    return

    @staticmethod
    def _percentile(samples: list[float], q: float) -> float:
        """Return the *q*-th percentile of *samples* in milliseconds.

        Parameters
        ----------
        samples : list of float
            Latency samples in seconds.
        q : float
            Percentile in [0, 100].

        Returns
        -------
        float
            Percentile in milliseconds, or ``0.0`` if *samples* is empty.
        """
        if not samples:
            return 0.0
        return float(np.percentile(np.asarray(samples, dtype=np.float64), q) * 1000.0)

    def latency_samples_s(self) -> list[float]:
        """Return a copy of the reservoir samples (seconds).

        Returns
        -------
        list of float
            Raw reservoir samples in seconds.  Used by the multi-process
            aggregator to re-percentile the union of per-process reservoirs;
            within a single process :meth:`snapshot` is the normal accessor.
        """
        with self._lock:
            return list(self._reservoir)

    def snapshot(self) -> AccountantSnapshot:
        """Return an immutable :class:`AccountantSnapshot`.

        Returns
        -------
        AccountantSnapshot
            Frozen Pydantic snapshot of all counters.
        """
        with self._lock:
            samples = list(self._reservoir)
            produced = self._produced
            acked = self._acked
            failed = self._failed
            errors = dict(self._errors_by_class)
            elapsed = self._clock() - self._t0
        return AccountantSnapshot(
            produced=produced,
            acked=acked,
            failed=failed,
            in_flight=produced - acked - failed,
            errors_by_class=errors,
            ack_latency_p50_ms=self._percentile(samples, 50.0),
            ack_latency_p95_ms=self._percentile(samples, 95.0),
            ack_latency_p99_ms=self._percentile(samples, 99.0),
            wallclock_s=elapsed,
        )
