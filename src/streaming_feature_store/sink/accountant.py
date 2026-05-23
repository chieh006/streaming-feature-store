"""Per-flush counters + per-partition tallies for the Postgres sink.

The :class:`SinkAccountant` is the sink-side analog of the consume- and
load-side accountants.  It tracks:

* the volume counters required by the design doc §4.3
  (``consumed`` / ``inserted`` / ``conflict_skipped`` / ``deserialize_failed``);
* a reservoir-sampled distribution of per-flush wall-clock latency in
  milliseconds, surfaced as p50 / p95 / p99 in the report; and
* a fixed-bucket histogram of batch sizes (so the report can confirm the
  1000-cap is the dominant trigger, not the 10 s timeout, at the feeder's
  steady-state rate); and
* a per-partition message count dict that doubles as the Zipfian-skew sanity
  check (design doc §2.8).
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

import numpy as np
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)

_RESERVOIR_SIZE: int = 4096
_BATCH_HIST_BUCKETS: tuple[int, ...] = (
    1,
    10,
    50,
    100,
    250,
    500,
    1000,
    2500,
    5000,
    10_000,
)


def _histogram_increment(
    buckets: tuple[int, ...], counts: list[int], value: int
) -> None:
    """Increment the right-edge bucket containing *value*.

    Parameters
    ----------
    buckets : tuple of int
        Strictly-increasing right edges.  The final bucket is
        ``[buckets[-2], buckets[-1]]``; values strictly greater than the
        last edge are folded into the final bucket.
    counts : list of int
        Mutable counter array, parallel to *buckets*; length
        ``len(buckets)``.
    value : int
        Sample.

    Notes
    -----
    The function uses :func:`numpy.searchsorted` so the cost stays O(log n)
    regardless of bucket count.
    """
    idx = int(np.searchsorted(buckets, value, side="left"))
    if idx >= len(buckets):
        idx = len(buckets) - 1
    counts[idx] += 1


class SinkSnapshot(BaseModel):
    """Immutable snapshot of accumulated sink counters.

    Parameters
    ----------
    consumed : int
        Total messages polled from Kafka, including ones that failed to
        deserialize.
    inserted : int
        Sum of :class:`BatchInsertResult.inserted` across all flushes.
    conflict_skipped : int
        Sum of :class:`BatchInsertResult.skipped` — non-zero only after a
        crash-replay (design doc §2.2).
    deserialize_failed : int
        Avro decode or Pydantic validation failures dropped by the runner.
    batches_flushed : int
        Number of completed :class:`PostgresWriter.flush` calls.
    batch_size_p50 : float
        50th-percentile flush batch size.
    batch_size_p99 : float
        99th-percentile flush batch size.
    flush_latency_ms_p50 : float
        50th-percentile flush latency in milliseconds.
    flush_latency_ms_p95 : float
        95th-percentile flush latency in milliseconds.
    flush_latency_ms_p99 : float
        99th-percentile flush latency in milliseconds.
    partition_counts : dict of int to int
        Per-partition message totals.  Used for the skew sanity check.
    partition_skew_ratio : float
        ``max(partition_counts.values()) / mean(partition_counts.values())``,
        or ``0.0`` when no messages have been recorded.
    partition_skew_pass : bool
        ``True`` iff ``partition_skew_ratio < skew_threshold``.
    skew_threshold : float
        Threshold used to compute ``partition_skew_pass`` (design doc §2.8).
    wallclock_s : float
        Seconds elapsed since the accountant started.
    """

    model_config = ConfigDict(frozen=True)

    consumed: int
    inserted: int
    conflict_skipped: int
    deserialize_failed: int
    batches_flushed: int
    batch_size_p50: float
    batch_size_p99: float
    flush_latency_ms_p50: float
    flush_latency_ms_p95: float
    flush_latency_ms_p99: float
    partition_counts: dict[int, int] = Field(default_factory=dict)
    partition_skew_ratio: float
    partition_skew_pass: bool
    skew_threshold: float
    wallclock_s: float


class SinkAccountant:
    """Aggregate per-flush counters and per-partition tallies.

    Parameters
    ----------
    reservoir_size : int, optional
        Capacity of the flush-latency reservoir sampler.  Defaults to
        ``4096``.
    skew_threshold : float, optional
        Maximum allowed ``max(partition_counts) / mean(partition_counts)``
        before :attr:`SinkSnapshot.partition_skew_pass` flips to ``False``.
        Defaults to ``2.0`` (design doc §2.8).
    seed : int, optional
        RNG seed for the reservoir sampler.  Defaults to ``0``.
    clock : callable, optional
        Monotonic clock function.  Defaults to :func:`time.monotonic`.

    Raises
    ------
    ValueError
        If ``reservoir_size`` is less than 1 or ``skew_threshold`` is not
        strictly positive.
    """

    def __init__(
        self,
        *,
        reservoir_size: int = _RESERVOIR_SIZE,
        skew_threshold: float = 2.0,
        seed: int = 0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if reservoir_size < 1:
            raise ValueError(f"reservoir_size must be >= 1, got {reservoir_size}")
        if skew_threshold <= 0.0:
            raise ValueError(
                f"skew_threshold must be > 0, got {skew_threshold}"
            )
        self._lock = threading.Lock()
        self._consumed: int = 0
        self._inserted: int = 0
        self._conflict_skipped: int = 0
        self._deserialize_failed: int = 0
        self._batches_flushed: int = 0
        self._reservoir_ms: list[float] = []
        self._reservoir_size = reservoir_size
        self._reservoir_seen: int = 0
        self._batch_sizes: list[int] = []
        self._batch_hist: list[int] = [0] * len(_BATCH_HIST_BUCKETS)
        self._partition_counts: dict[int, int] = {}
        self._skew_threshold = float(skew_threshold)
        self._rng = np.random.default_rng(seed)
        self._clock = clock
        self._t0 = clock()

    @property
    def skew_threshold(self) -> float:
        """Configured skew threshold (read-only).

        Returns
        -------
        float
            The threshold supplied at construction.
        """
        return self._skew_threshold

    def record_consumed(self, n: int = 1) -> None:
        """Increment the *consumed* counter by *n*.

        Parameters
        ----------
        n : int, optional
            Increment.  Defaults to ``1``.
        """
        with self._lock:
            self._consumed += int(n)

    def record_deserialize_failure(self) -> None:
        """Increment the *deserialize_failed* counter by one."""
        with self._lock:
            self._deserialize_failed += 1

    def record_partition(self, partition: int) -> None:
        """Tally one message from *partition*.

        Parameters
        ----------
        partition : int
            Kafka partition id.
        """
        with self._lock:
            self._partition_counts[partition] = (
                self._partition_counts.get(partition, 0) + 1
            )

    def _sample_latency_locked(self, latency_ms: float) -> None:
        """Insert *latency_ms* into the reservoir (lock held).

        Parameters
        ----------
        latency_ms : float
            Wall-clock flush duration in milliseconds.
        """
        self._reservoir_seen += 1
        if len(self._reservoir_ms) < self._reservoir_size:
            self._reservoir_ms.append(latency_ms)
            return
        idx = int(self._rng.integers(0, self._reservoir_seen))
        if idx < self._reservoir_size:
            self._reservoir_ms[idx] = latency_ms

    def record_flush(
        self,
        *,
        inserted: int,
        skipped: int,
        batch_size: int,
        latency_ms: float,
    ) -> None:
        """Record one completed :meth:`PostgresWriter.flush`.

        Parameters
        ----------
        inserted : int
            ``BatchInsertResult.inserted`` from the flush.
        skipped : int
            ``BatchInsertResult.skipped`` from the flush.
        batch_size : int
            Number of rows in the batch.
        latency_ms : float
            Wall-clock duration of the flush in milliseconds.

        Raises
        ------
        ValueError
            If any of the counters are negative.
        """
        if inserted < 0 or skipped < 0 or batch_size < 0 or latency_ms < 0:
            raise ValueError(
                "record_flush args must be non-negative; got "
                f"inserted={inserted} skipped={skipped} "
                f"batch_size={batch_size} latency_ms={latency_ms}"
            )
        with self._lock:
            self._inserted += int(inserted)
            self._conflict_skipped += int(skipped)
            self._batches_flushed += 1
            self._batch_sizes.append(int(batch_size))
            _histogram_increment(
                _BATCH_HIST_BUCKETS, self._batch_hist, int(batch_size)
            )
            self._sample_latency_locked(float(latency_ms))

    @staticmethod
    def _percentile(samples: list[float], q: float) -> float:
        """Return the *q*-th percentile of *samples* (passes through 0.0).

        Parameters
        ----------
        samples : list of float
            Source samples.
        q : float
            Percentile in ``[0, 100]``.

        Returns
        -------
        float
            Percentile value, or ``0.0`` when *samples* is empty.
        """
        if not samples:
            return 0.0
        return float(np.percentile(np.asarray(samples, dtype=np.float64), q))

    @staticmethod
    def _compute_skew_ratio(counts: dict[int, int]) -> float:
        """Return ``max(counts) / mean(counts)``.

        Parameters
        ----------
        counts : dict of int to int
            Per-partition counts.

        Returns
        -------
        float
            Skew ratio, or ``0.0`` when *counts* is empty / all-zero.
        """
        if not counts:
            return 0.0
        values = np.asarray(list(counts.values()), dtype=np.float64)
        mean = float(values.mean())
        if mean <= 0.0:
            return 0.0
        return float(values.max() / mean)

    def snapshot(self) -> SinkSnapshot:
        """Return an immutable :class:`SinkSnapshot`.

        Returns
        -------
        SinkSnapshot
            Frozen Pydantic snapshot of every counter.
        """
        with self._lock:
            consumed = self._consumed
            inserted = self._inserted
            conflict_skipped = self._conflict_skipped
            deserialize_failed = self._deserialize_failed
            batches_flushed = self._batches_flushed
            latencies = list(self._reservoir_ms)
            batch_sizes = list(self._batch_sizes)
            partition_counts = dict(self._partition_counts)
            elapsed = self._clock() - self._t0
        skew_ratio = self._compute_skew_ratio(partition_counts)
        skew_pass = skew_ratio == 0.0 or skew_ratio < self._skew_threshold
        return SinkSnapshot(
            consumed=consumed,
            inserted=inserted,
            conflict_skipped=conflict_skipped,
            deserialize_failed=deserialize_failed,
            batches_flushed=batches_flushed,
            batch_size_p50=self._percentile(
                [float(s) for s in batch_sizes], 50.0
            ),
            batch_size_p99=self._percentile(
                [float(s) for s in batch_sizes], 99.0
            ),
            flush_latency_ms_p50=self._percentile(latencies, 50.0),
            flush_latency_ms_p95=self._percentile(latencies, 95.0),
            flush_latency_ms_p99=self._percentile(latencies, 99.0),
            partition_counts=partition_counts,
            partition_skew_ratio=skew_ratio,
            partition_skew_pass=skew_pass,
            skew_threshold=self._skew_threshold,
            wallclock_s=elapsed,
        )
