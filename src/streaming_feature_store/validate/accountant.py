"""Counters + per-partition tallies for the validator runner.

The :class:`ValidatorAccountant` tracks:

* volume counters (consumed / validated / invalid totals);
* invalid breakdowns by error class, validator, and field path (for the
  top-N report);
* a reservoir-sampled distribution of per-event validation latency in
  microseconds, surfaced as p50 / p95 / p99 in the report;
* per-partition message counts on the source topic (the Zipfian-skew
  sanity check that mirrors the Postgres sink's
  :class:`SinkAccountant`).

All counters are thread-safe (single lock); the validator runner is
single-threaded but tests may probe the accountant from multiple
threads.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter
from collections.abc import Callable

import numpy as np
from pydantic import BaseModel, ConfigDict, Field

from streaming_feature_store.validate.dlq import ErrorClass

logger = logging.getLogger(__name__)

_RESERVOIR_SIZE: int = 4096
_TOP_FIELDS_K: int = 10


class ValidatorSnapshot(BaseModel):
    """Immutable snapshot of accumulated validator counters.

    Parameters
    ----------
    consumed : int
        Total messages polled from Kafka, including ones that failed to
        deserialize.
    validated : int
        Number of messages that passed the pipeline and were produced to
        ``validated-events``.
    invalid_total : int
        Number of messages routed to the DLQ.
    invalid_by_class : dict of ErrorClass to int
        Histogram by coarse error bucket.
    invalid_by_validator : dict of str to int
        Histogram by ``Validator.name``.
    invalid_by_field_path : dict of str to int
        Histogram of field paths that triggered a rejection; only the
        top-N are surfaced in the report.
    deserialize_failed : int
        Convenience accessor for
        ``invalid_by_class[ErrorClass.DESERIALIZE_FAILURE]``.
    schema_mismatches : int
        Convenience accessor for
        ``invalid_by_class[ErrorClass.SCHEMA_MISMATCH]``.
    pipeline_internal_errors : int
        Convenience accessor for
        ``invalid_by_class[ErrorClass.PIPELINE_INTERNAL_ERROR]``.
    invalid_rate : float
        ``invalid_total / consumed`` (or ``0.0`` when ``consumed`` is 0).
    validation_latency_us_p50 : float
        50th-percentile per-event validation latency in microseconds.
    validation_latency_us_p95 : float
        95th-percentile per-event validation latency in microseconds.
    validation_latency_us_p99 : float
        99th-percentile per-event validation latency in microseconds.
    partition_counts : dict of int to int
        Per-partition message totals on the source topic.
    partition_skew_ratio : float
        ``max(partition_counts) / mean(partition_counts)`` (or ``0.0``).
    partition_skew_pass : bool
        ``True`` when ``partition_skew_ratio < skew_threshold``.
    skew_threshold : float
        Threshold used for ``partition_skew_pass``.
    top_failing_fields : list of tuple of (str, int)
        Top-N entries from ``invalid_by_field_path`` sorted descending.
    wallclock_s : float
        Seconds elapsed since the accountant started.
    """

    model_config = ConfigDict(frozen=True)

    consumed: int
    validated: int
    invalid_total: int
    invalid_by_class: dict[ErrorClass, int] = Field(default_factory=dict)
    invalid_by_validator: dict[str, int] = Field(default_factory=dict)
    invalid_by_field_path: dict[str, int] = Field(default_factory=dict)
    deserialize_failed: int
    schema_mismatches: int
    pipeline_internal_errors: int
    invalid_rate: float
    validation_latency_us_p50: float
    validation_latency_us_p95: float
    validation_latency_us_p99: float
    partition_counts: dict[int, int] = Field(default_factory=dict)
    partition_skew_ratio: float
    partition_skew_pass: bool
    skew_threshold: float
    top_failing_fields: list[tuple[str, int]] = Field(default_factory=list)
    wallclock_s: float


class ValidatorAccountant:
    """Aggregate per-event counters, latencies, and partition tallies.

    Parameters
    ----------
    reservoir_size : int, optional
        Capacity of the validation-latency reservoir sampler.  Defaults to
        ``4096``.
    skew_threshold : float, optional
        Maximum allowed ``max / mean`` partition skew before
        :attr:`ValidatorSnapshot.partition_skew_pass` flips to ``False``.
        Defaults to ``2.0``.
    top_fields_k : int, optional
        Number of top failing field paths to surface in the snapshot.
        Defaults to ``10``.
    seed : int, optional
        RNG seed for the reservoir sampler.  Defaults to ``0``.
    clock : callable, optional
        Monotonic clock function.  Defaults to :func:`time.monotonic`.

    Raises
    ------
    ValueError
        If ``reservoir_size`` < 1, ``skew_threshold`` <= 0, or
        ``top_fields_k`` < 1.
    """

    def __init__(
        self,
        *,
        reservoir_size: int = _RESERVOIR_SIZE,
        skew_threshold: float = 2.0,
        top_fields_k: int = _TOP_FIELDS_K,
        seed: int = 0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if reservoir_size < 1:
            raise ValueError(f"reservoir_size must be >= 1, got {reservoir_size}")
        if skew_threshold <= 0.0:
            raise ValueError(
                f"skew_threshold must be > 0, got {skew_threshold}"
            )
        if top_fields_k < 1:
            raise ValueError(f"top_fields_k must be >= 1, got {top_fields_k}")
        self._lock = threading.Lock()
        self._consumed: int = 0
        self._validated: int = 0
        self._invalid_total: int = 0
        self._invalid_by_class: Counter[ErrorClass] = Counter()
        self._invalid_by_validator: Counter[str] = Counter()
        self._invalid_by_field_path: Counter[str] = Counter()
        self._partition_counts: dict[int, int] = {}
        self._reservoir_us: list[float] = []
        self._reservoir_size = reservoir_size
        self._reservoir_seen: int = 0
        self._top_fields_k = top_fields_k
        self._skew_threshold = float(skew_threshold)
        self._rng = np.random.default_rng(seed)
        self._clock = clock
        self._t0 = clock()

    @property
    def skew_threshold(self) -> float:
        """Configured partition-skew threshold.

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

    def record_valid(self) -> None:
        """Increment the *validated* counter by one."""
        with self._lock:
            self._validated += 1

    def record_invalid(
        self,
        *,
        error_class: ErrorClass,
        validator_name: str,
        error_field_path: str | None,
    ) -> None:
        """Increment the invalid counters by one.

        Parameters
        ----------
        error_class : ErrorClass
            Coarse error bucket.
        validator_name : str
            Validator that produced the rejection.
        error_field_path : str or None
            Dotted path of the offending field; field histogram entry is
            skipped when ``None``.
        """
        with self._lock:
            self._invalid_total += 1
            self._invalid_by_class[error_class] += 1
            self._invalid_by_validator[validator_name] += 1
            if error_field_path is not None:
                self._invalid_by_field_path[error_field_path] += 1

    def _sample_latency_locked(self, latency_us: float) -> None:
        """Insert *latency_us* into the reservoir (lock held).

        Parameters
        ----------
        latency_us : float
            Per-event validation latency in microseconds.
        """
        self._reservoir_seen += 1
        if len(self._reservoir_us) < self._reservoir_size:
            self._reservoir_us.append(latency_us)
            return
        idx = int(self._rng.integers(0, self._reservoir_seen))
        if idx < self._reservoir_size:
            self._reservoir_us[idx] = latency_us

    def record_validation_latency_us(self, latency_us: float) -> None:
        """Sample one per-event validation latency in microseconds.

        Parameters
        ----------
        latency_us : float
            Wall-clock duration of the validate-and-route step in
            microseconds.

        Raises
        ------
        ValueError
            If *latency_us* is negative.
        """
        if latency_us < 0:
            raise ValueError(
                f"latency_us must be non-negative, got {latency_us}"
            )
        with self._lock:
            self._sample_latency_locked(float(latency_us))

    @staticmethod
    def _percentile(samples: list[float], q: float) -> float:
        """Return the *q*-th percentile of *samples*; ``0.0`` if empty.

        Parameters
        ----------
        samples : list of float
            Source samples.
        q : float
            Percentile in ``[0, 100]``.

        Returns
        -------
        float
            Percentile value.
        """
        if not samples:
            return 0.0
        return float(np.percentile(np.asarray(samples, dtype=np.float64), q))

    @staticmethod
    def _compute_skew_ratio(counts: dict[int, int]) -> float:
        """Return ``max(counts) / mean(counts)`` or ``0.0`` when empty.

        Parameters
        ----------
        counts : dict of int to int
            Per-partition counts.

        Returns
        -------
        float
            Skew ratio.
        """
        if not counts:
            return 0.0
        values = np.asarray(list(counts.values()), dtype=np.float64)
        mean = float(values.mean())
        if mean <= 0.0:
            return 0.0
        return float(values.max() / mean)

    def snapshot(self) -> ValidatorSnapshot:
        """Return an immutable :class:`ValidatorSnapshot`.

        Returns
        -------
        ValidatorSnapshot
            Frozen Pydantic snapshot of every counter.
        """
        with self._lock:
            consumed = self._consumed
            validated = self._validated
            invalid_total = self._invalid_total
            invalid_by_class = dict(self._invalid_by_class)
            invalid_by_validator = dict(self._invalid_by_validator)
            invalid_by_field_path = dict(self._invalid_by_field_path)
            partition_counts = dict(self._partition_counts)
            latencies = list(self._reservoir_us)
            elapsed = self._clock() - self._t0
        skew_ratio = self._compute_skew_ratio(partition_counts)
        skew_pass = skew_ratio == 0.0 or skew_ratio < self._skew_threshold
        invalid_rate = (
            invalid_total / consumed if consumed > 0 else 0.0
        )
        deserialize_failed = invalid_by_class.get(
            ErrorClass.DESERIALIZE_FAILURE, 0
        )
        schema_mismatches = invalid_by_class.get(ErrorClass.SCHEMA_MISMATCH, 0)
        pipeline_internal_errors = invalid_by_class.get(
            ErrorClass.PIPELINE_INTERNAL_ERROR, 0
        )
        top_fields = sorted(
            invalid_by_field_path.items(), key=lambda kv: (-kv[1], kv[0])
        )[: self._top_fields_k]
        return ValidatorSnapshot(
            consumed=consumed,
            validated=validated,
            invalid_total=invalid_total,
            invalid_by_class=invalid_by_class,
            invalid_by_validator=invalid_by_validator,
            invalid_by_field_path=invalid_by_field_path,
            deserialize_failed=deserialize_failed,
            schema_mismatches=schema_mismatches,
            pipeline_internal_errors=pipeline_internal_errors,
            invalid_rate=invalid_rate,
            validation_latency_us_p50=self._percentile(latencies, 50.0),
            validation_latency_us_p95=self._percentile(latencies, 95.0),
            validation_latency_us_p99=self._percentile(latencies, 99.0),
            partition_counts=partition_counts,
            partition_skew_ratio=skew_ratio,
            partition_skew_pass=skew_pass,
            skew_threshold=self._skew_threshold,
            top_failing_fields=top_fields,
            wallclock_s=elapsed,
        )
