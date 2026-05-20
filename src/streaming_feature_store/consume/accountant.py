"""End-to-end-latency accountant for the consumer group.

The :class:`ConsumeAccountant` is the consume-side analog of
:class:`streaming_feature_store.load.accountant.DeliveryAccountant`: it
accumulates a *consumed* counter, a per-error-class deserialize-failure
histogram, a bounded reservoir of end-to-end latency samples, and a
consumer-lag series.  The lag series is linearly fit at snapshot time to
classify the run as steady-state (flat) or *ramping* — the "consumer
slower than producer" signature that the symmetric-GIL demonstration
turns on (design doc §2.5).

All mutating methods are guarded by one :class:`threading.Lock`; the
critical section is tiny.  The poll loop is single-threaded (design doc
§2.6) but the lock keeps the accountant correct under the contention the
unit suite exercises and mirrors ``DeliveryAccountant`` one-for-one.
"""

from __future__ import annotations

import logging
import threading
import time

import numpy as np
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)

_RESERVOIR_SIZE: int = 4096
_MIN_RAMP_SAMPLES: int = 3
_RAMP_RISE_FRACTION: float = 0.10


def _detect_lag_ramp(samples: list[int]) -> bool:
    """Return ``True`` iff the lag series is *ramping* (falling behind).

    Parameters
    ----------
    samples : list of int
        Consumer-lag samples in poll order.

    Returns
    -------
    bool
        ``True`` when a least-squares line through the series has a
        positive slope whose fitted total rise is at least
        :data:`_RAMP_RISE_FRACTION` of the peak lag.  The test is
        *relative* (a fraction of the peak), so it is host-speed
        independent — the same philosophy as the throughput investigation's
        "ratio not absolute" stance (design doc §10 Q2).

    Notes
    -----
    Fewer than :data:`_MIN_RAMP_SAMPLES` samples, an all-zero series, or a
    flat / draining series all classify as *not ramping*.
    """
    if len(samples) < _MIN_RAMP_SAMPLES:
        return False
    y = np.asarray(samples, dtype=np.float64)
    peak = float(np.max(y))
    if peak <= 0.0:
        return False
    x = np.arange(y.size, dtype=np.float64)
    slope = float(np.polyfit(x, y, 1)[0])
    if slope <= 0.0:
        return False
    fitted_rise = slope * float(y.size - 1)
    return fitted_rise >= _RAMP_RISE_FRACTION * peak


class ConsumeSnapshot(BaseModel):
    """Immutable snapshot of accumulated consume counters.

    Parameters
    ----------
    consumed : int
        Number of messages polled and accounted (every delivered record,
        regardless of deserialize outcome).
    deserialize_failed : int
        Number of records whose Pydantic conversion raised.
    errors_by_class : dict of str to int
        Deserialize-error histogram keyed by exception class name.
    e2e_p50_ms : float
        50th-percentile end-to-end latency in milliseconds.
    e2e_p95_ms : float
        95th-percentile end-to-end latency in milliseconds.
    e2e_p99_ms : float
        99th-percentile end-to-end latency in milliseconds.
    max_lag : int
        Maximum consumer lag observed across the run.
    end_lag : int
        Consumer lag at the last sample (``0`` ⇒ drained).
    lag_ramped : bool
        ``True`` when the lag series ramped (fell behind) — see
        :func:`_detect_lag_ramp`.
    wallclock_s : float
        Seconds elapsed since the accountant started.
    """

    model_config = ConfigDict(frozen=True)

    consumed: int
    deserialize_failed: int
    errors_by_class: dict[str, int] = Field(default_factory=dict)
    e2e_p50_ms: float
    e2e_p95_ms: float
    e2e_p99_ms: float
    max_lag: int
    end_lag: int
    lag_ramped: bool
    wallclock_s: float


class ConsumeAccountant:
    """Accumulate end-to-end latency, deserialize errors, and lag.

    Parameters
    ----------
    reservoir_size : int, optional
        Capacity of the end-to-end-latency reservoir sampler.  Defaults to
        ``4096`` (matches ``DeliveryAccountant``).
    seed : int, optional
        RNG seed for the reservoir sampler.  Defaults to ``0``.
    clock : callable, optional
        Monotonic clock used only for ``wallclock_s``.  Defaults to
        :func:`time.monotonic`.

    Raises
    ------
    ValueError
        If ``reservoir_size`` is less than ``1``.
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
        self._consumed: int = 0
        self._deserialize_failed: int = 0
        self._errors_by_class: dict[str, int] = {}
        self._reservoir: list[float] = []
        self._reservoir_size = reservoir_size
        self._reservoir_seen: int = 0
        self._rng = np.random.default_rng(seed)
        self._lag_samples: list[int] = []
        self._clock = clock
        self._t0 = clock()

    @property
    def consumed(self) -> int:
        """Number of messages accounted so far.

        Returns
        -------
        int
            Current consumed count.
        """
        with self._lock:
            return self._consumed

    def _sample_latency_locked(self, latency_s: float) -> None:
        """Insert *latency_s* into the reservoir (lock held).

        Parameters
        ----------
        latency_s : float
            End-to-end latency in seconds.
        """
        self._reservoir_seen += 1
        if len(self._reservoir) < self._reservoir_size:
            self._reservoir.append(latency_s)
            return
        idx = int(self._rng.integers(0, self._reservoir_seen))
        if idx < self._reservoir_size:
            self._reservoir[idx] = latency_s

    def record(self, *, e2e_latency_s: float) -> None:
        """Account one consumed message.

        Parameters
        ----------
        e2e_latency_s : float
            End-to-end latency (consumer-receive wall clock minus the
            record's produce timestamp), in seconds.  A negative value
            (timestamp unavailable) still counts the message but is **not**
            sampled into the latency reservoir — same convention as
            ``DeliveryAccountant`` skipping ``latency < 0``.
        """
        with self._lock:
            self._consumed += 1
            if e2e_latency_s >= 0:
                self._sample_latency_locked(e2e_latency_s)

    def record_deserialize_error(self, err_class: str) -> None:
        """Record a deserialize / validation failure.

        Parameters
        ----------
        err_class : str
            Exception class name (the histogram key).
        """
        with self._lock:
            self._deserialize_failed += 1
            self._errors_by_class[err_class] = (
                self._errors_by_class.get(err_class, 0) + 1
            )

    def sample_lag(self, lag: int) -> None:
        """Append a consumer-lag observation.

        Parameters
        ----------
        lag : int
            Total lag (Σ ``high_watermark − position``) at this poll cycle.
        """
        with self._lock:
            self._lag_samples.append(int(lag))

    def e2e_samples_s(self) -> list[float]:
        """Return a copy of the end-to-end-latency reservoir (seconds).

        Returns
        -------
        list of float
            Raw reservoir samples.  Used by the multi-process aggregator to
            re-percentile the union of per-process reservoirs.
        """
        with self._lock:
            return list(self._reservoir)

    @staticmethod
    def _percentile(samples: list[float], q: float) -> float:
        """Return the *q*-th percentile of *samples* in milliseconds.

        Parameters
        ----------
        samples : list of float
            Latency samples in seconds.
        q : float
            Percentile in ``[0, 100]``.

        Returns
        -------
        float
            Percentile in milliseconds, or ``0.0`` if *samples* is empty.
        """
        if not samples:
            return 0.0
        return float(np.percentile(np.asarray(samples, dtype=np.float64), q) * 1000.0)

    def snapshot(self) -> ConsumeSnapshot:
        """Return an immutable :class:`ConsumeSnapshot`.

        Returns
        -------
        ConsumeSnapshot
            Frozen snapshot with re-percentiled latency and lag verdict.
        """
        with self._lock:
            samples = list(self._reservoir)
            lag_samples = list(self._lag_samples)
            consumed = self._consumed
            deserialize_failed = self._deserialize_failed
            errors = dict(self._errors_by_class)
            elapsed = self._clock() - self._t0
        return ConsumeSnapshot(
            consumed=consumed,
            deserialize_failed=deserialize_failed,
            errors_by_class=errors,
            e2e_p50_ms=self._percentile(samples, 50.0),
            e2e_p95_ms=self._percentile(samples, 95.0),
            e2e_p99_ms=self._percentile(samples, 99.0),
            max_lag=max(lag_samples) if lag_samples else 0,
            end_lag=lag_samples[-1] if lag_samples else 0,
            lag_ramped=_detect_lag_ramp(lag_samples),
            wallclock_s=elapsed,
        )
