"""Thread-safe token-bucket pacer for the load-runner.

Refill is *lazy*: each :meth:`acquire` call computes ``now - last_refill_ts``
and tops up the bucket on demand — no background thread.  ``target_rate=None``
disables pacing entirely (the un-paced ceiling-finding mode).
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

logger = logging.getLogger(__name__)


class TokenBucketPacer:
    """Thread-safe token bucket for rate-limiting :class:`LoadRunner` workers.

    Parameters
    ----------
    target_rate : float or None
        Tokens added per second.  ``None`` disables pacing entirely.
    burst : int, optional
        Bucket capacity.  Defaults to ``4096``.
    clock : callable, optional
        Monotonic clock function, injectable for tests.  Defaults to
        :func:`time.monotonic`.

    Notes
    -----
    A single bucket is shared across worker threads — the rate is a
    *system-wide* knob, not per-worker.
    """

    def __init__(
        self,
        target_rate: float | None,
        *,
        burst: int = 4096,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if burst < 1:
            raise ValueError(f"burst must be >= 1, got {burst}")
        if target_rate is not None and target_rate <= 0:
            raise ValueError(f"target_rate must be > 0 or None, got {target_rate}")
        self._target_rate = target_rate
        self._burst = burst
        self._clock = clock
        self._tokens: float = float(burst)
        self._last_refill_ts: float = clock()
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

    @property
    def target_rate(self) -> float | None:
        """Configured target rate (events/sec) or ``None`` if un-paced.

        Returns
        -------
        float or None
            The target rate.
        """
        return self._target_rate

    @property
    def burst(self) -> int:
        """Bucket capacity.

        Returns
        -------
        int
            The configured burst size.
        """
        return self._burst

    def _refill_locked(self) -> None:
        """Top up the bucket lazily based on elapsed wall time.

        Notes
        -----
        Caller must hold ``self._lock``.
        """
        if self._target_rate is None:
            return
        now = self._clock()
        elapsed = now - self._last_refill_ts
        if elapsed <= 0:
            return
        self._tokens = min(self._burst, self._tokens + elapsed * self._target_rate)
        self._last_refill_ts = now

    def acquire(self, n: int = 1) -> None:
        """Block until *n* tokens are available, then consume them.

        Parameters
        ----------
        n : int, optional
            Number of tokens to acquire.  Must be ``>= 0`` and ``<= burst``.

        Raises
        ------
        ValueError
            If *n* is negative or exceeds the bucket capacity.
        """
        if n < 0:
            raise ValueError(f"n must be >= 0, got {n}")
        if n == 0:
            return
        if self._target_rate is None:
            return
        if n > self._burst:
            raise ValueError(
                f"n={n} exceeds burst={self._burst}; would deadlock"
            )
        with self._cond:
            while True:
                self._refill_locked()
                if self._tokens >= n:
                    self._tokens -= n
                    return
                deficit = n - self._tokens
                wait_s = deficit / float(self._target_rate)
                self._cond.wait(timeout=wait_s)
