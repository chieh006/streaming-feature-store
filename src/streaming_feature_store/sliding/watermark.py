"""Event-time watermark in plain Python (design doc §2.4 / §4.2).

:class:`WatermarkTracker` reproduces Flink's
``forBoundedOutOfOrderness(skew).withIdleness(idle)`` by hand: the watermark is
``max_event_time_seen − skew``, and when no event has arrived for
``idleness_ms`` of wall-clock time it falls back toward ``now − skew`` so
emissions do not stall on an idle partition assignment.
"""

from __future__ import annotations

import time
from collections.abc import Callable


class WatermarkTracker:
    """Bounded out-of-orderness watermark with an idleness fallback.

    Parameters
    ----------
    out_of_orderness_ms : int
        Skew budget subtracted from the maximum observed event time.
    idleness_ms : int
        Wall-clock idle duration after which the watermark falls back toward
        ``now − skew`` (design doc §2.4).
    clock : callable, optional
        Monotonic clock returning seconds.  Injected for deterministic tests;
        defaults to :func:`time.monotonic`.

    Notes
    -----
    The tracker is per process (over its assigned partitions).  Because users
    are partition-local (design doc §2.2), a user's windows are governed
    entirely by its owning process's watermark — no cross-process merge.
    """

    def __init__(
        self,
        out_of_orderness_ms: int,
        idleness_ms: int,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._skew_ms = out_of_orderness_ms
        self._idleness_ms = idleness_ms
        self._clock = clock
        self._max_event_ts: int | None = None
        self._last_event_monotonic: float = clock()

    def observe(self, event_ts_ms: int) -> None:
        """Record an event's timestamp, advancing the max and resetting idle.

        Parameters
        ----------
        event_ts_ms : int
            Event time in milliseconds since the Unix epoch.
        """
        if self._max_event_ts is None or event_ts_ms > self._max_event_ts:
            self._max_event_ts = event_ts_ms
        self._last_event_monotonic = self._clock()

    def watermark_ms(self, now_wallclock_ms: int) -> int | None:
        """Return the current watermark, or ``None`` before the first event.

        Parameters
        ----------
        now_wallclock_ms : int
            Current wall-clock time in milliseconds since the epoch, used only
            for the idleness fallback.

        Returns
        -------
        int or None
            ``max_event_ts − skew`` (raised toward ``now − skew`` when idle),
            or ``None`` if no event has been observed yet.
        """
        if self._max_event_ts is None:
            return None
        watermark = self._max_event_ts - self._skew_ms
        idle_for_ms = (self._clock() - self._last_event_monotonic) * 1000.0
        if idle_for_ms >= self._idleness_ms:
            watermark = max(watermark, now_wallclock_ms - self._skew_ms)
        return watermark
