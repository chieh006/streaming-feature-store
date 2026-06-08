"""In-memory pane ring-buffer + window manager (design doc §2.3 / §4.1–§4.3).

:class:`PanedSlidingWindow` is the per-``(user, resolution)`` pane buffer: a
``pane_index -> SlidingAccumulator`` dict where
``pane_index = event_ts_ms // slide_ms``.  A window ending at boundary ``E``
merges the panes whose index falls in ``[(E - size)/slide, E/slide)``.  State is
O(panes), not O(events) — the same pre-aggregation property Flink gives, in a
few dozen lines of explicit Python.

:class:`SlidingWindowManager` owns every active user's pane buffers for this
process, routes each event into all three resolutions, drives the
watermark-driven emission cursor (design doc §2.5), and implements
allowed-lateness re-emission (design doc §2.6) and rebalance state drops
(design doc §2.12).
"""

from __future__ import annotations

from collections.abc import Iterable

from streaming_feature_store.schemas import EcommerceEvent
from streaming_feature_store.sliding.aggregators import (
    AGGREGATOR_BY_RESOLUTION,
    SlidingWindowAggregator,
)
from streaming_feature_store.sliding.models import (
    SlidingAccumulator,
    SlidingFeatureRecord,
    WindowResolution,
)


class PanedSlidingWindow:
    """Per-``(user, resolution)`` ring buffer of pane accumulators.

    Parameters
    ----------
    resolution : WindowResolution
        Resolution this buffer serves; supplies the slide / size geometry.
    aggregator : SlidingWindowAggregator
        Aggregator used to create, fold, merge, and project accumulators.  A
        single instance is shared across users (the aggregator is stateless).
    """

    def __init__(
        self, resolution: WindowResolution, aggregator: SlidingWindowAggregator
    ) -> None:
        self._resolution = resolution
        self._aggregator = aggregator
        self._slide_ms = resolution.slide_ms
        self._size_ms = resolution.window_size_ms
        self._panes: dict[int, SlidingAccumulator] = {}

    @property
    def panes(self) -> dict[int, SlidingAccumulator]:
        """Live ``pane_index -> accumulator`` map (read-only view of state).

        Returns
        -------
        dict of int to SlidingAccumulator
            The internal pane dict.
        """
        return self._panes

    def add(self, event: EcommerceEvent, event_ts_ms: int) -> None:
        """Fold *event* into the pane containing its event timestamp.

        Parameters
        ----------
        event : EcommerceEvent
            Decoded event to aggregate.
        event_ts_ms : int
            Event time in milliseconds since the epoch.
        """
        pane_index = event_ts_ms // self._slide_ms
        acc = self._panes.get(pane_index)
        if acc is None:
            acc = self._aggregator.create_accumulator()
            self._panes[pane_index] = acc
        self._aggregator.add(event, acc)

    def window_record(self, window_end_ms: int) -> SlidingFeatureRecord | None:
        """Merge the panes composing the window ending at *window_end_ms*.

        Parameters
        ----------
        window_end_ms : int
            Exclusive upper bound of the window (slide-aligned).

        Returns
        -------
        SlidingFeatureRecord or None
            The projected record with window bounds filled, or ``None`` when no
            contributing pane exists (sparsity, design doc §2.7).
        """
        first = (window_end_ms - self._size_ms) // self._slide_ms
        last = window_end_ms // self._slide_ms  # exclusive
        merged = self._aggregator.create_accumulator()
        seen = False
        for idx in range(first, last):
            pane = self._panes.get(idx)
            if pane is not None:
                merged = self._aggregator.merge(merged, pane)
                seen = True
        if not seen:
            return None
        record = self._aggregator.get_result(merged)
        record.window_start_ms = window_end_ms - self._size_ms
        record.window_end_ms = window_end_ms
        return record

    def window_ends_including(self, event_ts_ms: int) -> list[int]:
        """Slide-aligned window-ends whose window contains *event_ts_ms*.

        Parameters
        ----------
        event_ts_ms : int
            Event time in milliseconds since the epoch.

        Returns
        -------
        list of int
            The ``panes_per_window`` window-end boundaries that include the
            event's pane (used to schedule late re-fires, design doc §2.6).
        """
        pane_index = event_ts_ms // self._slide_ms
        panes_per_window = self._size_ms // self._slide_ms
        return [
            (pane_index + step) * self._slide_ms
            for step in range(1, panes_per_window + 1)
        ]

    def gc(self, watermark_ms: int, lateness_ms: int) -> None:
        """Drop panes no live window (including the lateness tail) can include.

        Parameters
        ----------
        watermark_ms : int
            Current watermark in milliseconds.
        lateness_ms : int
            Allowed-lateness budget in milliseconds (design doc §2.6).
        """
        cutoff = (watermark_ms - self._size_ms - lateness_ms) // self._slide_ms
        for idx in [i for i in self._panes if i < cutoff]:
            del self._panes[idx]


class _EmissionCursor:
    """Mutable per-resolution emission cursor (design doc §4.3)."""

    __slots__ = ("next_end_ms",)

    def __init__(self) -> None:
        self.next_end_ms: int | None = None


class SlidingWindowManager:
    """Owns every active user's pane state for this process.

    Parameters
    ----------
    allowed_lateness_ms : int
        Pane-retention / re-emission budget in milliseconds (design doc §2.6).
    resolutions : iterable of WindowResolution, optional
        Resolutions to maintain.  Defaults to all three.

    Notes
    -----
    State is keyed by ``user_id`` (partition-local by construction, design doc
    §2.2).  The manager records each user's owning partition so a rebalance can
    drop exactly the revoked partitions' users (design doc §2.12).
    """

    def __init__(
        self,
        allowed_lateness_ms: int,
        resolutions: Iterable[WindowResolution] | None = None,
    ) -> None:
        self._lateness_ms = allowed_lateness_ms
        self._resolutions = tuple(resolutions or WindowResolution)
        # One shared (stateless) aggregator instance per resolution.
        self._aggregators: dict[WindowResolution, SlidingWindowAggregator] = {
            res: AGGREGATOR_BY_RESOLUTION[res]() for res in self._resolutions
        }
        self._users: dict[str, dict[WindowResolution, PanedSlidingWindow]] = {}
        self._user_partition: dict[str, int] = {}
        self._cursors: dict[WindowResolution, _EmissionCursor] = {
            res: _EmissionCursor() for res in self._resolutions
        }
        # emission_seq counter per (user_id, resolution, window_end_ms).
        self._seq: dict[tuple[str, WindowResolution, int], int] = {}
        # (user_id, resolution, window_end_ms) windows needing a late re-fire.
        self._dirty: set[tuple[str, WindowResolution, int]] = set()

    @property
    def active_user_count(self) -> int:
        """Number of users with live pane state on this process.

        Returns
        -------
        int
            Count of distinct active users.
        """
        return len(self._users)

    def _windows_for(self, user_id: str) -> dict[WindowResolution, PanedSlidingWindow]:
        """Return (creating if needed) the per-resolution buffers for a user.

        Parameters
        ----------
        user_id : str
            User identifier.

        Returns
        -------
        dict of WindowResolution to PanedSlidingWindow
            The user's pane buffers.
        """
        windows = self._users.get(user_id)
        if windows is None:
            windows = {
                res: PanedSlidingWindow(res, self._aggregators[res])
                for res in self._resolutions
            }
            self._users[user_id] = windows
        return windows

    def add(
        self,
        event: EcommerceEvent,
        event_ts_ms: int,
        watermark_ms: int | None,
        partition: int | None = None,
    ) -> EcommerceEvent | None:
        """Route an event into every resolution's pane buffer.

        Parameters
        ----------
        event : EcommerceEvent
            Decoded event.
        event_ts_ms : int
            Event time in milliseconds.
        watermark_ms : int or None
            Current watermark; ``None`` before the first watermark is known.
        partition : int or None, optional
            Source Kafka partition, recorded so a rebalance can drop the user.

        Returns
        -------
        EcommerceEvent or None
            The event itself when it is *very late* (older than
            ``watermark − lateness``) and must go to the late sink instead of a
            pane; otherwise ``None`` (design doc §2.6).
        """
        if watermark_ms is not None and event_ts_ms < watermark_ms - self._lateness_ms:
            return event

        if partition is not None:
            self._user_partition[event.user_id] = partition
        windows = self._windows_for(event.user_id)
        for resolution, paned in windows.items():
            paned.add(event, event_ts_ms)
            if watermark_ms is not None:
                self._mark_late_refires(
                    event.user_id, resolution, paned, event_ts_ms, watermark_ms
                )
        return None

    def _mark_late_refires(
        self,
        user_id: str,
        resolution: WindowResolution,
        paned: PanedSlidingWindow,
        event_ts_ms: int,
        watermark_ms: int,
    ) -> None:
        """Flag already-fired windows that a within-lateness event re-opens.

        Parameters
        ----------
        user_id : str
            User the event belongs to.
        resolution : WindowResolution
            Resolution being updated.
        paned : PanedSlidingWindow
            The user's pane buffer for *resolution*.
        event_ts_ms : int
            Event time in milliseconds.
        watermark_ms : int
            Current watermark in milliseconds.
        """
        for window_end in paned.window_ends_including(event_ts_ms):
            if window_end <= watermark_ms:
                self._dirty.add((user_id, resolution, window_end))

    def emit_due_windows(self, watermark_ms: int) -> list[SlidingFeatureRecord]:
        """Fire every window whose end the watermark has crossed.

        Parameters
        ----------
        watermark_ms : int
            Current watermark in milliseconds.

        Returns
        -------
        list of SlidingFeatureRecord
            Records for newly-closed windows plus any allowed-lateness re-fires,
            each stamped with its ``emission_seq`` (design doc §4.3).
        """
        records: list[SlidingFeatureRecord] = []
        for resolution, cursor in self._cursors.items():
            slide_ms = resolution.slide_ms
            if cursor.next_end_ms is None:
                cursor.next_end_ms = (watermark_ms // slide_ms) * slide_ms
            while watermark_ms >= cursor.next_end_ms:
                end = cursor.next_end_ms
                records.extend(self._fire_window(resolution, end))
                cursor.next_end_ms += slide_ms
        records.extend(self._emit_dirty())
        self._gc(watermark_ms)
        return records

    def _fire_window(
        self, resolution: WindowResolution, window_end_ms: int
    ) -> list[SlidingFeatureRecord]:
        """Emit one window-end across all active users (sparsity-aware).

        Parameters
        ----------
        resolution : WindowResolution
            Resolution being fired.
        window_end_ms : int
            Window-end boundary.

        Returns
        -------
        list of SlidingFeatureRecord
            One record per active user with a contributing pane.
        """
        out: list[SlidingFeatureRecord] = []
        for user_id, windows in self._users.items():
            record = windows[resolution].window_record(window_end_ms)
            if record is None:  # sparsity (design doc §2.7)
                continue
            record.emission_seq = self._next_seq(user_id, resolution, window_end_ms)
            out.append(record)
        return out

    def _emit_dirty(self) -> list[SlidingFeatureRecord]:
        """Re-fire windows flagged by within-lateness late events.

        Returns
        -------
        list of SlidingFeatureRecord
            Re-emitted records (with incremented ``emission_seq``); the dirty
            set is cleared.

        Notes
        -----
        Every dirty entry is invariant-guaranteed to reference a live user with
        a contributing pane: entries are added in :meth:`_mark_late_refires`
        (immediately after the event's pane is folded in) and purged for any
        user dropped by :meth:`drop_partitions`, and re-fires run before GC.
        """
        out: list[SlidingFeatureRecord] = []
        for user_id, resolution, end in sorted(
            self._dirty, key=lambda k: (k[0], k[1].value, k[2])
        ):
            record = self._users[user_id][resolution].window_record(end)
            record.emission_seq = self._next_seq(user_id, resolution, end)
            out.append(record)
        self._dirty.clear()
        return out

    def _next_seq(
        self, user_id: str, resolution: WindowResolution, window_end_ms: int
    ) -> int:
        """Return and advance the ``emission_seq`` for a window (design §2.9).

        Parameters
        ----------
        user_id : str
            User identifier.
        resolution : WindowResolution
            Resolution being fired.
        window_end_ms : int
            Window-end boundary.

        Returns
        -------
        int
            ``0`` on the first fire of the window; ``+1`` per subsequent fire.
        """
        key = (user_id, resolution, window_end_ms)
        seq = self._seq.get(key, 0)
        self._seq[key] = seq + 1
        return seq

    def _gc(self, watermark_ms: int) -> None:
        """Garbage-collect aged-out panes and stale ``emission_seq`` entries.

        Parameters
        ----------
        watermark_ms : int
            Current watermark in milliseconds.
        """
        for windows in self._users.values():
            for resolution, paned in windows.items():
                paned.gc(watermark_ms, self._lateness_ms)
        # Prune seq counters for windows that can no longer be re-fired.
        stale = [
            key
            for key in self._seq
            if key[2] < watermark_ms - key[1].window_size_ms - self._lateness_ms
        ]
        for key in stale:
            del self._seq[key]

    def drop_partitions(self, partitions: set[int]) -> int:
        """Drop the in-memory state of users owned by *partitions* (design §2.12).

        Parameters
        ----------
        partitions : set of int
            Partitions being revoked from this process.

        Returns
        -------
        int
            Number of users dropped.
        """
        victims = [
            user_id
            for user_id, part in self._user_partition.items()
            if part in partitions
        ]
        for user_id in victims:
            self._users.pop(user_id, None)
            self._user_partition.pop(user_id, None)
            self._seq = {k: v for k, v in self._seq.items() if k[0] != user_id}
            self._dirty = {k for k in self._dirty if k[0] != user_id}
        return len(victims)
