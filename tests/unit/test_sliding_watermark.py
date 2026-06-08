"""Unit tests for :mod:`streaming_feature_store.sliding.watermark`."""

from __future__ import annotations

from streaming_feature_store.sliding.watermark import WatermarkTracker


class _FakeClock:
    """Manually-advanced monotonic clock (seconds)."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_watermark_none_before_first_event() -> None:
    tracker = WatermarkTracker(out_of_orderness_ms=5_000, idleness_ms=30_000)
    assert tracker.watermark_ms(now_wallclock_ms=1_000_000) is None


def test_watermark_is_max_event_ts_minus_skew() -> None:
    tracker = WatermarkTracker(out_of_orderness_ms=5_000, idleness_ms=30_000)
    tracker.observe(10_000)
    # now_wallclock is irrelevant while not idle.
    assert tracker.watermark_ms(now_wallclock_ms=0) == 5_000


def test_watermark_tracks_the_maximum_event_ts() -> None:
    tracker = WatermarkTracker(out_of_orderness_ms=1_000, idleness_ms=30_000)
    tracker.observe(10_000)
    tracker.observe(4_000)  # out-of-order, lower — must not lower the watermark
    assert tracker.watermark_ms(now_wallclock_ms=0) == 9_000


def test_watermark_idleness_fallback_advances_toward_wallclock() -> None:
    clock = _FakeClock()
    tracker = WatermarkTracker(
        out_of_orderness_ms=5_000, idleness_ms=30_000, clock=clock
    )
    tracker.observe(10_000)  # last event at clock t=0
    clock.now = 60.0  # 60 s of wall-clock idleness > 30 s threshold
    watermark = tracker.watermark_ms(now_wallclock_ms=1_000_000)
    # max_event - skew = 5_000; fallback raises it toward now - skew = 995_000.
    assert watermark == 995_000


def test_watermark_no_fallback_before_idleness_threshold() -> None:
    clock = _FakeClock()
    tracker = WatermarkTracker(
        out_of_orderness_ms=5_000, idleness_ms=30_000, clock=clock
    )
    tracker.observe(10_000)
    clock.now = 10.0  # only 10 s idle < 30 s threshold
    assert tracker.watermark_ms(now_wallclock_ms=1_000_000) == 5_000
