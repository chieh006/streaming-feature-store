"""Unit tests for :class:`SinkAccountant` (no external dependencies)."""

from __future__ import annotations

import pytest

from streaming_feature_store.sink.accountant import (
    _BATCH_HIST_BUCKETS,
    SinkAccountant,
    _histogram_increment,
)


class StepClock:
    """Monotonic stub: returns ``start, start+step, start+2*step, ...``."""

    def __init__(self, step: float = 0.5, start: float = 0.0) -> None:
        self.t = start
        self.step = step

    def __call__(self) -> float:
        v = self.t
        self.t += self.step
        return v


# --- construction & validation ----------------------------------------------


def test_sink_accountant_rejects_zero_reservoir() -> None:
    with pytest.raises(ValueError):
        SinkAccountant(reservoir_size=0)


def test_sink_accountant_rejects_non_positive_threshold() -> None:
    with pytest.raises(ValueError):
        SinkAccountant(skew_threshold=0.0)


def test_sink_accountant_threshold_property() -> None:
    acc = SinkAccountant(skew_threshold=1.5)
    assert acc.skew_threshold == 1.5


# --- partition tracking -----------------------------------------------------


def test_sink_accountant_partition_counts_tracked() -> None:
    acc = SinkAccountant()
    for p in [0, 0, 1, 3, 3, 3]:
        acc.record_partition(p)
    snap = acc.snapshot()
    assert snap.partition_counts == {0: 2, 1: 1, 3: 3}


def test_sink_accountant_skew_ratio_uniform() -> None:
    acc = SinkAccountant()
    for p in range(12):
        for _ in range(100):
            acc.record_partition(p)
    snap = acc.snapshot()
    assert snap.partition_skew_ratio == pytest.approx(1.0)
    assert snap.partition_skew_pass is True


def test_sink_accountant_skew_ratio_pathological() -> None:
    acc = SinkAccountant()
    # 1100 in partition 0; ~9 per partition spread across 11 others.
    for _ in range(1100):
        acc.record_partition(0)
    for p in range(1, 12):
        for _ in range(9):
            acc.record_partition(p)
    snap = acc.snapshot()
    assert snap.partition_skew_ratio > 5.0
    assert snap.partition_skew_pass is False


def test_sink_accountant_no_partitions_returns_zero_skew() -> None:
    snap = SinkAccountant().snapshot()
    assert snap.partition_skew_ratio == 0.0
    # An empty distribution trivially passes — it's a degenerate case.
    assert snap.partition_skew_pass is True


# --- flush accounting -------------------------------------------------------


def test_sink_accountant_conflict_skip_increments() -> None:
    acc = SinkAccountant()
    acc.record_flush(inserted=95, skipped=5, batch_size=100, latency_ms=1.0)
    acc.record_flush(inserted=97, skipped=3, batch_size=100, latency_ms=1.0)
    snap = acc.snapshot()
    assert snap.conflict_skipped == 8
    assert snap.inserted == 192
    assert snap.batches_flushed == 2


def test_sink_accountant_record_flush_rejects_negative() -> None:
    acc = SinkAccountant()
    with pytest.raises(ValueError):
        acc.record_flush(inserted=-1, skipped=0, batch_size=1, latency_ms=1.0)
    with pytest.raises(ValueError):
        acc.record_flush(inserted=1, skipped=-1, batch_size=1, latency_ms=1.0)
    with pytest.raises(ValueError):
        acc.record_flush(inserted=1, skipped=0, batch_size=-1, latency_ms=1.0)
    with pytest.raises(ValueError):
        acc.record_flush(inserted=1, skipped=0, batch_size=1, latency_ms=-0.5)


def test_sink_accountant_records_consumed_and_failed() -> None:
    acc = SinkAccountant()
    acc.record_consumed(10)
    acc.record_consumed()  # default n=1
    acc.record_deserialize_failure()
    acc.record_deserialize_failure()
    snap = acc.snapshot()
    assert snap.consumed == 11
    assert snap.deserialize_failed == 2


def test_sink_accountant_percentiles_from_flushes() -> None:
    acc = SinkAccountant()
    for ms in [1.0, 2.0, 3.0, 4.0, 100.0]:
        acc.record_flush(inserted=1, skipped=0, batch_size=10, latency_ms=ms)
    snap = acc.snapshot()
    assert snap.flush_latency_ms_p50 == pytest.approx(3.0)
    # p99 is at or near the maximum
    assert snap.flush_latency_ms_p99 >= 90.0


def test_sink_accountant_empty_percentiles_zero() -> None:
    snap = SinkAccountant().snapshot()
    assert snap.flush_latency_ms_p50 == 0.0
    assert snap.flush_latency_ms_p95 == 0.0
    assert snap.flush_latency_ms_p99 == 0.0
    assert snap.batch_size_p50 == 0.0
    assert snap.batch_size_p99 == 0.0


def test_sink_accountant_wallclock_uses_clock() -> None:
    clock = StepClock(step=0.1)
    acc = SinkAccountant(clock=clock)
    # one step on t0 already consumed; advance by one snapshot call
    snap = acc.snapshot()
    assert snap.wallclock_s > 0.0


# --- reservoir replacement path ---------------------------------------------


def test_sink_accountant_reservoir_replaces_when_full() -> None:
    acc = SinkAccountant(reservoir_size=4, seed=0)
    for ms in [1.0, 2.0, 3.0, 4.0]:
        acc.record_flush(inserted=1, skipped=0, batch_size=1, latency_ms=ms)
    # Now push more samples — reservoir capped at 4, replacement RNG-driven.
    for ms in [5.0, 6.0, 7.0, 8.0, 9.0, 10.0]:
        acc.record_flush(inserted=1, skipped=0, batch_size=1, latency_ms=ms)
    snap = acc.snapshot()
    assert snap.batches_flushed == 10


# --- histogram helper -------------------------------------------------------


def test_histogram_increment_assigns_to_right_bucket() -> None:
    counts = [0] * len(_BATCH_HIST_BUCKETS)
    _histogram_increment(_BATCH_HIST_BUCKETS, counts, 1)
    _histogram_increment(_BATCH_HIST_BUCKETS, counts, 999)
    _histogram_increment(_BATCH_HIST_BUCKETS, counts, 99_999_999)
    assert sum(counts) == 3
    # Final bucket absorbs overflow values.
    assert counts[-1] >= 1
