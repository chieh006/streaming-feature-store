"""Unit tests for :class:`ConsumeAccountant`."""

from __future__ import annotations

import threading

import pytest

from streaming_feature_store.consume.accountant import (
    ConsumeAccountant,
    ConsumeSnapshot,
    _detect_lag_ramp,
)


def test_record_increments_consumed() -> None:
    a = ConsumeAccountant()
    a.record(e2e_latency_s=0.01)
    assert a.consumed == 1
    assert a.snapshot().consumed == 1


def test_negative_latency_counts_but_is_not_sampled() -> None:
    """A timestamp-unavailable record still counts; it is not in the reservoir."""
    a = ConsumeAccountant()
    a.record(e2e_latency_s=-1.0)
    assert a.consumed == 1
    assert a.e2e_samples_s() == []


def test_deserialize_error_classified() -> None:
    a = ConsumeAccountant()
    a.record_deserialize_error("ValueError")
    snap = a.snapshot()
    assert snap.errors_by_class["ValueError"] == 1
    assert snap.deserialize_failed == 1


def test_latency_reservoir_bounded() -> None:
    a = ConsumeAccountant()
    for _ in range(100_000):
        a.record(e2e_latency_s=0.001)
    assert len(a.e2e_samples_s()) == 4096


def test_latency_percentiles_monotonic() -> None:
    a = ConsumeAccountant()
    for i in range(2000):
        a.record(e2e_latency_s=(i + 1) * 0.0001)
    snap = a.snapshot()
    assert snap.e2e_p50_ms <= snap.e2e_p95_ms <= snap.e2e_p99_ms


def test_lag_ramped_true_on_rising_series() -> None:
    a = ConsumeAccountant()
    for lag in range(0, 5000, 100):
        a.sample_lag(lag)
    snap = a.snapshot()
    assert snap.lag_ramped is True
    assert snap.max_lag == 4900
    assert snap.end_lag == 4900


def test_lag_ramped_false_on_flat_series() -> None:
    a = ConsumeAccountant()
    for _ in range(20):
        a.sample_lag(0)
    snap = a.snapshot()
    assert snap.lag_ramped is False
    assert snap.max_lag == 0
    assert snap.end_lag == 0


def test_lag_ramped_false_on_constant_nonzero_series() -> None:
    """A high-but-flat lag is *not* ramping (it is keeping up at a backlog)."""
    a = ConsumeAccountant()
    for _ in range(20):
        a.sample_lag(500)
    assert a.snapshot().lag_ramped is False


def test_lag_ramped_false_when_drained_after_rising() -> None:
    """Rise then return to ~0 (the N-member drain) is not a ramp."""
    series = [0, 800, 1500, 1200, 600, 200, 50, 0, 0, 0]
    a = ConsumeAccountant()
    for lag in series:
        a.sample_lag(lag)
    assert a.snapshot().lag_ramped is False


def test_snapshot_is_frozen() -> None:
    a = ConsumeAccountant()
    snap = a.snapshot()
    assert isinstance(snap, ConsumeSnapshot)
    with pytest.raises(Exception):
        snap.consumed = 99  # type: ignore[misc]


def test_e2e_samples_s_returns_copy() -> None:
    a = ConsumeAccountant()
    a.record(e2e_latency_s=0.005)
    samples = a.e2e_samples_s()
    samples.append(0.999)
    assert a.e2e_samples_s() == [0.005]


def test_invalid_reservoir_size() -> None:
    with pytest.raises(ValueError):
        ConsumeAccountant(reservoir_size=0)


def test_thread_safety_under_contention() -> None:
    a = ConsumeAccountant()
    n, each = 8, 10_000

    def worker() -> None:
        for _ in range(each):
            a.record(e2e_latency_s=0.001)
            a.record_deserialize_error("ValueError")

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    snap = a.snapshot()
    assert snap.consumed == n * each
    assert snap.deserialize_failed == n * each
    assert snap.errors_by_class["ValueError"] == n * each


@pytest.mark.parametrize(
    "samples, expected",
    [
        ([], False),
        ([10, 20], False),  # fewer than the minimum sample count
        ([0, 0, 0, 0], False),
        ([5, 5, 5, 5, 5], False),
        ([0, 100, 200, 300, 400, 500], True),
        ([0, 50, 200, 120, 30, 0, 0], False),
    ],
)
def test_detect_lag_ramp_cases(samples, expected) -> None:
    assert _detect_lag_ramp(samples) is expected
