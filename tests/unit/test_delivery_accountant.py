"""Unit tests for :class:`DeliveryAccountant`."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaError

from streaming_feature_store.load.accountant import (
    AccountantSnapshot,
    DeliveryAccountant,
)


def _err(code) -> KafkaError:
    """Return a fake KafkaError with a stable ``name``."""
    err = MagicMock(spec=KafkaError)
    err.name.return_value = code
    return err


def _msg(latency_s: float | None = 0.001) -> MagicMock:
    """Return a fake delivered message."""
    m = MagicMock()
    m.latency.return_value = latency_s
    return m


def test_record_produced_increments_in_flight():
    a = DeliveryAccountant()
    a.record_produced()
    assert a.in_flight == 1


def test_record_success_increments_acked():
    a = DeliveryAccountant()
    a.record_produced()
    a.record(None, _msg(0.002))
    snap = a.snapshot()
    assert snap.acked == 1
    assert snap.in_flight == 0


def test_record_error_increments_failed_and_classifies():
    a = DeliveryAccountant()
    a.record_produced()
    a.record(_err("_TIMED_OUT"), None)
    snap = a.snapshot()
    assert snap.failed == 1
    assert snap.errors_by_class["_TIMED_OUT"] == 1


def test_wait_for_in_flight_below_returns_when_threshold_met():
    a = DeliveryAccountant()
    for _ in range(10):
        a.record_produced()
    fired = threading.Event()

    def waiter():
        a.wait_for_in_flight_below(5, timeout_s=2.0)
        fired.set()

    t = threading.Thread(target=waiter, daemon=True)
    t.start()
    for _ in range(6):
        a.record(None, _msg(0.001))
    t.join(timeout=2.0)
    assert fired.is_set()


def test_latency_reservoir_size_bounded():
    a = DeliveryAccountant(reservoir_size=64)
    for _ in range(5_000):
        a.record_produced()
        a.record(None, _msg(0.001))
    assert len(a._reservoir) == 64


def test_latency_percentiles_monotonic():
    a = DeliveryAccountant(reservoir_size=4096)
    for i in range(2000):
        a.record_produced()
        a.record(None, _msg((i + 1) * 0.0001))
    snap = a.snapshot()
    assert snap.ack_latency_p50_ms <= snap.ack_latency_p95_ms <= snap.ack_latency_p99_ms


def test_snapshot_consistency():
    a = DeliveryAccountant()
    for _ in range(10):
        a.record_produced()
    a.record(None, _msg(0.001))
    a.record(_err("_FAIL"), None)
    snap = a.snapshot()
    assert snap.produced == snap.acked + snap.failed + snap.in_flight


def test_snapshot_is_immutable():
    a = DeliveryAccountant()
    snap = a.snapshot()
    assert isinstance(snap, AccountantSnapshot)
    with pytest.raises(Exception):
        snap.produced = 99  # type: ignore[misc]


def test_thread_safety_under_contention():
    a = DeliveryAccountant()
    n = 8
    each = 1000

    def worker():
        for _ in range(each):
            a.record_produced()
            a.record(None, _msg(0.001))

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    snap = a.snapshot()
    assert snap.produced == n * each
    assert snap.acked == n * each
    assert snap.in_flight == 0


def test_invalid_reservoir_size():
    with pytest.raises(ValueError):
        DeliveryAccountant(reservoir_size=0)


def test_record_handles_message_without_latency():
    a = DeliveryAccountant()
    a.record_produced()
    bad = MagicMock()
    bad.latency.side_effect = RuntimeError("nope")
    a.record(None, bad)
    snap = a.snapshot()
    assert snap.acked == 1
