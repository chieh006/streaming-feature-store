"""Unit tests for :class:`TokenBucketPacer`."""

from __future__ import annotations

import threading
import time

import pytest

from streaming_feature_store.load.pacer import TokenBucketPacer


class FakeClock:
    """Manual monotonic clock for deterministic tests."""

    def __init__(self, start: float = 0.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def test_acquire_one_returns_immediately_when_full():
    clock = FakeClock()
    pacer = TokenBucketPacer(target_rate=1000.0, burst=10, clock=clock)
    pacer.acquire(1)  # should not block


def test_acquire_burst_consumes_all_tokens():
    clock = FakeClock()
    pacer = TokenBucketPacer(target_rate=1000.0, burst=10, clock=clock)
    pacer.acquire(10)
    # next acquire should require refill
    done = threading.Event()

    def runner():
        pacer.acquire(1)
        done.set()

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    time.sleep(0.05)
    assert not done.is_set()
    clock.advance(1.0)  # refills 1000 tokens
    with pacer._cond:  # wake the waiter
        pacer._cond.notify_all()
    t.join(timeout=1.0)
    assert done.is_set()


def test_target_rate_none_never_blocks():
    pacer = TokenBucketPacer(target_rate=None, burst=4)
    pacer.acquire(10**6)  # no-op


def test_acquire_zero_is_noop():
    pacer = TokenBucketPacer(target_rate=1000.0, burst=10)
    pacer.acquire(0)


def test_acquire_more_than_burst_raises():
    pacer = TokenBucketPacer(target_rate=1000.0, burst=10)
    with pytest.raises(ValueError):
        pacer.acquire(11)


def test_acquire_negative_raises():
    pacer = TokenBucketPacer(target_rate=1000.0, burst=10)
    with pytest.raises(ValueError):
        pacer.acquire(-1)


def test_invalid_burst_raises():
    with pytest.raises(ValueError):
        TokenBucketPacer(target_rate=1000.0, burst=0)


def test_invalid_target_rate_raises():
    with pytest.raises(ValueError):
        TokenBucketPacer(target_rate=0.0, burst=10)


def test_concurrent_acquires_serialized():
    pacer = TokenBucketPacer(target_rate=10_000.0, burst=2_000)
    counter = {"n": 0}
    lock = threading.Lock()

    def runner():
        for _ in range(500):
            pacer.acquire(1)
            with lock:
                counter["n"] += 1

    threads = [threading.Thread(target=runner) for _ in range(4)]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - t0
    assert counter["n"] == 2000
    # 2000 events at 10K/s + 2K burst -> well under 1 s; allow generous slack.
    assert elapsed < 2.0


def test_properties_exposed():
    pacer = TokenBucketPacer(target_rate=500.0, burst=64)
    assert pacer.target_rate == 500.0
    assert pacer.burst == 64
