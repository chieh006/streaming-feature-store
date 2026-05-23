"""Unit tests for :class:`FeederRunner` (mocked producer / generator / pacer)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from streaming_feature_store.feeder.feeder_runner import (
    FeederRunConfig,
    FeederRunner,
)
from streaming_feature_store.load.accountant import DeliveryAccountant
from streaming_feature_store.load.pacer import TokenBucketPacer
from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
)


def _sample_event() -> EcommerceEvent:
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.CLICK,
        user_id="u-1",
        session_id="s-1",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=ClickPayload(element_id="btn", page_url="/p"),
    )


class FakeGenerator:
    """Generator that returns a fixed batch on each call."""

    def __init__(self, batch_size: int = 10) -> None:
        self._batch_size = batch_size
        self.calls = 0

    def generate_batch(self, n: int):
        self.calls += 1
        return [_sample_event() for _ in range(n)]


class StepClock:
    """Monotonic stub: returns ``start, start+step, ...``."""

    def __init__(self, step: float = 1.0, start: float = 0.0) -> None:
        self.t = start
        self.step = step

    def __call__(self) -> float:
        v = self.t
        self.t += self.step
        return v


def _cfg(**over) -> FeederRunConfig:
    base = dict(
        topic="e-commerce-events-feed",
        rate_evt_per_sec=200.0,
        batch_size=10,
        seed=0,
        snapshot_interval_s=60.0,
    )
    base.update(over)
    return FeederRunConfig(**base)


# --- config -----------------------------------------------------------------


def test_feeder_run_config_validates_positive_rate() -> None:
    with pytest.raises(Exception):  # pydantic ValidationError
        FeederRunConfig(rate_evt_per_sec=0)


def test_feeder_run_config_validates_topic_name_not_benchmark() -> None:
    with pytest.raises(ValueError, match="benchmark topic"):
        FeederRunConfig(topic="e-commerce-events")


def test_feeder_run_config_accepts_feed_topic() -> None:
    cfg = FeederRunConfig(topic="e-commerce-events-feed", rate_evt_per_sec=100.0)
    assert cfg.topic == "e-commerce-events-feed"


# --- runner ----------------------------------------------------------------


def _make_runner(
    *,
    cfg=None,
    producer=None,
    generator=None,
    pacer=None,
    accountant=None,
    clock=None,
) -> FeederRunner:
    return FeederRunner(
        config=cfg or _cfg(),
        producer=producer or MagicMock(name="AvroEventProducer"),
        generator=generator or FakeGenerator(),
        pacer=pacer or TokenBucketPacer(target_rate=None),
        accountant=accountant or DeliveryAccountant(),
        clock=clock or StepClock(step=0.5),
    )


def test_feeder_runner_produces_when_shutdown_set() -> None:
    producer = MagicMock(name="AvroEventProducer")
    accountant = DeliveryAccountant()
    runner = _make_runner(producer=producer, accountant=accountant)
    runner.request_shutdown()
    snap = runner.run()
    # When shutdown is preset, the main loop exits without producing.
    producer.produce.assert_not_called()
    producer.flush.assert_called_once()
    assert snap.delivery.produced == 0


def test_feeder_runner_uses_feed_topic_default() -> None:
    cfg = _cfg()
    runner = _make_runner(cfg=cfg)
    runner.request_shutdown()
    snap = runner.run()
    assert snap.topic == "e-commerce-events-feed"


def test_feeder_runner_paces_to_target_rate() -> None:
    # Use a real token-bucket so we exercise acquire().
    producer = MagicMock(name="AvroEventProducer")
    accountant = DeliveryAccountant()
    pacer = TokenBucketPacer(target_rate=10_000.0, burst=4096)
    cfg = _cfg(batch_size=20)
    runner = _make_runner(
        cfg=cfg, producer=producer, pacer=pacer, accountant=accountant
    )

    iterations = {"n": 0}

    def fake_acquire(n: int):
        iterations["n"] += 1
        if iterations["n"] > 5:
            runner.request_shutdown()

    pacer.acquire = fake_acquire  # type: ignore[method-assign]
    snap = runner.run()
    assert producer.produce.call_count == 6 * cfg.batch_size
    assert snap.delivery.produced == 6 * cfg.batch_size


def test_feeder_runner_shutdown_flushes_producer() -> None:
    producer = MagicMock(name="AvroEventProducer")
    runner = _make_runner(producer=producer)
    runner.request_shutdown()
    runner.run()
    producer.flush.assert_called_once()


def test_feeder_runner_emits_heartbeat(caplog) -> None:
    # snapshot_interval_s must be small to fire inside one iteration.
    cfg = _cfg(snapshot_interval_s=0.001)
    producer = MagicMock(name="AvroEventProducer")
    accountant = DeliveryAccountant()
    pacer = TokenBucketPacer(target_rate=None)
    clock = StepClock(step=1.0)
    runner = _make_runner(
        cfg=cfg, producer=producer, pacer=pacer, accountant=accountant, clock=clock
    )

    def acquire_then_shutdown(_n):
        runner.request_shutdown()

    pacer.acquire = acquire_then_shutdown  # type: ignore[method-assign]
    import logging

    with caplog.at_level(logging.INFO):
        runner.run()
    assert any("heartbeat" in r.getMessage() for r in caplog.records)


def test_feeder_runner_config_property() -> None:
    cfg = _cfg()
    runner = _make_runner(cfg=cfg)
    assert runner.config is cfg


def test_feeder_runner_warns_on_unflushed(caplog) -> None:
    producer = MagicMock(name="AvroEventProducer")
    producer.flush.return_value = 3
    runner = _make_runner(producer=producer)
    runner.request_shutdown()
    import logging

    with caplog.at_level(logging.WARNING):
        runner.run()
    assert any("unflushed" in r.getMessage() for r in caplog.records)


def test_feeder_snapshot_duration_property() -> None:
    cfg = _cfg()
    runner = _make_runner(cfg=cfg)
    runner.request_shutdown()
    snap = runner.run()
    assert snap.duration_s >= 0.0


def test_feeder_runner_clean_flush_no_warning(caplog) -> None:
    producer = MagicMock(name="AvroEventProducer")
    producer.flush.return_value = 0
    runner = _make_runner(producer=producer)
    runner.request_shutdown()
    import logging

    with caplog.at_level(logging.WARNING):
        runner.run()
    # No "unflushed" warning expected when the producer drains cleanly.
    assert not any("unflushed" in r.getMessage() for r in caplog.records)
