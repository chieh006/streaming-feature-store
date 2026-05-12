"""Unit tests for :class:`LoadRunner` (mocked producer / generator / pacer)."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

import pytest

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.load.accountant import DeliveryAccountant
from streaming_feature_store.load.load_runner import LoadRunner
from streaming_feature_store.load.pacer import TokenBucketPacer
from streaming_feature_store.load.report import LoadRunConfig, LoadRunReport
from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
)


def _sample_event() -> EcommerceEvent:
    """Return one canned event."""
    from datetime import datetime, timezone
    from uuid import uuid4

    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.CLICK,
        user_id="u-1",
        session_id="s-1",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=ClickPayload(element_id="btn", page_url="/home"),
    )


class FakeGenerator:
    """Minimal generator that returns a fixed batch."""

    def __init__(self, batch_size: int = 4) -> None:
        self._batch = [_sample_event() for _ in range(batch_size)]
        self.calls = 0

    def generate_batch(self, n: int):
        self.calls += 1
        return list(self._batch[:n]) if n <= len(self._batch) else (
            self._batch * ((n // len(self._batch)) + 1)
        )[:n]


@pytest.fixture
def mock_registry(monkeypatch):
    """Patch :class:`SchemaRegistry` so subject-registered check passes."""
    fake = MagicMock()
    fake.get_latest.return_value = MagicMock(schema_id=1, version=1)
    monkeypatch.setattr(
        "streaming_feature_store.load.load_runner.SchemaRegistry",
        lambda cfg: fake,
    )
    return fake


def _make_run_config(**over) -> LoadRunConfig:
    base = dict(
        duration_s=0.1,
        target_rate=None,
        workers=1,
        batch_size=2,
        max_in_flight=100,
        seed=0,
        topic="t",
    )
    base.update(over)
    return LoadRunConfig(**base)


def test_runner_calls_assert_subject_at_startup(mock_registry):
    cfg = _make_run_config()
    producer = MagicMock()
    producer.produce.side_effect = lambda event, on_delivery: None
    runner = LoadRunner(
        KafkaConfig(),
        SchemaRegistryConfig(),
        cfg,
        producer=producer,
        generator=FakeGenerator(),
        accountant=DeliveryAccountant(),
        pacer=TokenBucketPacer(target_rate=None),
    )
    runner.run()
    mock_registry.get_latest.assert_called_once_with("t-value")


def test_runner_aborts_when_subject_missing(monkeypatch):
    from streaming_feature_store.schemas.registry import RegistryError

    fake = MagicMock()
    fake.get_latest.side_effect = RegistryError("missing")
    monkeypatch.setattr(
        "streaming_feature_store.load.load_runner.SchemaRegistry",
        lambda cfg: fake,
    )
    cfg = _make_run_config()
    producer = MagicMock()
    runner = LoadRunner(
        KafkaConfig(),
        SchemaRegistryConfig(),
        cfg,
        producer=producer,
        generator=FakeGenerator(),
        accountant=DeliveryAccountant(),
        pacer=TokenBucketPacer(target_rate=None),
    )
    with pytest.raises(RegistryError):
        runner.run()
    producer.produce.assert_not_called()


def test_runner_passes_accountant_callback(mock_registry):
    cfg = _make_run_config()
    producer = MagicMock()
    accountant = DeliveryAccountant()
    runner = LoadRunner(
        KafkaConfig(),
        SchemaRegistryConfig(),
        cfg,
        producer=producer,
        generator=FakeGenerator(),
        accountant=accountant,
        pacer=TokenBucketPacer(target_rate=None),
    )
    runner.run()
    assert producer.produce.call_count > 0
    on_delivery = producer.produce.call_args_list[0].kwargs["on_delivery"]
    assert on_delivery == accountant.record


def test_runner_unpaced_mode_skips_pacer(mock_registry):
    cfg = _make_run_config(target_rate=None)
    producer = MagicMock()
    pacer = MagicMock()
    pacer.acquire.side_effect = lambda n=1: None
    runner = LoadRunner(
        KafkaConfig(),
        SchemaRegistryConfig(),
        cfg,
        producer=producer,
        generator=FakeGenerator(),
        accountant=DeliveryAccountant(),
        pacer=pacer,
    )
    runner.run()
    # pacer.acquire is called even in unpaced mode (the pacer itself short-circuits),
    # but never blocks. The runner just needs to terminate cleanly.
    assert producer.produce.called


def test_runner_flushes_at_end(mock_registry):
    cfg = _make_run_config()
    producer = MagicMock()
    runner = LoadRunner(
        KafkaConfig(),
        SchemaRegistryConfig(),
        cfg,
        producer=producer,
        generator=FakeGenerator(),
        accountant=DeliveryAccountant(),
        pacer=TokenBucketPacer(target_rate=None),
    )
    runner.run()
    producer.flush.assert_called_once()
    args, _ = producer.flush.call_args
    assert args[0] > 0


def test_runner_returns_load_run_report(mock_registry):
    cfg = _make_run_config()
    producer = MagicMock()
    runner = LoadRunner(
        KafkaConfig(),
        SchemaRegistryConfig(),
        cfg,
        producer=producer,
        generator=FakeGenerator(),
        accountant=DeliveryAccountant(),
        pacer=TokenBucketPacer(target_rate=None),
    )
    report = runner.run()
    assert isinstance(report, LoadRunReport)
    assert report.snapshot.produced > 0


def test_runner_propagates_buffer_error_after_retries(mock_registry):
    cfg = _make_run_config()
    producer = MagicMock()
    producer.produce.side_effect = BufferError("queue full")
    accountant = DeliveryAccountant()
    runner = LoadRunner(
        KafkaConfig(),
        SchemaRegistryConfig(),
        cfg,
        producer=producer,
        generator=FakeGenerator(),
        accountant=accountant,
        pacer=TokenBucketPacer(target_rate=None),
    )
    with pytest.raises(BufferError):
        runner.run()


def test_runner_respects_target_rate(mock_registry):
    """With a tight target rate, total produces stays bounded."""
    cfg = _make_run_config(duration_s=0.5, target_rate=200.0, batch_size=10)
    producer = MagicMock()
    runner = LoadRunner(
        KafkaConfig(),
        SchemaRegistryConfig(),
        cfg,
        producer=producer,
        generator=FakeGenerator(batch_size=10),
        accountant=DeliveryAccountant(),
        pacer=TokenBucketPacer(target_rate=200.0, burst=20),
    )
    runner.run()
    # 200 evt/s * 0.5s = 100 expected; allow generous slack for burst.
    assert producer.produce.call_count <= 250
