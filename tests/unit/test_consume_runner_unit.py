"""Unit tests for :class:`ConsumeRunner` (mocked consumer / registry)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from confluent_kafka import TIMESTAMP_CREATE_TIME, TIMESTAMP_NOT_AVAILABLE

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consume.accountant import ConsumeAccountant
from streaming_feature_store.consume.consume_runner import ConsumeRunner
from streaming_feature_store.consume.report import ConsumeRunConfig, ConsumeRunReport
from streaming_feature_store.schemas.registry import RegistryError


class FakeMsg:
    """Minimal stand-in for a confluent_kafka ``Message``."""

    def __init__(self, ts_type: int, ts_ms: int, value: dict | None = None) -> None:
        self._ts = (ts_type, ts_ms)
        self._v = value if value is not None else {"k": "v"}

    def timestamp(self):
        return self._ts

    def value(self):
        return self._v


class StepClock:
    """Monotonic stub: returns ``start, start+step, start+2*step, ...``."""

    def __init__(self, step: float = 0.04, start: float = 0.0) -> None:
        self.t = start
        self.step = step

    def __call__(self) -> float:
        v = self.t
        self.t += self.step
        return v


@pytest.fixture
def mock_registry(monkeypatch):
    """Patch :class:`SchemaRegistry` so the subject-registered guard passes."""
    fake = MagicMock()
    fake.get_latest.return_value = MagicMock(schema_id=1, version=1)
    monkeypatch.setattr(
        "streaming_feature_store.consume.consume_runner.SchemaRegistry",
        lambda cfg: fake,
    )
    return fake


def _cfg(**over) -> ConsumeRunConfig:
    base = dict(duration_s=0.1, group_id="g", topic="t")
    base.update(over)
    return ConsumeRunConfig(**base)


def _runner(
    consumer,
    accountant,
    *,
    cfg=None,
    clock=None,
    monotonic=None,
) -> ConsumeRunner:
    return ConsumeRunner(
        KafkaConfig(),
        SchemaRegistryConfig(),
        cfg or _cfg(),
        consumer=consumer,
        accountant=accountant,
        clock=clock or (lambda: 1000.0),
        monotonic=monotonic or StepClock(),
    )


def _consumer(**over) -> MagicMock:
    c = MagicMock(name="AvroEventConsumer")
    c.poll_batch.return_value = over.get("poll", [])
    c.consumer_lag.return_value = over.get("lag", 100)
    c.assigned_partitions.return_value = over.get("partitions", [0, 1])
    return c


# --- §5.2 table -----------------------------------------------------------


def test_runner_aborts_when_subject_missing(monkeypatch) -> None:
    fake = MagicMock()
    fake.get_latest.side_effect = RegistryError("missing")
    monkeypatch.setattr(
        "streaming_feature_store.consume.consume_runner.SchemaRegistry",
        lambda cfg: fake,
    )
    consumer = _consumer()
    runner = _runner(consumer, ConsumeAccountant())
    with pytest.raises(RegistryError):
        runner.run()
    consumer.poll_batch.assert_not_called()
    consumer.subscribe.assert_not_called()


def test_runner_subscribes_before_first_poll(mock_registry) -> None:
    consumer = _consumer(poll=[])
    manager = MagicMock()
    manager.attach_mock(consumer.subscribe, "subscribe")
    manager.attach_mock(consumer.poll_batch, "poll_batch")
    _runner(consumer, ConsumeAccountant()).run()
    names = [c[0] for c in manager.mock_calls]
    assert names.index("subscribe") < names.index("poll_batch")
    assert consumer.subscribe.call_count == 1


def test_runner_commits_after_each_processed_batch(mock_registry) -> None:
    msg = FakeMsg(TIMESTAMP_CREATE_TIME, 999_000)
    consumer = _consumer()
    consumer.poll_batch.side_effect = [[msg], [msg]]
    accountant = MagicMock(wraps=ConsumeAccountant())
    manager = MagicMock()
    manager.attach_mock(accountant.record, "record")
    manager.attach_mock(consumer.commit, "commit")
    _runner(
        consumer, accountant, cfg=_cfg(deserialize_mode="raw")
    ).run()
    assert consumer.commit.call_count == 2
    names = [c[0] for c in manager.mock_calls]
    assert names.index("record") < names.index("commit")


def test_runner_never_commits_when_processing_raises(mock_registry) -> None:
    msg = FakeMsg(TIMESTAMP_CREATE_TIME, 999_000)
    consumer = _consumer()
    consumer.poll_batch.return_value = [msg]
    accountant = MagicMock()
    accountant.record.side_effect = RuntimeError("boom")
    runner = _runner(consumer, accountant, cfg=_cfg(deserialize_mode="raw"))
    with pytest.raises(RuntimeError, match="boom"):
        runner.run()
    consumer.commit.assert_not_called()
    consumer.close.assert_called_once()


def test_runner_records_e2e_from_msg_timestamp(mock_registry) -> None:
    # clock = 1000.0 s; msg CreateTime = 999_000 ms = 999.0 s → e2e = 1.0 s.
    msg = FakeMsg(TIMESTAMP_CREATE_TIME, 999_000)
    consumer = _consumer()
    consumer.poll_batch.side_effect = [[msg], []]
    accountant = ConsumeAccountant()
    _runner(
        consumer,
        accountant,
        cfg=_cfg(deserialize_mode="raw"),
        clock=lambda: 1000.0,
    ).run()
    assert accountant.e2e_samples_s() == [pytest.approx(1.0)]


def test_runner_unavailable_timestamp_counts_but_not_sampled(mock_registry) -> None:
    msg = FakeMsg(TIMESTAMP_NOT_AVAILABLE, -1)
    consumer = _consumer()
    consumer.poll_batch.side_effect = [[msg], []]
    accountant = ConsumeAccountant()
    _runner(consumer, accountant, cfg=_cfg(deserialize_mode="raw")).run()
    assert accountant.consumed == 1
    assert accountant.e2e_samples_s() == []


def test_runner_raw_mode_skips_pydantic(mock_registry, monkeypatch) -> None:
    spy = MagicMock()
    monkeypatch.setattr(
        "streaming_feature_store.consume.consume_runner.avro_dict_to_event", spy
    )
    msg = FakeMsg(TIMESTAMP_CREATE_TIME, 999_000)
    consumer = _consumer()
    consumer.poll_batch.side_effect = [[msg], []]
    _runner(
        consumer, ConsumeAccountant(), cfg=_cfg(deserialize_mode="raw")
    ).run()
    spy.assert_not_called()


def test_runner_pydantic_mode_records_deserialize_error(
    mock_registry, monkeypatch
) -> None:
    monkeypatch.setattr(
        "streaming_feature_store.consume.consume_runner.avro_dict_to_event",
        MagicMock(side_effect=ValueError("bad")),
    )
    msg = FakeMsg(TIMESTAMP_CREATE_TIME, 999_000)
    consumer = _consumer()
    consumer.poll_batch.side_effect = [[msg], []]
    accountant = ConsumeAccountant()
    _runner(consumer, accountant).run()
    snap = accountant.snapshot()
    assert snap.consumed == 1
    assert snap.deserialize_failed == 1
    assert snap.errors_by_class["ValueError"] == 1


def test_runner_until_caught_up_exits_on_zero_lag(mock_registry) -> None:
    consumer = _consumer(poll=[], lag=0)
    runner = _runner(
        consumer,
        ConsumeAccountant(),
        cfg=_cfg(duration_s=100.0, until_caught_up=True),
        monotonic=StepClock(step=0.001),
    )
    report = runner.run()
    assert consumer.poll_batch.call_count == 1
    assert isinstance(report, ConsumeRunReport)


def test_runner_closes_consumer_exactly_once(mock_registry) -> None:
    consumer = _consumer(poll=[])
    _runner(consumer, ConsumeAccountant()).run()
    consumer.close.assert_called_once()


def test_runner_returns_populated_report(mock_registry) -> None:
    msg = FakeMsg(TIMESTAMP_CREATE_TIME, 999_000)
    consumer = _consumer(partitions=[2, 3])
    consumer.poll_batch.side_effect = [[msg], []]
    report = _runner(
        consumer, ConsumeAccountant(), cfg=_cfg(deserialize_mode="raw")
    ).run()
    assert isinstance(report, ConsumeRunReport)
    assert report.snapshot.consumed == 1
    assert report.assigned_partitions == [2, 3]
    assert report.sustained_consume_eps > 0


# --- _compute_e2e_s branches ---------------------------------------------


@pytest.mark.parametrize(
    "ts_type, ts_ms, expected",
    [
        (TIMESTAMP_CREATE_TIME, 999_000, pytest.approx(1.0)),
        (TIMESTAMP_NOT_AVAILABLE, 0, -1.0),
        (TIMESTAMP_CREATE_TIME, -5, -1.0),
        (TIMESTAMP_CREATE_TIME, None, -1.0),
    ],
)
def test_compute_e2e_s(ts_type, ts_ms, expected) -> None:
    runner = ConsumeRunner(
        KafkaConfig(),
        SchemaRegistryConfig(),
        _cfg(),
        consumer=MagicMock(),
        accountant=ConsumeAccountant(),
        clock=lambda: 1000.0,
    )
    assert runner._compute_e2e_s(FakeMsg(ts_type, ts_ms)) == expected
