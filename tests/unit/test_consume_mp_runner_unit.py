"""Unit tests for :class:`MultiprocessConsumeRunner` (mocked spawn/registry)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consume.accountant import ConsumeSnapshot
from streaming_feature_store.consume.report import ConsumeRunConfig, ConsumeRunReport
from streaming_feature_store.consume_mp.mp_runner import MultiprocessConsumeRunner
from streaming_feature_store.consume_mp.report import (
    ConsumeOutcome,
    MultiprocessConsumeConfig,
)


def _mp_config(members: int = 2) -> MultiprocessConsumeConfig:
    return MultiprocessConsumeConfig(
        duration_s=0.1, group_id="grp", members=members, topic="t"
    )


def _outcome(idx: int) -> ConsumeOutcome:
    snap = ConsumeSnapshot(
        consumed=1000,
        deserialize_failed=0,
        errors_by_class={},
        e2e_p50_ms=1.0,
        e2e_p95_ms=2.0,
        e2e_p99_ms=3.0,
        max_lag=100,
        end_lag=0,
        lag_ramped=False,
        wallclock_s=0.1,
    )
    report = ConsumeRunReport(
        config=ConsumeRunConfig(duration_s=0.1, group_id="grp", topic="t"),
        started_at=datetime.now(tz=timezone.utc),
        snapshot=snap,
        sustained_consume_eps=10_000.0,
        assigned_partitions=[idx],
        floor_eps=0.0,
    )
    return ConsumeOutcome(process_index=idx, report=report, e2e_samples_s=[0.001])


@pytest.fixture
def mock_registry(monkeypatch):
    fake = MagicMock()
    fake.get_latest.return_value = MagicMock(schema_id=1, version=1)
    monkeypatch.setattr(
        "streaming_feature_store.consume_mp.mp_runner.SchemaRegistry",
        lambda cfg: fake,
    )
    return fake


def test_runner_asserts_subject_at_startup(mock_registry) -> None:
    runner = MultiprocessConsumeRunner(
        KafkaConfig(), SchemaRegistryConfig(), _mp_config(2)
    )
    with patch.object(
        runner, "_spawn_children", return_value=[_outcome(0), _outcome(1)]
    ):
        runner.run()
    mock_registry.get_latest.assert_called_once_with("t-value")


def test_runner_aborts_when_subject_missing(monkeypatch) -> None:
    from streaming_feature_store.schemas.registry import RegistryError

    fake = MagicMock()
    fake.get_latest.side_effect = RegistryError("missing")
    monkeypatch.setattr(
        "streaming_feature_store.consume_mp.mp_runner.SchemaRegistry",
        lambda cfg: fake,
    )
    runner = MultiprocessConsumeRunner(
        KafkaConfig(), SchemaRegistryConfig(), _mp_config()
    )
    with patch.object(runner, "_spawn_children") as spawn:
        with pytest.raises(RegistryError):
            runner.run()
        spawn.assert_not_called()


def test_runner_builds_one_args_per_member(mock_registry) -> None:
    runner = MultiprocessConsumeRunner(
        KafkaConfig(), SchemaRegistryConfig(), _mp_config(3)
    )
    args_list = runner._build_child_args()
    assert len(args_list) == 3
    assert [a.process_index for a in args_list] == [0, 1, 2]
    # All members share the same group id (broker performs the split).
    assert {a.run_config.group_id for a in args_list} == {"grp"}


def test_runner_returns_aggregate_report(mock_registry) -> None:
    runner = MultiprocessConsumeRunner(
        KafkaConfig(), SchemaRegistryConfig(), _mp_config(2), floor_eps=0.0
    )
    with patch.object(
        runner, "_spawn_children", return_value=[_outcome(0), _outcome(1)]
    ):
        report = runner.run()
    assert report.aggregate_snapshot.consumed == 2000
    assert len(report.process_outcomes) == 2
    assert report.passed is True


def test_runner_reexports_registry_error() -> None:
    from streaming_feature_store.schemas.registry import RegistryError

    assert MultiprocessConsumeRunner.RegistryError is RegistryError


def test_spawn_children_uses_spawn_pool(monkeypatch) -> None:
    """`_spawn_children` maps the worker over a spawn-context pool."""
    runner = MultiprocessConsumeRunner(
        KafkaConfig(), SchemaRegistryConfig(), _mp_config(2)
    )
    args_list = [object(), object()]

    class _FakePool:
        def __init__(self) -> None:
            self.map_args = None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def map(self, fn, items):
            self.map_args = (fn, items)
            return ["outcome-0", "outcome-1"]

    pool = _FakePool()

    class _FakeCtx:
        def __init__(self) -> None:
            self.pool_kw = None

        def Pool(self, processes):
            self.pool_kw = processes
            return pool

    ctx = _FakeCtx()
    monkeypatch.setattr(
        "streaming_feature_store.consume_mp.mp_runner.mp.get_context",
        lambda method: ctx,
    )
    out = runner._spawn_children(args_list)
    assert out == ["outcome-0", "outcome-1"]
    assert ctx.pool_kw == 2
    from streaming_feature_store.consume_mp.worker_entry import (
        run_consume_worker,
    )

    assert pool.map_args[0] is run_consume_worker
