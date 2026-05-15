"""Unit tests for :class:`MultiprocessLoadRunner` (mocked spawn/registry)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.load.accountant import AccountantSnapshot
from streaming_feature_store.load.report import LoadRunConfig, LoadRunReport
from streaming_feature_store.load_mp.mp_runner import MultiprocessLoadRunner
from streaming_feature_store.load_mp.report import (
    MultiprocessLoadConfig,
    ProcessOutcome,
)


def _mp_config(processes: int = 2) -> MultiprocessLoadConfig:
    return MultiprocessLoadConfig(
        duration_s=0.1,
        target_rate=None,
        processes=processes,
        workers_per_process=1,
        topic="t",
    )


def _outcome(idx: int) -> ProcessOutcome:
    snap = AccountantSnapshot(
        produced=1000,
        acked=1000,
        failed=0,
        in_flight=0,
        errors_by_class={},
        ack_latency_p50_ms=1.0,
        ack_latency_p95_ms=2.0,
        ack_latency_p99_ms=3.0,
        wallclock_s=0.1,
    )
    cfg = LoadRunConfig(duration_s=0.1, workers=1, topic="t")
    report = LoadRunReport(
        config=cfg,
        started_at=datetime.now(tz=timezone.utc),
        snapshot=snap,
        sustained_rate_eps=10_000.0,
        floor_eps=0.0,
    )
    return ProcessOutcome(
        process_index=idx, report=report, latency_samples_s=[0.001]
    )


@pytest.fixture
def mock_registry(monkeypatch):
    """Patch :class:`SchemaRegistry` so the subject-registered check passes."""
    fake = MagicMock()
    fake.get_latest.return_value = MagicMock(schema_id=1, version=1)
    monkeypatch.setattr(
        "streaming_feature_store.load_mp.mp_runner.SchemaRegistry",
        lambda cfg: fake,
    )
    return fake


def test_runner_calls_assert_subject_at_startup(mock_registry):
    """``_assert_subject_registered`` runs once before children spawn."""
    cfg = _mp_config(processes=2)
    runner = MultiprocessLoadRunner(KafkaConfig(), SchemaRegistryConfig(), cfg)
    with patch.object(
        runner,
        "_spawn_children",
        return_value=[_outcome(0), _outcome(1)],
    ):
        runner.run()
    mock_registry.get_latest.assert_called_once_with("t-value")


def test_runner_aborts_when_subject_missing(monkeypatch):
    """A missing subject raises and no children are spawned."""
    from streaming_feature_store.schemas.registry import RegistryError

    fake = MagicMock()
    fake.get_latest.side_effect = RegistryError("missing")
    monkeypatch.setattr(
        "streaming_feature_store.load_mp.mp_runner.SchemaRegistry",
        lambda cfg: fake,
    )
    cfg = _mp_config()
    runner = MultiprocessLoadRunner(KafkaConfig(), SchemaRegistryConfig(), cfg)
    with patch.object(runner, "_spawn_children") as spawn:
        with pytest.raises(RegistryError):
            runner.run()
        spawn.assert_not_called()


def test_runner_builds_one_args_per_process(mock_registry):
    """``_build_child_args`` returns one bundle per requested process."""
    cfg = _mp_config(processes=3)
    runner = MultiprocessLoadRunner(KafkaConfig(), SchemaRegistryConfig(), cfg)
    args_list = runner._build_child_args()
    assert len(args_list) == 3
    assert [a.process_index for a in args_list] == [0, 1, 2]


def test_runner_returns_aggregate_report(mock_registry):
    """``run()`` returns an aggregated :class:`MultiprocessLoadReport`."""
    cfg = _mp_config(processes=2)
    runner = MultiprocessLoadRunner(
        KafkaConfig(),
        SchemaRegistryConfig(),
        cfg,
        floor_eps=0.0,
    )
    with patch.object(
        runner,
        "_spawn_children",
        return_value=[_outcome(0), _outcome(1)],
    ):
        report = runner.run()
    assert report.aggregate_snapshot.produced == 2000
    assert report.aggregate_snapshot.acked == 2000
    assert len(report.process_outcomes) == 2


def test_runner_passes_per_process_target_rate(mock_registry):
    """Each child's args carries the per-process pacer rate."""
    cfg = MultiprocessLoadConfig(
        duration_s=0.1,
        target_rate=60_000.0,
        processes=4,
        workers_per_process=3,
        topic="t",
    )
    runner = MultiprocessLoadRunner(KafkaConfig(), SchemaRegistryConfig(), cfg)
    args_list = runner._build_child_args()
    assert all(a.run_config.target_rate == pytest.approx(15_000.0) for a in args_list)
