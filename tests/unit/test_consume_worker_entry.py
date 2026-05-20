"""Unit tests for :mod:`streaming_feature_store.consume_mp.worker_entry`."""

from __future__ import annotations

import pickle
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consume.accountant import ConsumeSnapshot
from streaming_feature_store.consume.report import ConsumeRunConfig, ConsumeRunReport
from streaming_feature_store.consume_mp.worker_entry import (
    WorkerProcessArgs,
    _configure_child_logging,
    run_consume_worker,
)


def _make_args(**over) -> WorkerProcessArgs:
    base = dict(
        process_index=0,
        kafka_config_dict=KafkaConfig().model_dump(),
        registry_config_dict=SchemaRegistryConfig().model_dump(mode="json"),
        run_config=ConsumeRunConfig(duration_s=0.1, group_id="g", topic="t"),
        log_level="WARNING",
    )
    base.update(over)
    return WorkerProcessArgs(**base)


def _fake_report(cfg: ConsumeRunConfig) -> ConsumeRunReport:
    snap = ConsumeSnapshot(
        consumed=42,
        deserialize_failed=0,
        errors_by_class={},
        e2e_p50_ms=1.0,
        e2e_p95_ms=2.0,
        e2e_p99_ms=3.0,
        max_lag=10,
        end_lag=0,
        lag_ramped=False,
        wallclock_s=0.1,
    )
    return ConsumeRunReport(
        config=cfg,
        started_at=datetime.now(tz=timezone.utc),
        snapshot=snap,
        sustained_consume_eps=420.0,
        assigned_partitions=[0, 1],
        floor_eps=0.0,
    )


def test_worker_args_pickleable() -> None:
    restored = pickle.loads(pickle.dumps(_make_args()))
    assert restored.process_index == 0
    assert restored.run_config.group_id == "g"


def test_worker_args_rejects_negative_index() -> None:
    with pytest.raises(ValueError):
        _make_args(process_index=-1)


def test_configure_child_logging_sets_level() -> None:
    import logging as logging_

    _configure_child_logging("WARNING")
    assert logging_.getLogger().level == logging_.WARNING


def test_run_consume_worker_injects_accountant() -> None:
    args = _make_args()
    fake_runner = MagicMock()
    fake_runner.run.return_value = _fake_report(args.run_config)
    with patch(
        "streaming_feature_store.consume_mp.worker_entry.ConsumeRunner",
        return_value=fake_runner,
    ) as patched:
        outcome = run_consume_worker(args)
    assert "accountant" in patched.call_args.kwargs
    assert outcome.process_index == 0
    assert outcome.report.snapshot.consumed == 42


def test_run_consume_worker_returns_e2e_samples() -> None:
    args = _make_args()
    fake_runner = MagicMock()
    fake_runner.run.return_value = _fake_report(args.run_config)
    fake_acct = MagicMock()
    fake_acct.e2e_samples_s.return_value = [0.001, 0.002]
    with patch(
        "streaming_feature_store.consume_mp.worker_entry.ConsumeRunner",
        return_value=fake_runner,
    ), patch(
        "streaming_feature_store.consume_mp.worker_entry.ConsumeAccountant",
        return_value=fake_acct,
    ):
        outcome = run_consume_worker(args)
    assert outcome.e2e_samples_s == [0.001, 0.002]


def test_run_consume_worker_propagates_errors() -> None:
    args = _make_args()
    fake_runner = MagicMock()
    fake_runner.run.side_effect = RuntimeError("boom")
    with patch(
        "streaming_feature_store.consume_mp.worker_entry.ConsumeRunner",
        return_value=fake_runner,
    ):
        with pytest.raises(RuntimeError, match="boom"):
            run_consume_worker(args)
