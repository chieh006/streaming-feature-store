"""Unit tests for :mod:`streaming_feature_store.load_mp.worker_entry`."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.load.accountant import AccountantSnapshot
from streaming_feature_store.load.report import LoadRunConfig, LoadRunReport
from streaming_feature_store.load_mp.worker_entry import (
    WorkerProcessArgs,
    _configure_child_logging,
    run_worker_process,
)


def _make_args(**over) -> WorkerProcessArgs:
    base = dict(
        process_index=0,
        kafka_config_dict=KafkaConfig().model_dump(),
        registry_config_dict=SchemaRegistryConfig().model_dump(mode="json"),
        run_config=LoadRunConfig(
            duration_s=0.1, workers=1, batch_size=2, topic="t"
        ),
        floor_eps=0.0,
        log_level="WARNING",
    )
    base.update(over)
    return WorkerProcessArgs(**base)


def test_worker_process_args_is_pickleable():
    """Args must pickle/unpickle cleanly for spawn IPC."""
    import pickle

    args = _make_args()
    restored = pickle.loads(pickle.dumps(args))
    assert restored.process_index == 0
    assert restored.run_config.topic == "t"


def test_worker_process_args_rejects_negative_index():
    """``process_index`` must be ``>= 0``."""
    with pytest.raises(ValueError):
        _make_args(process_index=-1)


def test_configure_child_logging_sets_root_level():
    """`_configure_child_logging` installs a handler with the requested level."""
    import logging as logging_

    _configure_child_logging("WARNING")
    assert logging_.getLogger().level == logging_.WARNING


def _fake_snapshot() -> AccountantSnapshot:
    return AccountantSnapshot(
        produced=10,
        acked=10,
        failed=0,
        in_flight=0,
        errors_by_class={},
        ack_latency_p50_ms=1.0,
        ack_latency_p95_ms=2.0,
        ack_latency_p99_ms=3.0,
        wallclock_s=0.1,
    )


def _fake_report(cfg: LoadRunConfig) -> LoadRunReport:
    return LoadRunReport(
        config=cfg,
        started_at=datetime.now(tz=timezone.utc),
        snapshot=_fake_snapshot(),
        sustained_rate_eps=100.0,
        floor_eps=0.0,
    )


def test_run_worker_process_builds_loadrunner_with_injected_accountant():
    """The worker constructs its own accountant and injects it into ``LoadRunner``."""
    args = _make_args()
    fake_loadrunner_instance = MagicMock()
    fake_loadrunner_instance.run.return_value = _fake_report(args.run_config)
    with patch(
        "streaming_feature_store.load_mp.worker_entry.LoadRunner",
        return_value=fake_loadrunner_instance,
    ) as patched_loadrunner:
        outcome = run_worker_process(args)
    # LoadRunner constructed with accountant kwarg set.
    kwargs = patched_loadrunner.call_args.kwargs
    assert "accountant" in kwargs
    assert outcome.process_index == 0
    assert outcome.report.snapshot.acked == 10


def test_run_worker_process_returns_latency_samples():
    """Latency samples from the local accountant are included in the outcome."""
    args = _make_args()
    fake_loadrunner_instance = MagicMock()
    fake_loadrunner_instance.run.return_value = _fake_report(args.run_config)
    fake_accountant = MagicMock()
    fake_accountant.latency_samples_s.return_value = [0.001, 0.002, 0.003]
    with patch(
        "streaming_feature_store.load_mp.worker_entry.LoadRunner",
        return_value=fake_loadrunner_instance,
    ), patch(
        "streaming_feature_store.load_mp.worker_entry.DeliveryAccountant",
        return_value=fake_accountant,
    ):
        outcome = run_worker_process(args)
    assert outcome.latency_samples_s == [0.001, 0.002, 0.003]


def test_run_worker_process_propagates_loadrunner_errors():
    """Errors from ``LoadRunner.run`` surface back to the parent unchanged."""
    args = _make_args()
    fake_loadrunner_instance = MagicMock()
    fake_loadrunner_instance.run.side_effect = RuntimeError("boom")
    with patch(
        "streaming_feature_store.load_mp.worker_entry.LoadRunner",
        return_value=fake_loadrunner_instance,
    ):
        with pytest.raises(RuntimeError, match="boom"):
            run_worker_process(args)
