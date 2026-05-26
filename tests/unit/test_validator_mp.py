"""Unit tests for :mod:`streaming_feature_store.validate_mp` plumbing.

Covers the planner, the report wrappers, and the aggregator.  The actual
:class:`MultiprocessValidatorRunner.run` requires real Kafka and lives in
the integration suite.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from streaming_feature_store.validate.accountant import ValidatorSnapshot
from streaming_feature_store.validate.dlq import ErrorClass
from streaming_feature_store.validate.report import ValidatorRunReport
from streaming_feature_store.validate.runner import ValidatorRunConfig
from streaming_feature_store.validate_mp.aggregator import aggregate_outcomes
from streaming_feature_store.validate_mp.process_planner import (
    available_cpus,
    plan_validator_processes,
    resolve_cpu_budget,
)
from streaming_feature_store.validate_mp.report import (
    MultiprocessValidatorConfig,
    ValidatorOutcome,
)


def _snapshot(consumed: int, validated: int, wallclock_s: float = 1.0) -> ValidatorSnapshot:
    return ValidatorSnapshot(
        consumed=consumed,
        validated=validated,
        invalid_total=consumed - validated,
        invalid_by_class={ErrorClass.OUT_OF_RANGE: max(consumed - validated, 0)},
        invalid_by_validator={"V": max(consumed - validated, 0)},
        invalid_by_field_path={},
        deserialize_failed=0,
        schema_mismatches=0,
        pipeline_internal_errors=0,
        invalid_rate=0.0,
        validation_latency_us_p50=1.0,
        validation_latency_us_p95=2.0,
        validation_latency_us_p99=3.0,
        partition_counts={},
        partition_skew_ratio=0.0,
        partition_skew_pass=True,
        skew_threshold=2.0,
        top_failing_fields=[],
        wallclock_s=wallclock_s,
    )


def _outcome(idx: int, *, consumed: int, validated: int, wallclock_s: float = 1.0) -> ValidatorOutcome:
    snap = _snapshot(consumed=consumed, validated=validated, wallclock_s=wallclock_s)
    report = ValidatorRunReport(
        source_topic="src",
        validated_topic="val",
        dlq_topic="dlq",
        consumer_group="g",
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ended_at=datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        snapshot=snap,
    )
    return ValidatorOutcome(process_index=idx, report=report)


# --- planner ---------------------------------------------------------------


def test_available_cpus_returns_positive() -> None:
    assert available_cpus() >= 1


def test_resolve_cpu_budget_on_host_brokers_halves() -> None:
    assert resolve_cpu_budget(on_host_brokers=True, cpus=8) == 4


def test_resolve_cpu_budget_remote_brokers_reserves_one() -> None:
    assert resolve_cpu_budget(on_host_brokers=False, cpus=8) == 7


def test_resolve_cpu_budget_floor() -> None:
    assert resolve_cpu_budget(on_host_brokers=True, cpus=1) == 1


def test_resolve_cpu_budget_remote_floor() -> None:
    assert resolve_cpu_budget(on_host_brokers=False, cpus=1) == 1


def test_plan_validator_auto_capped_by_partitions() -> None:
    plan = plan_validator_processes(partitions=3, cpu_budget=8)
    assert plan.members == 3
    assert "partition_cap" in plan.rationale


def test_plan_validator_auto_capped_by_cpu() -> None:
    plan = plan_validator_processes(partitions=12, cpu_budget=4)
    assert plan.members == 4
    assert "cpu_budget" in plan.rationale


def test_plan_validator_honors_requested() -> None:
    plan = plan_validator_processes(partitions=12, cpu_budget=8, requested=2)
    assert plan.members == 2


def test_plan_validator_rejects_zero_partitions() -> None:
    with pytest.raises(ValueError):
        plan_validator_processes(partitions=0, cpu_budget=4)


def test_plan_validator_rejects_zero_budget() -> None:
    with pytest.raises(ValueError):
        plan_validator_processes(partitions=4, cpu_budget=0)


def test_plan_validator_rejects_requested_zero() -> None:
    with pytest.raises(ValueError):
        plan_validator_processes(partitions=4, cpu_budget=4, requested=0)


def test_plan_validator_rejects_requested_above_partitions() -> None:
    with pytest.raises(ValueError):
        plan_validator_processes(partitions=4, cpu_budget=8, requested=5)


# --- MP config -------------------------------------------------------------


def test_mp_config_per_process_run_config_is_copy() -> None:
    base = ValidatorRunConfig()
    cfg = MultiprocessValidatorConfig(members=3, base_config=base)
    a = cfg.to_per_process_run_config(0)
    b = cfg.to_per_process_run_config(2)
    assert a == base
    assert b == base


def test_mp_config_rejects_out_of_range_process_index() -> None:
    base = ValidatorRunConfig()
    cfg = MultiprocessValidatorConfig(members=2, base_config=base)
    with pytest.raises(ValueError):
        cfg.to_per_process_run_config(2)


# --- aggregator ------------------------------------------------------------


def test_aggregate_outcomes_sums_counters() -> None:
    base = ValidatorRunConfig()
    cfg = MultiprocessValidatorConfig(members=2, base_config=base)
    outcomes = [
        _outcome(0, consumed=100, validated=98, wallclock_s=1.0),
        _outcome(1, consumed=200, validated=180, wallclock_s=2.0),
    ]
    started = datetime(2026, 1, 1, tzinfo=timezone.utc)
    report = aggregate_outcomes(
        config=cfg, started_at=started, outcomes=outcomes
    )
    assert report.total_consumed == 300
    assert report.total_validated == 278
    assert report.total_invalid == 22
    # sustained = total_consumed / max_member_wallclock = 300 / 2.0 = 150
    assert report.sustained_consume_eps == 150.0


def test_aggregate_outcomes_orders_by_process_index() -> None:
    base = ValidatorRunConfig()
    cfg = MultiprocessValidatorConfig(members=2, base_config=base)
    outcomes = [
        _outcome(1, consumed=10, validated=10),
        _outcome(0, consumed=20, validated=20),
    ]
    report = aggregate_outcomes(
        config=cfg,
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        outcomes=outcomes,
    )
    assert [o.process_index for o in report.process_outcomes] == [0, 1]


def test_aggregate_outcomes_rejects_empty() -> None:
    base = ValidatorRunConfig()
    cfg = MultiprocessValidatorConfig(members=1, base_config=base)
    with pytest.raises(ValueError):
        aggregate_outcomes(
            config=cfg,
            started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            outcomes=[],
        )
