"""Unit tests for :mod:`streaming_feature_store.consume_mp.process_planner`."""

from __future__ import annotations

import pytest

from streaming_feature_store.consume_mp.process_planner import (
    ConsumePlan,
    _reserve_cores,
    available_cpus,
    plan_consume_processes,
    resolve_cpu_budget,
)


def test_available_cpus_returns_positive_int() -> None:
    assert available_cpus() >= 1


@pytest.mark.parametrize(
    "cpus, on_host, expected",
    [(8, True, 4), (8, False, 7), (1, True, 1), (2, True, 1), (16, False, 15)],
)
def test_reserve_cores_matches_heuristic(cpus, on_host, expected) -> None:
    assert _reserve_cores(cpus, on_host_brokers=on_host) == expected


def test_resolve_cpu_budget_uses_override() -> None:
    assert resolve_cpu_budget(on_host_brokers=True, cpus=8) == 4
    assert resolve_cpu_budget(on_host_brokers=False, cpus=8) == 7


def test_members_capped_by_partitions() -> None:
    plan = plan_consume_processes(partitions=12, cpu_budget=32)
    assert plan.members == 12
    assert "binding=partition_cap" in plan.rationale


def test_members_capped_by_cpu_budget() -> None:
    plan = plan_consume_processes(partitions=12, cpu_budget=4)
    assert plan.members == 4
    assert "binding=cpu_budget" in plan.rationale


def test_requested_overrides_plan() -> None:
    plan = plan_consume_processes(partitions=12, cpu_budget=4, requested=3)
    assert plan.members == 3
    assert "user-requested" in plan.rationale


def test_requested_above_partitions_rejected() -> None:
    with pytest.raises(ValueError, match="exceeds the partition cap"):
        plan_consume_processes(partitions=12, cpu_budget=64, requested=20)


def test_requested_zero_rejected() -> None:
    with pytest.raises(ValueError, match="requested must be >= 1"):
        plan_consume_processes(partitions=12, cpu_budget=4, requested=0)


def test_zero_partitions_rejected() -> None:
    with pytest.raises(ValueError, match="partitions must be >= 1"):
        plan_consume_processes(partitions=0, cpu_budget=4)


def test_zero_cpu_budget_rejected() -> None:
    with pytest.raises(ValueError, match="cpu_budget must be >= 1"):
        plan_consume_processes(partitions=12, cpu_budget=0)


def test_no_workers_per_process_axis() -> None:
    plan = plan_consume_processes(partitions=12, cpu_budget=8)
    assert not hasattr(plan, "workers_per_process")


def test_plan_is_frozen() -> None:
    plan = plan_consume_processes(partitions=12, cpu_budget=8)
    assert isinstance(plan, ConsumePlan)
    with pytest.raises((ValueError, TypeError)):
        plan.members = 999  # type: ignore[misc]
