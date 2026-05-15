"""Unit tests for :mod:`streaming_feature_store.load_mp.process_planner`."""

from __future__ import annotations

import pytest

from streaming_feature_store.load_mp.process_planner import (
    ProcessPlan,
    _reserve_cores,
    available_cpus,
    plan_processes,
)


def test_available_cpus_returns_positive_int():
    """Sanity: at least one CPU is reported."""
    assert available_cpus() >= 1


@pytest.mark.parametrize(
    "cpus, on_host_brokers, expected",
    [
        (8, True, 4),
        (8, False, 7),
        (1, True, 1),
        (1, False, 1),
        (2, True, 1),
        (2, False, 1),
        (16, True, 8),
        (16, False, 15),
    ],
)
def test_reserve_cores_matches_heuristic(cpus, on_host_brokers, expected):
    """Reserve-cores rule: ``cpus // 2`` for on-host, ``cpus - 1`` for off-host."""
    assert _reserve_cores(cpus, on_host_brokers=on_host_brokers) == expected


def test_plan_processes_auto_caps_by_cpu_budget_when_smaller():
    """On 8 CPUs / 12 partitions, cpu_budget=4 is smaller than partition_cap=4 → 4."""
    plan = plan_processes(
        partitions=12,
        workers_per_process=3,
        cpus=8,
        on_host_brokers=True,
    )
    assert plan.processes == 4
    assert plan.workers_per_process == 3
    assert "binding=cpu_budget" in plan.rationale


def test_plan_processes_auto_caps_by_partition_when_smaller():
    """On 32 CPUs / 12 partitions, partition_cap=4 is smaller than cpu_budget=16 → 4."""
    plan = plan_processes(
        partitions=12,
        workers_per_process=3,
        cpus=32,
        on_host_brokers=True,
    )
    assert plan.processes == 4
    assert "binding=partition_cap" in plan.rationale


def test_plan_processes_respects_requested():
    """An explicit ``requested_processes`` overrides the heuristic."""
    plan = plan_processes(
        partitions=12,
        workers_per_process=3,
        requested_processes=2,
        cpus=8,
    )
    assert plan.processes == 2
    assert "user-requested" in plan.rationale


def test_plan_processes_rejects_request_above_partition_cap():
    """The partition cap is a hard limit even for explicit requests."""
    with pytest.raises(ValueError, match="exceeds the partition cap"):
        plan_processes(
            partitions=12,
            workers_per_process=3,
            requested_processes=5,
            cpus=64,
        )


def test_plan_processes_rejects_zero_request():
    """``requested_processes`` must be ``>= 1``."""
    with pytest.raises(ValueError, match="requested_processes must be >= 1"):
        plan_processes(partitions=12, workers_per_process=3, requested_processes=0)


def test_plan_processes_rejects_zero_partitions():
    """``partitions`` must be ``>= 1``."""
    with pytest.raises(ValueError, match="partitions must be >= 1"):
        plan_processes(partitions=0, workers_per_process=3)


def test_plan_processes_rejects_zero_workers_per_process():
    """``workers_per_process`` must be ``>= 1``."""
    with pytest.raises(ValueError, match="workers_per_process must be >= 1"):
        plan_processes(partitions=12, workers_per_process=0)


def test_plan_processes_target_rate_split_when_paced():
    """Paced runs split the aggregate target evenly across processes."""
    plan = plan_processes(
        partitions=12,
        workers_per_process=3,
        cpus=8,
        total_target_rate=60_000.0,
    )
    # cpu_budget=4, partition_cap=4 → 4 processes.
    assert plan.target_rate_per_process == pytest.approx(15_000.0)


def test_plan_processes_target_rate_none_when_unpaced():
    """``total_target_rate=None`` → ``target_rate_per_process=None``."""
    plan = plan_processes(
        partitions=12,
        workers_per_process=3,
        cpus=8,
        total_target_rate=None,
    )
    assert plan.target_rate_per_process is None


def test_plan_processes_off_host_brokers_uses_cpus_minus_one():
    """``on_host_brokers=False`` reserves only one CPU."""
    plan = plan_processes(
        partitions=12,
        workers_per_process=1,
        cpus=8,
        on_host_brokers=False,
    )
    # cpu_budget = 8 - 1 = 7; partition_cap = 12. min = 7.
    assert plan.processes == 7
    assert "on_host_brokers=False" in plan.rationale


def test_plan_processes_returns_pydantic_frozen():
    """The returned plan is frozen / immutable."""
    plan = plan_processes(partitions=12, workers_per_process=3, cpus=8)
    assert isinstance(plan, ProcessPlan)
    with pytest.raises((ValueError, TypeError)):
        plan.processes = 999  # type: ignore[misc]


def test_plan_processes_clamps_partition_cap_to_at_least_one():
    """A topic with fewer partitions than workers still yields a 1-process plan."""
    plan = plan_processes(
        partitions=2,
        workers_per_process=12,
        cpus=8,
    )
    assert plan.processes == 1
