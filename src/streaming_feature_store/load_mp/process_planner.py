"""Pick ``N_processes`` and ``workers_per_process`` for a multi-process load run.

Heuristic (see ``docs/results/week1_load_test_throughput_investigation.md``):

* On-host brokers (dev / WSL Compose):
  ``N = min(max(1, sched_affinity // 2), partitions // workers_per_process)``.
  Half the CPUs are reserved for the brokers, Schema Registry, Postgres,
  and the OS, all of which share the host with the producer.

* Off-host brokers (production):
  ``N = min(max(1, sched_affinity - 1), partitions // workers_per_process)``.
  Only one CPU is reserved for the OS / Docker daemon.

Both branches cap ``N`` by ``partitions // workers_per_process`` so each
worker thread inside each process can land on a distinct broker leader.

The planner is pure and side-effect-free; ``MultiprocessLoadRunner`` calls
it once at startup to materialise a :class:`ProcessPlan`.
"""

from __future__ import annotations

import logging
import os

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class ProcessPlan(BaseModel):
    """Resolved multi-process layout for a single run.

    Parameters
    ----------
    processes : int
        Number of producer processes to spawn.  ``>= 1``.
    workers_per_process : int
        Worker threads inside each process.  ``>= 1``.
    target_rate_per_process : float or None
        Per-process pacer rate (events/sec).  ``None`` when the run is
        un-paced.
    rationale : str
        Human-readable explanation of how the plan was derived (which limit
        was binding).  Surfaced in the rendered report.
    """

    model_config = ConfigDict(frozen=True)

    processes: int = Field(..., ge=1)
    workers_per_process: int = Field(..., ge=1)
    target_rate_per_process: float | None = None
    rationale: str


def available_cpus() -> int:
    """Return the number of CPUs usable by this process.

    Returns
    -------
    int
        ``len(os.sched_getaffinity(0))`` on Linux (respects WSL2 vCPU
        limits, cgroup quotas, and ``taskset``); falls back to
        ``os.cpu_count()`` on platforms that lack ``sched_getaffinity``.
        Always ``>= 1``.
    """
    try:
        return max(1, len(os.sched_getaffinity(0)))
    except AttributeError:  # pragma: no cover - non-Linux fallback
        return max(1, os.cpu_count() or 1)


def _reserve_cores(cpus: int, *, on_host_brokers: bool) -> int:
    """Compute the producer-process budget after reserving for non-Python work.

    Parameters
    ----------
    cpus : int
        Total CPUs available to this process.
    on_host_brokers : bool
        ``True`` when the brokers run on the same host (dev / WSL); reserve
        half the CPUs for the broker pool.  ``False`` when brokers are
        remote; reserve only one CPU for the OS.

    Returns
    -------
    int
        Producer-process budget, ``>= 1``.
    """
    if on_host_brokers:
        return max(1, cpus // 2)
    return max(1, cpus - 1)


def plan_processes(
    *,
    partitions: int,
    workers_per_process: int = 2,
    requested_processes: int | None = None,
    on_host_brokers: bool = True,
    total_target_rate: float | None = None,
    cpus: int | None = None,
) -> ProcessPlan:
    """Resolve a :class:`ProcessPlan` from request + topology.

    Parameters
    ----------
    partitions : int
        Number of partitions on the target topic.  ``>= 1``.
    workers_per_process : int, optional
        Worker threads per process.  Defaults to ``2``: empirically,
        fewer threads per process means less intra-process GIL
        contention, so two workers per process consistently
        outperformed three on the Week 1 benchmark (60k vs 34k evt/s
        with the same total worker count).
    requested_processes : int or None, optional
        User-supplied process count.  ``None`` (the default) lets the
        heuristic decide.
    on_host_brokers : bool, optional
        Whether the brokers share the host with the producer.  Affects the
        CPU-reservation rule.  Defaults to ``True`` (dev / WSL).
    total_target_rate : float or None, optional
        Aggregate target rate across all processes.  ``None`` disables
        pacing.  Each process receives ``total_target_rate / processes``.
    cpus : int or None, optional
        Override for :func:`available_cpus` (test seam).

    Returns
    -------
    ProcessPlan
        Resolved layout.

    Raises
    ------
    ValueError
        If ``partitions`` or ``workers_per_process`` is less than ``1``,
        or if ``requested_processes`` is less than ``1`` or larger than
        ``partitions // workers_per_process``.
    """
    if partitions < 1:
        raise ValueError(f"partitions must be >= 1, got {partitions}")
    if workers_per_process < 1:
        raise ValueError(
            f"workers_per_process must be >= 1, got {workers_per_process}"
        )

    partition_cap = max(1, partitions // workers_per_process)
    cpu_budget = _reserve_cores(
        cpus if cpus is not None else available_cpus(),
        on_host_brokers=on_host_brokers,
    )

    if requested_processes is not None:
        if requested_processes < 1:
            raise ValueError(
                f"requested_processes must be >= 1, got {requested_processes}"
            )
        if requested_processes > partition_cap:
            raise ValueError(
                f"requested_processes={requested_processes} exceeds the "
                f"partition cap (partitions={partitions} // "
                f"workers_per_process={workers_per_process} = {partition_cap})"
            )
        processes = requested_processes
        rationale = (
            f"user-requested processes={processes} "
            f"(cpu_budget={cpu_budget}, partition_cap={partition_cap})"
        )
    else:
        processes = min(cpu_budget, partition_cap)
        binding = "cpu_budget" if cpu_budget <= partition_cap else "partition_cap"
        rationale = (
            f"auto: min(cpu_budget={cpu_budget}, partition_cap={partition_cap})"
            f" = {processes} (binding={binding}, "
            f"on_host_brokers={on_host_brokers})"
        )

    per_proc_rate: float | None
    if total_target_rate is None:
        per_proc_rate = None
    else:
        per_proc_rate = float(total_target_rate) / float(processes)

    return ProcessPlan(
        processes=processes,
        workers_per_process=workers_per_process,
        target_rate_per_process=per_proc_rate,
        rationale=rationale,
    )
