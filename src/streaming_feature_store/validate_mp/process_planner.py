"""Pick the consumer-group member count for a multi-process validator run.

Mirrors :mod:`streaming_feature_store.consume_mp.process_planner` — the
unit of consumer parallelism is the **partition**, so the member count is
``min(partitions, cpu_budget)``, hard-capped at the partition count
because extra members beyond the partition count sit idle.
"""

from __future__ import annotations

import logging
import os

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class ValidatorPlan(BaseModel):
    """Resolved consumer-group layout for a multi-process validator run.

    Parameters
    ----------
    members : int
        Number of validator member processes to spawn.  ``>= 1``.
    rationale : str
        Human-readable explanation of how the plan was derived (which
        limit was binding).  Surfaced in the rendered report.
    """

    model_config = ConfigDict(frozen=True)

    members: int = Field(..., ge=1)
    rationale: str


def available_cpus() -> int:
    """Return the number of CPUs usable by this process.

    Returns
    -------
    int
        ``len(os.sched_getaffinity(0))`` on Linux; falls back to
        ``os.cpu_count()`` elsewhere.  Always ``>= 1``.
    """
    try:
        return max(1, len(os.sched_getaffinity(0)))
    except AttributeError:  # pragma: no cover - non-Linux fallback
        return max(1, os.cpu_count() or 1)


def _reserve_cores(cpus: int, *, on_host_brokers: bool) -> int:
    """Compute the validator-process budget after reserving non-Python work.

    Parameters
    ----------
    cpus : int
        Total CPUs available to this process.
    on_host_brokers : bool
        ``True`` when the brokers run on the same host; reserve half the
        CPUs for the broker pool.  ``False`` reserves only one CPU.

    Returns
    -------
    int
        Validator-process budget, ``>= 1``.
    """
    if on_host_brokers:
        return max(1, cpus // 2)
    return max(1, cpus - 1)


def resolve_cpu_budget(
    *, on_host_brokers: bool = True, cpus: int | None = None
) -> int:
    """Resolve the CPU budget the planner should cap members by.

    Parameters
    ----------
    on_host_brokers : bool, optional
        Whether the brokers share the host with the validator.  Defaults
        to ``True``.
    cpus : int or None, optional
        Override for :func:`available_cpus` (test seam).

    Returns
    -------
    int
        Effective CPU budget, ``>= 1``.
    """
    return _reserve_cores(
        cpus if cpus is not None else available_cpus(),
        on_host_brokers=on_host_brokers,
    )


def plan_validator_processes(
    *,
    partitions: int,
    cpu_budget: int,
    requested: int | None = None,
) -> ValidatorPlan:
    """Resolve a :class:`ValidatorPlan` from request + topology.

    Parameters
    ----------
    partitions : int
        Number of partitions on the source topic.  ``>= 1``.  This is the
        hard cap.
    cpu_budget : int
        CPU budget after reserving cores for brokers / OS.  ``>= 1``.
    requested : int or None, optional
        Explicit member count.  ``None`` (default) lets the heuristic
        decide.

    Returns
    -------
    ValidatorPlan
        Resolved layout.

    Raises
    ------
    ValueError
        If ``partitions`` or ``cpu_budget`` is less than ``1``, or if
        ``requested`` is less than ``1`` or larger than ``partitions``.
    """
    if partitions < 1:
        raise ValueError(f"partitions must be >= 1, got {partitions}")
    if cpu_budget < 1:
        raise ValueError(f"cpu_budget must be >= 1, got {cpu_budget}")

    if requested is not None:
        if requested < 1:
            raise ValueError(f"requested must be >= 1, got {requested}")
        if requested > partitions:
            raise ValueError(
                f"requested={requested} exceeds the partition cap "
                f"(partitions={partitions}); extra members would sit idle"
            )
        return ValidatorPlan(
            members=requested,
            rationale=(
                f"user-requested members={requested} "
                f"(cpu_budget={cpu_budget}, partition_cap={partitions})"
            ),
        )

    members = min(partitions, cpu_budget)
    binding = "partition_cap" if partitions <= cpu_budget else "cpu_budget"
    return ValidatorPlan(
        members=members,
        rationale=(
            f"auto: min(partition_cap={partitions}, cpu_budget={cpu_budget})"
            f" = {members} (binding={binding})"
        ),
    )
