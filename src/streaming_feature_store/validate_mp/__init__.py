"""Multi-process consumer-group escape hatch for :class:`ValidatorRunner`.

Mirrors :mod:`streaming_feature_store.consume_mp` for the validator: one
member process per partition subset, all sharing the same ``group.id`` so
the broker performs the work assignment (design doc §2.8).
"""

from streaming_feature_store.validate_mp.aggregator import aggregate_outcomes
from streaming_feature_store.validate_mp.mp_runner import (
    MultiprocessValidatorRunner,
)
from streaming_feature_store.validate_mp.process_planner import (
    ValidatorPlan,
    available_cpus,
    plan_validator_processes,
    resolve_cpu_budget,
)
from streaming_feature_store.validate_mp.report import (
    MultiprocessValidatorConfig,
    MultiprocessValidatorReport,
    ValidatorOutcome,
)
from streaming_feature_store.validate_mp.worker_entry import (
    WorkerProcessArgs,
    run_validator_worker,
)

__all__ = [
    "MultiprocessValidatorConfig",
    "MultiprocessValidatorReport",
    "MultiprocessValidatorRunner",
    "ValidatorOutcome",
    "ValidatorPlan",
    "WorkerProcessArgs",
    "aggregate_outcomes",
    "available_cpus",
    "plan_validator_processes",
    "resolve_cpu_budget",
    "run_validator_worker",
]
