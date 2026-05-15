"""Multi-process variant of the load-test harness.

This sub-module mirrors :mod:`streaming_feature_store.load` but spawns one
producer *process* per shard instead of one *thread* per shard.  Each child
process has its own Python interpreter, its own GIL, and its own
:class:`~streaming_feature_store.producer.avro_producer.AvroEventProducer`,
so the per-process throughput ceiling that constrains the threading runner
(GIL serialisation on the per-event Python path) scales roughly linearly
with the number of processes.

The threading runner (:mod:`streaming_feature_store.load`) remains the
recommended config-sanity-check harness; this sub-module is the
throughput-targeting harness.  They share the lower-level building blocks
(producer, generator, accountant, pacer) but the orchestration layer is
deliberately separate so either can be deleted without affecting the
other.
"""

from streaming_feature_store.load_mp.aggregator import aggregate_outcomes
from streaming_feature_store.load_mp.mp_runner import MultiprocessLoadRunner
from streaming_feature_store.load_mp.process_planner import (
    ProcessPlan,
    plan_processes,
)
from streaming_feature_store.load_mp.report import (
    MultiprocessLoadConfig,
    MultiprocessLoadReport,
    ProcessOutcome,
    render_markdown,
)

__all__ = [
    "MultiprocessLoadConfig",
    "MultiprocessLoadReport",
    "MultiprocessLoadRunner",
    "ProcessOutcome",
    "ProcessPlan",
    "aggregate_outcomes",
    "plan_processes",
    "render_markdown",
]
