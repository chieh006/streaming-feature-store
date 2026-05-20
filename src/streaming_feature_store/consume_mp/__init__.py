"""Multi-process variant of the consume harness — the GIL-escape twin.

This sub-package mirrors :mod:`streaming_feature_store.consume` but spawns
one consumer-group *member process* per partition-subset instead of running
a single in-process poll loop.  Each child has its own interpreter, its own
GIL, and its own
:class:`~streaming_feature_store.consumer.avro_consumer.AvroEventConsumer`,
so the per-process decode + Pydantic ceiling that constrains a single
member scales with the member count — the consume-side mirror of the
producer's ``load_mp`` escape.  Members share one ``group.id``; the broker
(not the app) shards partitions across them.
"""

from streaming_feature_store.consume_mp.aggregator import aggregate_outcomes
from streaming_feature_store.consume_mp.mp_runner import MultiprocessConsumeRunner
from streaming_feature_store.consume_mp.process_planner import (
    ConsumePlan,
    plan_consume_processes,
    resolve_cpu_budget,
)
from streaming_feature_store.consume_mp.report import (
    ConsumeOutcome,
    MultiprocessConsumeConfig,
    MultiprocessConsumeReport,
    render_markdown,
)

__all__ = [
    "ConsumeOutcome",
    "ConsumePlan",
    "MultiprocessConsumeConfig",
    "MultiprocessConsumeReport",
    "MultiprocessConsumeRunner",
    "aggregate_outcomes",
    "plan_consume_processes",
    "render_markdown",
    "resolve_cpu_budget",
]
