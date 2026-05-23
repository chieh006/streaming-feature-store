"""Kafka-to-PostgreSQL sink consumer package.

The sink subscribes to a Kafka topic, accumulates messages into a batch, and
performs idempotent bulk inserts into the ``raw_events`` table.  Offsets are
committed only after PostgreSQL ``COMMIT`` succeeds — the read-batch-write-
commit ordering described in
``docs/design/week1_06_postgres_sink_and_continuous_feeder.md`` §2.7.
"""

from streaming_feature_store.sink.accountant import (
    SinkAccountant,
    SinkSnapshot,
)
from streaming_feature_store.sink.postgres_writer import (
    BatchInsertResult,
    PostgresWriter,
)
from streaming_feature_store.sink.report import (
    SinkRunReport,
    render_markdown,
)
from streaming_feature_store.sink.sink_runner import (
    Batch,
    SinkRunConfig,
    SinkRunner,
)

__all__ = [
    "Batch",
    "BatchInsertResult",
    "PostgresWriter",
    "SinkAccountant",
    "SinkRunConfig",
    "SinkRunner",
    "SinkRunReport",
    "SinkSnapshot",
    "render_markdown",
]
