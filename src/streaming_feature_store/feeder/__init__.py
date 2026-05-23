"""Low-rate, long-running continuous event feeder for ``raw_events``.

The feeder is a single-process Python daemon that produces synthetic
:class:`EcommerceEvent` instances to a dedicated Kafka topic
(``e-commerce-events-feed`` by default) at a configurable rate (200 evt/s by
default).  Together with :class:`SinkRunner` it keeps the ``raw_events`` table
continuously populated — by Week 4 the table will contain hundreds of millions
of rows for offline feature computation and point-in-time joins
(``docs/design/week1_06_postgres_sink_and_continuous_feeder.md``).
"""

from streaming_feature_store.feeder.feeder_runner import (
    FeederRunConfig,
    FeederRunner,
    FeederSnapshot,
)

__all__ = ["FeederRunConfig", "FeederRunner", "FeederSnapshot"]
