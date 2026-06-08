"""Engine-neutral sliding-window feature computation (Week 2 PR #2).

A plain Python Kafka consumer group that maintains hopping windows in process
memory — the active implementation that superseded the PyFlink prototype
(design doc ``week2_02_sliding_window_features_plain_consumer.md`` §2.1).

The package is layered so the pure-Python core (models, aggregators, panes,
watermark) imports no Kafka / Redis symbols and is trivially unit-testable; the
sinks and consumer wire that core to the infrastructure.
"""

from streaming_feature_store.sliding.aggregators import (
    AGGREGATOR_BY_RESOLUTION,
    FiveMinuteAggregator,
    OneHourAggregator,
    SlidingWindowAggregator,
    TwentyFourHourAggregator,
)
from streaming_feature_store.sliding.consumer import (
    SlidingFeaturesConsumer,
    SlidingRunSnapshot,
)
from streaming_feature_store.sliding.models import (
    SlidingAccumulator,
    SlidingConsumerConfig,
    SlidingFeatureRecord,
    WindowResolution,
    event_timestamp_ms,
)
from streaming_feature_store.sliding.panes import (
    PanedSlidingWindow,
    SlidingWindowManager,
)
from streaming_feature_store.sliding.sinks import (
    KafkaLateEventsSink,
    KafkaSlidingFeaturesSink,
    RedisHashSink,
    load_sliding_schema_str,
)
from streaming_feature_store.sliding.watermark import WatermarkTracker

__all__ = [
    "AGGREGATOR_BY_RESOLUTION",
    "FiveMinuteAggregator",
    "KafkaLateEventsSink",
    "KafkaSlidingFeaturesSink",
    "OneHourAggregator",
    "PanedSlidingWindow",
    "RedisHashSink",
    "SlidingAccumulator",
    "SlidingConsumerConfig",
    "SlidingFeatureRecord",
    "SlidingFeaturesConsumer",
    "SlidingRunSnapshot",
    "SlidingWindowAggregator",
    "SlidingWindowManager",
    "TwentyFourHourAggregator",
    "WatermarkTracker",
    "WindowResolution",
    "event_timestamp_ms",
    "load_sliding_schema_str",
]
