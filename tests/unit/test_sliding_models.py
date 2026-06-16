"""Unit tests for :mod:`streaming_feature_store.sliding.models`."""

from __future__ import annotations

import io

import fastavro
import pytest
from pydantic import ValidationError

from streaming_feature_store.schemas.loader import SCHEMAS_ROOT, load_avro_file
from streaming_feature_store.serving.models import FeatureVector
from streaming_feature_store.sliding.aggregators import AGGREGATOR_BY_RESOLUTION
from streaming_feature_store.sliding.models import (
    _REDIS_FIELD_PREFIXES,
    REDIS_FIELD_PREFIXES,
    RESOLUTION_FEATURES,
    SlidingAccumulator,
    SlidingConsumerConfig,
    SlidingFeatureRecord,
    WindowResolution,
    event_timestamp_ms,
    expected_redis_fields,
)

_AVSC_PATH = SCHEMAS_ROOT / "sliding" / "v1" / "sliding_feature_record.avsc"


# ---------------------------------------------------------------------------
# WindowResolution
# ---------------------------------------------------------------------------


def test_window_resolution_values_stable() -> None:
    assert WindowResolution.W_5M_SLIDE_1M.value == "5m"
    assert WindowResolution.W_1H_SLIDE_5M.value == "1h"
    assert WindowResolution.W_24H_SLIDE_1H.value == "24h"


@pytest.mark.parametrize(
    ("resolution", "size_s", "slide_s", "panes"),
    [
        (WindowResolution.W_5M_SLIDE_1M, 300, 60, 5),
        (WindowResolution.W_1H_SLIDE_5M, 3600, 300, 12),
        (WindowResolution.W_24H_SLIDE_1H, 86400, 3600, 24),
    ],
)
def test_window_resolution_geometry(resolution, size_s, slide_s, panes) -> None:
    assert resolution.window_size_seconds == size_s
    assert resolution.slide_seconds == slide_s
    assert resolution.window_size_ms == size_s * 1000
    assert resolution.slide_ms == slide_s * 1000
    assert resolution.panes_per_window == panes


def test_window_resolution_avro_symbols_match_schema() -> None:
    schema = load_avro_file(_AVSC_PATH)
    enum_field = next(
        f for f in schema["fields"] if f["name"] == "window_resolution"
    )
    assert enum_field["type"]["symbols"] == [r.name for r in WindowResolution]


# ---------------------------------------------------------------------------
# SlidingFeatureRecord
# ---------------------------------------------------------------------------


def _record_5m() -> SlidingFeatureRecord:
    return SlidingFeatureRecord(
        user_id="u1",
        window_resolution=WindowResolution.W_5M_SLIDE_1M,
        window_start_ms=0,
        window_end_ms=300_000,
        emission_seq=2,
        click_count=7,
        page_view_count=3,
        purchase_count=1,
        revenue=12.5,
    )


def _record_24h() -> SlidingFeatureRecord:
    return SlidingFeatureRecord(
        user_id="u9",
        window_resolution=WindowResolution.W_24H_SLIDE_1H,
        window_start_ms=0,
        window_end_ms=86_400_000,
        purchase_count=2,
        revenue=40.0,
        distinct_products=2,
        avg_purchase_amount=20.0,
    )


def test_idempotency_key_format() -> None:
    assert _record_5m().idempotency_key() == "u1:5m:300000:2"


def test_kafka_key_format() -> None:
    assert _record_5m().kafka_key() == "u1:5m"
    assert _record_24h().kafka_key() == "u9:24h"


def test_redis_field_updates_5m_has_expected_fields() -> None:
    fields = _record_5m().redis_field_updates()
    assert set(fields) == {"clicks_5m", "page_views_5m", "purchases_5m", "revenue_5m"}
    assert fields["clicks_5m"] == "7"
    assert fields["revenue_5m"] == "12.5"


def test_redis_field_updates_omits_none() -> None:
    # The 5 m record never carries distinct_products / avg_purchase_amount.
    fields = _record_5m().redis_field_updates()
    assert "distinct_products_5m" not in fields
    assert "avg_purchase_amount_5m" not in fields


def test_redis_field_resolution_suffixes() -> None:
    assert "clicks_5m" in _record_5m().redis_field_updates()
    assert "purchases_24h" in _record_24h().redis_field_updates()
    assert "distinct_products_24h" in _record_24h().redis_field_updates()


def test_redis_field_updates_empty_when_all_features_none() -> None:
    bare = SlidingFeatureRecord(
        user_id="u1", window_resolution=WindowResolution.W_5M_SLIDE_1M
    )
    assert bare.redis_field_updates() == {}


def test_to_avro_dict_uses_symbol_name() -> None:
    assert _record_5m().to_avro_dict()["window_resolution"] == "W_5M_SLIDE_1M"


@pytest.mark.parametrize("record_factory", [_record_5m, _record_24h])
def test_avro_round_trip_through_fastavro(record_factory) -> None:
    record = record_factory()
    schema = fastavro.parse_schema(load_avro_file(_AVSC_PATH))
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, schema, record.to_avro_dict())
    buffer.seek(0)
    decoded = fastavro.schemaless_reader(buffer, schema)
    assert SlidingFeatureRecord.from_avro_dict(decoded) == record


def test_from_avro_dict_defaults_missing_optional_fields() -> None:
    minimal = {
        "user_id": "u1",
        "window_resolution": "W_1H_SLIDE_5M",
        "window_start_ms": 0,
        "window_end_ms": 3_600_000,
    }
    record = SlidingFeatureRecord.from_avro_dict(minimal)
    assert record.emission_seq == 0
    assert record.click_count is None
    assert record.window_resolution is WindowResolution.W_1H_SLIDE_5M


# ---------------------------------------------------------------------------
# event_timestamp_ms
# ---------------------------------------------------------------------------


def test_event_timestamp_ms(sliding_events) -> None:
    event = sliding_events.click(ts_ms=1_700_000_000_123)
    assert event_timestamp_ms(event) == 1_700_000_000_123


# ---------------------------------------------------------------------------
# SlidingConsumerConfig
# ---------------------------------------------------------------------------


def test_config_defaults() -> None:
    cfg = SlidingConsumerConfig()
    assert cfg.source_topic == "validated-events"
    assert cfg.sink_topic == "sliding-features"
    assert cfg.late_sink_topic == "sliding-features-late"
    assert cfg.num_workers == 1
    assert cfg.warmup_seek_back is True


def test_config_rejects_equal_topics() -> None:
    with pytest.raises(ValidationError, match="distinct"):
        SlidingConsumerConfig(source_topic="x", sink_topic="x")


def test_config_rejects_equal_sink_and_late_topics() -> None:
    with pytest.raises(ValidationError, match="distinct"):
        SlidingConsumerConfig(sink_topic="dup", late_sink_topic="dup")


def test_config_rejects_lateness_at_or_above_smallest_window() -> None:
    with pytest.raises(ValidationError, match="smallest window"):
        SlidingConsumerConfig(allowed_lateness_seconds=300)


def test_config_rejects_extra_fields() -> None:
    with pytest.raises(ValidationError):
        SlidingConsumerConfig(unknown_field=1)


def test_config_rejects_non_positive_ttl_factor() -> None:
    with pytest.raises(ValidationError):
        SlidingConsumerConfig(ttl_factor=0.0)


@pytest.mark.parametrize(
    ("resolution", "ttl"),
    [
        (WindowResolution.W_5M_SLIDE_1M, 450),
        (WindowResolution.W_1H_SLIDE_5M, 5400),
        (WindowResolution.W_24H_SLIDE_1H, 129_600),
    ],
)
def test_ttl_seconds_for(resolution, ttl) -> None:
    assert SlidingConsumerConfig().ttl_seconds_for(resolution) == ttl


# ---------------------------------------------------------------------------
# Read-side contract drift guards (design week3_01 §2.2 / §5)
# ---------------------------------------------------------------------------


def _populated_prefixes(resolution: WindowResolution) -> set[str]:
    """Return the Redis prefixes a resolution's aggregator actually populates.

    Builds a fully-populated accumulator (clicks + page-views + a purchase, so
    ``avg_purchase_amount`` avoids its no-purchase ``None``), projects it via
    ``get_result``, and maps every non-``None`` record field to its Redis
    prefix — the ground truth the serving layer must mirror.
    """
    aggregator = AGGREGATOR_BY_RESOLUTION[resolution]()
    acc = SlidingAccumulator(
        user_id="u",
        click_count=1,
        page_view_count=1,
        purchase_count=1,
        revenue=10.0,
        distinct_products={"a"},
    )
    record = aggregator.get_result(acc)
    return {
        prefix
        for field_name, prefix in REDIS_FIELD_PREFIXES.items()
        if getattr(record, field_name) is not None
    }


@pytest.mark.parametrize("resolution", list(WindowResolution))
def test_resolution_features_match_aggregators(resolution) -> None:
    assert set(RESOLUTION_FEATURES[resolution]) == _populated_prefixes(resolution)


def test_feature_vector_fields_equal_expected_redis_fields() -> None:
    assert set(FeatureVector.model_fields.keys()) == expected_redis_fields()


def test_redis_field_prefixes_alias_identity() -> None:
    assert REDIS_FIELD_PREFIXES is _REDIS_FIELD_PREFIXES


def test_expected_redis_fields_has_thirteen_members() -> None:
    fields = expected_redis_fields()
    assert len(fields) == 13
    assert "clicks_5m" in fields
    assert "avg_purchase_amount_24h" in fields
