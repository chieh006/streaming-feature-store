"""Unit tests for :class:`SyntheticEventGenerator`."""

from __future__ import annotations

from datetime import timezone

import pytest

from streaming_feature_store.load.synthetic import SyntheticEventGenerator
from streaming_feature_store.schemas.models import (
    EcommerceEvent,
    EventType,
    PurchasePayload,
)


def test_generate_batch_returns_n_events():
    gen = SyntheticEventGenerator(seed=1)
    events = gen.generate_batch(1024)
    assert len(events) == 1024
    assert all(isinstance(e, EcommerceEvent) for e in events)


def test_generate_batch_seed_is_deterministic():
    a = SyntheticEventGenerator(seed=7).generate_batch(64)
    b = SyntheticEventGenerator(seed=7).generate_batch(64)
    assert [e.user_id for e in a] == [e.user_id for e in b]
    assert [e.event_type for e in a] == [e.event_type for e in b]


def test_generate_batch_different_seed_diverges():
    a = SyntheticEventGenerator(seed=1).generate_batch(1024)
    b = SyntheticEventGenerator(seed=2).generate_batch(1024)
    different = sum(1 for x, y in zip(a, b) if x.event_id != y.event_id)
    assert different / 1024 > 0.99


def test_event_type_distribution_matches_weights():
    gen = SyntheticEventGenerator(seed=42)
    events = gen.generate_batch(20_000)
    counts = {t: 0 for t in EventType}
    for e in events:
        counts[e.event_type] += 1
    total = sum(counts.values())
    p_click = counts[EventType.CLICK] / total
    p_purchase = counts[EventType.PURCHASE] / total
    p_pageview = counts[EventType.PAGE_VIEW] / total
    assert abs(p_click - 0.7) < 0.03
    assert abs(p_purchase - 0.05) < 0.02
    assert abs(p_pageview - 0.25) < 0.03


def test_user_id_distribution_is_zipfian():
    gen = SyntheticEventGenerator(seed=3, num_users=10_000, user_zipf_alpha=1.1)
    events = gen.generate_batch(20_000)
    counts: dict[str, int] = {}
    for e in events:
        counts[e.user_id] = counts.get(e.user_id, 0) + 1
    sorted_counts = sorted(counts.values(), reverse=True)
    top_1pct = max(1, len(sorted_counts) // 100)
    head = sum(sorted_counts[:top_1pct])
    assert head / 20_000 > 0.10


def test_purchase_payload_quantity_positive():
    events = SyntheticEventGenerator(seed=11).generate_batch(2000)
    purchases = [e for e in events if isinstance(e.payload, PurchasePayload)]
    assert purchases, "expected at least one purchase event"
    assert all(p.payload.quantity > 0 for p in purchases)


def test_event_timestamp_is_timezone_aware_utc():
    events = SyntheticEventGenerator(seed=5).generate_batch(16)
    for e in events:
        assert e.event_timestamp.tzinfo is not None
        assert e.event_timestamp.utcoffset() == timezone.utc.utcoffset(None)


def test_zero_batch_size_returns_empty():
    assert SyntheticEventGenerator(seed=1).generate_batch(0) == []


def test_negative_batch_size_raises():
    with pytest.raises(ValueError):
        SyntheticEventGenerator(seed=1).generate_batch(-1)


def test_invalid_alpha_raises():
    with pytest.raises(ValueError):
        SyntheticEventGenerator(seed=1, user_zipf_alpha=0.9)


def test_invalid_num_users_raises():
    with pytest.raises(ValueError):
        SyntheticEventGenerator(seed=1, num_users=0)


def test_invalid_num_skus_raises():
    with pytest.raises(ValueError):
        SyntheticEventGenerator(seed=1, num_skus=0)


def test_invalid_type_weights_raises():
    with pytest.raises(ValueError):
        SyntheticEventGenerator(seed=1, type_weights=(0.5, 0.4, 0.2))


def test_user_id_is_within_population():
    gen = SyntheticEventGenerator(seed=1, num_users=100)
    events = gen.generate_batch(500)
    indices = {int(e.user_id.split("-")[1]) for e in events}
    assert all(0 <= i < 100 for i in indices)
