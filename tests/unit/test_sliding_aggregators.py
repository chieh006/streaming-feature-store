"""Unit tests for :mod:`streaming_feature_store.sliding.aggregators`."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from streaming_feature_store.sliding.aggregators import (
    AGGREGATOR_BY_RESOLUTION,
    FiveMinuteAggregator,
    OneHourAggregator,
    SlidingWindowAggregator,
    TwentyFourHourAggregator,
)
from streaming_feature_store.sliding.models import SlidingAccumulator, WindowResolution


def _fold(aggregator, events):
    acc = aggregator.create_accumulator()
    for event in events:
        aggregator.add(event, acc)
    return acc


# ---------------------------------------------------------------------------
# add / counts
# ---------------------------------------------------------------------------


def test_five_minute_counts_clicks(sliding_events) -> None:
    agg = FiveMinuteAggregator()
    acc = _fold(agg, [sliding_events.click() for _ in range(3)])
    record = agg.get_result(acc)
    assert record.click_count == 3
    assert record.purchase_count == 0
    assert record.distinct_products is None  # excluded at 5 m


def test_five_minute_counts_page_views(sliding_events) -> None:
    agg = FiveMinuteAggregator()
    record = agg.get_result(_fold(agg, [sliding_events.page_view() for _ in range(4)]))
    assert record.page_view_count == 4


def test_revenue_sums_price_cents_over_100_times_quantity(sliding_events) -> None:
    agg = FiveMinuteAggregator()
    events = [
        sliding_events.purchase(price_cents=1000, quantity=2),  # $10 × 2 = 20
        sliding_events.purchase(price_cents=500, quantity=3),  # $5 × 3 = 15
    ]
    record = agg.get_result(_fold(agg, events))
    assert record.revenue == pytest.approx(35.0)
    assert record.purchase_count == 2


def test_add_ignores_unknown_event_type() -> None:
    agg = FiveMinuteAggregator()
    acc = agg.create_accumulator()
    stub = SimpleNamespace(user_id="u1", event_type="OTHER", payload=None)
    agg.add(stub, acc)
    assert acc.user_id == "u1"
    assert acc.click_count == 0
    assert acc.purchase_count == 0


# ---------------------------------------------------------------------------
# distinct products (1 h / 24 h) — purchases carry the only product_id
# ---------------------------------------------------------------------------


def test_one_hour_counts_distinct_products(sliding_events) -> None:
    agg = OneHourAggregator()
    events = [
        sliding_events.purchase(product_id="A"),
        sliding_events.purchase(product_id="B"),
        sliding_events.purchase(product_id="A"),
    ]
    record = agg.get_result(_fold(agg, events))
    assert record.distinct_products == 2


def test_distinct_products_union_on_merge() -> None:
    agg = OneHourAggregator()
    a = SlidingAccumulator(user_id="u1", distinct_products={"A", "B"})
    b = SlidingAccumulator(user_id="u1", distinct_products={"B", "C"})
    merged = agg.merge(a, b)
    assert merged.distinct_products == {"A", "B", "C"}
    assert agg.get_result(merged).distinct_products == 3


# ---------------------------------------------------------------------------
# 24 h aggregator
# ---------------------------------------------------------------------------


def test_twenty_four_hour_excludes_click_count(sliding_events) -> None:
    agg = TwentyFourHourAggregator()
    record = agg.get_result(_fold(agg, [sliding_events.click() for _ in range(5)]))
    assert record.click_count is None
    assert record.page_view_count is None


def test_twenty_four_hour_avg_purchase_amount(sliding_events) -> None:
    agg = TwentyFourHourAggregator()
    events = [
        sliding_events.purchase(price_cents=1000, quantity=1),  # $10
        sliding_events.purchase(price_cents=3000, quantity=1),  # $30
    ]
    record = agg.get_result(_fold(agg, events))
    assert record.avg_purchase_amount == pytest.approx(20.0)


def test_twenty_four_hour_avg_purchase_amount_none_without_purchases(
    sliding_events,
) -> None:
    agg = TwentyFourHourAggregator()
    record = agg.get_result(_fold(agg, [sliding_events.page_view()]))
    assert record.avg_purchase_amount is None
    assert record.purchase_count == 0


# ---------------------------------------------------------------------------
# merge algebra
# ---------------------------------------------------------------------------


def _sample_accumulators() -> list[SlidingAccumulator]:
    return [
        SlidingAccumulator(user_id="u1", click_count=1, revenue=2.0, distinct_products={"A"}),
        SlidingAccumulator(user_id="u1", purchase_count=2, revenue=3.5, distinct_products={"B"}),
        SlidingAccumulator(user_id="u1", page_view_count=4, distinct_products={"A", "C"}),
    ]


def test_merge_is_commutative() -> None:
    agg = OneHourAggregator()
    a, b, _ = _sample_accumulators()
    assert agg.merge(a, b) == agg.merge(b, a)


def test_merge_is_associative() -> None:
    agg = OneHourAggregator()
    a, b, c = _sample_accumulators()
    assert agg.merge(a, agg.merge(b, c)) == agg.merge(agg.merge(a, b), c)


def test_merge_prefers_first_user_id_then_second() -> None:
    agg = OneHourAggregator()
    left_blank = SlidingAccumulator(user_id="")
    right = SlidingAccumulator(user_id="u2")
    assert agg.merge(left_blank, right).user_id == "u2"
    assert agg.merge(right, left_blank).user_id == "u2"


# ---------------------------------------------------------------------------
# base class + registry
# ---------------------------------------------------------------------------


def test_base_get_result_raises() -> None:
    with pytest.raises(NotImplementedError):
        SlidingWindowAggregator().get_result(SlidingAccumulator())


def test_aggregator_registry_maps_each_resolution() -> None:
    assert AGGREGATOR_BY_RESOLUTION == {
        WindowResolution.W_5M_SLIDE_1M: FiveMinuteAggregator,
        WindowResolution.W_1H_SLIDE_5M: OneHourAggregator,
        WindowResolution.W_24H_SLIDE_1H: TwentyFourHourAggregator,
    }
    for resolution, cls in AGGREGATOR_BY_RESOLUTION.items():
        assert cls.resolution is resolution
