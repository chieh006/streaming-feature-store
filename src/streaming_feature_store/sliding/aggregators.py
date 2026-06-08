"""Pane-level incremental aggregators (relocated, PyFlink-free; design §2.13).

The aggregator algebra — ``create_accumulator`` / ``add`` / ``merge`` /
``get_result`` — is engine-agnostic and reused verbatim by the in-memory
windowing driver (design doc §2.3).  ``add`` and ``merge`` are identical across
the three resolutions; only ``get_result`` differs, selecting which features a
resolution publishes (design doc §2.14).

Unlike the superseded PyFlink design, the base class is a plain object — the
``AggregateFunction`` shim is gone (design doc §2.13).
"""

from __future__ import annotations

from typing import ClassVar

from streaming_feature_store.schemas import EcommerceEvent, EventType
from streaming_feature_store.sliding.models import (
    SlidingAccumulator,
    SlidingFeatureRecord,
    WindowResolution,
)


class SlidingWindowAggregator:
    """Shared pane-level aggregator; subclassed per resolution.

    Notes
    -----
    ``add`` mutates a pane accumulator in place (O(1) per event); ``merge``
    combines two accumulators into a fresh one (associative + commutative, the
    property the pane driver relies on, design doc §2.3).  The per-resolution
    subclasses override only :meth:`get_result`.
    """

    resolution: ClassVar[WindowResolution]

    def create_accumulator(self) -> SlidingAccumulator:
        """Return a fresh, empty pane accumulator.

        Returns
        -------
        SlidingAccumulator
            Zeroed accumulator with an empty ``distinct_products`` set.
        """
        return SlidingAccumulator()

    def add(self, event: EcommerceEvent, acc: SlidingAccumulator) -> SlidingAccumulator:
        """Fold *event* into *acc* in place.

        Parameters
        ----------
        event : EcommerceEvent
            Decoded event to aggregate.
        acc : SlidingAccumulator
            Pane accumulator to mutate.

        Returns
        -------
        SlidingAccumulator
            The same *acc*, mutated.

        Notes
        -----
        ``distinct_products`` only grows for ``PURCHASE`` events — they are the
        sole event type whose payload carries a ``product_id`` in the current
        e-commerce schema.  Unrecognised event types leave the accumulator
        unchanged.
        """
        acc.user_id = event.user_id
        if event.event_type == EventType.CLICK:
            acc.click_count += 1
        elif event.event_type == EventType.PAGE_VIEW:
            acc.page_view_count += 1
        elif event.event_type == EventType.PURCHASE:
            acc.purchase_count += 1
            acc.revenue += event.payload.price_cents / 100.0 * event.payload.quantity
            acc.distinct_products.add(event.payload.product_id)
        return acc

    def merge(
        self, a: SlidingAccumulator, b: SlidingAccumulator
    ) -> SlidingAccumulator:
        """Combine two pane accumulators into a fresh accumulator.

        Parameters
        ----------
        a, b : SlidingAccumulator
            Accumulators to merge.

        Returns
        -------
        SlidingAccumulator
            New accumulator holding the field-wise sums and the set union of
            ``distinct_products``.
        """
        return SlidingAccumulator(
            user_id=a.user_id or b.user_id,
            click_count=a.click_count + b.click_count,
            page_view_count=a.page_view_count + b.page_view_count,
            purchase_count=a.purchase_count + b.purchase_count,
            revenue=a.revenue + b.revenue,
            distinct_products=a.distinct_products | b.distinct_products,
        )

    def get_result(self, acc: SlidingAccumulator) -> SlidingFeatureRecord:
        """Project a merged accumulator into a resolution-scoped record.

        Parameters
        ----------
        acc : SlidingAccumulator
            Merged window accumulator.

        Returns
        -------
        SlidingFeatureRecord
            Record carrying only this resolution's feature subset.

        Raises
        ------
        NotImplementedError
            Always, on the base class — subclasses must override.
        """
        raise NotImplementedError("get_result must be implemented by a subclass")


class FiveMinuteAggregator(SlidingWindowAggregator):
    """5 m / 1 m-slide aggregator — real-time event counts (design §2.14)."""

    resolution = WindowResolution.W_5M_SLIDE_1M

    def get_result(self, acc: SlidingAccumulator) -> SlidingFeatureRecord:
        """Return the 5 m feature slice (clicks, page-views, purchases, revenue).

        Parameters
        ----------
        acc : SlidingAccumulator
            Merged window accumulator.

        Returns
        -------
        SlidingFeatureRecord
            Record with ``click_count`` / ``page_view_count`` /
            ``purchase_count`` / ``revenue`` populated; window bounds left at 0
            for the driver to fill.
        """
        return SlidingFeatureRecord(
            user_id=acc.user_id,
            window_resolution=self.resolution,
            click_count=acc.click_count,
            page_view_count=acc.page_view_count,
            purchase_count=acc.purchase_count,
            revenue=acc.revenue,
        )


class OneHourAggregator(SlidingWindowAggregator):
    """1 h / 5 m-slide aggregator — short-history features (design §2.14)."""

    resolution = WindowResolution.W_1H_SLIDE_5M

    def get_result(self, acc: SlidingAccumulator) -> SlidingFeatureRecord:
        """Return the 1 h feature slice, adding ``distinct_products``.

        Parameters
        ----------
        acc : SlidingAccumulator
            Merged window accumulator.

        Returns
        -------
        SlidingFeatureRecord
            Record with the 5 m features plus ``distinct_products``.
        """
        return SlidingFeatureRecord(
            user_id=acc.user_id,
            window_resolution=self.resolution,
            click_count=acc.click_count,
            page_view_count=acc.page_view_count,
            purchase_count=acc.purchase_count,
            revenue=acc.revenue,
            distinct_products=len(acc.distinct_products),
        )


class TwentyFourHourAggregator(SlidingWindowAggregator):
    """24 h / 1 h-slide aggregator — daily purchase history (design §2.14)."""

    resolution = WindowResolution.W_24H_SLIDE_1H

    def get_result(self, acc: SlidingAccumulator) -> SlidingFeatureRecord:
        """Return the 24 h feature slice (purchase aggregates only).

        Parameters
        ----------
        acc : SlidingAccumulator
            Merged window accumulator.

        Returns
        -------
        SlidingFeatureRecord
            Record with ``purchase_count`` / ``revenue`` / ``distinct_products``
            and ``avg_purchase_amount`` (``None`` when there were no purchases,
            avoiding a divide-by-zero downstream).
        """
        avg = acc.revenue / acc.purchase_count if acc.purchase_count > 0 else None
        return SlidingFeatureRecord(
            user_id=acc.user_id,
            window_resolution=self.resolution,
            purchase_count=acc.purchase_count,
            revenue=acc.revenue,
            distinct_products=len(acc.distinct_products),
            avg_purchase_amount=avg,
        )


# Per-resolution aggregator classes, indexed for the window manager (design
# doc §4.1).
AGGREGATOR_BY_RESOLUTION: dict[WindowResolution, type[SlidingWindowAggregator]] = {
    WindowResolution.W_5M_SLIDE_1M: FiveMinuteAggregator,
    WindowResolution.W_1H_SLIDE_5M: OneHourAggregator,
    WindowResolution.W_24H_SLIDE_1H: TwentyFourHourAggregator,
}
