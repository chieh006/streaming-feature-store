"""Vectorized synthetic ``EcommerceEvent`` generator.

The generator pre-allocates batches of N events using ``numpy`` vectorized
random draws over a Zipfian user-id population, then materializes
:class:`EcommerceEvent` instances just-in-time.  Determinism via a seeded
``numpy.random.default_rng``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID

import numpy as np

from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
    PageViewPayload,
    PurchasePayload,
)

logger = logging.getLogger(__name__)

_TYPE_ORDER: tuple[EventType, EventType, EventType] = (
    EventType.CLICK,
    EventType.PURCHASE,
    EventType.PAGE_VIEW,
)


class SyntheticEventGenerator:
    """Generate :class:`EcommerceEvent` instances with vectorized random draws.

    Parameters
    ----------
    seed : int, optional
        RNG seed for reproducibility.  Defaults to ``42``.
    num_users : int, optional
        Population of unique ``user_id`` values; sampled with Zipfian skew.
        Defaults to ``100_000``.
    num_skus : int, optional
        Population of unique ``product_id`` values.  Defaults to ``10_000``.
    user_zipf_alpha : float, optional
        Zipf exponent governing user-id skew.  Must be ``> 1``.
        Defaults to ``1.1``.
    type_weights : tuple of float, optional
        Marginal probabilities of (CLICK, PURCHASE, PAGE_VIEW).  Must sum to
        ``1.0``.  Defaults to ``(0.7, 0.05, 0.25)``.

    Notes
    -----
    Construction allocates one ``numpy.random.Generator`` (not the global
    RNG); two generators with the same seed produce identical streams.
    """

    def __init__(
        self,
        seed: int = 42,
        *,
        num_users: int = 100_000,
        num_skus: int = 10_000,
        user_zipf_alpha: float = 1.1,
        type_weights: tuple[float, float, float] = (0.7, 0.05, 0.25),
    ) -> None:
        if num_users < 1:
            raise ValueError(f"num_users must be >= 1, got {num_users}")
        if num_skus < 1:
            raise ValueError(f"num_skus must be >= 1, got {num_skus}")
        if user_zipf_alpha <= 1.0:
            raise ValueError(f"user_zipf_alpha must be > 1, got {user_zipf_alpha}")
        if not np.isclose(sum(type_weights), 1.0):
            raise ValueError(f"type_weights must sum to 1.0, got {type_weights}")
        self._rng = np.random.default_rng(seed)
        self._num_users = num_users
        self._num_skus = num_skus
        self._alpha = user_zipf_alpha
        self._type_weights = np.asarray(type_weights, dtype=np.float64)

    def _draw_user_indices(self, n: int) -> np.ndarray:
        """Draw *n* user-id indices with Zipfian skew.

        Parameters
        ----------
        n : int
            Sample size.

        Returns
        -------
        numpy.ndarray
            Integer array of shape ``(n,)`` in ``[0, num_users)``.
        """
        raw = self._rng.zipf(self._alpha, size=n)
        return (raw - 1) % self._num_users

    def _draw_event_types(self, n: int) -> np.ndarray:
        """Draw *n* event-type indices.

        Parameters
        ----------
        n : int
            Sample size.

        Returns
        -------
        numpy.ndarray
            Integer array of shape ``(n,)`` in ``[0, 3)``.
        """
        return self._rng.choice(3, size=n, p=self._type_weights)

    def _make_payload(
        self,
        event_type: EventType,
        sku_index: int,
        quantity: int,
        price_cents: int,
    ) -> ClickPayload | PurchasePayload | PageViewPayload:
        """Construct the payload object matching *event_type*.

        Parameters
        ----------
        event_type : EventType
            Discriminator.
        sku_index : int
            SKU index for purchase events.
        quantity : int
            Quantity for purchase events.
        price_cents : int
            Price (cents) for purchase events.

        Returns
        -------
        ClickPayload, PurchasePayload, or PageViewPayload
            Concrete payload model.
        """
        if event_type is EventType.CLICK:
            return ClickPayload(element_id="btn-cta", page_url="/products")
        if event_type is EventType.PURCHASE:
            return PurchasePayload(
                product_id=f"sku-{sku_index:05d}",
                quantity=int(quantity),
                price_cents=int(price_cents),
            )
        return PageViewPayload(page_url="/products", referrer=None)

    def generate_batch(self, n: int) -> list[EcommerceEvent]:
        """Generate *n* :class:`EcommerceEvent` instances.

        Parameters
        ----------
        n : int
            Batch size.  Must be ``>= 0``.

        Returns
        -------
        list of EcommerceEvent
            Length-*n* list (empty if ``n == 0``).

        Raises
        ------
        ValueError
            If *n* is negative.
        """
        if n < 0:
            raise ValueError(f"n must be >= 0, got {n}")
        if n == 0:
            return []
        user_idx = self._draw_user_indices(n)
        type_idx = self._draw_event_types(n)
        sku_idx = self._rng.integers(0, self._num_skus, size=n)
        quantities = self._rng.integers(1, 5, size=n)
        # Log-normal pricing in cents, clipped to a sensible range.
        prices = np.clip(
            self._rng.lognormal(mean=7.5, sigma=0.6, size=n).astype(np.int64),
            50,
            500_000,
        )
        # Pre-generate UUID4 bytes vectorially.
        uuid_bytes = self._rng.integers(0, 256, size=(n, 16), dtype=np.uint8)
        now_us = int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000)
        # Spread timestamps over the past second so partitioning isn't degenerate.
        ts_us = now_us - self._rng.integers(0, 1_000_000, size=n, dtype=np.int64)

        events: list[EcommerceEvent] = []
        for i in range(n):
            event_type = _TYPE_ORDER[int(type_idx[i])]
            event_id = UUID(bytes=bytes(uuid_bytes[i].tolist()), version=4)
            payload = self._make_payload(
                event_type,
                int(sku_idx[i]),
                int(quantities[i]),
                int(prices[i]),
            )
            ts = datetime.fromtimestamp(int(ts_us[i]) / 1_000_000, tz=timezone.utc)
            events.append(
                EcommerceEvent(
                    event_id=event_id,
                    event_type=event_type,
                    user_id=f"u-{int(user_idx[i]):06d}",
                    session_id=f"s-{int(user_idx[i]):06d}-{i % 1000:03d}",
                    event_timestamp=ts,
                    payload=payload,
                )
            )
        return events
