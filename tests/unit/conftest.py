"""Shared unit-test fixtures.

Provides :func:`sliding_events` — a namespace of deterministic
:class:`EcommerceEvent` builders used by the sliding-window feature tests — and
:class:`StubAsyncRedis` / :func:`stub_async_redis_cls` for the Week 3 serving
tests' injected-client convention (design week3_01 §5).
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from streaming_feature_store.schemas import (
    ClickPayload,
    EcommerceEvent,
    EventType,
    PageViewPayload,
    PurchasePayload,
)


class StubAsyncRedis:
    """Minimal stand-in for ``redis.asyncio.Redis`` (design week3_01 §5).

    Parameters
    ----------
    hash_map : dict of str to str or None, optional
        Canned ``HGETALL`` response.  ``None`` (the default) models an absent
        key — an empty hash.
    fail_with : Exception or None, optional
        Exception instance raised by :meth:`hgetall` and :meth:`ping` for
        failure injection (e.g. ``ConnectionError`` / ``TimeoutError``).
    """

    def __init__(
        self,
        hash_map: dict[str, str] | None = None,
        *,
        fail_with: Exception | None = None,
    ) -> None:
        self._hash_map = dict(hash_map) if hash_map else {}
        self._fail_with = fail_with
        self.aclose_calls = 0
        self.last_key: str | None = None

    async def hgetall(self, key: str) -> dict[str, str]:
        """Return the canned hash (or raise the injected failure)."""
        self.last_key = key
        if self._fail_with is not None:
            raise self._fail_with
        return dict(self._hash_map)

    async def ping(self) -> bool:
        """Return ``True`` (or raise the injected failure)."""
        if self._fail_with is not None:
            raise self._fail_with
        return True

    async def aclose(self) -> None:
        """Record a close call (the reader must close its client once)."""
        self.aclose_calls += 1


@pytest.fixture
def stub_async_redis_cls() -> type[StubAsyncRedis]:
    """Return the :class:`StubAsyncRedis` class for the serving tests.

    Returns
    -------
    type[StubAsyncRedis]
        The stub class, constructed per-test with canned data / failures.
    """
    return StubAsyncRedis


def _ts(ms: int) -> datetime:
    """Return a UTC datetime for *ms* milliseconds since the epoch."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def make_click(
    user_id: str = "u1",
    ts_ms: int = 0,
    element_id: str = "btn",
    page_url: str = "/home",
) -> EcommerceEvent:
    """Build a CLICK event at *ts_ms* for *user_id*."""
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.CLICK,
        user_id=user_id,
        session_id="s",
        event_timestamp=_ts(ts_ms),
        payload=ClickPayload(element_id=element_id, page_url=page_url),
    )


def make_page_view(
    user_id: str = "u1",
    ts_ms: int = 0,
    page_url: str = "/products",
    referrer: str | None = None,
) -> EcommerceEvent:
    """Build a PAGE_VIEW event at *ts_ms* for *user_id*."""
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.PAGE_VIEW,
        user_id=user_id,
        session_id="s",
        event_timestamp=_ts(ts_ms),
        payload=PageViewPayload(page_url=page_url, referrer=referrer),
    )


def make_purchase(
    user_id: str = "u1",
    ts_ms: int = 0,
    product_id: str = "sku-1",
    quantity: int = 1,
    price_cents: int = 1000,
) -> EcommerceEvent:
    """Build a PURCHASE event at *ts_ms* for *user_id*."""
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.PURCHASE,
        user_id=user_id,
        session_id="s",
        event_timestamp=_ts(ts_ms),
        payload=PurchasePayload(
            product_id=product_id, quantity=quantity, price_cents=price_cents
        ),
    )


@pytest.fixture
def sliding_events() -> SimpleNamespace:
    """Return a namespace of event builders: ``.click`` / ``.page_view`` / ``.purchase``.

    Returns
    -------
    types.SimpleNamespace
        Namespace exposing the three deterministic event-builder callables.
    """
    return SimpleNamespace(
        click=make_click, page_view=make_page_view, purchase=make_purchase
    )
