"""Pydantic mirrors of the Avro event schemas.

The producer accepts a Pydantic model instance, which gives clear, structured
validation errors at construction time before bytes ever reach the Avro
serializer.  ``EcommerceEvent.to_avro_dict`` produces the exact dict shape that
``fastavro``/``confluent-kafka-python`` expect (UUID stringified, datetime in
microseconds since epoch, payload encoded as a tagged union).
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Union
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

PAYLOAD_NAMESPACE: str = "com.featurestore.ecommerce.v1"


class EventType(str, Enum):
    """Enumeration of e-commerce event types.

    Notes
    -----
    The string values match the Avro enum symbols on the wire
    (``CLICK``, ``PURCHASE``, ``PAGE_VIEW``).
    """

    CLICK = "CLICK"
    PURCHASE = "PURCHASE"
    PAGE_VIEW = "PAGE_VIEW"


class _ImmutableBase(BaseModel):
    """Base class enforcing immutability and forbidding extra fields."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class ClickPayload(_ImmutableBase):
    """Click event payload.

    Parameters
    ----------
    element_id : str
        DOM element identifier that was clicked.
    page_url : str
        URL of the page where the click occurred.
    """

    element_id: str = Field(..., min_length=1)
    page_url: str = Field(..., min_length=1)


class PurchasePayload(_ImmutableBase):
    """Purchase event payload.

    Parameters
    ----------
    product_id : str
        Catalog identifier for the purchased product.
    quantity : int
        Number of units purchased; must be ``>= 1``.
    price_cents : int
        Per-unit price in minor currency units; must be ``>= 0``.
    currency : str
        ISO 4217 three-letter currency code; defaults to ``"USD"``.
    """

    product_id: str = Field(..., min_length=1)
    quantity: int = Field(..., ge=1)
    price_cents: int = Field(..., ge=0)
    currency: str = Field(default="USD", pattern=r"^[A-Z]{3}$")


class PageViewPayload(_ImmutableBase):
    """Page-view event payload.

    Parameters
    ----------
    page_url : str
        URL of the page being viewed.
    referrer : str or None
        Optional HTTP referrer URL.
    """

    page_url: str = Field(..., min_length=1)
    referrer: str | None = None


Payload = Annotated[
    Union[ClickPayload, PurchasePayload, PageViewPayload],
    Field(discriminator=None),
]

_PAYLOAD_FQN: dict[type, str] = {
    ClickPayload: f"{PAYLOAD_NAMESPACE}.ClickPayload",
    PurchasePayload: f"{PAYLOAD_NAMESPACE}.PurchasePayload",
    PageViewPayload: f"{PAYLOAD_NAMESPACE}.PageViewPayload",
}


def _payload_to_tagged_union(
    payload: ClickPayload | PurchasePayload | PageViewPayload,
) -> tuple[str, dict]:
    """Encode a payload as ``fastavro``'s tagged-union tuple.

    Parameters
    ----------
    payload : ClickPayload, PurchasePayload, or PageViewPayload
        Concrete payload model instance.

    Returns
    -------
    tuple of (str, dict)
        ``(<fully.qualified.name>, <payload fields>)`` — the form ``fastavro``
        accepts for unions of named records (the dict form is not supported in
        recent ``fastavro`` releases).
    """
    fqn = _PAYLOAD_FQN[type(payload)]
    return fqn, payload.model_dump()


def _datetime_to_avro_micros(value: datetime) -> int:
    """Convert a (timezone-aware) datetime to Avro ``timestamp-micros``.

    Parameters
    ----------
    value : datetime
        Naive or timezone-aware datetime.  Naive values are interpreted as UTC.

    Returns
    -------
    int
        Microseconds since the Unix epoch (UTC).
    """
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = value - epoch
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


class EcommerceEvent(_ImmutableBase):
    """Envelope for all e-commerce events.

    Parameters
    ----------
    event_id : UUID
        Globally-unique event identifier.
    event_type : EventType
        Discriminator for the payload union.
    user_id : str
        Stable user identifier; used as the Kafka message key.
    session_id : str
        Browser/app session identifier.
    event_timestamp : datetime
        Event time; converted to Avro ``timestamp-micros`` on serialization.
    payload : ClickPayload, PurchasePayload, or PageViewPayload
        Event-type-specific payload.
    """

    event_id: UUID
    event_type: EventType
    user_id: str = Field(..., min_length=1)
    session_id: str = Field(..., min_length=1)
    event_timestamp: datetime
    payload: ClickPayload | PurchasePayload | PageViewPayload

    def to_avro_dict(self) -> dict:
        """Convert this event to a dict matching the Avro envelope schema.

        Returns
        -------
        dict
            Avro-shaped dict with stringified UUID, microsecond timestamp, and
            tagged-union payload.
        """
        return {
            "event_id": str(self.event_id),
            "event_type": self.event_type.value,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "event_timestamp": _datetime_to_avro_micros(self.event_timestamp),
            "payload": _payload_to_tagged_union(self.payload),
        }
