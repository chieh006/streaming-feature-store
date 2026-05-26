"""Concrete validators applied by :class:`ValidationPipeline`.

Each validator is stateless: ``validate(event)`` returns either
:class:`Valid` (the message passes) or :class:`Invalid` (the message is
rejected, with structured error metadata).  Validators that only apply to
a subset of event types declare ``applies_to`` as a frozenset of
:class:`EventType` values; the pipeline (not the validator) is responsible
for the filter (design doc §2.5 / §4.2).

Six validators are shipped:

1. :class:`RequiredFieldsValidator`
2. :class:`EventTypeAllowlistValidator`
3. :class:`UserIdShapeValidator`
4. :class:`PriceRangeValidator`
5. :class:`QuantityRangeValidator`
6. :class:`TimestampRangeValidator`
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from streaming_feature_store.schemas import EcommerceEvent, EventType, PurchasePayload
from streaming_feature_store.validate.dlq import ErrorClass
from streaming_feature_store.validate.pipeline import (
    Invalid,
    Valid,
    ValidateResult,
    Validator,
)

logger = logging.getLogger(__name__)

_DEFAULT_ALLOWED_TYPES: frozenset[EventType] = frozenset(
    {EventType.CLICK, EventType.PURCHASE, EventType.PAGE_VIEW}
)


class RequiredFieldsValidator:
    """Reject events where any required identity field is empty.

    Pydantic already enforces non-empty ``user_id`` / ``session_id`` /
    ``event_id`` at decode time, but a future producer that bypasses the
    Pydantic adapter could still emit an empty-string identifier; this
    validator is the post-decode safety net (design doc §2.5).
    """

    name: str = "RequiredFieldsValidator"
    applies_to: frozenset[EventType] | None = None

    def validate(self, event: EcommerceEvent) -> ValidateResult:
        """Check that identity fields are present and non-empty.

        Parameters
        ----------
        event : EcommerceEvent
            Decoded event.

        Returns
        -------
        Valid or Invalid
            ``Invalid(NULL_REQUIRED_FIELD)`` for the first empty field;
            ``Valid(event)`` otherwise.
        """
        if not event.user_id:
            return Invalid(
                error_class=ErrorClass.NULL_REQUIRED_FIELD,
                validator_name=self.name,
                error_field_path="user_id",
                error_message="user_id is empty",
            )
        if not event.session_id:
            return Invalid(
                error_class=ErrorClass.NULL_REQUIRED_FIELD,
                validator_name=self.name,
                error_field_path="session_id",
                error_message="session_id is empty",
            )
        return Valid(event=event)


class EventTypeAllowlistValidator:
    """Reject events whose ``event_type`` is outside the configured allowlist.

    Parameters
    ----------
    allowed : frozenset[EventType] or None, optional
        Set of accepted event types.  Defaults to the project's full
        :class:`EventType` enum.

    Notes
    -----
    Pydantic-level validation already coerces unknown string values into
    an ``EventType`` enum miss, so this validator's main role is for the
    *forward-compatibility* case: an old reader receiving a message whose
    Avro schema introduced a new enum symbol the reader does not yet know.
    """

    name: str = "EventTypeAllowlistValidator"
    applies_to: frozenset[EventType] | None = None

    def __init__(self, allowed: frozenset[EventType] | None = None) -> None:
        self._allowed = frozenset(allowed) if allowed else _DEFAULT_ALLOWED_TYPES

    @property
    def allowed(self) -> frozenset[EventType]:
        """Allowed event types.

        Returns
        -------
        frozenset of EventType
            Configured allowlist.
        """
        return self._allowed

    def validate(self, event: EcommerceEvent) -> ValidateResult:
        """Check ``event.event_type`` against the allowlist.

        Parameters
        ----------
        event : EcommerceEvent
            Decoded event.

        Returns
        -------
        Valid or Invalid
            ``Invalid(UNKNOWN_EVENT_TYPE)`` if the type is not in the
            allowlist; ``Valid`` otherwise.
        """
        if event.event_type not in self._allowed:
            observed = getattr(event.event_type, "value", str(event.event_type))
            return Invalid(
                error_class=ErrorClass.UNKNOWN_EVENT_TYPE,
                validator_name=self.name,
                error_field_path="event_type",
                error_message=(
                    f"event_type={observed!r} is not in "
                    f"allowlist={sorted(t.value for t in self._allowed)}"
                ),
            )
        return Valid(event=event)


class UserIdShapeValidator:
    """Reject events whose ``user_id`` is too long or carries newlines.

    Parameters
    ----------
    max_length : int, optional
        Inclusive upper bound on ``len(user_id)``.  Defaults to ``256``.

    Notes
    -----
    Pydantic already enforces ``min_length=1``; this validator covers the
    *upper* bound and the cheap PII-sanity check for embedded newlines or
    null bytes (which suggest the producer is concatenating raw user input
    into the identifier — a bug pattern worth catching early).
    """

    name: str = "UserIdShapeValidator"
    applies_to: frozenset[EventType] | None = None

    def __init__(self, max_length: int = 256) -> None:
        if max_length < 1:
            raise ValueError(f"max_length must be >= 1, got {max_length}")
        self._max_length = int(max_length)

    @property
    def max_length(self) -> int:
        """Configured upper bound for ``len(user_id)``.

        Returns
        -------
        int
            Maximum length.
        """
        return self._max_length

    def validate(self, event: EcommerceEvent) -> ValidateResult:
        """Check ``event.user_id`` length and forbidden characters.

        Parameters
        ----------
        event : EcommerceEvent
            Decoded event.

        Returns
        -------
        Valid or Invalid
            ``Invalid(MALFORMED_RECORD)`` if length or shape fails;
            ``Valid`` otherwise.
        """
        user_id = event.user_id
        if len(user_id) > self._max_length:
            return Invalid(
                error_class=ErrorClass.MALFORMED_RECORD,
                validator_name=self.name,
                error_field_path="user_id",
                error_message=(
                    f"user_id length {len(user_id)} exceeds "
                    f"max_length={self._max_length}"
                ),
            )
        if "\n" in user_id or "\x00" in user_id:
            return Invalid(
                error_class=ErrorClass.MALFORMED_RECORD,
                validator_name=self.name,
                error_field_path="user_id",
                error_message="user_id contains forbidden control character",
            )
        return Valid(event=event)


class PriceRangeValidator:
    """Reject ``PURCHASE`` events with non-positive or absurdly large prices.

    Parameters
    ----------
    min_price_cents : int, optional
        Exclusive lower bound on ``payload.price_cents``.  Defaults to ``0``
        (price must be strictly positive).
    max_price_cents : int, optional
        Inclusive upper bound.  Defaults to ``1_000_000_000`` (~ ten million
        dollars; the sanity cap rejects obviously-corrupt prices without
        rejecting legitimate big-ticket items).

    Notes
    -----
    Skips events whose ``event_type`` is not in :attr:`applies_to`.  The
    pipeline enforces the skip, not this class — see design doc §2.5 /
    §4.2.
    """

    name: str = "PriceRangeValidator"
    applies_to: frozenset[EventType] = frozenset({EventType.PURCHASE})

    def __init__(
        self,
        min_price_cents: int = 0,
        max_price_cents: int = 1_000_000_000,
    ) -> None:
        if min_price_cents < 0:
            raise ValueError(
                f"min_price_cents must be >= 0, got {min_price_cents}"
            )
        if max_price_cents <= min_price_cents:
            raise ValueError(
                f"max_price_cents={max_price_cents} must be > "
                f"min_price_cents={min_price_cents}"
            )
        self._min = int(min_price_cents)
        self._max = int(max_price_cents)

    @property
    def min_price_cents(self) -> int:
        """Configured exclusive lower bound.

        Returns
        -------
        int
            Minimum price in minor currency units.
        """
        return self._min

    @property
    def max_price_cents(self) -> int:
        """Configured inclusive upper bound.

        Returns
        -------
        int
            Maximum price in minor currency units.
        """
        return self._max

    def validate(self, event: EcommerceEvent) -> ValidateResult:
        """Check ``payload.price_cents`` against the configured bounds.

        Parameters
        ----------
        event : EcommerceEvent
            Decoded event; ``payload`` must be :class:`PurchasePayload`.

        Returns
        -------
        Valid or Invalid
            ``Invalid(OUT_OF_RANGE)`` when the price is outside bounds.
        """
        payload = event.payload
        if not isinstance(payload, PurchasePayload):
            # Defensive: the pipeline guarantees applies_to filtering, but a
            # caller that bypasses the pipeline would land here.
            return Valid(event=event)
        if payload.price_cents <= self._min:
            return Invalid(
                error_class=ErrorClass.OUT_OF_RANGE,
                validator_name=self.name,
                error_field_path="payload.price_cents",
                error_message=(
                    f"price_cents={payload.price_cents} <= "
                    f"min_price_cents={self._min}"
                ),
            )
        if payload.price_cents > self._max:
            return Invalid(
                error_class=ErrorClass.OUT_OF_RANGE,
                validator_name=self.name,
                error_field_path="payload.price_cents",
                error_message=(
                    f"price_cents={payload.price_cents} > "
                    f"max_price_cents={self._max}"
                ),
            )
        return Valid(event=event)


class QuantityRangeValidator:
    """Reject ``PURCHASE`` events with quantity outside ``[1, max_quantity]``.

    Parameters
    ----------
    max_quantity : int, optional
        Inclusive upper bound on ``payload.quantity``.  Defaults to
        ``10_000``.

    Notes
    -----
    Pydantic already enforces ``quantity >= 1`` at decode time, so this
    validator's primary contribution is the upper bound (used to catch
    likely overflow / off-by-one bugs in producers).
    """

    name: str = "QuantityRangeValidator"
    applies_to: frozenset[EventType] = frozenset({EventType.PURCHASE})

    def __init__(self, max_quantity: int = 10_000) -> None:
        if max_quantity < 1:
            raise ValueError(f"max_quantity must be >= 1, got {max_quantity}")
        self._max = int(max_quantity)

    @property
    def max_quantity(self) -> int:
        """Configured inclusive upper bound on ``payload.quantity``.

        Returns
        -------
        int
            Maximum quantity.
        """
        return self._max

    def validate(self, event: EcommerceEvent) -> ValidateResult:
        """Check ``payload.quantity`` against the upper bound.

        Parameters
        ----------
        event : EcommerceEvent
            Decoded event; ``payload`` must be :class:`PurchasePayload`.

        Returns
        -------
        Valid or Invalid
            ``Invalid(OUT_OF_RANGE)`` when ``quantity`` is out of bounds.
        """
        payload = event.payload
        if not isinstance(payload, PurchasePayload):
            return Valid(event=event)
        if payload.quantity < 1:
            return Invalid(
                error_class=ErrorClass.OUT_OF_RANGE,
                validator_name=self.name,
                error_field_path="payload.quantity",
                error_message=f"quantity={payload.quantity} < 1",
            )
        if payload.quantity > self._max:
            return Invalid(
                error_class=ErrorClass.OUT_OF_RANGE,
                validator_name=self.name,
                error_field_path="payload.quantity",
                error_message=(
                    f"quantity={payload.quantity} > max_quantity={self._max}"
                ),
            )
        return Valid(event=event)


class TimestampRangeValidator:
    """Reject events whose ``event_timestamp`` falls outside a sane window.

    Parameters
    ----------
    max_age : timedelta, optional
        Maximum allowed age (``now - event_timestamp``).  Defaults to
        ``timedelta(days=7)``.
    max_future_skew : timedelta, optional
        Maximum allowed future skew (``event_timestamp - now``).  Defaults
        to ``timedelta(hours=1)`` — generous on purpose to accommodate
        producer NTP drift without rejecting otherwise-valid events.
    now : callable, optional
        Zero-arg callable returning the current UTC :class:`datetime`.
        Injected by tests; defaults to :func:`datetime.now` in UTC.

    Notes
    -----
    The window is *symmetric* around the wall clock but with very different
    radii: the past horizon is days (legitimate offline backfills can
    occasionally produce week-old events) while the future horizon is the
    typical clock-skew tolerance.
    """

    name: str = "TimestampRangeValidator"
    applies_to: frozenset[EventType] | None = None

    def __init__(
        self,
        max_age: timedelta = timedelta(days=7),
        max_future_skew: timedelta = timedelta(hours=1),
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if max_age <= timedelta(0):
            raise ValueError(f"max_age must be > 0, got {max_age}")
        if max_future_skew <= timedelta(0):
            raise ValueError(
                f"max_future_skew must be > 0, got {max_future_skew}"
            )
        self._max_age = max_age
        self._max_future_skew = max_future_skew
        self._now = now if now is not None else _utc_now

    @property
    def max_age(self) -> timedelta:
        """Configured past horizon.

        Returns
        -------
        timedelta
            Maximum allowed age.
        """
        return self._max_age

    @property
    def max_future_skew(self) -> timedelta:
        """Configured future horizon.

        Returns
        -------
        timedelta
            Maximum allowed future skew.
        """
        return self._max_future_skew

    def validate(self, event: EcommerceEvent) -> ValidateResult:
        """Check ``event_timestamp`` against ``[now - max_age, now + skew]``.

        Parameters
        ----------
        event : EcommerceEvent
            Decoded event.

        Returns
        -------
        Valid or Invalid
            ``Invalid(OUT_OF_RANGE)`` when the timestamp is outside the
            window.
        """
        ts = event.event_timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        now = self._now()
        if ts > now + self._max_future_skew:
            return Invalid(
                error_class=ErrorClass.OUT_OF_RANGE,
                validator_name=self.name,
                error_field_path="event_timestamp",
                error_message=(
                    f"event_timestamp={ts.isoformat()} is more than "
                    f"{self._max_future_skew} in the future of now="
                    f"{now.isoformat()}"
                ),
            )
        if now - ts > self._max_age:
            return Invalid(
                error_class=ErrorClass.OUT_OF_RANGE,
                validator_name=self.name,
                error_field_path="event_timestamp",
                error_message=(
                    f"event_timestamp={ts.isoformat()} is older than "
                    f"{self._max_age} relative to now={now.isoformat()}"
                ),
            )
        return Valid(event=event)


def _utc_now() -> datetime:
    """Return the current wall-clock time in UTC.

    Returns
    -------
    datetime
        Timezone-aware datetime in UTC.
    """
    return datetime.now(tz=timezone.utc)


# Re-export the Validator Protocol for callers that want to import it from
# this module (matches the original public surface).
__all__ = [
    "EventTypeAllowlistValidator",
    "PriceRangeValidator",
    "QuantityRangeValidator",
    "RequiredFieldsValidator",
    "TimestampRangeValidator",
    "UserIdShapeValidator",
    "Validator",
]
