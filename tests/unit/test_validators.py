"""Unit tests for the concrete validator classes."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
    PageViewPayload,
    PurchasePayload,
)
from streaming_feature_store.validate.dlq import ErrorClass
from streaming_feature_store.validate.pipeline import Invalid, Valid
from streaming_feature_store.validate.validators import (
    EventTypeAllowlistValidator,
    PriceRangeValidator,
    QuantityRangeValidator,
    RequiredFieldsValidator,
    TimestampRangeValidator,
    UserIdShapeValidator,
)


def _click(
    user_id: str = "u-1",
    session_id: str = "s-1",
    ts: datetime | None = None,
) -> EcommerceEvent:
    """Return a canonical CLICK event for validator tests."""
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.CLICK,
        user_id=user_id,
        session_id=session_id,
        event_timestamp=ts or datetime.now(tz=timezone.utc),
        payload=ClickPayload(element_id="btn", page_url="/p"),
    )


def _purchase(
    *,
    user_id: str = "u-1",
    quantity: int = 1,
    price_cents: int = 999,
    ts: datetime | None = None,
) -> EcommerceEvent:
    """Return a canonical PURCHASE event for validator tests.

    Notes
    -----
    Pydantic enforces ``price_cents >= 0`` and ``quantity >= 1`` at
    construction time.  When tests need to simulate the post-decode escape
    (a malformed message that slipped past the adapter), they construct a
    valid event and then ``object.__setattr__`` the payload field to bypass
    the frozen model.
    """
    payload = PurchasePayload(
        product_id="sku-1",
        quantity=max(quantity, 1),
        price_cents=max(price_cents, 0),
    )
    event = EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.PURCHASE,
        user_id=user_id,
        session_id="s-1",
        event_timestamp=ts or datetime.now(tz=timezone.utc),
        payload=payload,
    )
    if price_cents != payload.price_cents:
        object.__setattr__(payload, "price_cents", price_cents)
    if quantity != payload.quantity:
        object.__setattr__(payload, "quantity", quantity)
    return event


def _page_view(ts: datetime | None = None) -> EcommerceEvent:
    """Return a canonical PAGE_VIEW event for validator tests."""
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.PAGE_VIEW,
        user_id="u-1",
        session_id="s-1",
        event_timestamp=ts or datetime.now(tz=timezone.utc),
        payload=PageViewPayload(page_url="/p", referrer=None),
    )


# --- RequiredFieldsValidator -----------------------------------------------


def test_required_fields_validator_passes_complete_event() -> None:
    result = RequiredFieldsValidator().validate(_click())
    assert isinstance(result, Valid)


def test_required_fields_validator_metadata() -> None:
    v = RequiredFieldsValidator()
    assert v.name == "RequiredFieldsValidator"
    assert v.applies_to is None


def test_required_fields_validator_rejects_empty_user_id() -> None:
    event = _click()
    object.__setattr__(event, "user_id", "")
    result = RequiredFieldsValidator().validate(event)
    assert isinstance(result, Invalid)
    assert result.error_class == ErrorClass.NULL_REQUIRED_FIELD
    assert result.error_field_path == "user_id"


def test_required_fields_validator_rejects_empty_session_id() -> None:
    event = _click()
    object.__setattr__(event, "session_id", "")
    result = RequiredFieldsValidator().validate(event)
    assert isinstance(result, Invalid)
    assert result.error_field_path == "session_id"


# --- EventTypeAllowlistValidator -------------------------------------------


def test_event_type_allowlist_validator_accepts_each_allowed_value() -> None:
    for et in (EventType.CLICK, EventType.PURCHASE, EventType.PAGE_VIEW):
        if et is EventType.PURCHASE:
            event = _purchase()
        elif et is EventType.PAGE_VIEW:
            event = _page_view()
        else:
            event = _click()
        result = EventTypeAllowlistValidator().validate(event)
        assert isinstance(result, Valid), f"expected Valid for {et!r}"


def test_event_type_allowlist_validator_rejects_unknown() -> None:
    event = _click()
    # The Pydantic model itself prevents constructing with an unknown enum,
    # so simulate the post-decode escape by monkey-patching the field.
    object.__setattr__(event, "event_type", "QUANTUM_TELEPORT")
    result = EventTypeAllowlistValidator().validate(event)
    assert isinstance(result, Invalid)
    assert result.error_class == ErrorClass.UNKNOWN_EVENT_TYPE
    assert result.error_field_path == "event_type"


def test_event_type_allowlist_validator_custom_allowlist() -> None:
    v = EventTypeAllowlistValidator(allowed=frozenset({EventType.CLICK}))
    assert isinstance(v.validate(_click()), Valid)
    assert isinstance(v.validate(_purchase()), Invalid)


def test_event_type_allowlist_validator_exposes_allowed_property() -> None:
    v = EventTypeAllowlistValidator(allowed=frozenset({EventType.CLICK}))
    assert v.allowed == frozenset({EventType.CLICK})


# --- UserIdShapeValidator --------------------------------------------------


def test_user_id_shape_validator_accepts_short_id() -> None:
    result = UserIdShapeValidator().validate(_click(user_id="u-1"))
    assert isinstance(result, Valid)


def test_user_id_shape_validator_rejects_too_long() -> None:
    event = _click()
    object.__setattr__(event, "user_id", "x" * 257)
    result = UserIdShapeValidator().validate(event)
    assert isinstance(result, Invalid)
    assert result.error_class == ErrorClass.MALFORMED_RECORD


def test_user_id_shape_validator_rejects_embedded_newline() -> None:
    event = _click()
    object.__setattr__(event, "user_id", "a\nb")
    result = UserIdShapeValidator().validate(event)
    assert isinstance(result, Invalid)


def test_user_id_shape_validator_rejects_null_byte() -> None:
    event = _click()
    object.__setattr__(event, "user_id", "a\x00b")
    result = UserIdShapeValidator().validate(event)
    assert isinstance(result, Invalid)


def test_user_id_shape_validator_rejects_zero_max_length() -> None:
    with pytest.raises(ValueError):
        UserIdShapeValidator(max_length=0)


def test_user_id_shape_validator_exposes_max_length() -> None:
    v = UserIdShapeValidator(max_length=42)
    assert v.max_length == 42


# --- PriceRangeValidator ---------------------------------------------------


def test_price_range_validator_skips_non_purchase_events() -> None:
    # Directly invoking validate() on a CLICK event returns Valid (the
    # pipeline would normally filter via applies_to, but the validator's
    # own contract is to be safe under direct invocation).
    result = PriceRangeValidator().validate(_click())
    assert isinstance(result, Valid)


def test_price_range_validator_rejects_negative_price() -> None:
    result = PriceRangeValidator().validate(_purchase(price_cents=-1))
    assert isinstance(result, Invalid)
    assert result.error_class == ErrorClass.OUT_OF_RANGE
    assert result.error_field_path == "payload.price_cents"


def test_price_range_validator_rejects_zero_price() -> None:
    # Pydantic enforces price_cents >= 0, so zero is the boundary; the
    # exclusive-lower-bound check is what rejects it.
    result = PriceRangeValidator().validate(_purchase(price_cents=0))
    assert isinstance(result, Invalid)


def test_price_range_validator_rejects_above_max() -> None:
    result = PriceRangeValidator(max_price_cents=100).validate(
        _purchase(price_cents=200)
    )
    assert isinstance(result, Invalid)


def test_price_range_validator_accepts_in_range() -> None:
    result = PriceRangeValidator(max_price_cents=1000).validate(
        _purchase(price_cents=999)
    )
    assert isinstance(result, Valid)


def test_price_range_validator_rejects_bad_constructor_args() -> None:
    with pytest.raises(ValueError):
        PriceRangeValidator(min_price_cents=-1)
    with pytest.raises(ValueError):
        PriceRangeValidator(min_price_cents=10, max_price_cents=5)


def test_price_range_validator_properties() -> None:
    v = PriceRangeValidator(min_price_cents=10, max_price_cents=200)
    assert v.min_price_cents == 10
    assert v.max_price_cents == 200


# --- QuantityRangeValidator ------------------------------------------------


def test_quantity_range_validator_accepts_in_range() -> None:
    result = QuantityRangeValidator().validate(_purchase(quantity=5))
    assert isinstance(result, Valid)


def test_quantity_range_validator_rejects_above_max() -> None:
    result = QuantityRangeValidator(max_quantity=10).validate(
        _purchase(quantity=11)
    )
    assert isinstance(result, Invalid)


def test_quantity_range_validator_rejects_zero_quantity() -> None:
    # Construct around Pydantic's ge=1 by mutating after construction.
    event = _purchase(quantity=1)
    object.__setattr__(event.payload, "quantity", 0)
    result = QuantityRangeValidator().validate(event)
    assert isinstance(result, Invalid)


def test_quantity_range_validator_skips_non_purchase() -> None:
    result = QuantityRangeValidator().validate(_click())
    assert isinstance(result, Valid)


def test_quantity_range_validator_rejects_bad_constructor() -> None:
    with pytest.raises(ValueError):
        QuantityRangeValidator(max_quantity=0)


def test_quantity_range_validator_max_property() -> None:
    assert QuantityRangeValidator(max_quantity=99).max_quantity == 99


# --- TimestampRangeValidator -----------------------------------------------


def test_timestamp_range_validator_accepts_now() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    v = TimestampRangeValidator(now=lambda: now)
    result = v.validate(_click(ts=now))
    assert isinstance(result, Valid)


def test_timestamp_range_validator_accepts_modest_future_skew() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    v = TimestampRangeValidator(now=lambda: now)
    result = v.validate(_click(ts=now + timedelta(minutes=30)))
    assert isinstance(result, Valid)


def test_timestamp_range_validator_rejects_far_future() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    v = TimestampRangeValidator(now=lambda: now)
    result = v.validate(_click(ts=now + timedelta(hours=2)))
    assert isinstance(result, Invalid)
    assert result.error_field_path == "event_timestamp"


def test_timestamp_range_validator_rejects_too_old() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    v = TimestampRangeValidator(now=lambda: now)
    result = v.validate(_click(ts=now - timedelta(days=30)))
    assert isinstance(result, Invalid)


def test_timestamp_range_validator_handles_naive_timestamp() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    v = TimestampRangeValidator(now=lambda: now)
    naive = datetime(2025, 12, 31, 23, 0, 0)
    event = _click()
    object.__setattr__(event, "event_timestamp", naive)
    result = v.validate(event)
    assert isinstance(result, Valid)


def test_timestamp_range_validator_rejects_non_positive_constructor() -> None:
    with pytest.raises(ValueError):
        TimestampRangeValidator(max_age=timedelta(0))
    with pytest.raises(ValueError):
        TimestampRangeValidator(max_future_skew=timedelta(0))


def test_timestamp_range_validator_default_now_returns_utc() -> None:
    # Constructed without an injected clock: the real wall-clock is used.
    v = TimestampRangeValidator()
    assert v.max_age == timedelta(days=7)
    assert v.max_future_skew == timedelta(hours=1)
    # default_now path: call validate with a current-time event.
    result = v.validate(_click())
    assert isinstance(result, Valid)
