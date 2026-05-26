"""Unit tests for :class:`ValidationPipeline` and ``default_validators``."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
    PurchasePayload,
)
from streaming_feature_store.validate.dlq import ErrorClass
from streaming_feature_store.validate.pipeline import (
    Invalid,
    Valid,
    ValidationPipeline,
    default_validators,
)
from streaming_feature_store.validate.validators import (
    EventTypeAllowlistValidator,
    PriceRangeValidator,
    RequiredFieldsValidator,
)


def _click() -> EcommerceEvent:
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.CLICK,
        user_id="u-1",
        session_id="s-1",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=ClickPayload(element_id="btn", page_url="/p"),
    )


def _purchase(price_cents: int = 999) -> EcommerceEvent:
    payload = PurchasePayload(
        product_id="sku-1",
        quantity=1,
        price_cents=max(price_cents, 0),
    )
    event = EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.PURCHASE,
        user_id="u-1",
        session_id="s-1",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=payload,
    )
    if price_cents != payload.price_cents:
        object.__setattr__(payload, "price_cents", price_cents)
    return event


class _AlwaysValid:
    name = "AlwaysValid"
    applies_to = None

    def validate(self, event):
        return Valid(event=event)


class _AlwaysInvalid:
    name = "AlwaysInvalid"
    applies_to = None

    def validate(self, event):
        return Invalid(
            error_class=ErrorClass.MALFORMED_RECORD,
            validator_name=self.name,
            error_field_path="synthetic",
            error_message="forced rejection",
        )


class _Bomb:
    name = "Bomb"
    applies_to = None

    def validate(self, event):
        raise RuntimeError("boom")


class _PurchaseOnly:
    name = "PurchaseOnly"
    applies_to = frozenset({EventType.PURCHASE})

    def __init__(self) -> None:
        self.called = 0

    def validate(self, event):
        self.called += 1
        return Valid(event=event)


def test_pipeline_passes_when_all_validators_pass() -> None:
    p = ValidationPipeline([_AlwaysValid()])
    result = p.validate(_click())
    assert isinstance(result, Valid)


def test_pipeline_first_failing_wins() -> None:
    p = ValidationPipeline([_AlwaysInvalid(), _AlwaysValid()])
    result = p.validate(_click())
    assert isinstance(result, Invalid)
    assert result.validator_name == "AlwaysInvalid"


def test_pipeline_short_circuits_after_invalid() -> None:
    # The second validator should never be reached.
    second_calls = {"n": 0}

    class _Counter(_AlwaysValid):
        name = "Counter"

        def validate(self, event):
            second_calls["n"] += 1
            return Valid(event=event)

    p = ValidationPipeline([_AlwaysInvalid(), _Counter()])
    result = p.validate(_click())
    assert isinstance(result, Invalid)
    assert second_calls["n"] == 0


def test_pipeline_applies_to_filter_skips_non_matching() -> None:
    only_purchase = _PurchaseOnly()
    p = ValidationPipeline([only_purchase])
    result = p.validate(_click())
    assert isinstance(result, Valid)
    assert only_purchase.called == 0


def test_pipeline_applies_to_filter_runs_on_matching() -> None:
    only_purchase = _PurchaseOnly()
    p = ValidationPipeline([only_purchase])
    result = p.validate(_purchase())
    assert isinstance(result, Valid)
    assert only_purchase.called == 1


def test_pipeline_internal_exception_wrapped_as_invalid() -> None:
    p = ValidationPipeline([_Bomb()])
    result = p.validate(_click())
    assert isinstance(result, Invalid)
    assert result.error_class == ErrorClass.PIPELINE_INTERNAL_ERROR
    assert result.validator_name == "Bomb"
    assert "boom" in result.error_message


def test_pipeline_internal_exception_preserves_traceback() -> None:
    p = ValidationPipeline([_Bomb()])
    result = p.validate(_click())
    assert isinstance(result, Invalid)
    assert "Traceback" in result.error_message


def test_pipeline_validators_property_is_tuple() -> None:
    chain = [_AlwaysValid(), _AlwaysInvalid()]
    p = ValidationPipeline(chain)
    assert isinstance(p.validators, tuple)
    assert len(p.validators) == 2


def test_default_validators_chain_length_and_order() -> None:
    chain = default_validators()
    assert len(chain) == 6
    names = [v.name for v in chain]
    assert names == [
        "RequiredFieldsValidator",
        "EventTypeAllowlistValidator",
        "UserIdShapeValidator",
        "PriceRangeValidator",
        "QuantityRangeValidator",
        "TimestampRangeValidator",
    ]


def test_pipeline_real_chain_passes_clean_click() -> None:
    # End-to-end across the real validator chain — a clean CLICK passes.
    p = ValidationPipeline(default_validators())
    assert isinstance(p.validate(_click()), Valid)


def test_pipeline_real_chain_rejects_bad_price() -> None:
    p = ValidationPipeline(
        [RequiredFieldsValidator(), PriceRangeValidator()]
    )
    result = p.validate(_purchase(price_cents=-1))
    assert isinstance(result, Invalid)
    assert result.error_class == ErrorClass.OUT_OF_RANGE


def test_pipeline_real_chain_rejects_unknown_event_type() -> None:
    p = ValidationPipeline(
        [EventTypeAllowlistValidator(allowed=frozenset({EventType.CLICK}))]
    )
    result = p.validate(_purchase())
    assert isinstance(result, Invalid)
    assert result.error_class == ErrorClass.UNKNOWN_EVENT_TYPE
