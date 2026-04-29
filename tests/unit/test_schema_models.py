"""Unit tests for the Pydantic event models."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from streaming_feature_store.schemas.loader import SCHEMAS_ROOT, load_schema_set
from streaming_feature_store.schemas.models import (
    PAYLOAD_NAMESPACE,
    ClickPayload,
    EcommerceEvent,
    EventType,
    PageViewPayload,
    PurchasePayload,
)


@pytest.fixture
def utc_now() -> datetime:
    return datetime(2026, 4, 22, 12, 0, 0, 123456, tzinfo=timezone.utc)


def _make_event(
    payload: ClickPayload | PurchasePayload | PageViewPayload,
    event_type: EventType,
    when: datetime,
) -> EcommerceEvent:
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=event_type,
        user_id="u-1",
        session_id="s-1",
        event_timestamp=when,
        payload=payload,
    )


def test_click_payload_valid() -> None:
    p = ClickPayload(element_id="btn", page_url="/")
    assert p.element_id == "btn"


def test_click_payload_rejects_empty_url() -> None:
    with pytest.raises(ValidationError):
        ClickPayload(element_id="btn", page_url="")


def test_purchase_payload_quantity_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        PurchasePayload(product_id="p-1", quantity=0, price_cents=100)


def test_purchase_payload_price_non_negative() -> None:
    with pytest.raises(ValidationError):
        PurchasePayload(product_id="p-1", quantity=1, price_cents=-1)


def test_purchase_payload_currency_pattern() -> None:
    with pytest.raises(ValidationError):
        PurchasePayload(product_id="p-1", quantity=1, price_cents=1, currency="usd")


def test_purchase_payload_currency_default() -> None:
    p = PurchasePayload(product_id="p-1", quantity=1, price_cents=1)
    assert p.currency == "USD"


def test_page_view_referrer_nullable() -> None:
    p = PageViewPayload(page_url="/", referrer=None)
    assert p.referrer is None


def test_ecommerce_event_discriminates_by_payload(utc_now: datetime) -> None:
    purchase = PurchasePayload(product_id="p-1", quantity=1, price_cents=1)
    event = _make_event(purchase, EventType.PURCHASE, utc_now)
    assert isinstance(event.payload, PurchasePayload)


def test_ecommerce_event_rejects_empty_user_id(utc_now: datetime) -> None:
    with pytest.raises(ValidationError):
        EcommerceEvent(
            event_id=uuid4(),
            event_type=EventType.CLICK,
            user_id="",
            session_id="s-1",
            event_timestamp=utc_now,
            payload=ClickPayload(element_id="btn", page_url="/"),
        )


def test_to_avro_dict_encodes_uuid_as_string(utc_now: datetime) -> None:
    event = _make_event(ClickPayload(element_id="btn", page_url="/"), EventType.CLICK, utc_now)
    d = event.to_avro_dict()
    assert isinstance(d["event_id"], str)


def test_to_avro_dict_encodes_timestamp_as_int_micros(utc_now: datetime) -> None:
    event = _make_event(ClickPayload(element_id="btn", page_url="/"), EventType.CLICK, utc_now)
    d = event.to_avro_dict()
    assert isinstance(d["event_timestamp"], int)
    expected = int(utc_now.timestamp() * 1_000_000)
    assert d["event_timestamp"] == expected


def test_to_avro_dict_naive_datetime_treated_as_utc() -> None:
    naive = datetime(2026, 4, 22, 12, 0, 0)
    event = _make_event(
        ClickPayload(element_id="btn", page_url="/"), EventType.CLICK, naive
    )
    d = event.to_avro_dict()
    expected = int(naive.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)
    assert d["event_timestamp"] == expected


def test_to_avro_dict_uses_tagged_union_for_payload(utc_now: datetime) -> None:
    event = _make_event(
        PurchasePayload(product_id="p-1", quantity=1, price_cents=1),
        EventType.PURCHASE,
        utc_now,
    )
    d = event.to_avro_dict()
    fqn, body = d["payload"]
    assert fqn == f"{PAYLOAD_NAMESPACE}.PurchasePayload"
    assert body["product_id"] == "p-1"


@pytest.mark.parametrize(
    ("model", "record_name"),
    [
        (ClickPayload, "ClickPayload"),
        (PurchasePayload, "PurchasePayload"),
        (PageViewPayload, "PageViewPayload"),
    ],
)
def test_pydantic_fields_match_avro_fields(
    model: type, record_name: str
) -> None:
    composite = load_schema_set(SCHEMAS_ROOT / "ecommerce" / "v1")
    payload_field = next(f for f in composite["fields"] if f["name"] == "payload")
    union = payload_field["type"]
    avro_record = next(r for r in union if isinstance(r, dict) and r["name"] == record_name)
    avro_fields = {f["name"] for f in avro_record["fields"]}
    pyd_fields = set(model.model_fields.keys())
    assert avro_fields == pyd_fields, (
        f"Pydantic/Avro drift for {record_name}: "
        f"avro={avro_fields} pydantic={pyd_fields}"
    )


def test_envelope_pydantic_fields_match_avro() -> None:
    composite = load_schema_set(SCHEMAS_ROOT / "ecommerce" / "v1")
    avro_fields = {f["name"] for f in composite["fields"]}
    pyd_fields = set(EcommerceEvent.model_fields.keys())
    assert avro_fields == pyd_fields
