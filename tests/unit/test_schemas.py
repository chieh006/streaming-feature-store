"""Unit tests for the on-disk Avro schema files."""

from __future__ import annotations

import json
from pathlib import Path

import fastavro
import pytest

from streaming_feature_store.schemas.loader import (
    SCHEMAS_ROOT,
    load_avro_file,
    load_schema_set,
)

SCHEMA_DIR: Path = SCHEMAS_ROOT / "ecommerce" / "v1"
EXPECTED_FILES: tuple[str, ...] = (
    "click_payload.avsc",
    "ecommerce_event.avsc",
    "page_view_payload.avsc",
    "purchase_payload.avsc",
)
EXPECTED_NAMESPACE: str = "com.featurestore.ecommerce.v1"


@pytest.fixture(scope="module")
def avsc_files() -> list[Path]:
    """Return the four ``.avsc`` files under v1, sorted by filename."""
    return sorted(SCHEMA_DIR.glob("*.avsc"))


@pytest.fixture(scope="module")
def envelope_schema() -> dict:
    """Return the parsed envelope schema dict."""
    return load_avro_file(SCHEMA_DIR / "ecommerce_event.avsc")


def test_all_avsc_files_exist(avsc_files: list[Path]) -> None:
    names = {p.name for p in avsc_files}
    assert names == set(EXPECTED_FILES)


def test_each_avsc_is_valid_json(avsc_files: list[Path]) -> None:
    for path in avsc_files:
        json.loads(path.read_text(encoding="utf-8"))


def test_each_avsc_has_namespace(avsc_files: list[Path]) -> None:
    for path in avsc_files:
        schema = json.loads(path.read_text(encoding="utf-8"))
        assert schema.get("namespace") == EXPECTED_NAMESPACE, path.name


def test_envelope_references_all_payloads(envelope_schema: dict) -> None:
    payload_field = next(f for f in envelope_schema["fields"] if f["name"] == "payload")
    union = payload_field["type"]
    assert isinstance(union, list)
    expected = {
        f"{EXPECTED_NAMESPACE}.ClickPayload",
        f"{EXPECTED_NAMESPACE}.PurchasePayload",
        f"{EXPECTED_NAMESPACE}.PageViewPayload",
    }
    assert set(union) == expected


def test_envelope_has_required_top_level_fields(envelope_schema: dict) -> None:
    fields = {f["name"]: f for f in envelope_schema["fields"]}
    assert set(fields) == {
        "event_id",
        "event_type",
        "user_id",
        "session_id",
        "event_timestamp",
        "payload",
    }
    assert fields["event_id"]["type"] == {"type": "string", "logicalType": "uuid"}
    assert fields["event_timestamp"]["type"] == {
        "type": "long",
        "logicalType": "timestamp-micros",
    }
    assert fields["user_id"]["type"] == "string"
    assert fields["session_id"]["type"] == "string"


def test_event_type_enum_symbols(envelope_schema: dict) -> None:
    event_type = next(f for f in envelope_schema["fields"] if f["name"] == "event_type")
    enum = event_type["type"]
    assert enum["type"] == "enum"
    assert enum["name"] == "EventType"
    assert set(enum["symbols"]) == {"CLICK", "PURCHASE", "PAGE_VIEW"}


def test_purchase_has_defaulted_currency() -> None:
    schema = load_avro_file(SCHEMA_DIR / "purchase_payload.avsc")
    currency = next(f for f in schema["fields"] if f["name"] == "currency")
    assert currency.get("default") == "USD"
    assert currency["type"] == "string"


def test_page_view_referrer_is_nullable() -> None:
    schema = load_avro_file(SCHEMA_DIR / "page_view_payload.avsc")
    referrer = next(f for f in schema["fields"] if f["name"] == "referrer")
    assert referrer["type"] == ["null", "string"]
    assert referrer.get("default") is None


def test_composite_schema_parses_with_fastavro() -> None:
    composite = load_schema_set(SCHEMA_DIR)
    parsed = fastavro.parse_schema(composite)
    assert parsed["name"] == f"{EXPECTED_NAMESPACE}.EcommerceEvent"
