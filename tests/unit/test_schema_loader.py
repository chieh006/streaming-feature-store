"""Unit tests for ``streaming_feature_store.schemas.loader``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from streaming_feature_store.schemas.loader import (
    SCHEMAS_ROOT,
    SchemaLoadError,
    dump_schema,
    load_avro_file,
    load_schema_set,
)

V1_DIR: Path = SCHEMAS_ROOT / "ecommerce" / "v1"


def test_load_avro_file_reads_valid() -> None:
    schema = load_avro_file(V1_DIR / "click_payload.avsc")
    assert schema["name"] == "ClickPayload"
    assert schema["namespace"] == "com.featurestore.ecommerce.v1"


def test_load_avro_file_raises_on_bad_json(tmp_path: Path) -> None:
    bad = tmp_path / "broken.avsc"
    bad.write_text("{ this is not json", encoding="utf-8")
    with pytest.raises(SchemaLoadError):
        load_avro_file(bad)


def test_load_avro_file_raises_on_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_avro_file(tmp_path / "absent.avsc")


def test_load_schema_set_combines_records() -> None:
    composite = load_schema_set(V1_DIR)
    assert composite["name"] == "EcommerceEvent"
    payload_field = next(f for f in composite["fields"] if f["name"] == "payload")
    union = payload_field["type"]
    record_names = {member["name"] for member in union if isinstance(member, dict)}
    assert record_names == {"ClickPayload", "PurchasePayload", "PageViewPayload"}


def test_load_schema_set_rejects_empty_dir(tmp_path: Path) -> None:
    with pytest.raises(SchemaLoadError):
        load_schema_set(tmp_path)


def test_load_schema_set_rejects_nondir(tmp_path: Path) -> None:
    fake = tmp_path / "not-a-dir.avsc"
    fake.write_text("{}", encoding="utf-8")
    with pytest.raises(SchemaLoadError):
        load_schema_set(fake)


def test_load_schema_set_requires_envelope(tmp_path: Path) -> None:
    (tmp_path / "click_payload.avsc").write_text(
        json.dumps(
            {
                "type": "record",
                "name": "ClickPayload",
                "namespace": "ns",
                "fields": [{"name": "x", "type": "string"}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(SchemaLoadError):
        load_schema_set(tmp_path)


def test_load_schema_set_rejects_unknown_payload_fqn(tmp_path: Path) -> None:
    (tmp_path / "ecommerce_event.avsc").write_text(
        json.dumps(
            {
                "type": "record",
                "name": "EcommerceEvent",
                "namespace": "ns",
                "fields": [
                    {"name": "payload", "type": ["ns.MissingPayload"]},
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(SchemaLoadError):
        load_schema_set(tmp_path)


def test_load_schema_set_rejects_envelope_without_payload_field(tmp_path: Path) -> None:
    (tmp_path / "ecommerce_event.avsc").write_text(
        json.dumps(
            {
                "type": "record",
                "name": "EcommerceEvent",
                "namespace": "ns",
                "fields": [{"name": "x", "type": "string"}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(SchemaLoadError):
        load_schema_set(tmp_path)


def test_load_schema_set_rejects_non_union_payload(tmp_path: Path) -> None:
    (tmp_path / "ecommerce_event.avsc").write_text(
        json.dumps(
            {
                "type": "record",
                "name": "EcommerceEvent",
                "namespace": "ns",
                "fields": [{"name": "payload", "type": "string"}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(SchemaLoadError):
        load_schema_set(tmp_path)


def test_dump_schema_is_canonical() -> None:
    schema = {"b": 2, "a": 1}
    assert dump_schema(schema) == dump_schema(schema)
    assert dump_schema(schema) == '{"a":1,"b":2}'


def test_schemas_root_is_absolute_path() -> None:
    assert SCHEMAS_ROOT.is_absolute()
    assert SCHEMAS_ROOT.exists()
