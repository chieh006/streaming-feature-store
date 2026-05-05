"""Unit tests for schema-evolution mutation helpers and the result model."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from streaming_feature_store.schemas import (
    SCHEMAS_ROOT,
    load_schema_set,
)
from streaming_feature_store.schemas.evolution import (
    EvolutionDrillResult,
    SchemaMutationError,
    add_optional_field,
    dump_to_directory,
    promote_field_type,
    remove_field,
)


@pytest.fixture
def baseline() -> dict:
    """Return the v1 composite schema used as the starting point for drills."""
    return load_schema_set(SCHEMAS_ROOT / "ecommerce" / "v1")


# ---------------------------------------------------------------------------
# add_optional_field
# ---------------------------------------------------------------------------


def test_add_optional_field_appends_nullable_union(baseline: dict) -> None:
    out = add_optional_field(baseline, name="device_type", avro_type="string")
    field = next(f for f in out["fields"] if f["name"] == "device_type")
    assert field["type"] == ["null", "string"]
    assert field["default"] is None


def test_add_optional_field_does_not_mutate_input(baseline: dict) -> None:
    snapshot = copy.deepcopy(baseline)
    _ = add_optional_field(baseline, name="device_type", avro_type="string")
    assert baseline == snapshot


def test_add_optional_field_rejects_existing_name(baseline: dict) -> None:
    with pytest.raises(SchemaMutationError):
        add_optional_field(baseline, name="event_id", avro_type="string")


def test_add_optional_field_custom_default(baseline: dict) -> None:
    out = add_optional_field(
        baseline, name="device_type", avro_type="string", default="unknown"
    )
    field = next(f for f in out["fields"] if f["name"] == "device_type")
    assert field["default"] == "unknown"


# ---------------------------------------------------------------------------
# remove_field
# ---------------------------------------------------------------------------


def test_remove_field_removes_from_named_record(baseline: dict) -> None:
    out = remove_field(
        baseline, record_name="PageViewPayload", field="referrer"
    )
    page_view = _find_inlined_record(out, "PageViewPayload")
    assert all(f["name"] != "referrer" for f in page_view["fields"])
    purchase = _find_inlined_record(out, "PurchasePayload")
    assert any(f["name"] == "product_id" for f in purchase["fields"])


def test_remove_field_rejects_field_without_default(baseline: dict) -> None:
    with pytest.raises(SchemaMutationError):
        remove_field(baseline, record_name="EcommerceEvent", field="event_id")


def test_remove_field_force_overrides_default_check(baseline: dict) -> None:
    out = remove_field(
        baseline, record_name="EcommerceEvent", field="event_id", force=True
    )
    assert all(f["name"] != "event_id" for f in out["fields"])


def test_remove_field_unknown_field_raises(baseline: dict) -> None:
    with pytest.raises(SchemaMutationError):
        remove_field(
            baseline, record_name="PageViewPayload", field="nonexistent"
        )


def test_remove_field_unknown_record_raises(baseline: dict) -> None:
    with pytest.raises(SchemaMutationError):
        remove_field(baseline, record_name="NoSuchRecord", field="x")


# ---------------------------------------------------------------------------
# promote_field_type
# ---------------------------------------------------------------------------


def test_promote_field_type_int_to_long_allowed(baseline: dict) -> None:
    out = promote_field_type(
        baseline,
        record_name="PurchasePayload",
        field="quantity",
        new_type="long",
    )
    purchase = _find_inlined_record(out, "PurchasePayload")
    quantity = next(f for f in purchase["fields"] if f["name"] == "quantity")
    assert quantity["type"] == "long"


@pytest.mark.parametrize(
    ("old_record", "old_field", "new_type"),
    [
        ("PurchasePayload", "product_id", "int"),
    ],
)
def test_promote_field_type_rejects_non_lattice(
    baseline: dict, old_record: str, old_field: str, new_type: str
) -> None:
    with pytest.raises(SchemaMutationError):
        promote_field_type(
            baseline,
            record_name=old_record,
            field=old_field,
            new_type=new_type,
        )


def test_promote_field_type_rejects_non_primitive(baseline: dict) -> None:
    with pytest.raises(SchemaMutationError):
        promote_field_type(
            baseline,
            record_name="EcommerceEvent",
            field="event_timestamp",
            new_type="double",
        )


def test_promote_field_type_same_type_is_noop(
    baseline: dict, caplog: pytest.LogCaptureFixture
) -> None:
    import logging

    with caplog.at_level(logging.WARNING):
        out = promote_field_type(
            baseline,
            record_name="PurchasePayload",
            field="quantity",
            new_type="int",
        )
    purchase = _find_inlined_record(out, "PurchasePayload")
    quantity = next(f for f in purchase["fields"] if f["name"] == "quantity")
    assert quantity["type"] == "int"
    assert any("no-op" in record.message for record in caplog.records)


def test_promote_field_type_unknown_field_raises(baseline: dict) -> None:
    with pytest.raises(SchemaMutationError):
        promote_field_type(
            baseline,
            record_name="PurchasePayload",
            field="missing",
            new_type="long",
        )


# ---------------------------------------------------------------------------
# dump_to_directory
# ---------------------------------------------------------------------------


def test_dump_to_directory_writes_only_changed_records(
    baseline: dict, tmp_path: Path
) -> None:
    out = promote_field_type(
        baseline,
        record_name="PurchasePayload",
        field="quantity",
        new_type="long",
    )
    written = dump_to_directory(
        out, tmp_path / "v1.3", changed_records=["PurchasePayload"]
    )
    names = {p.name for p in written}
    assert names == {"ecommerce_event.avsc", "purchase_payload.avsc"}


def test_dump_to_directory_envelope_has_fqn_payload_union(
    baseline: dict, tmp_path: Path
) -> None:
    out = add_optional_field(baseline, name="device_type", avro_type="string")
    dump_to_directory(out, tmp_path / "v1.1", changed_records=[])
    envelope = json.loads(
        (tmp_path / "v1.1" / "ecommerce_event.avsc").read_text(encoding="utf-8")
    )
    payload_field = next(
        f for f in envelope["fields"] if f["name"] == "payload"
    )
    assert all(isinstance(t, str) for t in payload_field["type"])


def test_dump_to_directory_creates_dir(baseline: dict, tmp_path: Path) -> None:
    target = tmp_path / "nested" / "v1.X"
    assert not target.exists()
    dump_to_directory(baseline, target)
    assert target.is_dir()


def test_dump_to_directory_default_writes_all_payloads(
    baseline: dict, tmp_path: Path
) -> None:
    written = dump_to_directory(baseline, tmp_path / "all")
    names = {p.name for p in written}
    assert "ecommerce_event.avsc" in names
    assert "page_view_payload.avsc" in names
    assert "purchase_payload.avsc" in names
    assert "click_payload.avsc" in names


def test_dump_to_directory_uses_pathlib(
    baseline: dict, tmp_path: Path
) -> None:
    """Cross-platform path handling per CLAUDE.md §2."""
    out_dir = tmp_path / "x"
    written = dump_to_directory(baseline, out_dir, changed_records=[])
    for path in written:
        assert path.is_absolute()


# ---------------------------------------------------------------------------
# EvolutionDrillResult
# ---------------------------------------------------------------------------


def test_evolution_drill_result_pydantic_validates_required_fields() -> None:
    with pytest.raises(ValidationError):
        EvolutionDrillResult(  # type: ignore[call-arg]
            drill_id="drill1",
            description="x",
            mutation={},
        )


def test_evolution_drill_result_serde_matrix_key_pattern() -> None:
    with pytest.raises(ValidationError):
        EvolutionDrillResult(
            drill_id="drill1",
            description="x",
            mutation={},
            registration_accepted=True,
            serde_matrix={"bogus-key": "ok"},
        )


def test_evolution_drill_result_accepts_valid_matrix() -> None:
    result = EvolutionDrillResult(
        drill_id="drill1",
        description="x",
        mutation={},
        registration_accepted=True,
        serde_matrix={
            "producer=v2,consumer=v1": "ok",
            "producer=v1,consumer=v2": "ok",
        },
    )
    assert result.serde_matrix["producer=v2,consumer=v1"] == "ok"


def test_evolution_drill_result_defaults_for_optional_fields() -> None:
    result = EvolutionDrillResult(
        drill_id="drill1",
        description="x",
        mutation={"kind": "test"},
        registration_accepted=True,
    )
    assert result.serde_matrix == {}
    assert result.notes is None
    assert result.registration_error is None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_inlined_record(composite: dict, name: str) -> dict:
    """Locate the named record inside the envelope's payload union.

    Parameters
    ----------
    composite : dict
        Composite schema (envelope with inlined payloads).
    name : str
        Unqualified record name.

    Returns
    -------
    dict
        The record dict.
    """
    payload_field = next(
        f for f in composite["fields"] if f["name"] == "payload"
    )
    for member in payload_field["type"]:
        if isinstance(member, dict) and member.get("name") == name:
            return member
    raise AssertionError(f"Record {name!r} not found in payload union")
