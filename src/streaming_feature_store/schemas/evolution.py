"""Pure-function helpers for schema-evolution drills.

This module implements the three documented `BACKWARD`-safe mutations as pure
transforms over a *composite* Avro schema dict (the in-memory structure
returned by :func:`streaming_feature_store.schemas.loader.load_schema_set`,
where payload records are inlined into the envelope's payload union).

It also exposes a :class:`EvolutionDrillResult` Pydantic model that the driver
populates and the report renderer consumes.

The mutation helpers never touch the filesystem; only :func:`dump_to_directory`
does I/O and is therefore separated for testability.
"""

from __future__ import annotations

import copy
import json
import logging
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator

from streaming_feature_store.schemas.loader import (
    ENVELOPE_PAYLOAD_FIELD,
    ENVELOPE_RECORD_NAME,
)

logger = logging.getLogger(__name__)

PROMOTION_LATTICE: dict[str, set[str]] = {
    "int": {"long", "float", "double"},
    "long": {"float", "double"},
    "float": {"double"},
}


class SchemaMutationError(ValueError):
    """Raised when a requested schema mutation is structurally invalid."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _snake_case(name: str) -> str:
    """Convert ``CamelCase`` to ``snake_case``.

    Parameters
    ----------
    name : str
        Camel-cased identifier.

    Returns
    -------
    str
        Snake-cased equivalent (one underscore between camel boundaries).
    """
    parts: list[str] = []
    for index, char in enumerate(name):
        if char.isupper() and index > 0 and not name[index - 1].isupper():
            parts.append("_")
        parts.append(char.lower())
    return "".join(parts)


def _iter_records(composite: dict) -> list[dict]:
    """Return the envelope plus every inlined payload record.

    Parameters
    ----------
    composite : dict
        Composite schema dict (envelope with inlined payloads).

    Returns
    -------
    list of dict
        The envelope first, followed by each payload record dict in declared
        union order.
    """
    records: list[dict] = [composite]
    payload_field = _find_field(composite, ENVELOPE_PAYLOAD_FIELD)
    if payload_field is not None and isinstance(payload_field.get("type"), list):
        for member in payload_field["type"]:
            if isinstance(member, dict) and member.get("type") == "record":
                records.append(member)
    return records


def _find_record(composite: dict, record_name: str) -> dict:
    """Locate a record by name within a composite schema.

    Parameters
    ----------
    composite : dict
        Composite schema dict.
    record_name : str
        Unqualified Avro record name (e.g. ``"PurchasePayload"``).

    Returns
    -------
    dict
        The mutable record dict (a reference into *composite*).

    Raises
    ------
    SchemaMutationError
        If no record with that name is found.
    """
    for record in _iter_records(composite):
        if record.get("name") == record_name:
            return record
    raise SchemaMutationError(
        f"Record {record_name!r} not found in composite schema"
    )


def _find_field(record: dict, field_name: str) -> dict | None:
    """Return the field dict with the given name, or ``None`` if absent.

    Parameters
    ----------
    record : dict
        Avro record dict.
    field_name : str
        Field name to search for.

    Returns
    -------
    dict or None
        The field dict (a mutable reference) or ``None``.
    """
    for field in record.get("fields", []):
        if field.get("name") == field_name:
            return field
    return None


def _field_has_default(field: dict) -> bool:
    """Return ``True`` if *field* declares a ``default`` key.

    Parameters
    ----------
    field : dict
        Avro field dict.

    Returns
    -------
    bool
        Whether the field has a default value declared (including ``null``).
    """
    return "default" in field


def _normalise_type(avro_type: Any) -> str | None:
    """Return the simple type-name for a primitive Avro type, else ``None``.

    Parameters
    ----------
    avro_type : Any
        An Avro type expression (string, dict, or list).

    Returns
    -------
    str or None
        ``"int"`` / ``"long"`` / ... if the type is a primitive named type;
        otherwise ``None``.
    """
    if isinstance(avro_type, str):
        return avro_type
    return None


# ---------------------------------------------------------------------------
# Public mutation API
# ---------------------------------------------------------------------------


def add_optional_field(
    base: dict,
    *,
    name: str,
    avro_type: str,
    default: Any = None,
) -> dict:
    """Append a nullable optional field to the *envelope* record.

    The resulting field has type ``["null", avro_type]`` with ``default=null``,
    which is the canonical `BACKWARD`-safe addition.

    Parameters
    ----------
    base : dict
        Composite schema dict (deep-copied internally; not mutated).
    name : str
        Name of the new field; must not collide with an existing envelope
        field.
    avro_type : str
        Avro type name for the non-null branch (e.g. ``"string"``).
    default : Any, optional
        Default value; ``None`` (the default) becomes Avro ``null``.

    Returns
    -------
    dict
        New composite schema with the field appended.

    Raises
    ------
    SchemaMutationError
        If a field with *name* already exists on the envelope.
    """
    out = copy.deepcopy(base)
    if _find_field(out, name) is not None:
        raise SchemaMutationError(
            f"Field {name!r} already exists on {out.get('name')!r}"
        )
    out.setdefault("fields", []).append(
        {
            "name": name,
            "type": ["null", avro_type],
            "default": default,
        }
    )
    return out


def remove_field(
    base: dict,
    *,
    record_name: str,
    field: str,
    force: bool = False,
) -> dict:
    """Remove *field* from the named record in a composite schema.

    Parameters
    ----------
    base : dict
        Composite schema dict (deep-copied internally; not mutated).
    record_name : str
        Unqualified record name containing the field to remove.
    field : str
        Field name to remove.
    force : bool, optional
        When ``True``, allow removal of a field that has no ``default`` value
        (used to construct intentionally invalid negative-control drills).
        Default ``False``.

    Returns
    -------
    dict
        New composite schema with the field removed.

    Raises
    ------
    SchemaMutationError
        If the field does not exist, or if it has no default and *force* is
        ``False``.
    """
    out = copy.deepcopy(base)
    record = _find_record(out, record_name)
    target = _find_field(record, field)
    if target is None:
        raise SchemaMutationError(
            f"Field {field!r} not found on record {record_name!r}"
        )
    if not force and not _field_has_default(target):
        raise SchemaMutationError(
            f"Refusing to remove {record_name}.{field}: no default value "
            f"declared (would be a BACKWARD-incompatible removal). Pass "
            f"force=True to override."
        )
    record["fields"] = [f for f in record["fields"] if f.get("name") != field]
    return out


def promote_field_type(
    base: dict,
    *,
    record_name: str,
    field: str,
    new_type: str,
) -> dict:
    """Promote a primitive numeric field to a wider Avro type.

    Avro's promotion lattice (``int → long/float/double``,
    ``long → float/double``, ``float → double``) is enforced; any other
    promotion is rejected.

    Parameters
    ----------
    base : dict
        Composite schema dict (deep-copied internally; not mutated).
    record_name : str
        Unqualified record name containing the field to promote.
    field : str
        Field name whose type will be widened.
    new_type : str
        Target primitive Avro type.

    Returns
    -------
    dict
        New composite schema with the field's type replaced.

    Raises
    ------
    SchemaMutationError
        If the field is missing, the original or new type is non-primitive,
        or the pair ``(old, new)`` is not on Avro's promotion lattice.
    """
    out = copy.deepcopy(base)
    record = _find_record(out, record_name)
    target = _find_field(record, field)
    if target is None:
        raise SchemaMutationError(
            f"Field {field!r} not found on record {record_name!r}"
        )
    old_type = _normalise_type(target.get("type"))
    if old_type is None:
        raise SchemaMutationError(
            f"Field {record_name}.{field} has non-primitive type "
            f"{target.get('type')!r}; promotion is not defined"
        )
    if old_type == new_type:
        logger.warning(
            f"promote_field_type called with no-op (old={old_type}, "
            f"new={new_type}) on {record_name}.{field}"
        )
        return out
    allowed = PROMOTION_LATTICE.get(old_type, set())
    if new_type not in allowed:
        raise SchemaMutationError(
            f"Promotion {old_type!r} → {new_type!r} is not on Avro's "
            f"numeric promotion lattice (allowed from {old_type!r}: "
            f"{sorted(allowed) or 'none'})"
        )
    target["type"] = new_type
    return out


# ---------------------------------------------------------------------------
# Disk snapshot
# ---------------------------------------------------------------------------


def _envelope_with_fqn_payload(composite: dict) -> dict:
    """Return a deep copy of *composite* with the payload union as FQN strings.

    Mirrors the on-disk ``v1/`` format, where the envelope's payload union is a
    list of fully-qualified payload record names (rather than inlined records).

    Parameters
    ----------
    composite : dict
        Composite schema (envelope with inlined payloads).

    Returns
    -------
    dict
        Envelope dict with ``payload`` field's union members converted to
        fully-qualified strings.
    """
    out = copy.deepcopy(composite)
    payload_field = _find_field(out, ENVELOPE_PAYLOAD_FIELD)
    if payload_field is None or not isinstance(payload_field.get("type"), list):
        return out
    refs: list = []
    for member in payload_field["type"]:
        if isinstance(member, dict) and member.get("type") == "record":
            ns = member.get("namespace", "")
            name = member.get("name")
            refs.append(f"{ns}.{name}" if ns else str(name))
        else:
            refs.append(member)
    payload_field["type"] = refs
    return out


def dump_to_directory(
    composite: dict,
    dest_dir: Path,
    *,
    changed_records: list[str] | None = None,
) -> list[Path]:
    """Split *composite* into per-record ``.avsc`` files under *dest_dir*.

    Always writes the envelope (with payload union restored to FQN string
    references). Writes only the payload records named in *changed_records*
    so the resulting Git diff is minimal. Pass ``changed_records=None`` to
    write every payload record (used by tests).

    Parameters
    ----------
    composite : dict
        Composite schema dict (envelope with inlined payloads).
    dest_dir : Path
        Destination directory; created if it does not exist.
    changed_records : list of str, optional
        Names of payload records to write alongside the envelope. ``None``
        writes every payload record.

    Returns
    -------
    list of Path
        Files written, in stable sorted order.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    envelope_only = _envelope_with_fqn_payload(composite)
    envelope_path = dest_dir / f"{_snake_case(ENVELOPE_RECORD_NAME)}.avsc"
    envelope_path.write_text(_pretty_json(envelope_only), encoding="utf-8")
    written.append(envelope_path)

    payload_field = _find_field(composite, ENVELOPE_PAYLOAD_FIELD)
    if payload_field is None or not isinstance(payload_field.get("type"), list):
        return sorted(written)

    for member in payload_field["type"]:
        if not isinstance(member, dict) or member.get("type") != "record":
            continue
        record_name = member.get("name", "")
        if changed_records is not None and record_name not in changed_records:
            continue
        path = dest_dir / f"{_snake_case(record_name)}.avsc"
        path.write_text(_pretty_json(member), encoding="utf-8")
        written.append(path)

    return sorted(written)


def _pretty_json(obj: dict) -> str:
    """Render *obj* as 2-space-indented JSON with a trailing newline.

    Parameters
    ----------
    obj : dict
        Any JSON-serializable dict.

    Returns
    -------
    str
        Pretty-printed JSON suitable for human review under ``git diff``.
    """
    return json.dumps(obj, indent=2, sort_keys=False) + "\n"


# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------

_SERDE_KEY_PATTERN = r"^producer=v\d+,consumer=v\d+$"


class EvolutionDrillResult(BaseModel):
    """Structured outcome of a single schema-evolution drill.

    Parameters
    ----------
    drill_id : str
        Stable identifier (e.g. ``"drill1"``).
    description : str
        Human-readable summary of the mutation.
    mutation : dict
        Machine-readable description of the mutation (kind + parameters).
    registration_accepted : bool
        Whether the Schema Registry accepted the candidate schema.
    registration_error : str or None
        Registry error message when rejected; ``None`` on success.
    registered_schema_id : int or None
        Globally-unique schema ID; ``None`` on rejection.
    registered_version : int or None
        Subject-local version number; ``None`` on rejection.
    serde_matrix : dict
        Mapping from ``"producer=vN,consumer=vM"`` keys to a short status
        string (``"ok"`` or an error fragment).
    notes : str or None
        Optional free-form notes (rendered verbatim into the report).
    """

    drill_id: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    mutation: dict
    registration_accepted: bool
    registration_error: str | None = None
    registered_schema_id: int | None = None
    registered_version: int | None = None
    serde_matrix: dict[str, str] = Field(default_factory=dict)
    notes: str | None = None

    @field_validator("serde_matrix")
    @classmethod
    def _validate_matrix_keys(cls, value: dict[str, str]) -> dict[str, str]:
        """Ensure every key matches the ``producer=vN,consumer=vM`` pattern.

        Parameters
        ----------
        value : dict of str to str
            Candidate matrix.

        Returns
        -------
        dict of str to str
            The same dict if every key is valid.

        Raises
        ------
        ValueError
            If any key violates the pattern.
        """
        import re

        pattern = re.compile(_SERDE_KEY_PATTERN)
        for key in value:
            if not pattern.match(key):
                raise ValueError(
                    f"serde_matrix key {key!r} does not match "
                    f"{_SERDE_KEY_PATTERN}"
                )
        return value
