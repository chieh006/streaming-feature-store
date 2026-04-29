"""Loading and assembling Avro ``.avsc`` schema files from disk.

The package layout treats the on-disk ``schemas/`` directory as the source of
truth.  A versioned subdirectory (``schemas/ecommerce/v1/``) contains one
``.avsc`` per Avro record.  The envelope record (``EcommerceEvent``) references
the payload records by fully-qualified name.  This module reads the files,
inlines the payload records into the envelope's payload union, and emits a
single self-contained schema document suitable for registration with the
Confluent Schema Registry or for ``fastavro.parse_schema``.
"""

from __future__ import annotations

import copy
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

SCHEMAS_ROOT: Path = Path(__file__).resolve().parents[3] / "schemas"

ENVELOPE_RECORD_NAME: str = "EcommerceEvent"
ENVELOPE_PAYLOAD_FIELD: str = "payload"


class SchemaLoadError(RuntimeError):
    """Raised when an Avro schema file cannot be loaded or assembled."""


def load_avro_file(path: Path) -> dict:
    """Read a single ``.avsc`` file and return its parsed JSON content.

    Parameters
    ----------
    path : Path
        Filesystem path to a ``.avsc`` file.

    Returns
    -------
    dict
        Parsed JSON object representing the Avro schema.

    Raises
    ------
    FileNotFoundError
        If *path* does not exist.
    SchemaLoadError
        If the file content is not valid JSON.
    """
    if not path.is_file():
        raise FileNotFoundError(f"Avro schema file not found: {path}")
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SchemaLoadError(f"Failed to parse {path} as JSON: {exc}") from exc


def _index_payload_records(schemas: list[dict]) -> tuple[dict | None, dict[str, dict]]:
    """Split a list of parsed schemas into the envelope and a FQN→record map.

    Parameters
    ----------
    schemas : list of dict
        Parsed Avro record dicts.

    Returns
    -------
    tuple of (dict or None, dict)
        The envelope record (or ``None`` if absent), and a mapping from each
        non-envelope record's fully-qualified name to its dict.
    """
    envelope: dict | None = None
    payloads: dict[str, dict] = {}
    for schema in schemas:
        name = schema.get("name")
        namespace = schema.get("namespace", "")
        fqn = f"{namespace}.{name}" if namespace else str(name)
        if name == ENVELOPE_RECORD_NAME:
            envelope = schema
        else:
            payloads[fqn] = schema
    return envelope, payloads


def _inline_payload_union(envelope: dict, payloads: dict[str, dict]) -> dict:
    """Replace FQN references in the envelope's payload union with full records.

    Parameters
    ----------
    envelope : dict
        Parsed envelope record schema.
    payloads : dict
        Mapping from FQN to parsed payload record schema.

    Returns
    -------
    dict
        Deep copy of *envelope* with the payload union resolved in place.

    Raises
    ------
    SchemaLoadError
        If the envelope has no ``payload`` field, or if a referenced FQN is
        missing from *payloads*.
    """
    composite = copy.deepcopy(envelope)
    payload_field = next(
        (f for f in composite.get("fields", []) if f.get("name") == ENVELOPE_PAYLOAD_FIELD),
        None,
    )
    if payload_field is None:
        raise SchemaLoadError(
            f"Envelope record is missing required field: {ENVELOPE_PAYLOAD_FIELD!r}"
        )
    union = payload_field.get("type")
    if not isinstance(union, list):
        raise SchemaLoadError(
            f"Envelope field {ENVELOPE_PAYLOAD_FIELD!r} must be a union (list)"
        )
    resolved: list = []
    for member in union:
        if isinstance(member, str) and member in payloads:
            resolved.append(payloads[member])
        elif isinstance(member, str) and "." in member:
            raise SchemaLoadError(
                f"Payload union references unknown record {member!r}"
            )
        else:
            resolved.append(member)
    payload_field["type"] = resolved
    return composite


def load_schema_set(directory: Path) -> dict:
    """Read all ``.avsc`` files in *directory* and assemble a composite schema.

    Parameters
    ----------
    directory : Path
        Directory containing one envelope file (``EcommerceEvent``) and one
        file per payload record.

    Returns
    -------
    dict
        Self-contained envelope schema with all payload records inlined.

    Raises
    ------
    SchemaLoadError
        If the directory has no ``.avsc`` files, or if the envelope record is
        missing.
    """
    if not directory.is_dir():
        raise SchemaLoadError(f"Not a directory: {directory}")
    files = sorted(directory.glob("*.avsc"))
    if not files:
        raise SchemaLoadError(f"No .avsc files found under {directory}")
    schemas = [load_avro_file(p) for p in files]
    envelope, payloads = _index_payload_records(schemas)
    if envelope is None:
        raise SchemaLoadError(
            f"No envelope record named {ENVELOPE_RECORD_NAME!r} found in {directory}"
        )
    composite = _inline_payload_union(envelope, payloads)
    logger.debug(
        f"Loaded composite schema from {directory} "
        f"(envelope={envelope.get('name')}, payloads={list(payloads)})"
    )
    return composite


def dump_schema(schema: dict) -> str:
    """Serialize a schema dict to canonical JSON (sorted keys, no whitespace).

    Parameters
    ----------
    schema : dict
        Avro schema dict.

    Returns
    -------
    str
        Canonical JSON string suitable for registration and hashing.
    """
    return json.dumps(schema, sort_keys=True, separators=(",", ":"))
