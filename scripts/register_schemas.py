#!/usr/bin/env python3
"""CLI: register the e-commerce Avro schemas with the Confluent Schema Registry.

Reads ``.avsc`` files from a versioned directory under ``schemas/`` (default
``schemas/ecommerce/v1/``), assembles the composite envelope schema, and
registers it under ``<topic>-value``.  Idempotent: re-running with unchanged
schemas returns the existing schema ID.

Examples
--------
Register the latest schema set::

    python scripts/register_schemas.py

Show what would be registered without contacting the Registry::

    python scripts/register_schemas.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.schemas import (
    SCHEMAS_ROOT,
    SchemaRegistry,
    dump_schema,
    load_schema_set,
)
from streaming_feature_store.schemas.registry import RegistryError

logger = logging.getLogger("register_schemas")


def _build_arg_parser() -> argparse.ArgumentParser:
    """Construct the CLI argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Parser with all supported options.
    """
    parser = argparse.ArgumentParser(
        description="Register Avro schemas with the Confluent Schema Registry.",
    )
    parser.add_argument(
        "--schemas-dir",
        type=Path,
        default=None,
        help=(
            "Directory containing .avsc files.  Defaults to the highest "
            "versioned subdirectory under schemas/ecommerce/."
        ),
    )
    parser.add_argument(
        "--subject",
        type=str,
        default=None,
        help="Schema Registry subject.  Defaults to '<topic>-value'.",
    )
    parser.add_argument(
        "--compatibility",
        type=str,
        default=None,
        help=(
            "Optional per-subject compatibility level to set after "
            "registration.  Leave unset to inherit the registry global."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be registered without contacting the Registry.",
    )
    parser.add_argument(
        "--print-schema",
        action="store_true",
        help="Print the full assembled schema JSON to stdout and exit.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser


def _resolve_schemas_dir(explicit: Path | None) -> Path:
    """Return the directory of ``.avsc`` files to register.

    Parameters
    ----------
    explicit : Path or None
        Caller-supplied path; if provided, used as-is.

    Returns
    -------
    Path
        Highest versioned directory under ``schemas/ecommerce/`` if *explicit*
        is ``None``.

    Raises
    ------
    FileNotFoundError
        If no versioned directory can be found.
    """
    if explicit is not None:
        if not explicit.is_dir():
            raise FileNotFoundError(f"Schemas directory not found: {explicit}")
        return explicit
    import re

    base = SCHEMAS_ROOT / "ecommerce"
    pattern = re.compile(r"^v\d+$")
    candidates = sorted(
        (p for p in base.glob("v*") if p.is_dir() and pattern.match(p.name)),
        key=lambda p: int(p.name[1:]),
    )
    if not candidates:
        raise FileNotFoundError(f"No versioned schema directories under {base}")
    return candidates[-1]


def _resolve_subject(explicit: str | None, kafka_config: KafkaConfig) -> str:
    """Compute the target subject under ``TopicNameStrategy``.

    Parameters
    ----------
    explicit : str or None
        Caller-supplied subject; used verbatim if provided.
    kafka_config : KafkaConfig
        Source of the default topic name.

    Returns
    -------
    str
        ``<topic>-value`` if *explicit* is ``None``.
    """
    if explicit:
        return explicit
    return f"{kafka_config.topic}-value"


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Parameters
    ----------
    argv : list of str, optional
        Argument vector.  Uses :data:`sys.argv` when ``None``.

    Returns
    -------
    int
        Process exit code: ``0`` on success, ``1`` on registration failure,
        ``2`` on argument/configuration errors.
    """
    args = _build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        schemas_dir = _resolve_schemas_dir(args.schemas_dir)
    except FileNotFoundError as exc:
        logger.error(f"{exc}")
        return 2

    kafka_config = KafkaConfig()
    registry_config = SchemaRegistryConfig()
    subject = _resolve_subject(args.subject, kafka_config)

    composite = load_schema_set(schemas_dir)
    schema_str = dump_schema(composite)
    logger.info(
        f"Loaded composite schema from {schemas_dir} "
        f"({len(schema_str)} bytes), target subject={subject!r}, "
        f"registry={registry_config.url}"
    )

    if args.dry_run:
        logger.info("--dry-run set: not contacting Schema Registry")
        logger.info(f"Schema preview: {schema_str[:200]}{'...' if len(schema_str) > 200 else ''}")
        return 0

    if args.print_schema:
        import json
        print(json.dumps(composite, indent=2))
        return 0

    registry = SchemaRegistry(registry_config)
    try:
        schema_id = registry.register(subject, schema_str)
    except RegistryError as exc:
        logger.error(f"Registration failed: {exc}")
        return 1

    try:
        latest = registry.get_latest(subject)
    except RegistryError as exc:
        logger.warning(f"Registered but could not fetch latest version: {exc}")
        latest = None

    if latest is not None:
        logger.info(
            f"Subject={subject!r} schema_id={schema_id} "
            f"version={latest.version}"
        )

    if args.compatibility:
        try:
            registry.set_compatibility(subject, args.compatibility)
        except RegistryError as exc:
            logger.error(f"Could not set compatibility: {exc}")
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
