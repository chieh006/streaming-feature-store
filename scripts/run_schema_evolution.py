#!/usr/bin/env python3
"""CLI driver: run BACKWARD-compat schema-evolution drills end-to-end.

For each of the three documented mutations the driver:

1. Computes a candidate composite schema from the v1 baseline via the pure
   helpers in :mod:`streaming_feature_store.schemas.evolution`.
2. Snapshots the candidate as ``schemas/ecommerce/v1.X/`` so reviewers can
   read the diff in Git.
3. Attempts to register the candidate with the Schema Registry and captures
   the verdict (accepted / rejected, with the registry's reason).
4. When accepted, runs a four-cell serde matrix (producer × consumer in
   {prior, new}) using ``AvroEventProducer`` and ``AvroEventConsumer``.
5. Records each drill's outcome as an :class:`EvolutionDrillResult` and
   renders the collected results to a Markdown report.

Cleanup soft-deletes the experiment versions from the subject so subsequent
Week 1 PRs see a baseline-only state; pass ``--keep-subject`` to retain them.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consumer import AvroEventConsumer
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.schemas import (
    SCHEMAS_ROOT,
    ClickPayload,
    EcommerceEvent,
    EventType,
    PageViewPayload,
    PurchasePayload,
    SchemaRegistry,
    dump_schema,
    load_schema_set,
)
from streaming_feature_store.schemas.evolution import (
    EvolutionDrillResult,
    add_optional_field,
    dump_to_directory,
    promote_field_type,
    remove_field,
)
from streaming_feature_store.schemas.registry import RegistryError

logger = logging.getLogger("run_schema_evolution")

DEFAULT_REPORT_PATH = Path("docs/results/week1_schema_evolution_results.md")
DEFAULT_BASELINE_DIR = SCHEMAS_ROOT / "ecommerce" / "v1"
EVENTS_PER_DIRECTION = 5
SUBJECT_TEMPLATE = "{topic}-value"


# ---------------------------------------------------------------------------
# Drill specifications
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DrillSpec:
    """Recipe for a single evolution drill.

    Parameters
    ----------
    drill_id : str
        Stable identifier (e.g. ``"drill1"``).
    label : str
        Short label (e.g. ``"v1.1"``) used for the snapshot subdirectory.
    description : str
        Human-readable summary of the mutation.
    mutation : dict
        Machine-readable description of the mutation (kind + parameters).
    apply : Callable
        Pure function that takes the baseline composite and returns the
        candidate composite.
    changed_records : list of str
        Names of payload records whose ``.avsc`` file should be written by
        :func:`dump_to_directory` (the envelope is always written).
    """

    drill_id: str
    label: str
    description: str
    mutation: dict
    apply: Callable[[dict], dict]
    changed_records: list[str]


def build_drill_specs() -> list[DrillSpec]:
    """Return the three documented BACKWARD-safe drills in execution order.

    Returns
    -------
    list of DrillSpec
        Drill 1 (add optional field), drill 2 (remove defaulted field),
        drill 3 (int → long promotion).
    """
    return [
        DrillSpec(
            drill_id="drill1",
            label="v1.1",
            description="Add optional `device_type` field to EcommerceEvent",
            mutation={
                "kind": "add_optional_field",
                "record": "EcommerceEvent",
                "field": "device_type",
                "avro_type": "string",
            },
            apply=lambda base: add_optional_field(
                base, name="device_type", avro_type="string"
            ),
            changed_records=[],
        ),
        DrillSpec(
            drill_id="drill2",
            label="v1.2",
            description="Remove defaulted `referrer` field from PageViewPayload",
            mutation={
                "kind": "remove_field",
                "record": "PageViewPayload",
                "field": "referrer",
            },
            apply=lambda base: remove_field(
                base, record_name="PageViewPayload", field="referrer"
            ),
            changed_records=["PageViewPayload"],
        ),
        DrillSpec(
            drill_id="drill3",
            label="v1.3",
            description="Promote `PurchasePayload.quantity` from int to long",
            mutation={
                "kind": "promote_field_type",
                "record": "PurchasePayload",
                "field": "quantity",
                "new_type": "long",
            },
            apply=lambda base: promote_field_type(
                base,
                record_name="PurchasePayload",
                field="quantity",
                new_type="long",
            ),
            changed_records=["PurchasePayload"],
        ),
    ]


# ---------------------------------------------------------------------------
# Sample event factory
# ---------------------------------------------------------------------------


def _build_sample_events(count: int) -> list[EcommerceEvent]:
    """Construct *count* deterministic events that cycle through event types.

    Parameters
    ----------
    count : int
        Number of events to generate.

    Returns
    -------
    list of EcommerceEvent
        Events suitable for round-tripping through the serde matrix.
    """
    events: list[EcommerceEvent] = []
    base_time = datetime.now(tz=timezone.utc)
    for index in range(count):
        choice = index % 3
        if choice == 0:
            payload: ClickPayload | PurchasePayload | PageViewPayload = ClickPayload(
                element_id=f"btn-{index}", page_url="/home"
            )
            event_type = EventType.CLICK
        elif choice == 1:
            payload = PurchasePayload(
                product_id=f"sku-{index}", quantity=index + 1, price_cents=999
            )
            event_type = EventType.PURCHASE
        else:
            payload = PageViewPayload(page_url="/products", referrer=None)
            event_type = EventType.PAGE_VIEW
        events.append(
            EcommerceEvent(
                event_id=uuid.uuid4(),
                event_type=event_type,
                user_id=f"u-{index:04d}",
                session_id=f"s-{index:04d}",
                event_timestamp=base_time,
                payload=payload,
            )
        )
    return events


# ---------------------------------------------------------------------------
# Driver primitives
# ---------------------------------------------------------------------------


def _attempt_registration(
    registry: SchemaRegistry, subject: str, schema_str: str
) -> tuple[bool, str | None, int | None]:
    """Register *schema_str* under *subject* and capture the outcome.

    Parameters
    ----------
    registry : SchemaRegistry
        Wrapped Schema Registry client.
    subject : str
        Target subject.
    schema_str : str
        Candidate Avro schema JSON string.

    Returns
    -------
    tuple
        ``(accepted, error_message, schema_id)``.  On rejection, *schema_id*
        is ``None`` and *error_message* contains the registry's reason.
    """
    try:
        schema_id = registry.register(subject, schema_str)
    except RegistryError as exc:
        return False, str(exc), None
    return True, None, schema_id


def _resolve_version(
    registry: SchemaRegistry, subject: str, schema_id: int
) -> int | None:
    """Best-effort lookup of the subject-local version for *schema_id*.

    Parameters
    ----------
    registry : SchemaRegistry
        Wrapped client.
    subject : str
        Subject under which the schema was registered.
    schema_id : int
        Globally-unique schema ID.

    Returns
    -------
    int or None
        Version number, or ``None`` if the lookup fails.
    """
    try:
        latest = registry.get_latest(subject)
    except RegistryError:
        return None
    if latest.schema_id == schema_id:
        return latest.version
    return latest.version


def _run_serde_cell(
    *,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    topic: str,
    reader_schema_str: str | None,
    events: list[EcommerceEvent],
    poll_timeout_s: float,
) -> str:
    """Produce *events* and consume them back; return a one-line status.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry configuration.
    topic : str
        Target topic.
    reader_schema_str : str or None
        Reader schema for the consumer.  ``None`` falls back to writer schema.
    events : list of EcommerceEvent
        Events to round-trip.
    poll_timeout_s : float
        Per-call poll budget for the consumer.

    Returns
    -------
    str
        ``"ok (N/N)"`` on success or ``"error: <reason>"`` on any exception.
    """
    group_id = f"schema-evolution-{uuid.uuid4().hex[:8]}"
    expected = len(events)
    try:
        with AvroEventProducer(kafka_config, registry_config, topic=topic) as producer:
            for event in events:
                producer.produce(event)
            producer.flush(timeout_s=10.0)

        with AvroEventConsumer(
            kafka_config,
            registry_config,
            group_id=group_id,
            topic=topic,
            reader_schema_str=reader_schema_str,
        ) as consumer:
            consumed = consumer.consume(
                timeout_s=poll_timeout_s, max_messages=expected
            )
    except Exception as exc:  # noqa: BLE001
        return f"error: {exc}"
    if len(consumed) == expected:
        return f"ok ({len(consumed)}/{expected})"
    return f"partial ({len(consumed)}/{expected})"


def _run_serde_matrix(
    *,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    topic: str,
    prior_schema_str: str,
    new_schema_str: str,
    poll_timeout_s: float,
) -> dict[str, str]:
    """Run all four producer × consumer cells for one drill.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry configuration.
    topic : str
        Target topic.
    prior_schema_str : str
        Schema string for the v(N-1) reader.
    new_schema_str : str
        Schema string for the v(N) reader.  Currently unused — present so the
        signature is forward-compatible if we want to assert the producer
        chose this schema in the future.
    poll_timeout_s : float
        Per-cell consumer poll budget.

    Returns
    -------
    dict of str to str
        Mapping from ``"producer=vN,consumer=vM"`` to a status string.
    """
    del new_schema_str  # producer schema is selected by the registry's "latest"

    events = _build_sample_events(EVENTS_PER_DIRECTION)
    cells: dict[str, str] = {}

    cells["producer=v2,consumer=v1"] = _run_serde_cell(
        kafka_config=kafka_config,
        registry_config=registry_config,
        topic=topic,
        reader_schema_str=prior_schema_str,
        events=events,
        poll_timeout_s=poll_timeout_s,
    )

    cells["producer=v1,consumer=v2"] = _run_serde_cell(
        kafka_config=kafka_config,
        registry_config=registry_config,
        topic=topic,
        reader_schema_str=None,
        events=events,
        poll_timeout_s=poll_timeout_s,
    )

    return cells


def run_drill(
    spec: DrillSpec,
    *,
    baseline_composite: dict,
    prior_schema_str: str,
    snapshot_root: Path,
    registry: SchemaRegistry,
    subject: str,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    snapshot_only: bool,
    poll_timeout_s: float,
) -> EvolutionDrillResult:
    """Execute one drill end-to-end and return its structured result.

    Parameters
    ----------
    spec : DrillSpec
        Drill recipe.
    baseline_composite : dict
        v1 composite schema.
    prior_schema_str : str
        Canonical JSON string of the v(N-1) schema (for the consumer).
    snapshot_root : Path
        Root under which ``v1.X`` directories are written.
    registry : SchemaRegistry
        Wrapped client (only contacted when *snapshot_only* is ``False``).
    subject : str
        Target subject.
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry configuration.
    snapshot_only : bool
        When ``True``, skip registry / serde calls entirely.
    poll_timeout_s : float
        Per-cell consumer poll budget.

    Returns
    -------
    EvolutionDrillResult
        Structured outcome, ready for inclusion in the report.
    """
    candidate = spec.apply(baseline_composite)
    candidate_str = dump_schema(candidate)

    snapshot_dir = snapshot_root / spec.label
    dump_to_directory(
        candidate, snapshot_dir, changed_records=spec.changed_records
    )
    logger.info(f"Drill {spec.drill_id}: snapshot written to {snapshot_dir}")

    if snapshot_only:
        return EvolutionDrillResult(
            drill_id=spec.drill_id,
            description=spec.description,
            mutation=spec.mutation,
            registration_accepted=False,
            registration_error="--snapshot-only set: registration skipped",
        )

    accepted, error, schema_id = _attempt_registration(
        registry, subject, candidate_str
    )
    if not accepted:
        logger.warning(
            f"Drill {spec.drill_id}: registration REJECTED: {error}"
        )
        return EvolutionDrillResult(
            drill_id=spec.drill_id,
            description=spec.description,
            mutation=spec.mutation,
            registration_accepted=False,
            registration_error=error,
        )

    version = _resolve_version(registry, subject, schema_id) if schema_id else None
    logger.info(
        f"Drill {spec.drill_id}: REGISTERED schema_id={schema_id} "
        f"version={version}"
    )

    matrix = _run_serde_matrix(
        kafka_config=kafka_config,
        registry_config=registry_config,
        topic=kafka_config.topic,
        prior_schema_str=prior_schema_str,
        new_schema_str=candidate_str,
        poll_timeout_s=poll_timeout_s,
    )
    for cell, status in matrix.items():
        logger.info(f"  serde {cell}  {status}")

    return EvolutionDrillResult(
        drill_id=spec.drill_id,
        description=spec.description,
        mutation=spec.mutation,
        registration_accepted=True,
        registration_error=None,
        registered_schema_id=schema_id,
        registered_version=version,
        serde_matrix=matrix,
    )


def cleanup_experiment_versions(
    registry: SchemaRegistry, subject: str, baseline_version: int
) -> None:
    """Soft-delete experiment versions, keeping the baseline.

    Parameters
    ----------
    registry : SchemaRegistry
        Wrapped client.
    subject : str
        Subject from which to drop the experiment versions.
    baseline_version : int
        Version number to retain (typically ``1``).

    Notes
    -----
    Errors are logged but not raised — cleanup is best-effort.
    """
    try:
        deleted = registry.delete_subject(subject, permanent=False)
    except RegistryError as exc:
        logger.warning(f"Cleanup: soft-delete failed for {subject!r}: {exc}")
        return
    logger.info(f"Cleanup: soft-deleted versions {deleted} from {subject!r}")
    # Re-register the baseline so the live subject is intact for downstream PRs.
    baseline_str = dump_schema(load_schema_set(DEFAULT_BASELINE_DIR))
    try:
        schema_id = registry.register(subject, baseline_str)
    except RegistryError as exc:
        logger.warning(
            f"Cleanup: failed to re-register baseline (was version "
            f"{baseline_version}): {exc}"
        )
        return
    logger.info(f"Cleanup: re-registered baseline schema_id={schema_id}")


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------


def _verdict_icon(accepted: bool) -> str:
    """Return a check or cross icon for a registration outcome.

    Parameters
    ----------
    accepted : bool
        Registration verdict.

    Returns
    -------
    str
        Unicode icon for inclusion in the report.
    """
    return "[OK]" if accepted else "[REJECTED]"


def _render_drill_section(result: EvolutionDrillResult) -> str:
    """Render one drill's section as Markdown.

    Parameters
    ----------
    result : EvolutionDrillResult
        Structured drill outcome.

    Returns
    -------
    str
        Markdown fragment (with trailing newline).
    """
    lines: list[str] = [
        f"## {result.drill_id} — {result.description}",
        "",
        "| Field | Value |",
        "|---|---|",
        f"| Mutation | `{result.mutation}` |",
    ]
    if result.registration_accepted:
        lines.append(
            f"| Registration | {_verdict_icon(True)} accepted "
            f"(schema_id={result.registered_schema_id}, "
            f"version={result.registered_version}) |"
        )
        for cell, status in sorted(result.serde_matrix.items()):
            lines.append(f"| Serde {cell} | {status} |")
    else:
        lines.append(
            f"| Registration | {_verdict_icon(False)} rejected — "
            f"{result.registration_error} |"
        )
    if result.notes:
        lines.append(f"| Notes | {result.notes} |")
    lines.append("")
    return "\n".join(lines)


def render_report(
    results: list[EvolutionDrillResult],
    *,
    subject: str,
    compatibility: str,
    generated_at: datetime,
) -> str:
    """Render the full Markdown report for a list of drill results.

    Parameters
    ----------
    results : list of EvolutionDrillResult
        Drills in execution order.
    subject : str
        Target Schema Registry subject.
    compatibility : str
        Compatibility level applied to the subject.
    generated_at : datetime
        Timestamp embedded in the report header.

    Returns
    -------
    str
        Complete Markdown document.
    """
    header = [
        "# Week 1 — Schema Evolution Drill Results",
        "",
        f"**Generated:** {generated_at.isoformat()}",
        f"**Subject:** {subject}",
        f"**Compatibility level:** {compatibility}",
        "",
    ]
    sections = [_render_drill_section(r) for r in results]
    return "\n".join(header) + "\n".join(sections)


# ---------------------------------------------------------------------------
# CLI plumbing
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    """Construct the CLI argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Parser with all supported options.
    """
    parser = argparse.ArgumentParser(
        description="Run BACKWARD schema-evolution drills end-to-end.",
    )
    parser.add_argument(
        "--drill",
        choices=["1", "2", "3", "all"],
        default="all",
        help="Which drill to run (default: all).",
    )
    parser.add_argument(
        "--snapshot-only",
        action="store_true",
        help="Only snapshot v1.X/ on disk; do not contact the Registry.",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=DEFAULT_REPORT_PATH,
        help=f"Output Markdown report path (default: {DEFAULT_REPORT_PATH}).",
    )
    parser.add_argument(
        "--keep-subject",
        action="store_true",
        help="Skip cleanup; leave experiment versions registered.",
    )
    parser.add_argument(
        "--snapshot-root",
        type=Path,
        default=SCHEMAS_ROOT / "ecommerce",
        help="Parent directory under which v1.X/ snapshots are written.",
    )
    parser.add_argument(
        "--poll-timeout-s",
        type=float,
        default=15.0,
        help="Per-cell consumer poll budget (seconds).",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging."
    )
    return parser


def _select_specs(choice: str, specs: list[DrillSpec]) -> list[DrillSpec]:
    """Filter the drill list according to the ``--drill`` flag.

    Parameters
    ----------
    choice : str
        One of ``"1"``, ``"2"``, ``"3"``, or ``"all"``.
    specs : list of DrillSpec
        All registered drills.

    Returns
    -------
    list of DrillSpec
        Filtered list (preserves order).
    """
    if choice == "all":
        return list(specs)
    index = int(choice) - 1
    return [specs[index]]


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Parameters
    ----------
    argv : list of str, optional
        Argument vector.  Uses :data:`sys.argv` when ``None``.

    Returns
    -------
    int
        Process exit code.
    """
    args = _build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    kafka_config = KafkaConfig()
    registry_config = SchemaRegistryConfig()
    subject = SUBJECT_TEMPLATE.format(topic=kafka_config.topic)

    baseline = load_schema_set(DEFAULT_BASELINE_DIR)
    baseline_str = dump_schema(baseline)
    specs = _select_specs(args.drill, build_drill_specs())

    if args.snapshot_only:
        registry = None  # type: ignore[assignment]
    else:
        registry = SchemaRegistry(registry_config)
        try:
            registry.set_compatibility(subject, "BACKWARD")
        except RegistryError as exc:
            logger.error(f"Could not pin compatibility on {subject!r}: {exc}")
            return 1

    results: list[EvolutionDrillResult] = []
    for spec in specs:
        result = run_drill(
            spec,
            baseline_composite=baseline,
            prior_schema_str=baseline_str,
            snapshot_root=args.snapshot_root,
            registry=registry,  # type: ignore[arg-type]
            subject=subject,
            kafka_config=kafka_config,
            registry_config=registry_config,
            snapshot_only=args.snapshot_only,
            poll_timeout_s=args.poll_timeout_s,
        )
        results.append(result)

    if not args.snapshot_only and not args.keep_subject and registry is not None:
        # Soft-delete then sleep briefly to let the Registry settle before
        # re-registering the baseline.
        cleanup_experiment_versions(registry, subject, baseline_version=1)
        time.sleep(0.1)

    report = render_report(
        results,
        subject=subject,
        compatibility="BACKWARD",
        generated_at=datetime.now(tz=timezone.utc),
    )
    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(report, encoding="utf-8")
    logger.info(f"Wrote report to {args.report_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover - manual entry point only
    sys.exit(main())
