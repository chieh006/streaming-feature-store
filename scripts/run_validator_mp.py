"""CLI driver for the multi-process validator consumer group.

Spawns ``N`` member processes via the ``spawn`` start method (no fork-
related ``librdkafka`` background-thread duplication); each member runs
an independent :class:`ValidatorRunner` with the same ``group.id`` so
the broker assigns each a disjoint partition subset.  Aggregate report
is written to ``docs/results/week2_validator_results.md`` on shutdown.

Design doc: ``docs/design/week2_01_validation_layer_and_dlq.md`` §2.8.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.schemas import (
    SCHEMAS_ROOT,
    SchemaRegistry,
    dump_schema,
    load_schema_set,
)
from streaming_feature_store.validate.dlq import DlqProducer
from streaming_feature_store.validate.report import (
    ValidatorRunReport,
    render_markdown,
)
from streaming_feature_store.validate.runner import (
    DEFAULT_DLQ_TOPIC,
    DEFAULT_GROUP_ID,
    DEFAULT_SOURCE_TOPIC,
    DEFAULT_VALIDATED_TOPIC,
    ValidatorRunConfig,
)
from streaming_feature_store.validate_mp.mp_runner import (
    MultiprocessValidatorRunner,
)
from streaming_feature_store.validate_mp.process_planner import (
    plan_validator_processes,
    resolve_cpu_budget,
)
from streaming_feature_store.validate_mp.report import (
    MultiprocessValidatorConfig,
    MultiprocessValidatorReport,
)

logger = logging.getLogger(__name__)

_DEFAULT_REPORT_PATH = (
    Path(__file__).resolve().parent.parent
    / "docs"
    / "results"
    / "week2_validator_results_mp.md"
)

_BENCH_SOURCE_TOPIC: str = "e-commerce-events"
_BENCH_GROUP_ID: str = "validator-bench"

_VALIDATED_PARTITIONS: int = 12
_DLQ_PARTITIONS: int = 3
_DLQ_REPLICATION_FACTOR: int = 3
_VALIDATED_RETENTION_MS: int = 7 * 24 * 60 * 60 * 1000
_DLQ_RETENTION_MS: int = 30 * 24 * 60 * 60 * 1000
# Subpath under ``schemas/`` for the composite EcommerceEvent schema
# bound to the ``validated-events`` topic (design doc §6.x).
_VALIDATED_SCHEMA_VERSION_DIR: str = "ecommerce/v1"


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Configured parser.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Run the inline validator + DLQ router as a consumer group of "
            "processes."
        )
    )
    parser.add_argument(
        "--procs",
        type=int,
        default=None,
        help=(
            "Number of member processes.  Defaults to "
            "min(partition_count, cpu_budget)."
        ),
    )
    parser.add_argument(
        "--source",
        choices=("feed", "bench"),
        default="bench",
        help=(
            "Source-topic mode.  Defaults to 'bench' for MP runs (the "
            "feeder rate is well below the GIL ceiling and single-proc "
            "suffices)."
        ),
    )
    parser.add_argument(
        "--source-topic",
        default=None,
        help="Override the source topic (otherwise derived from --source).",
    )
    parser.add_argument("--validated-topic", default=DEFAULT_VALIDATED_TOPIC)
    parser.add_argument("--dlq-topic", default=DEFAULT_DLQ_TOPIC)
    parser.add_argument(
        "--group-id",
        dest="group_id",
        default=None,
        help="Override the consumer group id.",
    )
    parser.add_argument("--poll-timeout-s", type=float, default=1.0)
    parser.add_argument("--poll-max-records", type=int, default=500)
    parser.add_argument("--flush-timeout-s", type=float, default=5.0)
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--registry", default=None)
    parser.add_argument(
        "--ensure-topics",
        dest="ensure_topics",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--no-ensure-topics",
        dest="ensure_topics",
        action="store_false",
    )
    parser.add_argument("--report-path", type=Path, default=_DEFAULT_REPORT_PATH)
    parser.add_argument("--validator-version", default="1.0.0")
    parser.add_argument(
        "--child-log-level",
        default="INFO",
        help="Python logging level used by each child process.",
    )
    return parser


def _resolve_source_topic_and_group(
    args: argparse.Namespace,
) -> tuple[str, str]:
    """Derive ``(source_topic, group_id)`` from CLI flags."""
    if args.source == "bench":
        topic = args.source_topic or _BENCH_SOURCE_TOPIC
        group = args.group_id or _BENCH_GROUP_ID
        return topic, group
    topic = args.source_topic or DEFAULT_SOURCE_TOPIC
    group = args.group_id or DEFAULT_GROUP_ID
    return topic, group


def _resolve_kafka_config(args: argparse.Namespace) -> KafkaConfig:
    """Return a :class:`KafkaConfig` honoring the ``--bootstrap`` override."""
    base = KafkaConfig()
    if args.bootstrap is None:
        return base
    return base.model_copy(update={"bootstrap_servers": args.bootstrap})


def _resolve_registry_config(args: argparse.Namespace) -> SchemaRegistryConfig:
    """Return a :class:`SchemaRegistryConfig` honoring ``--registry`` override."""
    base = SchemaRegistryConfig()
    if args.registry is None:
        return base
    return base.model_copy(update={"url": args.registry})


def _ensure_validated_schema_registered(
    registry_config: SchemaRegistryConfig,
    *,
    validated_topic: str,
    schema_version_dir: str = _VALIDATED_SCHEMA_VERSION_DIR,
) -> int:
    """Idempotently register the composite ``EcommerceEvent`` Avro schema.

    Mirrors the single-process helper in :mod:`scripts.run_validator` and
    :meth:`DlqProducer._ensure_schema_registered`.  Called once from the
    parent so child processes do not race on subject creation when their
    :class:`AvroEventProducer` instances first attempt
    ``GET /subjects/{topic}-value/versions/latest``.

    Parameters
    ----------
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    validated_topic : str
        Destination topic for valid events.  The subject is derived as
        ``f"{validated_topic}-value"`` under the default
        ``TopicNameStrategy``.
    schema_version_dir : str, optional
        Subpath under ``schemas/`` containing the composite
        :class:`EcommerceEvent` ``.avsc`` files.  Defaults to
        :data:`_VALIDATED_SCHEMA_VERSION_DIR` (``"ecommerce/v1"``).

    Returns
    -------
    int
        Schema id returned by the registry.
    """
    schema_dir = SCHEMAS_ROOT / schema_version_dir
    schema_str = dump_schema(load_schema_set(schema_dir))
    subject = f"{validated_topic}-value"
    registry = SchemaRegistry(registry_config)
    schema_id = registry.register(subject, schema_str)
    logger.info(
        f"run_validator_mp: ensured validated-events schema registered "
        f"subject={subject!r} schema_id={schema_id}"
    )
    return schema_id


def _ensure_topics_and_schema(
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    *,
    validated_topic: str,
    dlq_topic: str,
) -> int:
    """Idempotently create output topics and register both Avro schemas.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    validated_topic : str
        Validated-events topic name.
    dlq_topic : str
        Dead-letter-queue topic name.

    Returns
    -------
    int
        Number of source-topic partitions (used by the planner to cap
        member count).
    """
    admin = TopicAdmin(kafka_config)
    admin.ensure_topic(
        validated_topic,
        num_partitions=_VALIDATED_PARTITIONS,
        replication_factor=kafka_config.replication_factor,
        configs={"retention.ms": str(_VALIDATED_RETENTION_MS)},
    )
    admin.ensure_topic(
        dlq_topic,
        num_partitions=_DLQ_PARTITIONS,
        replication_factor=min(
            _DLQ_REPLICATION_FACTOR, kafka_config.replication_factor
        ),
        configs={"retention.ms": str(_DLQ_RETENTION_MS)},
    )
    # Register both subjects once from the parent so children do not
    # race on subject creation.
    _ensure_validated_schema_registered(
        registry_config, validated_topic=validated_topic
    )
    dlq = DlqProducer(
        kafka_config,
        registry_config,
        topic=dlq_topic,
        register_schema=True,
    )
    dlq.close()
    desc = admin.describe_topic(kafka_config.topic)
    return desc.num_partitions


def _resolve_partition_count(
    kafka_config: KafkaConfig, source_topic: str
) -> int:
    """Return the partition count of *source_topic*.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    source_topic : str
        Topic name.

    Returns
    -------
    int
        Number of partitions, as observed via :class:`TopicAdmin`.
    """
    admin = TopicAdmin(kafka_config)
    desc = admin.describe_topic(source_topic)
    return desc.num_partitions


def _summarize_report(report: MultiprocessValidatorReport) -> str:
    """Render a parent-level summary atop the per-member Markdown reports.

    Parameters
    ----------
    report : MultiprocessValidatorReport
        Aggregate result.

    Returns
    -------
    str
        Markdown text combining the parent summary with each child's
        rendered report.
    """
    cfg = report.config
    lines: list[str] = []
    lines.append("# Week 2 — Multi-Process Validator Run Results")
    lines.append("")
    lines.append(f"**Started:** {report.started_at.isoformat()}")
    lines.append(f"**Members:** {cfg.members}")
    lines.append(f"**Source:** `{cfg.base_config.source_topic}`")
    lines.append(f"**Group:** `{cfg.base_config.consumer_group_id}`")
    lines.append("")
    lines.append("## Aggregate counters")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|---|---|")
    lines.append(f"| Consumed (sum) | {report.total_consumed:_} |")
    lines.append(f"| Validated (sum) | {report.total_validated:_} |")
    lines.append(f"| Invalid (sum) | {report.total_invalid:_} |")
    lines.append(
        f"| Sustained consume | {report.sustained_consume_eps:,.0f} evt/s |"
    )
    lines.append("")
    lines.append("## Per-member reports")
    lines.append("")
    for outcome in report.process_outcomes:
        lines.append(f"### Member {outcome.process_index}")
        lines.append("")
        lines.append(render_markdown(outcome.report))
        lines.append("")
    return "\n".join(lines)


def _run(args: argparse.Namespace) -> int:
    """Execute the multi-process validator run end-to-end.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    int
        Process exit code.
    """
    kafka_config = _resolve_kafka_config(args)
    registry_config = _resolve_registry_config(args)
    source_topic, group_id = _resolve_source_topic_and_group(args)

    if args.ensure_topics:
        _ensure_topics_and_schema(
            kafka_config,
            registry_config,
            validated_topic=args.validated_topic,
            dlq_topic=args.dlq_topic,
        )

    partitions = _resolve_partition_count(kafka_config, source_topic)
    cpu_budget = resolve_cpu_budget(on_host_brokers=True)
    plan = plan_validator_processes(
        partitions=partitions,
        cpu_budget=cpu_budget,
        requested=args.procs,
    )
    logger.info(f"ValidatorPlan: {plan.rationale}")

    base_config = ValidatorRunConfig(
        source_topic=source_topic,
        validated_topic=args.validated_topic,
        dlq_topic=args.dlq_topic,
        consumer_group_id=group_id,
        poll_timeout_s=args.poll_timeout_s,
        poll_max_records=args.poll_max_records,
        flush_timeout_s=args.flush_timeout_s,
    )
    mp_config = MultiprocessValidatorConfig(
        members=plan.members, base_config=base_config
    )
    runner = MultiprocessValidatorRunner(
        kafka_config,
        registry_config,
        mp_config,
        validator_version=args.validator_version,
        child_log_level=args.child_log_level,
    )
    report = runner.run()

    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(_summarize_report(report), encoding="utf-8")
    logger.info(f"Wrote {args.report_path}")
    return 0


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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args(argv)
    return _run(args)


if __name__ == "__main__":  # pragma: no cover - manual run only
    sys.exit(main())


# Keep import alive for test imports / Pylance.
_ = ValidatorRunReport
