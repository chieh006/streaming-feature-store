"""CLI driver for the single-process inline validator.

Bootstraps the source / validated / DLQ topics via :class:`TopicAdmin`,
ensures the DLQ Avro schema is registered, installs SIGTERM / SIGINT
handlers, runs the validator loop, and writes a Markdown report on
shutdown.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from pathlib import Path

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import (
    KafkaConfig,
    SchemaRegistryConfig,
)
from streaming_feature_store.consumer.avro_consumer import AvroEventConsumer
from streaming_feature_store.eos import (
    CommitStrategy,
    TransactionalConfig,
    derive_transactional_id,
)
from streaming_feature_store.producer.avro_producer import (
    AvroEventProducer,
)
from streaming_feature_store.schemas import (
    SCHEMAS_ROOT,
    SchemaRegistry,
    dump_schema,
    load_schema_set,
)
from streaming_feature_store.validate.accountant import ValidatorAccountant
from streaming_feature_store.validate.dlq import DlqProducer
from streaming_feature_store.validate.eos_wiring import build_validator_eos
from streaming_feature_store.validate.pipeline import (
    ValidationPipeline,
    default_validators,
)
from streaming_feature_store.validate.report import render_markdown
from streaming_feature_store.validate.runner import (
    DEFAULT_DLQ_TOPIC,
    DEFAULT_GROUP_ID,
    DEFAULT_SOURCE_TOPIC,
    DEFAULT_VALIDATED_TOPIC,
    ValidatorRunConfig,
    ValidatorRunner,
)

logger = logging.getLogger(__name__)

_DEFAULT_REPORT_PATH = (
    Path(__file__).resolve().parent.parent
    / "docs"
    / "results"
    / "week2_validator_results.md"
)

_BENCH_SOURCE_TOPIC: str = "e-commerce-events"
_BENCH_GROUP_ID: str = "validator-bench"

# Output-topic partition counts (design doc §2.10).
_VALIDATED_PARTITIONS: int = 12
_DLQ_PARTITIONS: int = 3
_DLQ_REPLICATION_FACTOR: int = 3
# 7-day / 30-day retentions in milliseconds.
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
        description="Run the inline validator + DLQ router daemon."
    )
    parser.add_argument(
        "--source",
        choices=("feed", "bench"),
        default="feed",
        help=(
            "Source-topic mode: 'feed' (default) consumes from "
            "e-commerce-events-feed; 'bench' consumes from the benchmark "
            "topic e-commerce-events with a distinct consumer group."
        ),
    )
    parser.add_argument(
        "--source-topic",
        default=None,
        help="Override the source topic (otherwise derived from --source).",
    )
    parser.add_argument(
        "--validated-topic", default=DEFAULT_VALIDATED_TOPIC,
    )
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
        "--eos",
        action="store_true",
        default=False,
        help=(
            "Enable transactional exactly-once: route validated-events + DLQ "
            "writes and the input-offset commit through one transaction "
            "(design week2_03)."
        ),
    )
    parser.add_argument(
        "--transactional-id",
        dest="transactional_id",
        default=None,
        help="Producer transactional.id (default: f'{group_id}-0').",
    )
    parser.add_argument(
        "--group-instance-id",
        dest="group_instance_id",
        default=None,
        help="Static-membership group.instance.id (default: the txn id).",
    )
    parser.add_argument("--transaction-timeout-ms", type=int, default=60_000)
    parser.add_argument("--commit-timeout-s", type=float, default=30.0)
    return parser


def _resolve_source_topic_and_group(
    args: argparse.Namespace,
) -> tuple[str, str]:
    """Derive ``(source_topic, group_id)`` from CLI flags.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    tuple of (str, str)
        Resolved source topic and consumer group id.
    """
    if args.source == "bench":
        topic = args.source_topic or _BENCH_SOURCE_TOPIC
        group = args.group_id or _BENCH_GROUP_ID
        return topic, group
    topic = args.source_topic or DEFAULT_SOURCE_TOPIC
    group = args.group_id or DEFAULT_GROUP_ID
    return topic, group


def _resolve_kafka_config(args: argparse.Namespace) -> KafkaConfig:
    """Return a :class:`KafkaConfig` honoring the ``--bootstrap`` override.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    KafkaConfig
        Resolved configuration.
    """
    base = KafkaConfig()
    if args.bootstrap is None:
        return base
    return base.model_copy(update={"bootstrap_servers": args.bootstrap})


def _resolve_registry_config(args: argparse.Namespace) -> SchemaRegistryConfig:
    """Return a :class:`SchemaRegistryConfig` honoring ``--registry`` override.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    SchemaRegistryConfig
        Resolved registry configuration.
    """
    base = SchemaRegistryConfig()
    if args.registry is None:
        return base
    return base.model_copy(update={"url": args.registry})


def _ensure_topics(
    kafka_config: KafkaConfig,
    *,
    validated_topic: str,
    dlq_topic: str,
) -> None:
    """Idempotently create the validator's output topics.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    validated_topic : str
        Validated-events topic name.
    dlq_topic : str
        Dead-letter-queue topic name.
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


def _ensure_validated_schema_registered(
    registry_config: SchemaRegistryConfig,
    *,
    validated_topic: str,
    schema_version_dir: str = _VALIDATED_SCHEMA_VERSION_DIR,
) -> int:
    """Idempotently register the composite ``EcommerceEvent`` Avro schema.

    The :class:`AvroEventProducer` bound to the validated-events topic is
    configured with ``auto.register.schemas=False`` and
    ``use.latest.version=True`` — the subject ``f"{validated_topic}-value"``
    must therefore exist before the first ``produce()`` call.  This helper
    mirrors :meth:`DlqProducer._ensure_schema_registered` for the DLQ
    subject so the validator is self-bootstrapping (design doc §6 /
    `__7.2__`).

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
        f"run_validator: ensured validated-events schema registered "
        f"subject={subject!r} schema_id={schema_id}"
    )
    return schema_id


def _resolve_txn_config(
    args: argparse.Namespace, group_id: str
) -> TransactionalConfig | None:
    """Resolve the EOS transactional config for this run, or ``None``.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.
    group_id : str
        Consumer group id (drives the default transactional id).

    Returns
    -------
    TransactionalConfig or None
        The EOS config when ``--eos`` is set — the ``transactional.id`` defaults
        to ``f"{group_id}-0"`` and the static-membership ``group.instance.id``
        defaults to the same value — else ``None``.
    """
    if not args.eos:
        return None
    txn_id = args.transactional_id or derive_transactional_id(group_id, 0)
    return TransactionalConfig(
        enabled=True,
        transactional_id=txn_id,
        group_instance_id=args.group_instance_id or txn_id,
        transaction_timeout_ms=args.transaction_timeout_ms,
        commit_timeout_s=args.commit_timeout_s,
    )


def _build_producers_and_strategy(
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    config: ValidatorRunConfig,
    txn_config: TransactionalConfig | None,
) -> tuple[object, object, CommitStrategy | None]:
    """Build the route producers and the commit strategy for this run.

    Returns the two standalone producers + ``None`` strategy for the default
    at-least-once path, or two adapters over a single transactional producer +
    a :class:`TransactionalCommit` when *txn_config* is set (design §2.2 / §2.4).

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    config : ValidatorRunConfig
        Resolved run configuration (topic names).
    txn_config : TransactionalConfig or None
        EOS config from :func:`_resolve_txn_config`; ``None`` selects the
        at-least-once path.

    Returns
    -------
    tuple
        ``(validated_producer, dlq_producer, commit_strategy)``.
    """
    if txn_config is None:
        validated_producer = AvroEventProducer(
            kafka_config, registry_config, topic=config.validated_topic
        )
        dlq_producer = DlqProducer(
            kafka_config, registry_config, topic=config.dlq_topic
        )
        return validated_producer, dlq_producer, None

    logger.info(f"EOS enabled: transactional.id={txn_config.transactional_id!r}")
    return build_validator_eos(
        kafka_config,
        registry_config,
        validated_topic=config.validated_topic,
        dlq_topic=config.dlq_topic,
        txn_config=txn_config,
    )


def _install_signal_handlers(runner: ValidatorRunner) -> None:
    """Install ``SIGTERM`` / ``SIGINT`` handlers that request shutdown.

    Parameters
    ----------
    runner : ValidatorRunner
        Runner whose shutdown flag should be set.
    """

    def _handler(signum: int, frame: object) -> None:  # noqa: ARG001
        logger.info(
            f"ValidatorRunner: received signal {signum}, requesting shutdown."
        )
        runner.request_shutdown()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def _run(args: argparse.Namespace) -> int:
    """Execute the validator run end-to-end.

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
        _ensure_topics(
            kafka_config,
            validated_topic=args.validated_topic,
            dlq_topic=args.dlq_topic,
        )
        _ensure_validated_schema_registered(
            registry_config,
            validated_topic=args.validated_topic,
        )

    config = ValidatorRunConfig(
        source_topic=source_topic,
        validated_topic=args.validated_topic,
        dlq_topic=args.dlq_topic,
        consumer_group_id=group_id,
        poll_timeout_s=args.poll_timeout_s,
        poll_max_records=args.poll_max_records,
        flush_timeout_s=args.flush_timeout_s,
    )
    txn_config = _resolve_txn_config(args, group_id)
    consumer = AvroEventConsumer(
        kafka_config,
        registry_config,
        group_id=config.consumer_group_id,
        topic=config.source_topic,
        group_instance_id=txn_config.group_instance_id if txn_config else None,
    )
    validated_producer, dlq_producer, commit_strategy = (
        _build_producers_and_strategy(
            kafka_config, registry_config, config, txn_config
        )
    )
    pipeline = ValidationPipeline(default_validators())
    accountant = ValidatorAccountant()
    runner = ValidatorRunner(
        consumer=consumer,
        validated_producer=validated_producer,
        dlq_producer=dlq_producer,
        pipeline=pipeline,
        accountant=accountant,
        config=config,
        commit_strategy=commit_strategy,
        validator_version=args.validator_version,
    )
    _install_signal_handlers(runner)
    report = runner.run()

    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(render_markdown(report), encoding="utf-8")
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
