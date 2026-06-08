"""CLI driver for the sliding-window features consumer (Week 2 PR #2).

Bootstraps the output topics (``sliding-features`` + ``sliding-features-late``),
registers their Avro subjects, installs ``SIGTERM`` / ``SIGINT`` handlers, runs
the consume → window → emit loop, and writes a Markdown smoke-run report.  With
``--num-workers N`` it launches a consumer group of ``N`` OS processes that
share the ``sliding-features-job`` group (design doc §2.11 / §10.4).
"""

from __future__ import annotations

import argparse
import logging
import multiprocessing
import signal
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.eos import (
    TransactionalAvroProducer,
    TransactionalConfig,
    derive_transactional_id,
    transactional_producer_conf,
)
from streaming_feature_store.producer.avro_producer import _event_to_dict
from streaming_feature_store.schemas import (
    SCHEMAS_ROOT,
    SchemaRegistry,
    dump_schema,
    load_schema_set,
)
from streaming_feature_store.sliding.consumer import (
    SlidingFeaturesConsumer,
    SlidingRunSnapshot,
)
from streaming_feature_store.sliding.models import SlidingConsumerConfig
from streaming_feature_store.sliding.sinks import _record_to_dict, load_sliding_schema_str

logger = logging.getLogger(__name__)

_DEFAULT_REPORT_PATH = (
    Path(__file__).resolve().parent.parent
    / "docs"
    / "results"
    / "week2_sliding_features_results.md"
)

# Output-topic layout (design doc §2.11).
_SLIDING_PARTITIONS: int = 12
_LATE_PARTITIONS: int = 3
_SLIDING_RETENTION_MS: int = 7 * 24 * 60 * 60 * 1000
_LATE_RETENTION_MS: int = 30 * 24 * 60 * 60 * 1000
# The late topic carries the raw EcommerceEvent, so its subject takes the
# composite ecommerce schema.
_ECOMMERCE_SCHEMA_VERSION_DIR: str = "ecommerce/v1"


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Configured parser.
    """
    parser = argparse.ArgumentParser(
        description="Run the sliding-window features consumer."
    )
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--registry", default=None)
    parser.add_argument("--source-topic", default="validated-events")
    parser.add_argument("--sink-topic", default="sliding-features")
    parser.add_argument("--late-sink-topic", default="sliding-features-late")
    parser.add_argument("--consumer-group", default="sliding-features-job")
    parser.add_argument("--out-of-orderness-seconds", type=int, default=5)
    parser.add_argument("--idleness-seconds", type=int, default=30)
    parser.add_argument("--allowed-lateness-seconds", type=int, default=30)
    parser.add_argument("--poll-timeout-seconds", type=float, default=1.0)
    parser.add_argument(
        "--isolation-level",
        choices=("read_uncommitted", "read_committed"),
        default="read_committed",
        help=(
            "librdkafka isolation.level. Defaults to read_committed: "
            "validated-events is produced transactionally under EOS, so read "
            "only past the LSO and drop aborted records (week2_03 §2.5)."
        ),
    )
    parser.add_argument("--num-workers", type=int, default=1)
    parser.add_argument("--redis-host", default="redis")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--ttl-factor", type=float, default=1.5)
    parser.add_argument(
        "--warmup-seek-back",
        dest="warmup_seek_back",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--no-warmup-seek-back",
        dest="warmup_seek_back",
        action="store_false",
    )
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
    parser.add_argument(
        "--eos",
        action="store_true",
        default=False,
        help=(
            "Enable transactional exactly-once: produce sliding-features + "
            "sliding-features-late and commit input offsets in one transaction, "
            "writing Redis only after commit (design week2_03 §2.4 / §2.6)."
        ),
    )
    parser.add_argument(
        "--transactional-id",
        dest="transactional_id",
        default=None,
        help="Producer transactional.id for worker 0 (default: f'{group}-0').",
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


def _config_from_args(args: argparse.Namespace) -> SlidingConsumerConfig:
    """Build a :class:`SlidingConsumerConfig` from parsed CLI flags.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    SlidingConsumerConfig
        Validated consumer configuration.
    """
    defaults = SlidingConsumerConfig()
    return SlidingConsumerConfig(
        bootstrap=args.bootstrap or defaults.bootstrap,
        registry_url=args.registry or defaults.registry_url,
        source_topic=args.source_topic,
        sink_topic=args.sink_topic,
        late_sink_topic=args.late_sink_topic,
        consumer_group=args.consumer_group,
        out_of_orderness_seconds=args.out_of_orderness_seconds,
        idleness_seconds=args.idleness_seconds,
        allowed_lateness_seconds=args.allowed_lateness_seconds,
        poll_timeout_seconds=args.poll_timeout_seconds,
        isolation_level=args.isolation_level,
        num_workers=args.num_workers,
        warmup_seek_back=args.warmup_seek_back,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        ttl_factor=args.ttl_factor,
    )


def _kafka_config(config: SlidingConsumerConfig) -> KafkaConfig:
    """Return a :class:`KafkaConfig` bound to *config*'s bootstrap servers.

    Parameters
    ----------
    config : SlidingConsumerConfig
        Consumer configuration.

    Returns
    -------
    KafkaConfig
        Kafka connection config.
    """
    return KafkaConfig().model_copy(update={"bootstrap_servers": config.bootstrap})


def _registry_config(config: SlidingConsumerConfig) -> SchemaRegistryConfig:
    """Return a :class:`SchemaRegistryConfig` bound to *config*'s registry URL.

    Parameters
    ----------
    config : SlidingConsumerConfig
        Consumer configuration.

    Returns
    -------
    SchemaRegistryConfig
        Schema Registry connection config.
    """
    return SchemaRegistryConfig().model_copy(update={"url": config.registry_url})


def _ensure_topics(config: SlidingConsumerConfig, kafka_config: KafkaConfig) -> None:
    """Idempotently create the consumer's two output topics (design §2.11).

    Parameters
    ----------
    config : SlidingConsumerConfig
        Consumer configuration (topic names).
    kafka_config : KafkaConfig
        Kafka connection config (replication factor).
    """
    admin = TopicAdmin(kafka_config)
    admin.ensure_topic(
        config.sink_topic,
        num_partitions=_SLIDING_PARTITIONS,
        replication_factor=kafka_config.replication_factor,
        configs={"retention.ms": str(_SLIDING_RETENTION_MS)},
    )
    admin.ensure_topic(
        config.late_sink_topic,
        num_partitions=_LATE_PARTITIONS,
        replication_factor=min(3, kafka_config.replication_factor),
        configs={"retention.ms": str(_LATE_RETENTION_MS)},
    )


def _ensure_schemas(
    config: SlidingConsumerConfig, registry_config: SchemaRegistryConfig
) -> None:
    """Register the sliding-feature and late-event Avro subjects (design §4.6).

    Parameters
    ----------
    config : SlidingConsumerConfig
        Consumer configuration (topic names).
    registry_config : SchemaRegistryConfig
        Schema Registry connection config.
    """
    registry = SchemaRegistry(registry_config)
    sliding_subject = f"{config.sink_topic}-value"
    registry.register(sliding_subject, load_sliding_schema_str())
    late_subject = f"{config.late_sink_topic}-value"
    ecommerce_schema = dump_schema(
        load_schema_set(SCHEMAS_ROOT / _ECOMMERCE_SCHEMA_VERSION_DIR)
    )
    registry.register(late_subject, ecommerce_schema)
    logger.info(
        f"registered subjects {sliding_subject!r} and {late_subject!r}"
    )


def _install_signal_handlers(consumer: SlidingFeaturesConsumer) -> None:
    """Install ``SIGTERM`` / ``SIGINT`` handlers that request shutdown.

    Parameters
    ----------
    consumer : SlidingFeaturesConsumer
        Consumer whose shutdown flag should be set on signal.
    """

    def _handler(signum: int, frame: object) -> None:  # noqa: ARG001
        logger.info(f"received signal {signum}, requesting shutdown")
        consumer.request_shutdown()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def render_report(
    snapshot: SlidingRunSnapshot,
    config: SlidingConsumerConfig,
    started_at: datetime,
    ended_at: datetime,
) -> str:
    """Render a Markdown smoke-run report from a run snapshot.

    Parameters
    ----------
    snapshot : SlidingRunSnapshot
        End-of-run counters.
    config : SlidingConsumerConfig
        Configuration the run used.
    started_at, ended_at : datetime
        Run start / end timestamps.

    Returns
    -------
    str
        Markdown report body.
    """
    duration_s = (ended_at - started_at).total_seconds()
    lines = [
        "# Week 2 — Sliding-Window Features Smoke Run",
        "",
        f"- Source topic: `{config.source_topic}`",
        f"- Sink topic: `{config.sink_topic}` / late: `{config.late_sink_topic}`",
        f"- Consumer group: `{config.consumer_group}`",
        f"- Started: {started_at.isoformat()}",
        f"- Ended: {ended_at.isoformat()}",
        f"- Duration: {duration_s:.1f} s",
        "",
        "## Counters",
        "",
        f"- Events consumed: {snapshot.consumed}",
        f"- Very-late events: {snapshot.late}",
        f"- Active users at shutdown: {snapshot.active_users}",
        "",
        "### Emissions per resolution",
        "",
        "| Resolution | Records emitted |",
        "|---|---|",
    ]
    for resolution, count in snapshot.emitted_by_resolution.items():
        lines.append(f"| {resolution} | {count} |")
    lines.append("")
    return "\n".join(lines)


def _txn_config_for(
    args: argparse.Namespace, config: SlidingConsumerConfig, ordinal: int
) -> TransactionalConfig | None:
    """Build this worker's :class:`TransactionalConfig`, or ``None`` if no EOS.

    Each worker gets its own stable-unique ``transactional.id`` derived from the
    group id and its ordinal (design week2_03 §2.3); ``--transactional-id`` only
    overrides worker 0 so a multi-worker group never shares an id.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.
    config : SlidingConsumerConfig
        Consumer configuration (supplies the group id).
    ordinal : int
        This worker's index in ``[0, num_workers)``.

    Returns
    -------
    TransactionalConfig or None
        The EOS config when ``--eos`` is set, else ``None``.
    """
    if not args.eos:
        return None
    if args.transactional_id and ordinal == 0:
        txn_id = args.transactional_id
    else:
        txn_id = derive_transactional_id(config.consumer_group, ordinal)
    return TransactionalConfig(
        enabled=True,
        transactional_id=txn_id,
        group_instance_id=args.group_instance_id or txn_id,
        transaction_timeout_ms=args.transaction_timeout_ms,
        commit_timeout_s=args.commit_timeout_s,
    )


def _build_sliding_txn_producer(
    config: SlidingConsumerConfig,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    txn_config: TransactionalConfig,
) -> TransactionalAvroProducer:
    """Build the single transactional producer for both Kafka sinks (§2.4).

    Parameters
    ----------
    config : SlidingConsumerConfig
        Consumer configuration (topic names).
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    txn_config : TransactionalConfig
        EOS knobs supplying the ``transactional.id``.

    Returns
    -------
    TransactionalAvroProducer
        Multiplexes ``sliding-features`` (``SlidingFeatureRecord`` Avro) and
        ``sliding-features-late`` (``EcommerceEvent`` Avro) through one
        transaction.
    """
    registry = SchemaRegistry(registry_config)
    feature_serializer = AvroSerializer(
        schema_registry_client=registry.client,
        schema_str=load_sliding_schema_str(),
        to_dict=_record_to_dict,
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )
    ecommerce_schema = dump_schema(
        load_schema_set(SCHEMAS_ROOT / _ECOMMERCE_SCHEMA_VERSION_DIR)
    )
    late_serializer = AvroSerializer(
        schema_registry_client=registry.client,
        schema_str=ecommerce_schema,
        to_dict=_event_to_dict,
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )

    def _topic_serializer(serializer: AvroSerializer, topic: str):
        ctx = SerializationContext(topic, MessageField.VALUE)
        return lambda value: serializer(value, ctx)

    serializers = {
        config.sink_topic: _topic_serializer(feature_serializer, config.sink_topic),
        config.late_sink_topic: _topic_serializer(
            late_serializer, config.late_sink_topic
        ),
    }
    return TransactionalAvroProducer(
        serializers, conf=transactional_producer_conf(kafka_config, txn_config)
    )


def _build_consumer(
    args: argparse.Namespace,
    config: SlidingConsumerConfig,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    ordinal: int,
) -> SlidingFeaturesConsumer:
    """Build a consumer, wiring transactional EOS when ``--eos`` is set."""
    txn_config = _txn_config_for(args, config, ordinal)
    txn_producer = (
        _build_sliding_txn_producer(config, kafka_config, registry_config, txn_config)
        if txn_config is not None
        else None
    )
    if txn_config is not None:
        logger.info(f"EOS enabled: transactional.id={txn_config.transactional_id!r}")
    return SlidingFeaturesConsumer(
        config, txn_producer=txn_producer, txn_config=txn_config
    )


def _run_single(
    args: argparse.Namespace,
    config: SlidingConsumerConfig,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
) -> int:
    """Run one in-process consumer and write the report.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.
    config : SlidingConsumerConfig
        Consumer configuration.
    kafka_config, registry_config : config
        Connection settings (used to build the EOS producer when enabled).

    Returns
    -------
    int
        Process exit code.
    """
    consumer = _build_consumer(args, config, kafka_config, registry_config, 0)
    _install_signal_handlers(consumer)
    started_at = datetime.now(tz=UTC)
    snapshot = consumer.run()
    ended_at = datetime.now(tz=UTC)
    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(
        render_report(snapshot, config, started_at, ended_at), encoding="utf-8"
    )
    logger.info(f"wrote {args.report_path}")
    return 0


def _worker_entry(
    args: argparse.Namespace, config: SlidingConsumerConfig, ordinal: int
) -> None:
    """Process target for a consumer-group worker (design doc §2.11).

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.
    config : SlidingConsumerConfig
        Consumer configuration shared by every worker in the group.
    ordinal : int
        This worker's index, which drives its own ``transactional.id`` (§2.3).
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    kafka_config = _kafka_config(config)
    registry_config = _registry_config(config)
    consumer = _build_consumer(args, config, kafka_config, registry_config, ordinal)
    _install_signal_handlers(consumer)
    consumer.run()


def _run_group(args: argparse.Namespace, config: SlidingConsumerConfig) -> int:
    """Launch a multiprocessing consumer group and supervise it.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.
    config : SlidingConsumerConfig
        Consumer configuration shared by every worker.

    Returns
    -------
    int
        Process exit code.
    """
    processes = [
        multiprocessing.Process(
            target=_worker_entry, args=(args, config, i), name=f"sliding-worker-{i}"
        )
        for i in range(config.num_workers)
    ]
    for proc in processes:
        proc.start()
    logger.info(f"launched {config.num_workers} sliding-features workers")
    for proc in processes:
        proc.join()
    return 0


def _run(args: argparse.Namespace) -> int:
    """Execute the consumer run end-to-end.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    int
        Process exit code.
    """
    config = _config_from_args(args)
    kafka_config = _kafka_config(config)
    registry_config = _registry_config(config)
    if args.ensure_topics:
        _ensure_topics(config, kafka_config)
        _ensure_schemas(config, registry_config)
    if config.num_workers > 1:
        return _run_group(args, config)
    return _run_single(args, config, kafka_config, registry_config)


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
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    parser = _build_parser()
    args = parser.parse_args(argv)
    return _run(args)


if __name__ == "__main__":  # pragma: no cover - manual run only
    import sys

    sys.exit(main())
