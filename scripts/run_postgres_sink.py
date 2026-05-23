"""CLI driver for the Kafka-to-PostgreSQL sink consumer.

Bootstraps the feed topic via :class:`TopicAdmin`, asserts the value subject
is registered, installs SIGTERM / SIGINT handlers, runs the sink loop, and
writes a Markdown report on shutdown.
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
    PostgresConfig,
    SchemaRegistryConfig,
)
from streaming_feature_store.consumer.avro_consumer import AvroEventConsumer
from streaming_feature_store.sink.accountant import SinkAccountant
from streaming_feature_store.sink.postgres_writer import PostgresWriter
from streaming_feature_store.sink.report import render_markdown
from streaming_feature_store.sink.sink_runner import SinkRunConfig, SinkRunner

logger = logging.getLogger(__name__)

_DEFAULT_REPORT_PATH = (
    Path(__file__).resolve().parent.parent
    / "docs"
    / "results"
    / "week1_postgres_sink_results.md"
)


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Configured parser.
    """
    parser = argparse.ArgumentParser(
        description="Run the Kafka-to-PostgreSQL sink consumer daemon."
    )
    parser.add_argument("--topic", default="e-commerce-events-feed")
    parser.add_argument("--group-id", dest="group_id", default="postgres-sink")
    parser.add_argument("--batch-max-rows", type=int, default=1000)
    parser.add_argument("--batch-max-age-s", type=float, default=10.0)
    parser.add_argument("--poll-timeout-s", type=float, default=1.0)
    parser.add_argument("--poll-max-records", type=int, default=500)
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--registry", default=None)
    parser.add_argument("--dsn", default=None)
    parser.add_argument(
        "--ensure-topic",
        dest="ensure_topic",
        action="store_true",
        default=True,
    )
    parser.add_argument(
        "--no-ensure-topic",
        dest="ensure_topic",
        action="store_false",
    )
    parser.add_argument("--report-path", type=Path, default=_DEFAULT_REPORT_PATH)
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=30_000,
        help="PostgreSQL session statement_timeout in ms.",
    )
    return parser


def _resolve_kafka_config(args: argparse.Namespace) -> KafkaConfig:
    """Return a :class:`KafkaConfig` honoring ``--bootstrap`` override.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    KafkaConfig
        Resolved Kafka configuration.
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


def _resolve_dsn(args: argparse.Namespace) -> str:
    """Return the PostgreSQL DSN, honoring ``--dsn`` override.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    str
        DSN string suitable for :func:`psycopg.connect`.
    """
    if args.dsn:
        return args.dsn
    return PostgresConfig().dsn_with_password()


def _install_signal_handlers(runner: SinkRunner) -> None:
    """Install ``SIGTERM`` / ``SIGINT`` handlers that request shutdown.

    Parameters
    ----------
    runner : SinkRunner
        The runner whose shutdown flag should be set.
    """

    def _handler(signum: int, frame: object) -> None:  # noqa: ARG001
        logger.info(f"SinkRunner: received signal {signum}, requesting shutdown.")
        runner.request_shutdown()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def _run(args: argparse.Namespace) -> int:
    """Execute the sink run end-to-end.

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
    dsn = _resolve_dsn(args)

    if args.ensure_topic:
        admin = TopicAdmin(kafka_config)
        result = admin.ensure_topic(
            args.topic,
            num_partitions=kafka_config.num_partitions,
            replication_factor=kafka_config.replication_factor,
        )
        logger.info(f"TopicAdmin.ensure_topic {args.topic!r} -> {result.outcome.value}")

    config = SinkRunConfig(
        topic=args.topic,
        consumer_group_id=args.group_id,
        batch_max_rows=args.batch_max_rows,
        batch_max_age_s=args.batch_max_age_s,
        poll_timeout_s=args.poll_timeout_s,
        poll_max_records=args.poll_max_records,
    )
    consumer = AvroEventConsumer(
        kafka_config,
        registry_config,
        group_id=config.consumer_group_id,
        topic=config.topic,
    )
    writer = PostgresWriter(
        dsn,
        statement_timeout_ms=args.statement_timeout_ms,
    )
    accountant = SinkAccountant()
    runner = SinkRunner(
        consumer=consumer,
        writer=writer,
        accountant=accountant,
        config=config,
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
