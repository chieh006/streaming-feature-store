"""CLI driver for the low-rate continuous event feeder daemon.

Bootstraps the feed topic via :class:`TopicAdmin`, installs SIGTERM / SIGINT
handlers, runs the feeder loop, and emits a final summary log line on
shutdown.  The feeder is intended to run indefinitely (``nohup`` /
``docker compose`` style) and produces ~17 M events/day at the default 200
evt/s rate.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import (
    KafkaConfig,
    SchemaRegistryConfig,
)
from streaming_feature_store.feeder.feeder_runner import (
    FeederRunConfig,
    FeederRunner,
)
from streaming_feature_store.load.accountant import DeliveryAccountant
from streaming_feature_store.load.pacer import TokenBucketPacer
from streaming_feature_store.load.synthetic import SyntheticEventGenerator
from streaming_feature_store.producer.avro_producer import AvroEventProducer

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Configured parser.
    """
    parser = argparse.ArgumentParser(
        description="Continuous low-rate event feeder daemon."
    )
    parser.add_argument("--topic", default="e-commerce-events-feed")
    parser.add_argument("--rate-evt-per-sec", type=float, default=200.0)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--snapshot-interval-s", type=float, default=60.0)
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--registry", default=None)
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


def _install_signal_handlers(runner: FeederRunner) -> None:
    """Install ``SIGTERM`` / ``SIGINT`` handlers that request shutdown.

    Parameters
    ----------
    runner : FeederRunner
        The runner whose shutdown flag should be set.
    """

    def _handler(signum: int, frame: object) -> None:  # noqa: ARG001
        logger.info(f"FeederRunner: received signal {signum}, requesting shutdown.")
        runner.request_shutdown()

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def _run(args: argparse.Namespace) -> int:
    """Execute the feeder run end-to-end.

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

    if args.ensure_topic:
        admin = TopicAdmin(kafka_config)
        result = admin.ensure_topic(
            args.topic,
            num_partitions=kafka_config.num_partitions,
            replication_factor=kafka_config.replication_factor,
        )
        logger.info(
            f"TopicAdmin.ensure_topic {args.topic!r} -> {result.outcome.value}"
        )

    config = FeederRunConfig(
        topic=args.topic,
        rate_evt_per_sec=args.rate_evt_per_sec,
        batch_size=args.batch_size,
        seed=args.seed,
        snapshot_interval_s=args.snapshot_interval_s,
    )
    producer = AvroEventProducer(
        kafka_config,
        registry_config,
        topic=config.topic,
    )
    generator = SyntheticEventGenerator(seed=config.seed)
    # burst capped at batch_size so the first iteration cannot drain a large
    # pre-filled bucket — that would let the feeder emit a multi-thousand-event
    # spike on startup before pacing kicked in.  See design doc §2.5.
    pacer = TokenBucketPacer(config.rate_evt_per_sec, burst=config.batch_size)
    accountant = DeliveryAccountant()
    runner = FeederRunner(
        config=config,
        producer=producer,
        generator=generator,
        pacer=pacer,
        accountant=accountant,
    )
    _install_signal_handlers(runner)
    snapshot = runner.run()
    sustained = snapshot.delivery.acked / max(snapshot.delivery.wallclock_s, 1e-9)
    logger.info(
        f"FeederRunner final: produced={snapshot.delivery.produced} "
        f"acked={snapshot.delivery.acked} failed={snapshot.delivery.failed} "
        f"sustained={sustained:,.0f} evt/s "
        f"duration={snapshot.duration_s:.2f}s topic={snapshot.topic}"
    )
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
