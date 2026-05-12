"""CLI driver for the synthetic event load-runner.

Bootstraps the topic via :class:`TopicAdmin`, asserts the value subject is
registered, runs the configured load, writes the Markdown report, and exits
``0`` if the sustained rate meets the floor or ``1`` otherwise.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.load.load_runner import LoadRunner
from streaming_feature_store.load.report import LoadRunConfig, render_markdown

logger = logging.getLogger(__name__)

_DEFAULT_REPORT_PATH = (
    Path(__file__).resolve().parent.parent
    / "docs"
    / "results"
    / "week1_load_test_results.md"
)


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Configured parser.
    """
    parser = argparse.ArgumentParser(description="Run the synthetic event load test.")
    parser.add_argument("--duration-s", type=float, default=10.0)
    parser.add_argument("--target-rate", type=float, default=60_000.0)
    parser.add_argument("--unpaced", action="store_true", help="Disable pacing.")
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument("--max-in-flight", type=int, default=50_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--floor-eps", type=float, default=50_000.0)
    parser.add_argument("--report-path", type=Path, default=_DEFAULT_REPORT_PATH)
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
    parser.add_argument("--topic", default=None)
    return parser


def _resolve_workers(arg_workers: int | None, kafka_config: KafkaConfig) -> int:
    """Compute the worker count: explicit > min(num_partitions, cpu_count).

    Parameters
    ----------
    arg_workers : int or None
        Value passed to ``--workers``.
    kafka_config : KafkaConfig
        Source of ``num_partitions``.

    Returns
    -------
    int
        Resolved worker count (>= 1).
    """
    import os

    if arg_workers is not None:
        return max(1, arg_workers)
    return max(1, min(kafka_config.num_partitions, os.cpu_count() or 4))


def _run(args: argparse.Namespace) -> int:
    """Execute the configured load run.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    int
        Process exit code.
    """
    kafka_config = KafkaConfig()
    registry_config = SchemaRegistryConfig()
    topic = args.topic or kafka_config.topic

    if args.ensure_topic:
        admin = TopicAdmin(kafka_config)
        result = admin.ensure_topic(
            topic,
            num_partitions=kafka_config.num_partitions,
            replication_factor=kafka_config.replication_factor,
        )
        logger.info(f"TopicAdmin.ensure_topic {topic!r} -> {result.outcome.value}")

    workers = _resolve_workers(args.workers, kafka_config)
    target_rate = None if args.unpaced else args.target_rate
    cfg = LoadRunConfig(
        duration_s=args.duration_s,
        target_rate=target_rate,
        workers=workers,
        batch_size=args.batch_size,
        max_in_flight=args.max_in_flight,
        seed=args.seed,
        topic=topic,
    )
    runner = LoadRunner(kafka_config, registry_config, cfg, floor_eps=args.floor_eps)
    report = runner.run()

    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(render_markdown(report), encoding="utf-8")
    logger.info(f"Wrote {args.report_path}")
    if report.passed:
        logger.info(
            f"✅ Sustained {report.sustained_rate_eps:,.0f} evt/s ≥ "
            f"{int(report.floor_eps):_} evt/s floor"
        )
        return 0
    logger.error(
        f"❌ Sustained {report.sustained_rate_eps:,.0f} evt/s < "
        f"{int(report.floor_eps):_} evt/s floor (failed={report.snapshot.failed})"
    )
    return 1


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
