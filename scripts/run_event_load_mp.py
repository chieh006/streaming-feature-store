"""CLI driver for the multi-process synthetic event load-runner.

Mirrors :mod:`scripts.run_event_load` but spawns ``N`` producer
*processes* instead of running a single in-process thread pool.  The
process count and per-process worker count are resolved from a
:class:`ProcessPlan` (see ``streaming_feature_store.load_mp.process_planner``).

Each child runs the same :class:`LoadRunner` used by the threading
harness — only the orchestration layer differs.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from streaming_feature_store.admin.topic_admin import TopicAdmin
from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.load_mp.mp_runner import MultiprocessLoadRunner
from streaming_feature_store.load_mp.process_planner import plan_processes
from streaming_feature_store.load_mp.report import (
    MultiprocessLoadConfig,
    render_markdown,
)

logger = logging.getLogger(__name__)

_DEFAULT_REPORT_PATH = (
    Path(__file__).resolve().parent.parent
    / "docs"
    / "results"
    / "week1_load_test_results_mp.md"
)


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Configured parser.
    """
    parser = argparse.ArgumentParser(
        description="Run the multi-process synthetic event load test."
    )
    parser.add_argument("--duration-s", type=float, default=10.0)
    parser.add_argument(
        "--target-rate",
        type=float,
        default=60_000.0,
        help="Aggregate target events/sec across all processes.",
    )
    parser.add_argument("--unpaced", action="store_true", help="Disable pacing.")
    parser.add_argument(
        "--processes",
        type=int,
        default=None,
        help="Number of producer processes. Default: auto via plan_processes().",
    )
    parser.add_argument(
        "--workers-per-process",
        type=int,
        default=2,
        help="Worker threads per process (default 2). Fewer threads per "
        "process means less intra-process GIL contention.",
    )
    parser.add_argument(
        "--off-host-brokers",
        action="store_true",
        help="Use the prod core-reservation rule (cpus - 1) instead of "
        "the dev rule (cpus // 2).",
    )
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument(
        "--max-in-flight",
        type=int,
        default=50_000,
        help="Per-process backpressure cap.",
    )
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


def _ensure_topic(kafka_config: KafkaConfig, topic: str) -> None:
    """Idempotently create the target topic.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    topic : str
        Topic name.
    """
    admin = TopicAdmin(kafka_config)
    result = admin.ensure_topic(
        topic,
        num_partitions=kafka_config.num_partitions,
        replication_factor=kafka_config.replication_factor,
    )
    logger.info(f"TopicAdmin.ensure_topic {topic!r} -> {result.outcome.value}")


def _run(args: argparse.Namespace) -> int:
    """Execute the configured multi-process load run.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    int
        Process exit code (``0`` on pass, ``1`` on fail).
    """
    kafka_config = KafkaConfig()
    registry_config = SchemaRegistryConfig()
    topic = args.topic or kafka_config.topic

    if args.ensure_topic:
        _ensure_topic(kafka_config, topic)

    target_rate = None if args.unpaced else args.target_rate
    plan = plan_processes(
        partitions=kafka_config.num_partitions,
        workers_per_process=args.workers_per_process,
        requested_processes=args.processes,
        on_host_brokers=not args.off_host_brokers,
        total_target_rate=target_rate,
    )
    logger.info(f"ProcessPlan: {plan.rationale}")

    mp_config = MultiprocessLoadConfig(
        duration_s=args.duration_s,
        target_rate=target_rate,
        processes=plan.processes,
        workers_per_process=plan.workers_per_process,
        batch_size=args.batch_size,
        max_in_flight=args.max_in_flight,
        seed=args.seed,
        topic=topic,
    )
    runner = MultiprocessLoadRunner(
        kafka_config,
        registry_config,
        mp_config,
        floor_eps=args.floor_eps,
    )
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
        f"{int(report.floor_eps):_} evt/s floor "
        f"(failed={report.aggregate_snapshot.failed})"
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
