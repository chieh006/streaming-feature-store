"""CLI driver for the multi-process consumer-group end-to-end-latency run.

Mirrors :mod:`scripts.run_event_load_mp` on the consume side.  The member
count is resolved from a :class:`ConsumePlan`
(``streaming_feature_store.consume_mp.process_planner``); each child runs
the same :class:`ConsumeRunner`, only the orchestration differs.

``--members 1`` is the *control* case (the single-process GIL ceiling); the
planned-N form is the escape.  The two are the halves of the symmetric-
ceiling demonstration (design doc §4.9).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consume_mp.mp_runner import MultiprocessConsumeRunner
from streaming_feature_store.consume_mp.process_planner import (
    plan_consume_processes,
    resolve_cpu_budget,
)
from streaming_feature_store.consume_mp.report import (
    MultiprocessConsumeConfig,
    render_markdown,
)

logger = logging.getLogger(__name__)

_DEFAULT_REPORT_PATH = (
    Path(__file__).resolve().parent.parent
    / "docs"
    / "results"
    / "week1_consume_results_mp.md"
)


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser.

    Returns
    -------
    argparse.ArgumentParser
        Configured parser.
    """
    parser = argparse.ArgumentParser(
        description="Run the multi-process consumer-group end-to-end-latency test."
    )
    parser.add_argument("--duration-s", type=float, default=10.0)
    parser.add_argument(
        "--members",
        type=int,
        default=None,
        help="Member processes. Default: auto via plan_consume_processes() "
        "(min(partitions, cpu_budget)). Pass 1 for the GIL-ceiling control.",
    )
    parser.add_argument("--group-id", default="wk1-consume")
    parser.add_argument("--topic", default=None)
    parser.add_argument(
        "--until-caught-up",
        action="store_true",
        help="End each member early once its consumer lag reaches 0.",
    )
    parser.add_argument(
        "--isolation-level",
        choices=["read_uncommitted", "read_committed"],
        default="read_uncommitted",
        help="librdkafka isolation.level (read_committed is the deferred "
        "read-side EOS seam; inert vs the non-transactional producer).",
    )
    parser.add_argument(
        "--deserialize-mode",
        choices=["pydantic", "raw"],
        default="pydantic",
        help="pydantic = full avro_dict_to_event path (production); "
        "raw = decode only (measurement control).",
    )
    parser.add_argument("--poll-timeout-s", type=float, default=1.0)
    parser.add_argument("--max-batch", type=int, default=1024)
    parser.add_argument(
        "--off-host-brokers",
        action="store_true",
        help="Use the prod core-reservation rule (cpus - 1) instead of "
        "the dev rule (cpus // 2).",
    )
    parser.add_argument("--floor-eps", type=float, default=0.0)
    parser.add_argument("--report-path", type=Path, default=_DEFAULT_REPORT_PATH)
    return parser


def _run(args: argparse.Namespace) -> int:
    """Execute the configured multi-process consume run.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed CLI arguments.

    Returns
    -------
    int
        Process exit code: ``0`` when the group drained without a
        ramping-lag verdict (and at / above ``--floor-eps``), else ``1``.
    """
    kafka_config = KafkaConfig()
    registry_config = SchemaRegistryConfig()
    topic = args.topic or kafka_config.topic

    cpu_budget = resolve_cpu_budget(on_host_brokers=not args.off_host_brokers)
    plan = plan_consume_processes(
        partitions=kafka_config.num_partitions,
        cpu_budget=cpu_budget,
        requested=args.members,
    )
    logger.info(f"ConsumePlan: {plan.rationale}")

    mp_config = MultiprocessConsumeConfig(
        duration_s=args.duration_s,
        group_id=args.group_id,
        members=plan.members,
        topic=topic,
        poll_timeout_s=args.poll_timeout_s,
        max_batch=args.max_batch,
        until_caught_up=args.until_caught_up,
        isolation_level=args.isolation_level,
        deserialize_mode=args.deserialize_mode,
    )
    runner = MultiprocessConsumeRunner(
        kafka_config,
        registry_config,
        mp_config,
        floor_eps=args.floor_eps,
    )
    report = runner.run()

    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(render_markdown(report), encoding="utf-8")
    logger.info(f"Wrote {args.report_path}")

    snap = report.aggregate_snapshot
    if report.passed:
        logger.info(
            f"✅ Group drained; e2e p50/p95/p99 = "
            f"{snap.e2e_p50_ms:.1f} / {snap.e2e_p95_ms:.1f} / "
            f"{snap.e2e_p99_ms:.1f} ms; end lag {snap.end_lag}"
        )
        return 0
    logger.error(
        f"❌ Fell behind / did not pass: lag_ramped={snap.lag_ramped} "
        f"end_lag={snap.end_lag} sustained={report.sustained_consume_eps:,.0f} "
        f"evt/s deserialize_failed={snap.deserialize_failed}"
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
