"""Top-level child-process entry point for the multi-process consume runner.

:func:`run_consume_worker` is invoked once per child via
``multiprocessing.get_context("spawn").Pool.map``.  It rebuilds the runtime
objects from a Pydantic args bundle (configs are passed as ``model_dump``
dicts so the child does not re-read environment variables), constructs a
:class:`ConsumeAccountant` it can probe afterwards for raw latency samples,
runs a :class:`ConsumeRunner`, and returns a :class:`ConsumeOutcome`.

The function must live at module top-level so ``spawn`` can import it
without executing arbitrary parent state.  One-for-one with
:mod:`streaming_feature_store.load_mp.worker_entry`.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, ConfigDict, Field

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consume.accountant import ConsumeAccountant
from streaming_feature_store.consume.consume_runner import ConsumeRunner
from streaming_feature_store.consume.report import ConsumeRunConfig
from streaming_feature_store.consume_mp.report import ConsumeOutcome

logger = logging.getLogger(__name__)


class WorkerProcessArgs(BaseModel):
    """Pickleable bundle of arguments passed from parent to a child member.

    Parameters
    ----------
    process_index : int
        Zero-based child index.
    kafka_config_dict : dict
        ``model_dump()`` of the parent's :class:`KafkaConfig`.
    registry_config_dict : dict
        ``model_dump(mode="json")`` of the parent's
        :class:`SchemaRegistryConfig`.
    run_config : ConsumeRunConfig
        Per-member :class:`ConsumeRunConfig` (every member shares the same
        ``group_id``).
    log_level : str, optional
        Python logging level for the child.  Defaults to ``"INFO"``.
    """

    model_config = ConfigDict(frozen=True)

    process_index: int = Field(..., ge=0)
    kafka_config_dict: dict
    registry_config_dict: dict
    run_config: ConsumeRunConfig
    log_level: str = "INFO"


def _configure_child_logging(level: str) -> None:
    """Initialise logging in the child process.

    Parameters
    ----------
    level : str
        Python logging level name.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [pid=%(process)d] %(name)s %(message)s",
        force=True,
    )


def run_consume_worker(args: WorkerProcessArgs) -> ConsumeOutcome:
    """Run a single member's consume slice and return its outcome.

    Parameters
    ----------
    args : WorkerProcessArgs
        Pickleable argument bundle.

    Returns
    -------
    ConsumeOutcome
        The child's :class:`ConsumeRunReport` plus raw end-to-end-latency
        samples for parent-side percentile merging.

    Notes
    -----
    Exceptions inside this function propagate back to the parent's
    :meth:`multiprocessing.pool.Pool.map` call; the parent re-raises the
    first error after joining all children.
    """
    _configure_child_logging(args.log_level)
    logger.info(
        f"Member process_index={args.process_index} starting: "
        f"group={args.run_config.group_id} topic={args.run_config.topic} "
        f"deserialize_mode={args.run_config.deserialize_mode}"
    )

    kafka_config = KafkaConfig(**args.kafka_config_dict)
    registry_config = SchemaRegistryConfig(**args.registry_config_dict)
    accountant = ConsumeAccountant(seed=args.process_index)

    runner = ConsumeRunner(
        kafka_config,
        registry_config,
        args.run_config,
        accountant=accountant,
    )
    report = runner.run()
    samples = accountant.e2e_samples_s()

    logger.info(
        f"Member process_index={args.process_index} done: "
        f"consumed={report.snapshot.consumed} "
        f"sustained={report.sustained_consume_eps:.0f} evt/s "
        f"end_lag={report.snapshot.end_lag} "
        f"lag_ramped={report.snapshot.lag_ramped}"
    )
    return ConsumeOutcome(
        process_index=args.process_index,
        report=report,
        e2e_samples_s=samples,
    )
