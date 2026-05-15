"""Top-level child-process entry point for the multi-process load runner.

The function :func:`run_worker_process` is invoked once per child via
``multiprocessing.get_context("spawn").Pool.map``.  It re-builds the
runtime objects from a Pydantic args bundle (configs are passed as
``model_dump`` dicts to avoid re-reading environment variables in the
child), constructs a :class:`DeliveryAccountant` it can probe afterwards
for raw latency samples, runs a :class:`LoadRunner`, and returns a
:class:`ProcessOutcome`.

The function must live at module top-level so that ``spawn`` can import it
without executing arbitrary parent state.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, ConfigDict, Field

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.load.accountant import DeliveryAccountant
from streaming_feature_store.load.load_runner import LoadRunner
from streaming_feature_store.load.report import LoadRunConfig
from streaming_feature_store.load_mp.report import ProcessOutcome

logger = logging.getLogger(__name__)


class WorkerProcessArgs(BaseModel):
    """Pickleable bundle of arguments passed from parent to a child process.

    Parameters
    ----------
    process_index : int
        Zero-based child index.
    kafka_config_dict : dict
        ``model_dump()`` of the parent's :class:`KafkaConfig`.  Reconstructed
        in the child without re-reading environment variables, so all
        children see identical config even if env changes mid-run.
    registry_config_dict : dict
        ``model_dump()`` of the parent's :class:`SchemaRegistryConfig`.
    run_config : LoadRunConfig
        Per-process :class:`LoadRunConfig` (already scaled to this child's
        share of the aggregate target rate).
    floor_eps : float
        Per-process throughput floor.  The parent always passes ``0.0``;
        the aggregate floor is evaluated at the parent.
    log_level : str, optional
        Python logging level for the child.  Defaults to ``"INFO"``.
    """

    model_config = ConfigDict(frozen=True)

    process_index: int = Field(..., ge=0)
    kafka_config_dict: dict
    registry_config_dict: dict
    run_config: LoadRunConfig
    floor_eps: float = 0.0
    log_level: str = "INFO"


def _configure_child_logging(level: str) -> None:
    """Initialise logging in the child process.

    Parameters
    ----------
    level : str
        Python logging level name.

    Notes
    -----
    A spawn child starts with the bare root logger; we install a stderr
    handler with the same format as the CLI.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [pid=%(process)d] %(name)s %(message)s",
        force=True,
    )


def run_worker_process(args: WorkerProcessArgs) -> ProcessOutcome:
    """Run a single child's load-test slice and return its outcome.

    Parameters
    ----------
    args : WorkerProcessArgs
        Pickleable argument bundle.

    Returns
    -------
    ProcessOutcome
        The child's :class:`LoadRunReport` plus raw latency samples for
        parent-side percentile merging.

    Notes
    -----
    Exceptions inside this function propagate back to the parent's
    :meth:`multiprocessing.pool.Pool.map` call.  The parent re-raises the
    first error after joining all children.
    """
    _configure_child_logging(args.log_level)
    logger.info(
        f"Child process_index={args.process_index} starting: "
        f"target_rate={args.run_config.target_rate}, "
        f"workers={args.run_config.workers}, "
        f"topic={args.run_config.topic}"
    )

    kafka_config = KafkaConfig(**args.kafka_config_dict)
    registry_config = SchemaRegistryConfig(**args.registry_config_dict)
    accountant = DeliveryAccountant(seed=args.run_config.seed)

    runner = LoadRunner(
        kafka_config,
        registry_config,
        args.run_config,
        accountant=accountant,
        floor_eps=args.floor_eps,
    )
    report = runner.run()
    samples = accountant.latency_samples_s()

    logger.info(
        f"Child process_index={args.process_index} done: "
        f"acked={report.snapshot.acked} "
        f"sustained={report.sustained_rate_eps:.0f} evt/s"
    )
    return ProcessOutcome(
        process_index=args.process_index,
        report=report,
        latency_samples_s=samples,
    )
