"""Parent-side orchestrator for the multi-process validator runner.

The runner spawns ``N`` member processes via the ``spawn`` start method
(no ``fork``-related ``librdkafka`` background-thread duplication); each
member runs an independent :class:`ValidatorRunner` with the same
``group.id`` so the broker assigns it a disjoint partition subset; and
the parent aggregates outcomes into a :class:`MultiprocessValidatorReport`.

Structural twin of
:class:`streaming_feature_store.consume_mp.mp_runner.MultiprocessConsumeRunner`.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
from datetime import datetime, timezone

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.validate_mp.aggregator import aggregate_outcomes
from streaming_feature_store.validate_mp.report import (
    MultiprocessValidatorConfig,
    MultiprocessValidatorReport,
)
from streaming_feature_store.validate_mp.worker_entry import (
    WorkerProcessArgs,
    run_validator_worker,
)

logger = logging.getLogger(__name__)


class MultiprocessValidatorRunner:
    """Spawn ``N`` validator member processes and aggregate outcomes.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    mp_config : MultiprocessValidatorConfig
        Multi-process knobs (``members``, shared ``base_config``).
    validator_version : str, optional
        Semver of the validator catalog, threaded through to children.
        Defaults to ``"1.0.0"``.
    child_log_level : str, optional
        Python logging level passed to each child.  Defaults to ``"INFO"``.
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        registry_config: SchemaRegistryConfig,
        mp_config: MultiprocessValidatorConfig,
        *,
        validator_version: str = "1.0.0",
        child_log_level: str = "INFO",
    ) -> None:
        self._kafka_config = kafka_config
        self._registry_config = registry_config
        self._mp_config = mp_config
        self._validator_version = validator_version
        self._child_log_level = child_log_level

    def _build_child_args(self) -> list[WorkerProcessArgs]:
        """Materialise per-member :class:`WorkerProcessArgs` bundles.

        Returns
        -------
        list of WorkerProcessArgs
            One entry per member, in order ``0..members-1``.
        """
        kafka_dump = self._kafka_config.model_dump()
        registry_dump = self._registry_config.model_dump(mode="json")
        return [
            WorkerProcessArgs(
                process_index=i,
                kafka_config_dict=kafka_dump,
                registry_config_dict=registry_dump,
                run_config=self._mp_config.to_per_process_run_config(i),
                validator_version=self._validator_version,
                log_level=self._child_log_level,
            )
            for i in range(self._mp_config.members)
        ]

    def _spawn_children(self, args_list: list[WorkerProcessArgs]) -> list:
        """Run all members to completion via a spawn-context pool.

        Parameters
        ----------
        args_list : list of WorkerProcessArgs
            Per-member argument bundles.

        Returns
        -------
        list of ValidatorOutcome
            Outcomes returned by each member.
        """
        ctx = mp.get_context("spawn")
        n = len(args_list)
        logger.info(f"Spawning {n} validator member process(es) via spawn context")
        with ctx.Pool(processes=n) as pool:
            return pool.map(run_validator_worker, args_list)

    def run(self) -> MultiprocessValidatorReport:
        """Execute the multi-process validator run end-to-end.

        Returns
        -------
        MultiprocessValidatorReport
            Aggregate report.
        """
        args_list = self._build_child_args()
        started_at = datetime.now(tz=timezone.utc)
        outcomes = self._spawn_children(args_list)
        report = aggregate_outcomes(
            config=self._mp_config,
            started_at=started_at,
            outcomes=outcomes,
        )
        logger.info(
            f"MultiprocessValidatorRunner done: "
            f"consumed={report.total_consumed} "
            f"validated={report.total_validated} "
            f"invalid={report.total_invalid} "
            f"sustained={report.sustained_consume_eps:.0f} evt/s"
        )
        return report
