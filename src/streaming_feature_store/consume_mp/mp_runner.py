"""Parent-side orchestrator for the multi-process consume runner.

The runner:

1. Verifies the topic's value subject is registered (single fail-fast
   network call, shared across all members).
2. Spawns ``N`` member processes via the ``spawn`` start method (no
   ``fork``-related librdkafka background-thread duplication).
3. Each member runs an independent :class:`ConsumeRunner` with the **same**
   ``group.id`` so the broker assigns it a disjoint partition subset, and
   returns a :class:`ConsumeOutcome`.
4. The parent aggregates outcomes into a :class:`MultiprocessConsumeReport`.

Structural twin of :class:`streaming_feature_store.load_mp.mp_runner.MultiprocessLoadRunner`.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
from datetime import datetime, timezone

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consume_mp.aggregator import aggregate_outcomes
from streaming_feature_store.consume_mp.report import (
    MultiprocessConsumeConfig,
    MultiprocessConsumeReport,
)
from streaming_feature_store.consume_mp.worker_entry import (
    WorkerProcessArgs,
    run_consume_worker,
)
from streaming_feature_store.schemas.registry import RegistryError, SchemaRegistry

logger = logging.getLogger(__name__)


class MultiprocessConsumeRunner:
    """Spawn ``N`` consumer-group member processes and aggregate outcomes.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    mp_config : MultiprocessConsumeConfig
        Multi-process knobs (``members``, ``group_id``, isolation level,
        deserialize mode, ...).
    floor_eps : float, optional
        Aggregate sustained-rate floor for the verdict.  Defaults to
        ``0.0`` (the verdict is driven by the lag signature).
    child_log_level : str, optional
        Python logging level passed to each child.  Defaults to ``"INFO"``.
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        registry_config: SchemaRegistryConfig,
        mp_config: MultiprocessConsumeConfig,
        *,
        floor_eps: float = 0.0,
        child_log_level: str = "INFO",
    ) -> None:
        self._kafka_config = kafka_config
        self._registry_config = registry_config
        self._mp_config = mp_config
        self._floor_eps = floor_eps
        self._child_log_level = child_log_level

    def _assert_subject_registered(self) -> None:
        """Fail fast if the ``<topic>-value`` subject is not registered.

        Raises
        ------
        RegistryError
            If the registry call fails or the subject is missing.
        """
        subject = f"{self._mp_config.topic}-value"
        registry = SchemaRegistry(self._registry_config)
        latest = registry.get_latest(subject)
        logger.info(
            f"Subject {subject!r}: registered "
            f"(id={latest.schema_id}, version={latest.version})"
        )

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
        list of ConsumeOutcome
            Outcomes returned by each member.
        """
        ctx = mp.get_context("spawn")
        n = len(args_list)
        logger.info(f"Spawning {n} member process(es) via spawn context")
        with ctx.Pool(processes=n) as pool:
            return pool.map(run_consume_worker, args_list)

    def run(self) -> MultiprocessConsumeReport:
        """Execute the multi-process consume run end-to-end.

        Returns
        -------
        MultiprocessConsumeReport
            Aggregate result.
        """
        self._assert_subject_registered()
        args_list = self._build_child_args()
        started_at = datetime.now(tz=timezone.utc)
        outcomes = self._spawn_children(args_list)
        report = aggregate_outcomes(
            config=self._mp_config,
            started_at=started_at,
            outcomes=outcomes,
            floor_eps=self._floor_eps,
        )
        agg = report.aggregate_snapshot
        logger.info(
            f"MultiprocessConsumeRunner done: consumed={agg.consumed} "
            f"deserialize_failed={agg.deserialize_failed} "
            f"sustained={report.sustained_consume_eps:.0f} evt/s "
            f"max_lag={agg.max_lag} end_lag={agg.end_lag} "
            f"lag_ramped={agg.lag_ramped}"
        )
        return report

    # Re-export for static-type users / tests.
    RegistryError = RegistryError
