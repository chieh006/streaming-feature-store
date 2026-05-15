"""Parent-side orchestrator for the multi-process load runner.

The runner:

1. Verifies the topic's value subject is registered (single fail-fast
   network call, shared across all children).
2. Spawns ``N`` child processes via the ``spawn`` start method (no
   ``fork``-related librdkafka background-thread duplication issues).
3. Each child runs an independent :class:`LoadRunner` and returns a
   :class:`ProcessOutcome`.
4. The parent aggregates outcomes into a :class:`MultiprocessLoadReport`.

The threading runner is left untouched; the two harnesses share the
producer / generator / accountant / pacer building blocks but their
orchestration layers are deliberately separate.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
from datetime import datetime, timezone

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.load_mp.aggregator import aggregate_outcomes
from streaming_feature_store.load_mp.report import (
    MultiprocessLoadConfig,
    MultiprocessLoadReport,
)
from streaming_feature_store.load_mp.worker_entry import (
    WorkerProcessArgs,
    run_worker_process,
)
from streaming_feature_store.schemas.registry import RegistryError, SchemaRegistry

logger = logging.getLogger(__name__)


class MultiprocessLoadRunner:
    """Spawn ``N`` producer processes and aggregate their outcomes.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap server configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    mp_config : MultiprocessLoadConfig
        Multi-process knobs (``processes``, ``workers_per_process``,
        aggregate target rate, etc.).
    floor_eps : float, optional
        Aggregate-throughput floor for the pass / fail verdict.  Defaults
        to ``50_000``.
    child_log_level : str, optional
        Python logging level passed to each child.  Defaults to ``"INFO"``.
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        registry_config: SchemaRegistryConfig,
        mp_config: MultiprocessLoadConfig,
        *,
        floor_eps: float = 50_000.0,
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
        """Materialise per-child :class:`WorkerProcessArgs` bundles.

        Returns
        -------
        list of WorkerProcessArgs
            One entry per child process, in order ``0..processes-1``.
        """
        kafka_dump = self._kafka_config.model_dump()
        registry_dump = self._registry_config.model_dump(mode="json")
        return [
            WorkerProcessArgs(
                process_index=i,
                kafka_config_dict=kafka_dump,
                registry_config_dict=registry_dump,
                run_config=self._mp_config.to_per_process_run_config(i),
                floor_eps=0.0,
                log_level=self._child_log_level,
            )
            for i in range(self._mp_config.processes)
        ]

    def _spawn_children(self, args_list: list[WorkerProcessArgs]) -> list:
        """Run all children to completion via a spawn-context pool.

        Parameters
        ----------
        args_list : list of WorkerProcessArgs
            Per-child argument bundles.

        Returns
        -------
        list of ProcessOutcome
            Outcomes returned by each child.
        """
        ctx = mp.get_context("spawn")
        n = len(args_list)
        logger.info(f"Spawning {n} child process(es) via spawn context")
        with ctx.Pool(processes=n) as pool:
            return pool.map(run_worker_process, args_list)

    def run(self) -> MultiprocessLoadReport:
        """Execute the multi-process load run end-to-end.

        Returns
        -------
        MultiprocessLoadReport
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
            f"MultiprocessLoadRunner done: produced={agg.produced} "
            f"acked={agg.acked} failed={agg.failed} "
            f"sustained={report.sustained_rate_eps:.0f} evt/s "
            f"(floor={int(self._floor_eps):_})"
        )
        return report

    # Re-export for static-type users / tests.
    RegistryError = RegistryError
