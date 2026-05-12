"""Idempotent Kafka topic bootstrap via ``confluent_kafka.admin.AdminClient``.

The :class:`TopicAdmin` wrapper distinguishes between *created*,
*already-exists-matching*, and *already-exists-mismatched* outcomes; the last
case is logged but never auto-altered.  Mismatch handling is conservative on
purpose — see ``docs/design/week1_04_synthetic_event_producer.md`` §2.1.
"""

from __future__ import annotations

import logging
from enum import Enum
from types import TracebackType
from typing import Optional

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from pydantic import BaseModel, ConfigDict, Field

from streaming_feature_store.config import KafkaConfig

logger = logging.getLogger(__name__)


class EnsureTopicOutcome(str, Enum):
    """Possible outcomes of :meth:`TopicAdmin.ensure_topic`."""

    CREATED = "CREATED"
    ALREADY_EXISTS_MATCHING = "ALREADY_EXISTS_MATCHING"
    ALREADY_EXISTS_MISMATCH = "ALREADY_EXISTS_MISMATCH"


class TopicDiff(BaseModel):
    """Differences between expected and observed topic configuration.

    Parameters
    ----------
    field : str
        Name of the differing field (e.g. ``"num_partitions"``).
    expected : int or str
        Value declared by the caller.
    actual : int or str
        Value observed on the broker.
    """

    model_config = ConfigDict(frozen=True)

    field: str
    expected: int | str
    actual: int | str


class EnsureTopicResult(BaseModel):
    """Structured result of :meth:`TopicAdmin.ensure_topic`.

    Parameters
    ----------
    outcome : EnsureTopicOutcome
        High-level outcome classification.
    topic : str
        Name of the topic.
    diff : list of TopicDiff
        Per-field differences.  Empty unless ``outcome`` is ``ALREADY_EXISTS_MISMATCH``.
    """

    model_config = ConfigDict(frozen=True)

    outcome: EnsureTopicOutcome
    topic: str
    diff: list[TopicDiff] = Field(default_factory=list)


class TopicPartitionInfo(BaseModel):
    """Per-partition metadata snapshot.

    Parameters
    ----------
    partition_id : int
        Partition identifier.
    leader : int or None
        Broker ID of the partition leader, or ``None`` if unknown.
    replicas : list of int
        Broker IDs of in-sync and offline replicas.
    """

    model_config = ConfigDict(frozen=True)

    partition_id: int
    leader: int | None
    replicas: list[int]


class TopicDescription(BaseModel):
    """Pydantic snapshot of topic metadata returned by :meth:`TopicAdmin.describe_topic`.

    Parameters
    ----------
    name : str
        Topic name.
    num_partitions : int
        Partition count.
    replication_factor : int
        Replication factor (taken from partition 0's replica set).
    partitions : list of TopicPartitionInfo
        Per-partition leader / replica metadata.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    num_partitions: int
    replication_factor: int
    partitions: list[TopicPartitionInfo]


class TopicAdmin:
    """Wrapper around :class:`AdminClient` exposing idempotent topic operations.

    Parameters
    ----------
    kafka_config : KafkaConfig
        Bootstrap server configuration.

    Notes
    -----
    The underlying ``AdminClient`` is stateless on close, so :meth:`close`
    is a no-op preserved purely for symmetry with the project's other
    resource wrappers.
    """

    def __init__(self, kafka_config: KafkaConfig) -> None:
        self._kafka_config = kafka_config
        self._client = self._build_admin_client()

    def _build_admin_client(self) -> AdminClient:
        """Construct the underlying :class:`AdminClient`.

        Returns
        -------
        AdminClient
            Configured from ``self._kafka_config``.
        """
        return AdminClient(
            {
                "bootstrap.servers": self._kafka_config.bootstrap_servers,
                "security.protocol": self._kafka_config.security_protocol,
            }
        )

    @property
    def client(self) -> AdminClient:
        """Underlying :class:`AdminClient` instance.

        Returns
        -------
        AdminClient
            The wrapped low-level admin client.
        """
        return self._client

    def _topic_metadata(self, name: str, *, timeout_s: float):
        """Return raw cluster metadata for a single topic.

        Parameters
        ----------
        name : str
            Topic name.
        timeout_s : float
            Metadata request timeout in seconds.

        Returns
        -------
        confluent_kafka.admin.TopicMetadata or None
            Metadata, or ``None`` if the topic is absent or marked with an
            error indicating non-existence.
        """
        cluster = self._client.list_topics(timeout=timeout_s)
        meta = cluster.topics.get(name)
        if meta is None:
            return None
        if meta.error is not None:
            return None
        return meta

    def _topic_exists(self, name: str, *, timeout_s: float) -> bool:
        """Check whether *name* is a known topic on the cluster.

        Parameters
        ----------
        name : str
            Topic name.
        timeout_s : float
            Metadata request timeout in seconds.

        Returns
        -------
        bool
            ``True`` if the topic is present and error-free in metadata.
        """
        return self._topic_metadata(name, timeout_s=timeout_s) is not None

    def _compare_topic(
        self,
        name: str,
        *,
        expected_partitions: int,
        expected_rf: int,
        timeout_s: float,
    ) -> list[TopicDiff]:
        """Compare on-broker config against caller expectations.

        Parameters
        ----------
        name : str
            Topic name.
        expected_partitions : int
            Expected number of partitions.
        expected_rf : int
            Expected replication factor.
        timeout_s : float
            Metadata request timeout in seconds.

        Returns
        -------
        list of TopicDiff
            Empty if every field matches; otherwise one entry per mismatch.
        """
        meta = self._topic_metadata(name, timeout_s=timeout_s)
        if meta is None:
            return []
        actual_partitions = len(meta.partitions)
        first = meta.partitions[0] if meta.partitions else None
        actual_rf = len(first.replicas) if first is not None else 0
        diffs: list[TopicDiff] = []
        if actual_partitions != expected_partitions:
            diffs.append(
                TopicDiff(
                    field="num_partitions",
                    expected=expected_partitions,
                    actual=actual_partitions,
                )
            )
        if actual_rf != expected_rf:
            diffs.append(
                TopicDiff(
                    field="replication_factor",
                    expected=expected_rf,
                    actual=actual_rf,
                )
            )
        return diffs

    def ensure_topic(
        self,
        name: str,
        *,
        num_partitions: int,
        replication_factor: int,
        configs: dict[str, str] | None = None,
        timeout_s: float = 10.0,
    ) -> EnsureTopicResult:
        """Idempotently ensure that *name* exists with the requested layout.

        Parameters
        ----------
        name : str
            Topic name.
        num_partitions : int
            Desired partition count.
        replication_factor : int
            Desired replication factor.
        configs : dict of str to str, optional
            Topic-level config overrides (e.g. ``{"retention.ms": "604800000"}``).
        timeout_s : float, optional
            Operation timeout in seconds.  Defaults to ``10.0``.

        Returns
        -------
        EnsureTopicResult
            Outcome (``CREATED`` / ``ALREADY_EXISTS_MATCHING`` /
            ``ALREADY_EXISTS_MISMATCH``) plus any field-level diffs.
        """
        if self._topic_exists(name, timeout_s=timeout_s):
            diff = self._compare_topic(
                name,
                expected_partitions=num_partitions,
                expected_rf=replication_factor,
                timeout_s=timeout_s,
            )
            if not diff:
                logger.info(
                    f"TopicAdmin.ensure_topic {name!r} -> ALREADY_EXISTS_MATCHING"
                )
                return EnsureTopicResult(
                    outcome=EnsureTopicOutcome.ALREADY_EXISTS_MATCHING,
                    topic=name,
                )
            logger.warning(
                f"TopicAdmin.ensure_topic {name!r} -> ALREADY_EXISTS_MISMATCH "
                f"diff={[d.model_dump() for d in diff]}"
            )
            return EnsureTopicResult(
                outcome=EnsureTopicOutcome.ALREADY_EXISTS_MISMATCH,
                topic=name,
                diff=diff,
            )

        new_topic = NewTopic(
            topic=name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            config=configs or {},
        )
        futures = self._client.create_topics([new_topic], request_timeout=timeout_s)
        future = futures[name]
        try:
            future.result(timeout=timeout_s)
        except KafkaException as exc:
            err = exc.args[0] if exc.args else None
            if isinstance(err, KafkaError) and err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.debug(
                    f"TopicAdmin.ensure_topic {name!r} race (TOPIC_ALREADY_EXISTS) "
                    f"-> CREATED"
                )
                return EnsureTopicResult(
                    outcome=EnsureTopicOutcome.CREATED, topic=name
                )
            raise
        logger.info(
            f"TopicAdmin.ensure_topic {name!r} -> CREATED "
            f"(partitions={num_partitions}, RF={replication_factor})"
        )
        return EnsureTopicResult(outcome=EnsureTopicOutcome.CREATED, topic=name)

    def describe_topic(self, name: str, *, timeout_s: float = 5.0) -> TopicDescription:
        """Return a Pydantic snapshot of *name*'s broker-side metadata.

        Parameters
        ----------
        name : str
            Topic name.
        timeout_s : float, optional
            Metadata request timeout in seconds.

        Returns
        -------
        TopicDescription
            Partition count, replication factor, and per-partition leaders.

        Raises
        ------
        KeyError
            If the topic is not present in cluster metadata.
        """
        meta = self._topic_metadata(name, timeout_s=timeout_s)
        if meta is None:
            raise KeyError(f"Topic {name!r} not found in cluster metadata")
        partitions = [
            TopicPartitionInfo(
                partition_id=p.id,
                leader=p.leader if p.leader >= 0 else None,
                replicas=list(p.replicas),
            )
            for p in meta.partitions.values()
        ]
        partitions.sort(key=lambda p: p.partition_id)
        rf = len(partitions[0].replicas) if partitions else 0
        return TopicDescription(
            name=name,
            num_partitions=len(partitions),
            replication_factor=rf,
            partitions=partitions,
        )

    def delete_topic(self, name: str, *, timeout_s: float = 10.0) -> None:
        """Delete *name* from the cluster.

        Parameters
        ----------
        name : str
            Topic name.
        timeout_s : float, optional
            Operation timeout in seconds.

        Notes
        -----
        Used **only** by integration-test teardown.  Never invoked from the
        load-runner.
        """
        futures = self._client.delete_topics([name], request_timeout=timeout_s)
        futures[name].result(timeout=timeout_s)
        logger.info(f"TopicAdmin.delete_topic {name!r} -> deleted")

    def close(self) -> None:
        """Release the admin client (no-op; preserved for symmetry)."""
        # AdminClient has no explicit close; method exists for context-manager symmetry.
        return None

    def __enter__(self) -> "TopicAdmin":
        """Return ``self`` for use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        """Close on context-manager exit."""
        self.close()


def _main(argv: list[str] | None = None) -> int:
    """CLI entry point for ``make topic-ensure`` / ``topic-describe``.

    Parameters
    ----------
    argv : list of str, optional
        Argument vector.  Uses :data:`sys.argv` when ``None``.

    Returns
    -------
    int
        Process exit code.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Kafka topic admin helper.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_ensure = sub.add_parser("ensure", help="Ensure the configured topic exists.")
    p_ensure.add_argument("--name", default=None)
    p_describe = sub.add_parser("describe", help="Describe the configured topic.")
    p_describe.add_argument("--name", default=None)
    p_delete = sub.add_parser("delete", help="Delete a topic (test-helper).")
    p_delete.add_argument("--name", required=True)

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    kafka_config = KafkaConfig()
    admin = TopicAdmin(kafka_config)
    topic_name = args.name or kafka_config.topic

    if args.cmd == "ensure":
        result = admin.ensure_topic(
            topic_name,
            num_partitions=kafka_config.num_partitions,
            replication_factor=kafka_config.replication_factor,
        )
        logger.info(f"ensure_topic outcome: {result.outcome.value}")
        return 0
    if args.cmd == "describe":
        desc = admin.describe_topic(topic_name)
        logger.info(
            f"describe {topic_name}: partitions={desc.num_partitions} "
            f"RF={desc.replication_factor}"
        )
        for p in desc.partitions:
            logger.info(
                f"  partition={p.partition_id} leader={p.leader} replicas={p.replicas}"
            )
        return 0
    # delete
    admin.delete_topic(args.name)
    return 0


if __name__ == "__main__":  # pragma: no cover - manual CLI only
    import sys

    sys.exit(_main())


# Keep ConfigResource import alive for downstream extension (config diffs).
_ = ConfigResource
