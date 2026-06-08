"""The commit-lifecycle seam: at-least-once vs transactional (design §2.2 / §4.3).

A consume-process-produce runner delegates its end-of-batch commit to a
:class:`CommitStrategy`.  Two implementations are provided:

- :class:`AtLeastOnceCommit` — today's contract: flush the producers, *then*
  commit consumer offsets (produce-before-commit, design §2.7 of week2_01).
  ``init`` / ``begin`` / ``abort`` are no-ops, so the loop body reads
  identically in both modes.
- :class:`TransactionalCommit` — wrap the batch in a Kafka transaction so the
  output records and the input offsets commit-or-abort atomically (design
  §2.4 / §2.8).

Keeping both behind one protocol means the runner never learns the transaction
vocabulary and the at-least-once path stays first-class and tested.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


def requires_abort(exc: Exception) -> bool:
    """Classify whether a transactional API error mandates an abort (design §2.8).

    ``confluent_kafka`` raises a ``KafkaException`` whose first arg is a
    ``KafkaError`` exposing ``txn_requires_abort()``.  An *abortable* error
    means: discard the open transaction, leave offsets unadvanced, and
    re-process the batch.  A non-abortable (fatal / fenced) error should be
    re-raised so the process exits and the supervisor restarts it.

    Parameters
    ----------
    exc : Exception
        The caught exception (typically ``confluent_kafka.KafkaException``).

    Returns
    -------
    bool
        ``True`` if the underlying error reports ``txn_requires_abort()``;
        ``False`` for errors that lack the method or report non-abortable.
    """
    if not exc.args:
        return False
    err = exc.args[0]
    checker = getattr(err, "txn_requires_abort", None)
    if checker is None:
        return False
    return bool(checker())


@runtime_checkable
class CommitStrategy(Protocol):
    """Protocol for end-of-batch commit behavior (design §2.2)."""

    def init(self) -> None:
        """Run once at startup (e.g. ``init_transactions``); else a no-op."""

    def begin(self) -> None:
        """Open a per-batch scope (``begin_transaction``); else a no-op."""

    def commit(self, *, consumer: object) -> None:
        """Commit the batch's produced output and the consumed offsets."""

    def abort(self) -> None:
        """Discard the open scope (``abort_transaction``); else a no-op."""

    def finalize(self, *, consumer: object) -> None:
        """Drain on graceful shutdown after the loop exits."""


class AtLeastOnceCommit:
    """Flush producers, then commit consumer offsets (design §2.7 ordering).

    Parameters
    ----------
    producers : iterable
        Producer objects exposing ``flush(timeout_s)``.  Flushed in iteration
        order *before* the offset commit so a crash in the gap replays the
        batch (absorbed downstream by idempotency keys).
    flush_timeout_s : float
        Per-producer flush budget.

    Notes
    -----
    ``init`` / ``begin`` / ``abort`` are intentional no-ops; ``finalize``
    performs one final flush-and-commit so a graceful shutdown drains
    in-flight produces.
    """

    def __init__(self, producers: Iterable[object], flush_timeout_s: float) -> None:
        self._producers = tuple(producers)
        self._flush_timeout_s = flush_timeout_s

    def init(self) -> None:
        """No-op: there is no transactional id to register."""

    def begin(self) -> None:
        """No-op: there is no transaction to open."""

    def commit(self, *, consumer: object) -> None:
        """Flush every producer, then synchronously commit consumer offsets.

        Parameters
        ----------
        consumer : object
            Consumer exposing ``commit()``.
        """
        for producer in self._producers:
            producer.flush(self._flush_timeout_s)
        consumer.commit()

    def abort(self) -> None:
        """No-op: there is no transaction to abort."""

    def finalize(self, *, consumer: object) -> None:
        """Drain on shutdown — identical to one more :meth:`commit`.

        Parameters
        ----------
        consumer : object
            Consumer exposing ``commit()``.
        """
        self.commit(consumer=consumer)


class TransactionalCommit:
    """Wrap the batch in a Kafka transaction (design §2.4 / §2.8).

    Parameters
    ----------
    producer : TransactionalAvroProducer
        The single multi-topic transactional producer all routes share.
    commit_timeout_s : float or None, optional
        Budget for ``commit_transaction`` / ``abort_transaction``.  ``None``
        defers to the librdkafka default.

    Notes
    -----
    ``finalize`` is a no-op: each batch is already committed atomically, so
    there is nothing pending when the loop exits.
    """

    def __init__(
        self, producer: object, commit_timeout_s: float | None = None
    ) -> None:
        self._producer = producer
        self._commit_timeout_s = commit_timeout_s

    def init(self) -> None:
        """Register the transactional id and fence any prior epoch."""
        self._producer.init_transactions()

    def begin(self) -> None:
        """Open the transaction for the current batch."""
        self._producer.begin_transaction()

    def commit(self, *, consumer: object) -> None:
        """Bind offsets into the transaction and commit atomically.

        Parameters
        ----------
        consumer : object
            Consumer exposing ``assignment()``, ``position(partitions)`` and
            ``consumer_group_metadata()``.
        """
        self._producer.send_offsets_to_transaction(
            consumer.position(consumer.assignment()),
            consumer.consumer_group_metadata(),
        )
        self._producer.commit_transaction(self._commit_timeout_s)

    def abort(self) -> None:
        """Abort the open transaction; offsets stay unadvanced for replay."""
        self._producer.abort_transaction(self._commit_timeout_s)

    def finalize(self, *, consumer: object) -> None:  # noqa: ARG002
        """No-op: per-batch transactions leave nothing pending at shutdown."""
