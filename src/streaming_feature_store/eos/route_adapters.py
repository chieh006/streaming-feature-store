"""Adapters that route the validator's two outputs through one txn producer.

The :class:`~streaming_feature_store.validate.runner.ValidatorRunner` produces
valid events via ``validated_producer.produce(event)`` and rejects via
``dlq_producer.send(record)``.  Under EOS those two topics must share a single
transactional producer so they commit atomically (design week2_03 §2.4).

These thin adapters present the *exact* surface the runner expects
(``topic`` / ``produce`` / ``send`` / ``flush`` / ``close``) while delegating
every produce to one shared :class:`TransactionalAvroProducer`, preserving each
route's existing key (``user_id`` for valid events, the source
``topic:partition:offset`` idempotency key for the DLQ).
"""

from __future__ import annotations

from collections.abc import Callable
from types import TracebackType

from streaming_feature_store.eos.transactional_producer import (
    TransactionalAvroProducer,
)


class TransactionalValidatedRoute:
    """Make a shared txn producer look like the validated-events producer.

    Parameters
    ----------
    producer : TransactionalAvroProducer
        The single transactional producer shared with the DLQ route.
    topic : str
        Destination topic (``validated-events``).

    Notes
    -----
    Mirrors the :class:`~streaming_feature_store.producer.avro_producer.AvroEventProducer`
    surface the runner relies on; the ``on_delivery`` callback is ignored
    because delivery is governed by the transaction commit, not per-message
    callbacks.
    """

    def __init__(self, producer: TransactionalAvroProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    @property
    def topic(self) -> str:
        """Destination topic.

        Returns
        -------
        str
            Topic name.
        """
        return self._topic

    def produce(self, event: object, on_delivery: Callable | None = None) -> None:
        """Produce *event* keyed on ``event.user_id`` via the txn producer.

        Parameters
        ----------
        event : object
            Validated event exposing ``user_id``.
        on_delivery : callable, optional
            Ignored under EOS (delivery is decided at transaction commit).
        """
        self._producer.produce(self._topic, event.user_id, event)

    def flush(self, timeout_s: float = 10.0) -> int:
        """Flush the shared producer.

        Parameters
        ----------
        timeout_s : float, optional
            Flush budget.  Defaults to ``10.0``.

        Returns
        -------
        int
            Messages still queued after the call.
        """
        return self._producer.flush(timeout_s)

    def close(self) -> None:
        """Close the shared producer (idempotent across both routes)."""
        self._producer.close()

    def __enter__(self) -> TransactionalValidatedRoute:
        """Return ``self`` for use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close on context-manager exit."""
        self.close()


class TransactionalDlqRoute:
    """Make a shared txn producer look like the DLQ producer.

    Parameters
    ----------
    producer : TransactionalAvroProducer
        The single transactional producer shared with the validated route.
    topic : str
        Destination DLQ topic (``dead-letter-queue``).
    """

    def __init__(self, producer: TransactionalAvroProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    @property
    def topic(self) -> str:
        """Destination DLQ topic.

        Returns
        -------
        str
            Topic name.
        """
        return self._topic

    def send(self, record: object, on_delivery: Callable | None = None) -> None:
        """Produce *record* keyed on its idempotency key via the txn producer.

        Parameters
        ----------
        record : object
            DLQ envelope exposing ``idempotency_key()``.
        on_delivery : callable, optional
            Ignored under EOS (delivery is decided at transaction commit).
        """
        self._producer.produce(self._topic, record.idempotency_key(), record)

    def flush(self, timeout_s: float = 10.0) -> int:
        """Flush the shared producer.

        Parameters
        ----------
        timeout_s : float, optional
            Flush budget.  Defaults to ``10.0``.

        Returns
        -------
        int
            Messages still queued after the call.
        """
        return self._producer.flush(timeout_s)

    def close(self) -> None:
        """Close the shared producer (idempotent across both routes)."""
        self._producer.close()

    def __enter__(self) -> TransactionalDlqRoute:
        """Return ``self`` for use as a context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close on context-manager exit."""
        self.close()
