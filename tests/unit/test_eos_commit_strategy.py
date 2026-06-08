"""Unit tests for the commit strategies (design week2_03 §2.2 / §2.8)."""

from __future__ import annotations

from unittest.mock import MagicMock

from streaming_feature_store.eos import (
    AtLeastOnceCommit,
    CommitStrategy,
    TransactionalCommit,
    requires_abort,
)


class _Err:
    """Fake KafkaError exposing ``txn_requires_abort``."""

    def __init__(self, abort: bool) -> None:
        self._abort = abort

    def txn_requires_abort(self) -> bool:
        return self._abort


# --- requires_abort ---------------------------------------------------------


def test_requires_abort_true() -> None:
    assert requires_abort(Exception(_Err(abort=True))) is True


def test_requires_abort_false() -> None:
    assert requires_abort(Exception(_Err(abort=False))) is False


def test_requires_abort_no_args() -> None:
    assert requires_abort(Exception()) is False


def test_requires_abort_arg_without_method() -> None:
    assert requires_abort(Exception("plain string")) is False


# --- AtLeastOnceCommit ------------------------------------------------------


def test_at_least_once_is_a_commit_strategy() -> None:
    strat = AtLeastOnceCommit((), 1.0)
    assert isinstance(strat, CommitStrategy)


def test_at_least_once_lifecycle_calls_are_noops() -> None:
    producer = MagicMock()
    strat = AtLeastOnceCommit((producer,), 1.0)
    strat.init()
    strat.begin()
    strat.abort()
    producer.assert_not_called()
    producer.flush.assert_not_called()


def test_at_least_once_commit_flushes_then_commits_in_order() -> None:
    order: list[str] = []
    validated = MagicMock()
    dlq = MagicMock()
    consumer = MagicMock()
    validated.flush.side_effect = lambda _t: order.append("validated")
    dlq.flush.side_effect = lambda _t: order.append("dlq")
    consumer.commit.side_effect = lambda: order.append("commit")

    AtLeastOnceCommit((validated, dlq), 0.5).commit(consumer=consumer)

    assert order == ["validated", "dlq", "commit"]
    validated.flush.assert_called_once_with(0.5)


def test_at_least_once_finalize_is_a_commit() -> None:
    producer = MagicMock()
    consumer = MagicMock()
    AtLeastOnceCommit((producer,), 2.0).finalize(consumer=consumer)
    producer.flush.assert_called_once_with(2.0)
    consumer.commit.assert_called_once_with()


# --- TransactionalCommit ----------------------------------------------------


def test_transactional_is_a_commit_strategy() -> None:
    assert isinstance(TransactionalCommit(MagicMock()), CommitStrategy)


def test_transactional_init_registers_transactions() -> None:
    producer = MagicMock()
    TransactionalCommit(producer).init()
    producer.init_transactions.assert_called_once_with()


def test_transactional_begin_opens_transaction() -> None:
    producer = MagicMock()
    TransactionalCommit(producer).begin()
    producer.begin_transaction.assert_called_once_with()


def test_transactional_commit_binds_offsets_then_commits() -> None:
    producer = MagicMock()
    consumer = MagicMock()
    consumer.assignment.return_value = ["tp0"]
    consumer.position.return_value = ["pos0"]
    consumer.consumer_group_metadata.return_value = "meta"

    TransactionalCommit(producer, commit_timeout_s=12.0).commit(consumer=consumer)

    consumer.position.assert_called_once_with(["tp0"])
    producer.send_offsets_to_transaction.assert_called_once_with(["pos0"], "meta")
    producer.commit_transaction.assert_called_once_with(12.0)


def test_transactional_abort_uses_timeout() -> None:
    producer = MagicMock()
    TransactionalCommit(producer, commit_timeout_s=4.0).abort()
    producer.abort_transaction.assert_called_once_with(4.0)


def test_transactional_finalize_is_noop() -> None:
    producer = MagicMock()
    consumer = MagicMock()
    TransactionalCommit(producer).finalize(consumer=consumer)
    producer.commit_transaction.assert_not_called()
    producer.abort_transaction.assert_not_called()
