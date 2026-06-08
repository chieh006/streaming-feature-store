"""Unit tests for the single-producer route adapters (design week2_03 §2.4)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from streaming_feature_store.eos import (
    TransactionalDlqRoute,
    TransactionalValidatedRoute,
)


def test_validated_route_topic_property() -> None:
    route = TransactionalValidatedRoute(MagicMock(), "validated-events")
    assert route.topic == "validated-events"


def test_validated_route_produce_keys_on_user_id() -> None:
    producer = MagicMock()
    route = TransactionalValidatedRoute(producer, "validated-events")
    event = SimpleNamespace(user_id="u-42")
    route.produce(event)
    producer.produce.assert_called_once_with("validated-events", "u-42", event)


def test_validated_route_ignores_on_delivery() -> None:
    producer = MagicMock()
    route = TransactionalValidatedRoute(producer, "validated-events")
    route.produce(SimpleNamespace(user_id="u"), on_delivery=lambda *a: None)
    producer.produce.assert_called_once()


def test_validated_route_flush_and_close_delegate() -> None:
    producer = MagicMock()
    producer.flush.return_value = 3
    route = TransactionalValidatedRoute(producer, "validated-events")
    assert route.flush(1.5) == 3
    route.close()
    producer.flush.assert_called_once_with(1.5)
    producer.close.assert_called_once_with()


def test_validated_route_context_manager_closes() -> None:
    producer = MagicMock()
    with TransactionalValidatedRoute(producer, "validated-events"):
        pass
    producer.close.assert_called_once_with()


def test_dlq_route_topic_property() -> None:
    route = TransactionalDlqRoute(MagicMock(), "dead-letter-queue")
    assert route.topic == "dead-letter-queue"


def test_dlq_route_send_keys_on_idempotency_key() -> None:
    producer = MagicMock()
    route = TransactionalDlqRoute(producer, "dead-letter-queue")
    record = SimpleNamespace(idempotency_key=lambda: "src:0:5")
    route.send(record)
    producer.produce.assert_called_once_with("dead-letter-queue", "src:0:5", record)


def test_dlq_route_ignores_on_delivery() -> None:
    producer = MagicMock()
    route = TransactionalDlqRoute(producer, "dead-letter-queue")
    record = SimpleNamespace(idempotency_key=lambda: "k")
    route.send(record, on_delivery=lambda *a: None)
    producer.produce.assert_called_once()


def test_dlq_route_flush_and_close_delegate() -> None:
    producer = MagicMock()
    producer.flush.return_value = 0
    route = TransactionalDlqRoute(producer, "dead-letter-queue")
    assert route.flush(2.0) == 0
    route.close()
    producer.flush.assert_called_once_with(2.0)
    producer.close.assert_called_once_with()


def test_dlq_route_context_manager_closes() -> None:
    producer = MagicMock()
    with TransactionalDlqRoute(producer, "dead-letter-queue"):
        pass
    producer.close.assert_called_once_with()
