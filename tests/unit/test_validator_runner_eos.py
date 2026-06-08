"""Tests for the EOS commit-strategy seam in :class:`ValidatorRunner`.

Drives ``run()`` with an injected recording strategy to assert the
init → begin → commit → finalize lifecycle and the abort / fatal branches
(design week2_03 §2.2 / §2.8).
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from confluent_kafka import KafkaException

from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
)
from streaming_feature_store.validate.accountant import ValidatorAccountant
from streaming_feature_store.validate.pipeline import Valid, ValidationPipeline
from streaming_feature_store.validate.runner import (
    ValidatorRunConfig,
    ValidatorRunner,
)


def _click() -> EcommerceEvent:
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.CLICK,
        user_id="u-1",
        session_id="s-1",
        event_timestamp=datetime.now(tz=UTC),
        payload=ClickPayload(element_id="btn", page_url="/p"),
    )


def _avro_dict(event: EcommerceEvent) -> dict:
    return {
        "event_id": str(event.event_id),
        "event_type": event.event_type.value,
        "user_id": event.user_id,
        "session_id": event.session_id,
        "event_timestamp": int(event.event_timestamp.timestamp() * 1_000_000),
        "payload": (
            "com.featurestore.ecommerce.v1.ClickPayload",
            event.payload.model_dump(),
        ),
    }


class _FakeMsg:
    def __init__(self, event: EcommerceEvent) -> None:
        self._event = event

    def value(self):
        return _avro_dict(self._event)

    def partition(self):
        return 0


class _AlwaysValid:
    name = "AlwaysValid"
    applies_to = None

    def validate(self, event):
        return Valid(event=event)


class _Err:
    """Fake KafkaError with a configurable ``txn_requires_abort``."""

    def __init__(self, abort: bool) -> None:
        self._abort = abort

    def txn_requires_abort(self) -> bool:
        return self._abort


class _RecordingStrategy:
    """CommitStrategy stand-in that records calls and can fail ``commit``."""

    def __init__(self, commit_exc: Exception | None = None) -> None:
        self.events: list[str] = []
        self._commit_exc = commit_exc

    def init(self) -> None:
        self.events.append("init")

    def begin(self) -> None:
        self.events.append("begin")

    def commit(self, *, consumer) -> None:  # noqa: ARG002
        self.events.append("commit")
        if self._commit_exc is not None:
            raise self._commit_exc

    def abort(self) -> None:
        self.events.append("abort")

    def finalize(self, *, consumer) -> None:  # noqa: ARG002
        self.events.append("finalize")


def _runner(strategy, *, consumer):
    validated = MagicMock()
    validated.topic = "validated-events"
    dlq = MagicMock()
    dlq.topic = "dead-letter-queue"
    return ValidatorRunner(
        consumer=consumer,
        validated_producer=validated,
        dlq_producer=dlq,
        pipeline=ValidationPipeline([_AlwaysValid()]),
        accountant=ValidatorAccountant(),
        config=ValidatorRunConfig(
            poll_timeout_s=0.01, poll_max_records=10, flush_timeout_s=0.1
        ),
        commit_strategy=strategy,
    )


def _one_batch_consumer(runner_holder: dict) -> MagicMock:
    """Consumer that yields one batch, then sets shutdown and drains."""
    consumer = MagicMock()
    state = {"served": False}

    def poll_side_effect(*_a, **_kw):
        if not state["served"]:
            state["served"] = True
            return [_FakeMsg(_click())]
        runner_holder["runner"].request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    return consumer


def test_transactional_lifecycle_order() -> None:
    holder: dict = {}
    strategy = _RecordingStrategy()
    consumer = _one_batch_consumer(holder)
    runner = _runner(strategy, consumer=consumer)
    holder["runner"] = runner

    runner.run()

    # init precedes the loop; begin precedes commit; finalize is last.
    assert strategy.events[0] == "init"
    assert strategy.events.index("begin") < strategy.events.index("commit")
    assert strategy.events[-1] == "finalize"


def test_init_called_before_subscribe() -> None:
    calls: list[str] = []
    strategy = _RecordingStrategy()
    strategy.init = lambda: calls.append("init")  # type: ignore[method-assign]
    consumer = MagicMock()
    consumer.subscribe.side_effect = lambda: calls.append("subscribe")
    consumer.poll_batch.return_value = []
    runner = _runner(strategy, consumer=consumer)
    runner.request_shutdown()
    runner.run()
    assert calls == ["init", "subscribe"]


def test_abortable_commit_error_triggers_abort_and_continues() -> None:
    holder: dict = {}
    strategy = _RecordingStrategy(
        commit_exc=KafkaException(_Err(abort=True))
    )
    consumer = _one_batch_consumer(holder)
    runner = _runner(strategy, consumer=consumer)
    holder["runner"] = runner

    runner.run()  # must NOT raise

    assert "abort" in strategy.events
    # Offsets were not committed by the aborted batch; the run still finalized.
    assert strategy.events[-1] == "finalize"


def test_fatal_commit_error_propagates() -> None:
    holder: dict = {}
    strategy = _RecordingStrategy(
        commit_exc=KafkaException(_Err(abort=False))
    )
    consumer = _one_batch_consumer(holder)
    runner = _runner(strategy, consumer=consumer)
    holder["runner"] = runner

    with pytest.raises(KafkaException):
        runner.run()
    assert "abort" not in strategy.events


def test_default_strategy_is_at_least_once_when_unset() -> None:
    # No commit_strategy supplied → behaviour-preserving AtLeastOnceCommit.
    from streaming_feature_store.eos import AtLeastOnceCommit

    consumer = MagicMock()
    consumer.poll_batch.return_value = []
    validated = MagicMock()
    validated.topic = "validated-events"
    dlq = MagicMock()
    dlq.topic = "dead-letter-queue"
    runner = ValidatorRunner(
        consumer=consumer,
        validated_producer=validated,
        dlq_producer=dlq,
        pipeline=ValidationPipeline([_AlwaysValid()]),
        accountant=ValidatorAccountant(),
        config=ValidatorRunConfig(),
    )
    assert isinstance(runner._strategy, AtLeastOnceCommit)
