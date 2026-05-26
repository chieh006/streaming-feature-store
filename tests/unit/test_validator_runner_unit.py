"""Unit tests for :class:`ValidatorRunner` and :class:`ValidatorRunConfig`."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from streaming_feature_store.schemas.models import (
    ClickPayload,
    EcommerceEvent,
    EventType,
    PurchasePayload,
)
from streaming_feature_store.validate.accountant import ValidatorAccountant
from streaming_feature_store.validate.dlq import ErrorClass
from streaming_feature_store.validate.pipeline import (
    Invalid,
    Valid,
    ValidationPipeline,
)
from streaming_feature_store.validate.runner import (
    ValidatorRunConfig,
    ValidatorRunner,
)


def _click(user_id: str = "u-1") -> EcommerceEvent:
    return EcommerceEvent(
        event_id=uuid4(),
        event_type=EventType.CLICK,
        user_id=user_id,
        session_id="s-1",
        event_timestamp=datetime.now(tz=timezone.utc),
        payload=ClickPayload(element_id="btn", page_url="/p"),
    )


def _avro_dict(event: EcommerceEvent) -> dict:
    if isinstance(event.payload, ClickPayload):
        fqn = "com.featurestore.ecommerce.v1.ClickPayload"
    elif isinstance(event.payload, PurchasePayload):
        fqn = "com.featurestore.ecommerce.v1.PurchasePayload"
    else:
        fqn = "com.featurestore.ecommerce.v1.PageViewPayload"
    return {
        "event_id": str(event.event_id),
        "event_type": event.event_type.value,
        "user_id": event.user_id,
        "session_id": event.session_id,
        "event_timestamp": int(event.event_timestamp.timestamp() * 1_000_000),
        "payload": (fqn, event.payload.model_dump()),
    }


class FakeMsg:
    """Minimal stand-in for a confluent_kafka ``Message``."""

    def __init__(
        self,
        event: EcommerceEvent | None,
        *,
        partition: int = 0,
        offset: int = 0,
        topic: str = "e-commerce-events-feed",
        raw_value: object | None = None,
    ) -> None:
        self._event = event
        self._partition = partition
        self._offset = offset
        self._topic = topic
        self._raw_value = raw_value
        self.value_bytes = b"\x00\x00\x00\x00\x01payload"

    def value(self):
        if self._raw_value is not None:
            return self._raw_value
        if self._event is None:
            return {"corrupt": True}  # missing keys → triggers KeyError → DESERIALIZE_FAILURE
        return _avro_dict(self._event)

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def topic(self):
        return self._topic

    def timestamp(self):
        return (1, 1_700_000_000_000)

    def key(self):
        return b"k"


class _AlwaysValid:
    name = "AlwaysValid"
    applies_to = None

    def validate(self, event):
        return Valid(event=event)


class _AlwaysInvalid:
    name = "AlwaysInvalid"
    applies_to = None

    def validate(self, event):
        return Invalid(
            error_class=ErrorClass.OUT_OF_RANGE,
            validator_name=self.name,
            error_field_path="x",
            error_message="forced",
        )


def _make_runner(
    *,
    consumer,
    validated_producer,
    dlq_producer,
    pipeline,
    accountant=None,
    config=None,
) -> ValidatorRunner:
    cfg = config or ValidatorRunConfig(
        poll_timeout_s=0.01,
        poll_max_records=100,
        flush_timeout_s=0.5,
    )
    return ValidatorRunner(
        consumer=consumer,
        validated_producer=validated_producer,
        dlq_producer=dlq_producer,
        pipeline=pipeline,
        accountant=accountant or ValidatorAccountant(),
        config=cfg,
    )


def _make_mocks(
    *, validated_topic="validated-events", dlq_topic="dead-letter-queue"
):
    consumer = MagicMock(name="AvroEventConsumer")
    validated = MagicMock(name="AvroEventProducer")
    validated.topic = validated_topic
    dlq = MagicMock(name="DlqProducer")
    dlq.topic = dlq_topic
    return consumer, validated, dlq


# --- ValidatorRunConfig ----------------------------------------------------


def test_run_config_defaults() -> None:
    cfg = ValidatorRunConfig()
    assert cfg.source_topic == "e-commerce-events-feed"
    assert cfg.validated_topic == "validated-events"
    assert cfg.dlq_topic == "dead-letter-queue"


def test_run_config_rejects_duplicate_topics() -> None:
    cfg = ValidatorRunConfig(validated_topic="dup", dlq_topic="dup")
    with pytest.raises(ValueError, match="distinct"):
        cfg._assert_topic_disjoint()


def test_run_config_rejects_self_loop() -> None:
    cfg = ValidatorRunConfig(
        source_topic="loop", validated_topic="loop", dlq_topic="dlq"
    )
    with pytest.raises(ValueError):
        cfg._assert_topic_disjoint()


def test_run_config_validator_stub_passes_through() -> None:
    cfg = ValidatorRunConfig(validated_topic="vt")
    assert cfg.validated_topic == "vt"


# --- runner construction ---------------------------------------------------


def test_runner_rejects_validated_producer_topic_mismatch() -> None:
    consumer, validated, dlq = _make_mocks(validated_topic="WRONG")
    pipeline = ValidationPipeline([_AlwaysValid()])
    with pytest.raises(ValueError, match="validated_producer.topic"):
        _make_runner(
            consumer=consumer,
            validated_producer=validated,
            dlq_producer=dlq,
            pipeline=pipeline,
        )


def test_runner_rejects_dlq_producer_topic_mismatch() -> None:
    consumer, validated, dlq = _make_mocks(dlq_topic="WRONG")
    pipeline = ValidationPipeline([_AlwaysValid()])
    with pytest.raises(ValueError, match="dlq_producer.topic"):
        _make_runner(
            consumer=consumer,
            validated_producer=validated,
            dlq_producer=dlq,
            pipeline=pipeline,
        )


def test_runner_config_property() -> None:
    consumer, validated, dlq = _make_mocks()
    pipeline = ValidationPipeline([_AlwaysValid()])
    runner = _make_runner(
        consumer=consumer,
        validated_producer=validated,
        dlq_producer=dlq,
        pipeline=pipeline,
    )
    assert runner.config.source_topic == "e-commerce-events-feed"


# --- runner loop -----------------------------------------------------------


def test_runner_routes_valid_event_to_validated_topic() -> None:
    event = _click()
    msg = FakeMsg(event, partition=2)
    consumer, validated, dlq = _make_mocks()
    pipeline = ValidationPipeline([_AlwaysValid()])
    accountant = ValidatorAccountant()
    runner = _make_runner(
        consumer=consumer,
        validated_producer=validated,
        dlq_producer=dlq,
        pipeline=pipeline,
        accountant=accountant,
    )

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return [msg]
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()

    validated.produce.assert_called_once()
    snap = accountant.snapshot()
    assert snap.validated == 1
    assert snap.invalid_total == 0
    assert snap.partition_counts == {2: 1}


def test_runner_routes_invalid_event_to_dlq() -> None:
    event = _click()
    msg = FakeMsg(event)
    consumer, validated, dlq = _make_mocks()
    pipeline = ValidationPipeline([_AlwaysInvalid()])
    accountant = ValidatorAccountant()
    runner = _make_runner(
        consumer=consumer,
        validated_producer=validated,
        dlq_producer=dlq,
        pipeline=pipeline,
        accountant=accountant,
    )

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return [msg]
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()

    dlq.send.assert_called_once()
    assert validated.produce.call_count == 0
    snap = accountant.snapshot()
    assert snap.invalid_total == 1
    assert snap.invalid_by_class[ErrorClass.OUT_OF_RANGE] == 1


def test_runner_routes_deserialize_failure_to_dlq() -> None:
    bad_msg = FakeMsg(None)
    consumer, validated, dlq = _make_mocks()
    pipeline = ValidationPipeline([_AlwaysValid()])
    accountant = ValidatorAccountant()
    runner = _make_runner(
        consumer=consumer,
        validated_producer=validated,
        dlq_producer=dlq,
        pipeline=pipeline,
        accountant=accountant,
    )

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return [bad_msg]
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()

    dlq.send.assert_called_once()
    snap = accountant.snapshot()
    assert snap.invalid_by_class[ErrorClass.DESERIALIZE_FAILURE] == 1


def test_runner_flush_ordered_before_commit() -> None:
    event = _click()
    msg = FakeMsg(event)
    consumer, validated, dlq = _make_mocks()
    pipeline = ValidationPipeline([_AlwaysValid()])
    accountant = ValidatorAccountant()
    runner = _make_runner(
        consumer=consumer,
        validated_producer=validated,
        dlq_producer=dlq,
        pipeline=pipeline,
        accountant=accountant,
    )

    order: list[str] = []
    validated.flush.side_effect = lambda _t: order.append("validated_flush")
    dlq.flush.side_effect = lambda _t: order.append("dlq_flush")
    consumer.commit.side_effect = lambda: order.append("commit")

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return [msg]
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()

    # The first iteration produced one validated event; the flush + commit
    # block must fire in order, and both producers' flush() must precede
    # the commit.
    assert order.index("validated_flush") < order.index("commit")
    assert order.index("dlq_flush") < order.index("commit")


def test_runner_shutdown_closes_clients() -> None:
    consumer, validated, dlq = _make_mocks()
    pipeline = ValidationPipeline([_AlwaysValid()])
    runner = _make_runner(
        consumer=consumer,
        validated_producer=validated,
        dlq_producer=dlq,
        pipeline=pipeline,
    )
    consumer.poll_batch.return_value = []
    runner.request_shutdown()
    runner.run()
    consumer.close.assert_called_once()
    validated.close.assert_called_once()
    dlq.close.assert_called_once()


def test_runner_records_validation_latency() -> None:
    event = _click()
    msg = FakeMsg(event)
    consumer, validated, dlq = _make_mocks()
    pipeline = ValidationPipeline([_AlwaysValid()])
    accountant = ValidatorAccountant()
    runner = _make_runner(
        consumer=consumer,
        validated_producer=validated,
        dlq_producer=dlq,
        pipeline=pipeline,
        accountant=accountant,
    )

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return [msg]
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()
    snap = accountant.snapshot()
    assert snap.validation_latency_us_p50 >= 0.0


def test_runner_routes_pydantic_validation_error_to_dlq() -> None:
    # A dict with a non-uuid event_id triggers a Pydantic ValidationError
    # from avro_dict_to_event → routed as NULL_REQUIRED_FIELD per the
    # runner's classifier.
    bad_raw = {
        "event_id": "not-a-uuid",
        "event_type": "CLICK",
        "user_id": "u-1",
        "session_id": "s-1",
        "event_timestamp": int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000),
        "payload": (
            "com.featurestore.ecommerce.v1.ClickPayload",
            {"element_id": "b", "page_url": "/p"},
        ),
    }
    bad_msg = FakeMsg(None, raw_value=bad_raw)
    consumer, validated, dlq = _make_mocks()
    pipeline = ValidationPipeline([_AlwaysValid()])
    accountant = ValidatorAccountant()
    runner = _make_runner(
        consumer=consumer,
        validated_producer=validated,
        dlq_producer=dlq,
        pipeline=pipeline,
        accountant=accountant,
    )

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return [bad_msg]
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()
    dlq.send.assert_called()


def test_runner_handles_none_partition_gracefully() -> None:
    event = _click()
    msg = FakeMsg(event, partition=0)
    msg._partition = None  # exercise the None-guard
    consumer, validated, dlq = _make_mocks()
    pipeline = ValidationPipeline([_AlwaysValid()])
    accountant = ValidatorAccountant()
    runner = _make_runner(
        consumer=consumer,
        validated_producer=validated,
        dlq_producer=dlq,
        pipeline=pipeline,
        accountant=accountant,
    )

    def poll_side_effect(*_a, **_kw):
        if accountant.snapshot().consumed == 0:
            return [msg]
        runner.request_shutdown()
        return []

    consumer.poll_batch.side_effect = poll_side_effect
    runner.run()
    snap = accountant.snapshot()
    assert snap.partition_counts == {}
