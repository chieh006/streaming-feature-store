"""Unit tests for the transactional producer + config (design week2_03 §2.4)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from streaming_feature_store.config import KafkaConfig
from streaming_feature_store.eos import (
    TransactionalAvroProducer,
    TransactionalConfig,
    transactional_producer_conf,
)


class _FakeProducer:
    """Records every transactional call for assertion."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.flush_remaining = 0

    def init_transactions(self, timeout_s) -> None:
        self.calls.append(("init", timeout_s))

    def begin_transaction(self) -> None:
        self.calls.append(("begin",))

    def produce(self, *, topic, key, value) -> None:
        self.calls.append(("produce", topic, key, value))

    def send_offsets_to_transaction(self, offsets, group_metadata) -> None:
        self.calls.append(("send_offsets", offsets, group_metadata))

    def commit_transaction(self, *args) -> None:
        self.calls.append(("commit", args))

    def abort_transaction(self, *args) -> None:
        self.calls.append(("abort", args))

    def flush(self, timeout_s) -> int:
        self.calls.append(("flush", timeout_s))
        return self.flush_remaining


# --- TransactionalConfig ----------------------------------------------------


def test_config_defaults_disabled() -> None:
    cfg = TransactionalConfig()
    assert cfg.enabled is False
    assert cfg.transactional_id is None
    assert cfg.transaction_timeout_ms == 60_000
    assert cfg.commit_timeout_s == 30.0


def test_config_requires_id_when_enabled() -> None:
    with pytest.raises(ValidationError, match="transactional_id is required"):
        TransactionalConfig(enabled=True)


def test_config_blank_id_when_enabled_rejected() -> None:
    with pytest.raises(ValidationError, match="transactional_id is required"):
        TransactionalConfig(enabled=True, transactional_id="   ")


def test_config_enabled_with_id_ok() -> None:
    cfg = TransactionalConfig(enabled=True, transactional_id="val-0")
    assert cfg.transactional_id == "val-0"


def test_config_allows_no_id_when_disabled() -> None:
    cfg = TransactionalConfig(enabled=False, transactional_id=None)
    assert cfg.enabled is False


def test_config_rejects_low_timeout() -> None:
    with pytest.raises(ValidationError):
        TransactionalConfig(transaction_timeout_ms=999)


def test_config_rejects_nonpositive_commit_timeout() -> None:
    with pytest.raises(ValidationError):
        TransactionalConfig(commit_timeout_s=0.0)


def test_config_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        TransactionalConfig(unknown="x")


# --- transactional_producer_conf -------------------------------------------


def test_conf_forces_idempotent_foundation() -> None:
    cfg = TransactionalConfig(enabled=True, transactional_id="val-0")
    conf = transactional_producer_conf(KafkaConfig(), cfg)
    assert conf["transactional.id"] == "val-0"
    assert conf["enable.idempotence"] is True
    assert conf["acks"] == "all"
    assert "group.instance.id" not in conf


def test_conf_excludes_group_instance_id() -> None:
    # group.instance.id is a consumer property; it must NOT land on the
    # producer conf (design week2_03 §2.3 / §10.1).
    cfg = TransactionalConfig(
        enabled=True, transactional_id="val-0", group_instance_id="val-0"
    )
    conf = transactional_producer_conf(KafkaConfig(), cfg)
    assert "group.instance.id" not in conf


def test_conf_requires_transactional_id() -> None:
    cfg = TransactionalConfig()  # disabled, no id
    with pytest.raises(ValueError, match="transactional_id is required"):
        transactional_producer_conf(KafkaConfig(), cfg)


# --- TransactionalAvroProducer ---------------------------------------------


def _producer(serializers=None) -> tuple[TransactionalAvroProducer, _FakeProducer]:
    fake = _FakeProducer()
    sers = serializers or {
        "validated-events": lambda v: f"v:{v}".encode(),
        "dead-letter-queue": lambda v: f"d:{v}".encode(),
    }
    return TransactionalAvroProducer(sers, producer=fake), fake


def test_producer_requires_serializers() -> None:
    with pytest.raises(ValueError, match="at least one topic"):
        TransactionalAvroProducer({}, producer=_FakeProducer())


def test_producer_requires_conf_or_producer() -> None:
    with pytest.raises(ValueError, match="either conf or producer"):
        TransactionalAvroProducer({"t": lambda v: b""})


def test_producer_can_build_from_conf() -> None:
    # Exercises the real-Producer construction branch without a broker; no
    # transactional call is made, so nothing connects.
    prod = TransactionalAvroProducer(
        {"t": lambda v: b""}, conf={"bootstrap.servers": "localhost:9092"}
    )
    assert prod.topics == ("t",)
    assert prod.initialised is False
    prod.close()


def test_topics_sorted() -> None:
    prod, _ = _producer()
    assert prod.topics == ("dead-letter-queue", "validated-events")


def test_init_transactions_sets_initialised() -> None:
    prod, fake = _producer()
    prod.init_transactions(5.0)
    assert prod.initialised is True
    assert ("init", 5.0) in fake.calls


def test_begin_delegates() -> None:
    prod, fake = _producer()
    prod.begin_transaction()
    assert ("begin",) in fake.calls


def test_produce_selects_serializer_by_topic() -> None:
    prod, fake = _producer()
    prod.produce("validated-events", "u1", "EVT")
    prod.produce("dead-letter-queue", "t:0:5", "REC")
    assert ("produce", "validated-events", "u1", b"v:EVT") in fake.calls
    assert ("produce", "dead-letter-queue", "t:0:5", b"d:REC") in fake.calls


def test_produce_unknown_topic_raises() -> None:
    prod, _ = _producer()
    with pytest.raises(KeyError, match="no serializer registered"):
        prod.produce("nope", "k", "v")


def test_send_offsets_delegates() -> None:
    prod, fake = _producer()
    prod.send_offsets_to_transaction(["off"], "meta")
    assert ("send_offsets", ["off"], "meta") in fake.calls


def test_commit_with_and_without_timeout() -> None:
    prod, fake = _producer()
    prod.commit_transaction(12.0)
    prod.commit_transaction(None)
    assert ("commit", (12.0,)) in fake.calls
    assert ("commit", ()) in fake.calls


def test_abort_with_and_without_timeout() -> None:
    prod, fake = _producer()
    prod.abort_transaction(3.0)
    prod.abort_transaction(None)
    assert ("abort", (3.0,)) in fake.calls
    assert ("abort", ()) in fake.calls


def test_flush_returns_remaining() -> None:
    prod, fake = _producer()
    fake.flush_remaining = 4
    assert prod.flush(2.0) == 4


def test_close_is_idempotent() -> None:
    prod, fake = _producer()
    prod.close()
    prod.close()
    assert sum(1 for c in fake.calls if c[0] == "flush") == 1


def test_close_warns_when_messages_remain(caplog) -> None:
    prod, fake = _producer()
    fake.flush_remaining = 2
    with caplog.at_level("WARNING"):
        prod.close()
    assert "remain unflushed" in caplog.text


def test_context_manager_closes() -> None:
    fake = _FakeProducer()
    with TransactionalAvroProducer({"t": lambda v: b""}, producer=fake) as prod:
        assert prod.initialised is False
    assert any(c[0] == "flush" for c in fake.calls)
