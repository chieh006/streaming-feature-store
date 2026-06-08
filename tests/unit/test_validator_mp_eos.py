"""Tests for multi-process validator EOS wiring (design week2_03 §2.3 / §2.4).

Covers the shared :mod:`validate.eos_wiring` builder, the per-member
``transactional.id`` derivation in :func:`run_validator_worker`, and the
threading of EOS knobs through :class:`MultiprocessValidatorRunner`.
"""

from __future__ import annotations

import pickle
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.eos import (
    TransactionalCommit,
    TransactionalConfig,
    TransactionalDlqRoute,
    TransactionalValidatedRoute,
)
from streaming_feature_store.validate import eos_wiring as ew
from streaming_feature_store.validate.accountant import ValidatorAccountant
from streaming_feature_store.validate.report import ValidatorRunReport
from streaming_feature_store.validate.runner import ValidatorRunConfig
from streaming_feature_store.validate_mp import worker_entry as we
from streaming_feature_store.validate_mp.mp_runner import (
    MultiprocessValidatorRunner,
)
from streaming_feature_store.validate_mp.report import (
    MultiprocessValidatorConfig,
)


def _args(*, eos: bool, index: int = 0, group: str = "validator-feed"):
    return we.WorkerProcessArgs(
        process_index=index,
        kafka_config_dict=KafkaConfig().model_dump(),
        registry_config_dict=SchemaRegistryConfig().model_dump(mode="json"),
        run_config=ValidatorRunConfig(consumer_group_id=group),
        eos=eos,
    )


def _report() -> ValidatorRunReport:
    return ValidatorRunReport(
        source_topic="s",
        validated_topic="v",
        dlq_topic="d",
        consumer_group="g",
        started_at=datetime.now(tz=UTC),
        ended_at=datetime.now(tz=UTC),
        snapshot=ValidatorAccountant().snapshot(),
    )


# --- shared eos_wiring builder ---------------------------------------------


def test_build_validator_txn_producer_registers_both_topics() -> None:
    txn = TransactionalConfig(enabled=True, transactional_id="v-0")
    with patch.object(ew, "TransactionalAvroProducer") as tap:
        ew.build_validator_txn_producer(
            KafkaConfig(),
            SchemaRegistryConfig(),
            validated_topic="validated-events",
            dlq_topic="dead-letter-queue",
            txn_config=txn,
        )
    serializers = tap.call_args.args[0]
    assert set(serializers) == {"validated-events", "dead-letter-queue"}
    assert tap.call_args.kwargs["conf"]["transactional.id"] == "v-0"


def test_build_validator_eos_shares_one_producer() -> None:
    fake_producer = MagicMock()
    with patch.object(ew, "build_validator_txn_producer", return_value=fake_producer):
        validated, dlq, strategy = ew.build_validator_eos(
            KafkaConfig(),
            SchemaRegistryConfig(),
            validated_topic="validated-events",
            dlq_topic="dead-letter-queue",
            txn_config=TransactionalConfig(enabled=True, transactional_id="v-0"),
        )
    assert isinstance(validated, TransactionalValidatedRoute)
    assert isinstance(dlq, TransactionalDlqRoute)
    assert isinstance(strategy, TransactionalCommit)
    # All three reference the *same* producer → one transaction (§2.4).
    assert validated._producer is fake_producer
    assert dlq._producer is fake_producer
    assert strategy._producer is fake_producer
    assert validated.topic == "validated-events"
    assert dlq.topic == "dead-letter-queue"


# --- per-member transactional.id derivation --------------------------------


def test_member_txn_config_derives_per_member_id() -> None:
    txn = we._member_txn_config(_args(eos=True, index=3, group="validator-feed"))
    assert txn is not None
    assert txn.transactional_id == "validator-feed-3"
    assert txn.group_instance_id == "validator-feed-3"
    assert txn.enabled is True


def test_member_txn_config_none_when_not_eos() -> None:
    assert we._member_txn_config(_args(eos=False)) is None


def test_build_member_io_eos_calls_build_validator_eos() -> None:
    args = _args(eos=True, index=1, group="validator-feed")
    txn = we._member_txn_config(args)
    captured: dict = {}

    def _fake(kc, rc, *, validated_topic, dlq_topic, txn_config):  # noqa: ANN001
        captured["txn"] = txn_config
        return "vr", "dr", "strat"

    with patch.object(we, "build_validator_eos", side_effect=_fake):
        out = we._build_member_io(args, KafkaConfig(), SchemaRegistryConfig(), txn)

    assert out == ("vr", "dr", "strat")
    assert captured["txn"] is txn


def test_build_member_io_non_eos_returns_two_producers() -> None:
    args = _args(eos=False)
    with (
        patch.object(we, "AvroEventProducer") as ap,
        patch.object(we, "DlqProducer") as dp,
    ):
        validated, dlq, strategy = we._build_member_io(
            args, KafkaConfig(), SchemaRegistryConfig(), None
        )
    assert strategy is None
    ap.assert_called_once()
    dp.assert_called_once()


# --- WorkerProcessArgs EOS fields ------------------------------------------


def test_worker_args_eos_defaults_false_and_pickleable() -> None:
    assert _args(eos=False).eos is False
    restored = pickle.loads(pickle.dumps(_args(eos=True, index=2)))
    assert restored.eos is True
    assert restored.process_index == 2
    assert restored.transaction_timeout_ms == 60_000


# --- run_validator_worker threads the strategy through ----------------------


def test_run_validator_worker_eos_passes_strategy_and_static_membership() -> None:
    args = _args(eos=True, index=1, group="validator-feed")
    fake_runner = MagicMock()
    fake_runner.run.return_value = _report()
    with (
        patch.object(we, "AvroEventConsumer") as consumer_cls,
        patch.object(we, "build_validator_eos", return_value=("vr", "dr", "strat")),
        patch.object(we, "ValidatorRunner", return_value=fake_runner) as runner_cls,
    ):
        outcome = we.run_validator_worker(args)
    assert runner_cls.call_args.kwargs["commit_strategy"] == "strat"
    # Each member's consumer is a static member with its own group.instance.id.
    assert consumer_cls.call_args.kwargs["group_instance_id"] == "validator-feed-1"
    assert outcome.process_index == 1


def test_run_validator_worker_non_eos_passes_no_strategy() -> None:
    args = _args(eos=False)
    fake_runner = MagicMock()
    fake_runner.run.return_value = _report()
    with (
        patch.object(we, "AvroEventConsumer") as consumer_cls,
        patch.object(we, "AvroEventProducer"),
        patch.object(we, "DlqProducer"),
        patch.object(we, "ValidatorRunner", return_value=fake_runner) as runner_cls,
    ):
        we.run_validator_worker(args)
    assert runner_cls.call_args.kwargs["commit_strategy"] is None
    assert consumer_cls.call_args.kwargs["group_instance_id"] is None


# --- MultiprocessValidatorRunner threads EOS into child args ----------------


def test_mp_runner_threads_eos_into_child_args() -> None:
    mp_config = MultiprocessValidatorConfig(
        members=2, base_config=ValidatorRunConfig(consumer_group_id="g")
    )
    runner = MultiprocessValidatorRunner(
        KafkaConfig(),
        SchemaRegistryConfig(),
        mp_config,
        eos=True,
        transaction_timeout_ms=12_345,
        commit_timeout_s=7.0,
    )
    child_args = runner._build_child_args()
    assert [a.process_index for a in child_args] == [0, 1]
    assert all(a.eos for a in child_args)
    assert all(a.transaction_timeout_ms == 12_345 for a in child_args)
    assert all(a.commit_timeout_s == 7.0 for a in child_args)


def test_mp_runner_default_is_not_eos() -> None:
    mp_config = MultiprocessValidatorConfig(
        members=1, base_config=ValidatorRunConfig()
    )
    runner = MultiprocessValidatorRunner(
        KafkaConfig(), SchemaRegistryConfig(), mp_config
    )
    assert runner._build_child_args()[0].eos is False
