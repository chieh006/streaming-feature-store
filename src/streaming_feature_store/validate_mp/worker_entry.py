"""Top-level child-process entry point for the multi-process validator.

:func:`run_validator_worker` is invoked once per child via
``multiprocessing.get_context("spawn").Pool.map``.  It rebuilds the
runtime objects from a Pydantic args bundle, constructs a
:class:`ValidatorRunner`, runs it, and returns a :class:`ValidatorOutcome`.

The function must live at module top-level so ``spawn`` can import it
without executing arbitrary parent state.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel, ConfigDict, Field

from streaming_feature_store.config import KafkaConfig, SchemaRegistryConfig
from streaming_feature_store.consumer.avro_consumer import AvroEventConsumer
from streaming_feature_store.eos import (
    TransactionalConfig,
    derive_transactional_id,
)
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.validate.accountant import ValidatorAccountant
from streaming_feature_store.validate.dlq import DlqProducer
from streaming_feature_store.validate.eos_wiring import build_validator_eos
from streaming_feature_store.validate.pipeline import (
    ValidationPipeline,
    default_validators,
)
from streaming_feature_store.validate.runner import (
    ValidatorRunConfig,
    ValidatorRunner,
)
from streaming_feature_store.validate_mp.report import ValidatorOutcome

logger = logging.getLogger(__name__)


class WorkerProcessArgs(BaseModel):
    """Pickleable bundle of arguments passed from parent to a child member.

    Parameters
    ----------
    process_index : int
        Zero-based child index.
    kafka_config_dict : dict
        ``model_dump()`` of the parent's :class:`KafkaConfig`.
    registry_config_dict : dict
        ``model_dump(mode="json")`` of the parent's
        :class:`SchemaRegistryConfig`.
    run_config : ValidatorRunConfig
        Per-member :class:`ValidatorRunConfig` (every member shares the
        same ``consumer_group_id``).
    validator_version : str, optional
        Semver of the validator catalog.  Defaults to ``"1.0.0"``.
    log_level : str, optional
        Python logging level for the child.  Defaults to ``"INFO"``.
    eos : bool, optional
        When ``True``, the worker wraps its consume-validate-produce cycle in a
        transaction with a per-process ``transactional.id`` derived from the
        shared group id and ``process_index`` (design week2_03 §2.3 — N members
        means N ids and N independent transaction scopes).  Defaults to
        ``False`` (at-least-once).
    transaction_timeout_ms : int, optional
        librdkafka ``transaction.timeout.ms`` when *eos* is set.  Defaults to
        ``60_000``.
    commit_timeout_s : float, optional
        ``commit_transaction`` budget when *eos* is set.  Defaults to ``30.0``.
    group_instance_id : str or None, optional
        Static-membership id; defaults to the derived ``transactional.id``.
    """

    model_config = ConfigDict(frozen=True)

    process_index: int = Field(..., ge=0)
    kafka_config_dict: dict
    registry_config_dict: dict
    run_config: ValidatorRunConfig
    validator_version: str = "1.0.0"
    log_level: str = "INFO"
    eos: bool = False
    transaction_timeout_ms: int = Field(default=60_000, ge=1_000)
    commit_timeout_s: float = Field(default=30.0, gt=0.0)
    group_instance_id: str | None = None


def _configure_child_logging(level: str) -> None:
    """Initialise logging in the child process.

    Parameters
    ----------
    level : str
        Python logging level name.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [pid=%(process)d] %(name)s %(message)s",
        force=True,
    )


def _member_txn_config(args: WorkerProcessArgs) -> TransactionalConfig | None:
    """Resolve this member's EOS transactional config, or ``None``.

    The ``transactional.id`` is derived per member from the shared group id and
    ``process_index`` (design §2.3 — N members = N ids); the static-membership
    ``group.instance.id`` defaults to the same value.

    Parameters
    ----------
    args : WorkerProcessArgs
        The member's argument bundle.

    Returns
    -------
    TransactionalConfig or None
        EOS config when ``args.eos`` is set, else ``None``.
    """
    if not args.eos:
        return None
    txn_id = derive_transactional_id(
        args.run_config.consumer_group_id, args.process_index
    )
    return TransactionalConfig(
        enabled=True,
        transactional_id=txn_id,
        group_instance_id=args.group_instance_id or txn_id,
        transaction_timeout_ms=args.transaction_timeout_ms,
        commit_timeout_s=args.commit_timeout_s,
    )


def _build_member_io(
    args: WorkerProcessArgs,
    kafka_config: KafkaConfig,
    registry_config: SchemaRegistryConfig,
    txn_config: TransactionalConfig | None,
) -> tuple[object, object, object | None]:
    """Build this member's route producers and commit strategy.

    Parameters
    ----------
    args : WorkerProcessArgs
        The member's argument bundle.
    kafka_config : KafkaConfig
        Bootstrap configuration.
    registry_config : SchemaRegistryConfig
        Schema Registry connection settings.
    txn_config : TransactionalConfig or None
        EOS config from :func:`_member_txn_config`; ``None`` selects the
        at-least-once path.

    Returns
    -------
    tuple
        ``(validated_producer, dlq_producer, commit_strategy)`` — the two
        standalone producers + ``None`` for the at-least-once path, or two route
        adapters over one transactional producer + a ``TransactionalCommit``
        when *txn_config* is set.
    """
    if txn_config is None:
        validated = AvroEventProducer(
            kafka_config, registry_config, topic=args.run_config.validated_topic
        )
        dlq = DlqProducer(
            kafka_config, registry_config, topic=args.run_config.dlq_topic
        )
        return validated, dlq, None

    logger.info(
        f"member {args.process_index} EOS enabled: "
        f"transactional.id={txn_config.transactional_id!r}"
    )
    return build_validator_eos(
        kafka_config,
        registry_config,
        validated_topic=args.run_config.validated_topic,
        dlq_topic=args.run_config.dlq_topic,
        txn_config=txn_config,
    )


def run_validator_worker(args: WorkerProcessArgs) -> ValidatorOutcome:
    """Run a single validator member and return its outcome.

    Parameters
    ----------
    args : WorkerProcessArgs
        Pickleable argument bundle.

    Returns
    -------
    ValidatorOutcome
        The member's :class:`ValidatorRunReport` wrapped with its index.
    """
    _configure_child_logging(args.log_level)
    logger.info(
        f"Validator member process_index={args.process_index} starting: "
        f"group={args.run_config.consumer_group_id} "
        f"source={args.run_config.source_topic} "
        f"validated={args.run_config.validated_topic} "
        f"dlq={args.run_config.dlq_topic}"
    )

    kafka_config = KafkaConfig(**args.kafka_config_dict)
    registry_config = SchemaRegistryConfig(**args.registry_config_dict)

    txn_config = _member_txn_config(args)
    consumer = AvroEventConsumer(
        kafka_config,
        registry_config,
        group_id=args.run_config.consumer_group_id,
        topic=args.run_config.source_topic,
        group_instance_id=(
            txn_config.group_instance_id if txn_config is not None else None
        ),
    )
    validated_producer, dlq_producer, commit_strategy = _build_member_io(
        args, kafka_config, registry_config, txn_config
    )
    pipeline = ValidationPipeline(default_validators())
    accountant = ValidatorAccountant(seed=args.process_index)
    runner = ValidatorRunner(
        consumer=consumer,
        validated_producer=validated_producer,
        dlq_producer=dlq_producer,
        pipeline=pipeline,
        accountant=accountant,
        config=args.run_config,
        commit_strategy=commit_strategy,
        validator_version=args.validator_version,
    )
    report = runner.run()
    logger.info(
        f"Validator member process_index={args.process_index} done: "
        f"consumed={report.snapshot.consumed} "
        f"validated={report.snapshot.validated} "
        f"invalid={report.snapshot.invalid_total}"
    )
    return ValidatorOutcome(process_index=args.process_index, report=report)
