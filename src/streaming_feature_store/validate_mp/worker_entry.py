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
from streaming_feature_store.producer.avro_producer import AvroEventProducer
from streaming_feature_store.validate.accountant import ValidatorAccountant
from streaming_feature_store.validate.dlq import DlqProducer
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
    """

    model_config = ConfigDict(frozen=True)

    process_index: int = Field(..., ge=0)
    kafka_config_dict: dict
    registry_config_dict: dict
    run_config: ValidatorRunConfig
    validator_version: str = "1.0.0"
    log_level: str = "INFO"


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

    consumer = AvroEventConsumer(
        kafka_config,
        registry_config,
        group_id=args.run_config.consumer_group_id,
        topic=args.run_config.source_topic,
    )
    validated_producer = AvroEventProducer(
        kafka_config,
        registry_config,
        topic=args.run_config.validated_topic,
    )
    dlq_producer = DlqProducer(
        kafka_config,
        registry_config,
        topic=args.run_config.dlq_topic,
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
