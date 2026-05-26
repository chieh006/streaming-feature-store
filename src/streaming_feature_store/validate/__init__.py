"""Inline validation stage with dead-letter-queue routing.

The validator subscribes to a Kafka topic, applies a chain of stateless
validators to each :class:`EcommerceEvent`, and routes the message to one of
two output topics:

* ``validated-events`` when every validator returns :class:`Valid`;
* ``dead-letter-queue`` as a structured :class:`DlqRecord` Avro envelope
  when any validator returns :class:`Invalid` (or when the underlying
  Avro/Pydantic adapter rejects the bytes).

Design doc: ``docs/design/week2_01_validation_layer_and_dlq.md``.
"""

from streaming_feature_store.validate.accountant import (
    ValidatorAccountant,
    ValidatorSnapshot,
)
from streaming_feature_store.validate.dlq import (
    DlqProducer,
    DlqRecord,
    ErrorClass,
    serialize_dlq_record,
)
from streaming_feature_store.validate.pipeline import (
    Invalid,
    Valid,
    ValidationPipeline,
    default_validators,
)
from streaming_feature_store.validate.report import (
    ValidatorRunReport,
    render_markdown,
)
from streaming_feature_store.validate.runner import (
    ValidatorRunConfig,
    ValidatorRunner,
)
from streaming_feature_store.validate.validators import (
    EventTypeAllowlistValidator,
    PriceRangeValidator,
    QuantityRangeValidator,
    RequiredFieldsValidator,
    TimestampRangeValidator,
    UserIdShapeValidator,
    Validator,
)

__all__ = [
    "DlqProducer",
    "DlqRecord",
    "ErrorClass",
    "EventTypeAllowlistValidator",
    "Invalid",
    "PriceRangeValidator",
    "QuantityRangeValidator",
    "RequiredFieldsValidator",
    "TimestampRangeValidator",
    "UserIdShapeValidator",
    "Valid",
    "ValidationPipeline",
    "Validator",
    "ValidatorAccountant",
    "ValidatorRunConfig",
    "ValidatorRunReport",
    "ValidatorRunner",
    "ValidatorSnapshot",
    "default_validators",
    "render_markdown",
    "serialize_dlq_record",
]
