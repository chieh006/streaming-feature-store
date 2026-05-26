"""Composable, first-failure-wins validation pipeline.

The pipeline applies a fixed sequence of :class:`Validator` instances to
each event.  The first to return :class:`Invalid` short-circuits and the
pipeline returns that decision; if no validator rejects, :class:`Valid` is
returned with the original event echoed.

Per-event-type filtering uses each validator's ``applies_to`` attribute:
when ``applies_to`` is a frozenset that does not include
``event.event_type``, the validator is *skipped* for that event (this is
not a rejection — the validator simply does not apply).

Any unexpected exception raised inside a validator's ``validate()`` is
caught and re-wrapped as :class:`Invalid` with
``ErrorClass.PIPELINE_INTERNAL_ERROR`` so a buggy validator cannot kill
the runner (design doc §2.6).
"""

from __future__ import annotations

import logging
import traceback
from dataclasses import dataclass
from typing import Protocol, Sequence, Union, runtime_checkable

from streaming_feature_store.schemas import EcommerceEvent, EventType
from streaming_feature_store.validate.dlq import ErrorClass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Valid:
    """Pipeline decision: the event passed every applicable validator.

    Parameters
    ----------
    event : EcommerceEvent
        The (unmodified) event that passed validation.
    """

    event: EcommerceEvent


@dataclass(frozen=True)
class Invalid:
    """Pipeline decision: a validator rejected the event.

    Parameters
    ----------
    error_class : ErrorClass
        Coarse error bucket (matches the DLQ Avro enum).
    validator_name : str
        ``Validator.name`` of the rejecting validator.
    error_field_path : str or None
        Dotted path of the offending field; ``None`` when the rejection is
        not field-scoped.
    error_message : str
        Human-readable explanation.
    """

    error_class: ErrorClass
    validator_name: str
    error_field_path: str | None
    error_message: str


ValidateResult = Union[Valid, Invalid]


@runtime_checkable
class Validator(Protocol):
    """Stateless event validator.

    Attributes
    ----------
    name : str
        Stable identifier surfaced in DLQ records and dashboards.
    applies_to : frozenset[EventType] or None
        Subset of event types this validator applies to.  ``None`` means
        "apply to all event types".  Honored by :class:`ValidationPipeline`,
        not by the validator itself.

    Methods
    -------
    validate(event) -> Valid | Invalid
        Inspect *event* and return the pipeline decision.
    """

    name: str
    applies_to: frozenset[EventType] | None

    def validate(self, event: EcommerceEvent) -> ValidateResult:
        """Return :class:`Valid` if *event* is acceptable, else :class:`Invalid`.

        Parameters
        ----------
        event : EcommerceEvent
            Decoded event after :func:`avro_dict_to_event`.

        Returns
        -------
        Valid or Invalid
            Pipeline decision.
        """
        ...


class ValidationPipeline:
    """Apply a fixed sequence of :class:`Validator` instances to each event.

    Parameters
    ----------
    validators : Sequence[Validator]
        Validators applied in the order given.  The first to return
        :class:`Invalid` short-circuits.

    Notes
    -----
    Validators that declare ``applies_to`` are *skipped* for events whose
    ``event_type`` is not in that set; the skip is a no-op, not a failure.
    Validators with ``applies_to=None`` apply to every event type.

    Exceptions raised inside a validator's ``validate()`` are caught and
    converted into :class:`Invalid` with
    :attr:`ErrorClass.PIPELINE_INTERNAL_ERROR`; the truncated traceback is
    surfaced in ``error_message``.  This is the load-bearing property that
    keeps the runner alive when a validator misbehaves (design doc §2.6).
    """

    def __init__(self, validators: Sequence[Validator]) -> None:
        self._validators: tuple[Validator, ...] = tuple(validators)

    @property
    def validators(self) -> tuple[Validator, ...]:
        """Read-only tuple of validators in pipeline order.

        Returns
        -------
        tuple of Validator
            Validators in the order they will be applied.
        """
        return self._validators

    def validate(self, event: EcommerceEvent) -> ValidateResult:
        """Run *event* through the pipeline.

        Parameters
        ----------
        event : EcommerceEvent
            Decoded event after :func:`avro_dict_to_event`.

        Returns
        -------
        Valid or Invalid
            First :class:`Invalid` returned by any applicable validator,
            or :class:`Valid` if every validator passed.
        """
        for validator in self._validators:
            if not _applies(validator, event):
                continue
            try:
                outcome = validator.validate(event)
            except Exception as exc:  # noqa: BLE001 - classified, not swallowed
                tb = traceback.format_exc(limit=8)
                logger.warning(
                    f"ValidationPipeline: validator {validator.name!r} raised "
                    f"{type(exc).__name__}; routing event to DLQ."
                )
                return Invalid(
                    error_class=ErrorClass.PIPELINE_INTERNAL_ERROR,
                    validator_name=validator.name,
                    error_field_path=None,
                    error_message=tb,
                )
            if isinstance(outcome, Invalid):
                return outcome
            # outcome is Valid → keep going
        return Valid(event=event)


def _applies(validator: Validator, event: EcommerceEvent) -> bool:
    """Return ``True`` iff *validator* applies to *event*.

    Parameters
    ----------
    validator : Validator
        Validator instance.
    event : EcommerceEvent
        Decoded event.

    Returns
    -------
    bool
        ``True`` when ``validator.applies_to`` is ``None`` (universal) or
        contains ``event.event_type``.
    """
    if validator.applies_to is None:
        return True
    return event.event_type in validator.applies_to


def default_validators() -> tuple[Validator, ...]:
    """Construct the project's default validator chain.

    Returns
    -------
    tuple of Validator
        Six validators in the order documented in design doc §2.5:

        1. :class:`RequiredFieldsValidator`
        2. :class:`EventTypeAllowlistValidator`
        3. :class:`UserIdShapeValidator`
        4. :class:`PriceRangeValidator`
        5. :class:`QuantityRangeValidator`
        6. :class:`TimestampRangeValidator`
    """
    # Import at call time to break the load-order dependency between
    # :mod:`validators` (which imports :class:`Valid` / :class:`Invalid` /
    # :class:`Validator` from this module) and this module.
    from streaming_feature_store.validate.validators import (
        EventTypeAllowlistValidator,
        PriceRangeValidator,
        QuantityRangeValidator,
        RequiredFieldsValidator,
        TimestampRangeValidator,
        UserIdShapeValidator,
    )

    return (
        RequiredFieldsValidator(),
        EventTypeAllowlistValidator(),
        UserIdShapeValidator(),
        PriceRangeValidator(),
        QuantityRangeValidator(),
        TimestampRangeValidator(),
    )
